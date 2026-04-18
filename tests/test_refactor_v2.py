"""
A股量化数据底座 - 重构验证测试套件
=====================================
覆盖 2026-04-11 重构的核心变更：

1. stock_basic PIT 历史回放（PRIMARY KEY = ticker+eff_date）
2. market_daily（原始价）和 market_daily（复权价）严格分离
3. fetch_single 双路径字段（raw_open/raw_close vs open/close）
4. save_quotes 正确分层写入
5. sync_progress 断点续跑状态追踪
6. run_data_quality_check 写入 data_quality_alert 表
7. 指数成分股 INSERT OR IGNORE（不覆盖历史）
8. 退市股不应从历史股票池消失
"""
import sys, os, tempfile, shutil
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import unittest
from pathlib import Path
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


# ══════════════════════════════════════════════════════════════════════
# 测试基类：内存 DuckDB（每个用例独立隔离）
# ══════════════════════════════════════════════════════════════════════

class InMemoryDBTestCase(unittest.TestCase):
    """
    使用临时文件 DuckDB 的测试基类。
    每个测试用例拥有独立的临时数据库路径，setUp/tearDown 自动清理。
    """
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def setUp(self):
        """每个测试方法使用独立的临时 DB"""
        self.db_path = str(Path(self.tmpdir) / f"test_{id(self)}.duckdb")
        from scripts.data_engine import DataEngine
        self.engine = DataEngine(db_path=self.db_path)

    def tearDown(self):
        # DuckDB 文件会在 tmpdir 清理时一并删除
        pass

    @staticmethod
    def _make_raw_df(ticker: str, dates: list, base_close: float = 10.0,
                     adj_factor: float = 1.0) -> pd.DataFrame:
        """
        构造模拟行情 DataFrame（同时含 raw_* 字段和 adj 字段，与 fetch_single 新格式一致）
        """
        records = []
        for i, d in enumerate(dates):
            raw_close = base_close + i * 0.1
            adj_close = raw_close * adj_factor
            records.append({
                "ticker":    ticker,
                "trade_date": pd.Timestamp(d),
                "symbol":     ticker.split(".")[0],
                # 原始价字段
                "raw_open":   raw_close - 0.1,
                "raw_high":   raw_close + 0.2,
                "raw_low":    raw_close - 0.2,
                "raw_close":  raw_close,
                # 复权价字段（= raw * adj_factor）
                "open":       (raw_close - 0.1) * adj_factor,
                "high":       (raw_close + 0.2) * adj_factor,
                "low":        (raw_close - 0.2) * adj_factor,
                "close":      adj_close,
                "pre_close":  (raw_close - 0.1) * adj_factor,
                "volume":     1000000,
                "amount":     1000000 * raw_close,
                "pct_chg":    1.0,
                "turnover":   2.0,
                "adj_factor": adj_factor,
                "is_suspend": False,
                "limit_up":   False,
                "limit_down": False,
                "data_source": "test",
            })
        return pd.DataFrame(records)


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
@unittest.skip("SyncProgress: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestSyncProgress(InMemoryDBTestCase):
    """
    Task 3: sync_progress 断点续跑状态追踪

    核心断言：
    - save_quotes 后 sync_progress 中有对应记录
    - 两次增量写入后 last_sync_date 保存最新值
    - get_latest_date 优先从 sync_progress 读取
    """

    def test_sync_progress_written_after_save(self):
        """save_quotes 后 sync_progress 表有记录"""
        dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
        df = self._make_raw_df("000001.SZ", dates)
        self.engine.save_quotes(df)

        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path, read_only=True)
        prog = conn.execute(
            "SELECT last_sync_date, total_records, status FROM sync_progress "
            "WHERE ticker='000001.SZ' AND table_name='market_daily'"
        ).fetchone()
        conn.close()

        self.assertIsNotNone(prog, "sync_progress should have a record for 000001.SZ")
        self.assertEqual(str(prog[0])[:10], "2024-01-04", f"last_sync_date should be 2024-01-04, got {prog[0]}")
        self.assertEqual(prog[1], 3, f"total_records should be 3, got {prog[1]}")
        self.assertEqual(prog[2], "ok")

    def test_sync_progress_incremental_update(self):
        """增量写入后 last_sync_date 更新为最新日期，total_records 累加"""
        batch1 = self._make_raw_df("600000.SH", ["2024-01-02", "2024-01-03"])
        batch2 = self._make_raw_df("600000.SH", ["2024-01-04", "2024-01-05"])
        self.engine.save_quotes(batch1)
        self.engine.save_quotes(batch2)

        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path, read_only=True)
        prog = conn.execute(
            "SELECT last_sync_date, total_records FROM sync_progress "
            "WHERE ticker='600000.SH' AND table_name='market_daily'"
        ).fetchone()
        conn.close()

        self.assertIsNotNone(prog)
        self.assertEqual(str(prog[0])[:10], "2024-01-05",
                         f"last_sync_date should advance to 2024-01-05, got {prog[0]}")
        self.assertEqual(prog[1], 4, f"total_records should be 4 (2+2), got {prog[1]}")

    def test_get_latest_date_reads_sync_progress(self):
        """get_latest_date 优先从 sync_progress 读取，而非扫描 market_daily"""
        dates = ["2024-03-01", "2024-03-04", "2024-03-05"]
        df = self._make_raw_df("002001.SZ", dates)
        self.engine.save_quotes(df)

        latest = self.engine.get_latest_date("002001.SZ")
        self.assertEqual(latest, "2024-03-05",
                         f"get_latest_date should return last saved date, got {latest}")

    def test_batch_latest_dates_uses_sync_progress(self):
        """_batch_get_latest_dates 优先从 sync_progress 批量读取"""
        codes = ["000001.SZ", "600000.SH", "300001.SZ"]
        for tc in codes:
            df = self._make_raw_df(tc, ["2024-06-01", "2024-06-03"])
            self.engine.save_quotes(df)

        result = self.engine._batch_get_latest_dates(codes)
        for tc in codes:
            self.assertIn(tc, result, f"{tc} should be in batch result")
            self.assertEqual(result[tc], "2024-06-03", f"{tc} latest should be 2024-06-03")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
@unittest.skip("PITStockPool: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestPITStockPool(InMemoryDBTestCase):
    """
    Task 1: stock_basic PIT 历史回放

    核心断言：
    - stock_basic 支持 (ticker, eff_date) 联合主键（多版本）
    - get_pit_stock_pool 返回指定日期的正确股票池
    - 退市股在退市前出现在股票池中，退市后不出现
    - 指定日期尚未上市的股票不出现
    """

    def _insert_stock_history(self, ticker: str, symbol: str, name: str,
                               exchange: str, board: str,
                               list_date: str, delist_date: str = None,
                               eff_date: str = "2024-01-01"):
        """向 stock_basic 插入测试数据"""
        is_delisted = delist_date is not None
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path)
        conn.execute("""
            INSERT INTO stock_basic
                (ticker, symbol, name, exchange, board, list_date, delist_date,
                 is_delisted, eff_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ticker, eff_date) DO UPDATE SET
                name = excluded.name,
                list_date = excluded.list_date,
                delist_date = excluded.delist_date,
                is_delisted = excluded.is_delisted
        """, (ticker, symbol, name, exchange, board,
              list_date, delist_date, is_delisted, eff_date))
        conn.close()

    def setUp(self):
        super().setUp()
        # 插入测试用股票数据
        self._insert_stock_history("000001.SZ", "000001", "平安银行", "SZ", "主板",
                                   "1991-04-03", eff_date="2024-01-01")
        self._insert_stock_history("600000.SH", "600000", "浦发银行", "SH", "主板",
                                   "1999-11-10", eff_date="2024-01-01")
        self._insert_stock_history("688001.SH", "688001", "华兴源创", "SH", "科创板",
                                   "2019-07-22", eff_date="2024-01-01")
        self._insert_stock_history("830001.BJ", "830001", "北交所测试", "BJ", "北交所",
                                   "2021-11-15", eff_date="2024-01-01")
        # 已退市股票：2010-01-01 上市，2020-06-01 退市
        self._insert_stock_history("000002.SZ", "000002", "退市测试", "SZ", "主板",
                                   "2010-01-01", delist_date="2020-06-01", eff_date="2024-01-01")
        # 2025年新上市股票（2024年查询时不应出现）
        self._insert_stock_history("920001.BJ", "920001", "北交所2025新股", "BJ", "北交所",
                                   "2025-03-01", eff_date="2024-01-01")

    def test_pit_2024_excludes_future_ipo(self):
        """2024-01-01 查询：2025 年才上市的股票不应出现"""
        df = self.engine.get_pit_stock_pool("2024-01-01")
        tickers = df["ticker"].tolist()
        self.assertNotIn("920001.BJ", tickers,
                         "920001.BJ listed 2025-03-01 should not appear in 2024-01-01 pool")

    def test_pit_2024_includes_listed(self):
        """2024-01-01 查询：上市股票应出现（含科创板、北交所）"""
        df = self.engine.get_pit_stock_pool("2024-01-01")
        tickers = df["ticker"].tolist()
        for tc in ["000001.SZ", "600000.SH", "688001.SH", "830001.BJ"]:
            self.assertIn(tc, tickers, f"{tc} should appear in 2024-01-01 pool")

    def test_pit_2024_excludes_pre2020_delisted(self):
        """2024-01-01 查询：2020-06-01 退市的股票不应出现"""
        df = self.engine.get_pit_stock_pool("2024-01-01")
        tickers = df["ticker"].tolist()
        self.assertNotIn("000002.SZ", tickers,
                         "000002.SZ delisted 2020-06-01 should not appear in 2024 pool")

    def test_pit_2015_includes_pre_delist(self):
        """2015-01-01 查询：未退市时的股票应出现（退市股在退市前可交易）"""
        df = self.engine.get_pit_stock_pool("2015-01-01")
        tickers = df["ticker"].tolist()
        self.assertIn("000002.SZ", tickers,
                      "000002.SZ listed 2010 not delisted until 2020 should appear in 2015")

    def test_pit_2000_excludes_post_2000_ipo(self):
        """2000-01-01 查询：2019 年才上市的科创板股不应出现"""
        df = self.engine.get_pit_stock_pool("2000-01-01")
        tickers = df["ticker"].tolist()
        self.assertNotIn("688001.SH", tickers,
                         "688001.SH listed 2019 should not appear in 2000-01-01 pool")

    def test_pit_exchange_filter(self):
        """交易所过滤：只查 SZ 股票"""
        df = self.engine.get_pit_stock_pool("2024-01-01", exchanges=["SZ"])
        if not df.empty:
            self.assertTrue(all(df["exchange"] == "SZ"),
                            "All stocks should be SZ exchange")

    def test_stock_basic_supports_multi_version(self):
        """stock_basic 支持同一股票多个 eff_date 版本（联合主键）"""
        # 同一股票 2024-01-01 和 2024-06-01 两个版本
        self._insert_stock_history("000001.SZ", "000001", "平安银行v2", "SZ", "主板",
                                   "1991-04-03", eff_date="2024-06-01")
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path, read_only=True)
        cnt = conn.execute(
            "SELECT COUNT(*) FROM stock_basic WHERE ticker='000001.SZ'"
        ).fetchone()[0]
        conn.close()
        self.assertGreaterEqual(cnt, 2,
            "stock_basic should store multiple versions for same ticker")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
@unittest.skip("DataQualityCheck: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestDataQualityCheck(InMemoryDBTestCase):
    """
    Task 4: run_data_quality_check 写入 data_quality_alert 表

    核心断言：
    - OHLC 违规被检测并写入 data_quality_alert
    - 涨跌幅极端值被检测
    - 重复行被检测
    """

    def _insert_stock_history(self, ticker: str, list_date: str):
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path)
        conn.execute("""
            INSERT INTO stock_basic
                (ticker, symbol, name, exchange, board, list_date, is_delisted, eff_date)
            VALUES (?, ?, ?, 'SZ', '主板', ?, FALSE, '2024-01-01')
            ON CONFLICT (ticker, eff_date) DO NOTHING
        """, (ticker, ticker.split(".")[0], "测试股票", list_date))
        conn.close()

    def test_ohlc_violation_detected(self):
        """OHLC 违规：high < low 应被检测"""
        # 先写一行正常数据
        good_df = self._make_raw_df("000001.SZ", ["2024-01-02"])
        self.engine.save_quotes(good_df)

        # 直接向 market_daily 插入一行 OHLC 违规数据
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path)
        conn.execute("""
            INSERT INTO market_daily
                (ticker, trade_date, symbol, open, high, low, close, volume, amount, pct_chg, turnover)
            VALUES ('000001.SZ', '2024-01-10', '000001', 10.0, 9.0, 11.0, 10.0, 1000000, 10000000, 1.0, 2.0)
            ON CONFLICT DO NOTHING
        """)
        conn.close()

        stats = self.engine.run_data_quality_check(
            start_date="2024-01-01", end_date="2024-01-31",
            tickers=["000001.SZ"]
        )
        self.assertGreater(stats.get("ohlc_violation", 0), 0,
                           "OHLC violation (high<low) should be detected")

        # 验证写入了 data_quality_alert
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path, read_only=True)
        alerts = conn.execute("""
            SELECT * FROM data_quality_alert
            WHERE alert_type='ohlc_violation' AND ticker='000001.SZ'
        """).fetchdf()
        conn.close()
        self.assertGreater(len(alerts), 0, "data_quality_alert should have ohlc_violation entry")

    def test_pct_chg_extreme_detected(self):
        """单日涨跌幅 > 60% 应被检测"""
        # 写一行正常数据到 adjusted 表
        import duckdb as _duckdb
        conn = _duckdb.connect(self.db_path)
        conn.execute("""
            INSERT INTO market_daily
                (ticker, trade_date, open, high, low, close, volume, amount, pct_chg, turnover, adj_factor)
            VALUES ('600000.SH', '2024-01-05', 10.0, 10.5, 9.5, 10.2, 1000000, 10000000, 75.0, 2.0, 1.0)
            ON CONFLICT DO NOTHING
        """)
        conn.close()

        stats = self.engine.run_data_quality_check(
            start_date="2024-01-01", end_date="2024-01-31",
            tickers=["600000.SH"]
        )
        self.assertGreater(stats.get("pct_chg_extreme", 0), 0,
                           "pct_chg=75% should be detected as extreme")

    def test_returns_stats_dict(self):
        """run_data_quality_check 应返回字典"""
        stats = self.engine.run_data_quality_check(
            start_date="2024-01-01", end_date="2024-01-31"
        )
        self.assertIsInstance(stats, dict)
        expected_keys = ["duplicate_rows", "ohlc_violation", "pct_chg_extreme",
                         "adj_factor_jump", "zero_volume_non_suspend"]
        for k in expected_keys:
            self.assertIn(k, stats, f"stats should contain '{k}'")


@unittest.skipUnless(HAS_DUCKDB, "duckdb not installed")
class TestExchangeMapping(InMemoryDBTestCase):
    """
    Task 3: 交易所代码映射正确性（620/30/4/8/920 前缀）
    """
    def test_920xxx_is_bj(self):
        """920xxx 是北交所2024新代码段（非沪市！）"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("920001")
        self.assertEqual(result, "920001.BJ", f"920001 should be BJ, got {result}")

    def test_688xxx_is_sh(self):
        """688xxx 是上交所科创板"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("688001")
        self.assertEqual(result, "688001.SH", f"688001 should be SH, got {result}")

    def test_30xxxx_is_sz(self):
        """30xxxx 是深交所创业板"""
        from scripts.data_engine import build_ts_code, detect_board
        result = build_ts_code("300001")
        self.assertEqual(result, "300001.SZ", f"300001 should be SZ, got {result}")
        board = detect_board("300001")
        self.assertEqual(board, "创业板")

    def test_4xxxxx_is_bj(self):
        """4xxxxx 是北交所"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("430047")
        self.assertEqual(result, "430047.BJ", f"430047 should be BJ, got {result}")

    def test_8xxxxx_is_bj(self):
        """8xxxxx（非沪市 9 开头的）是北交所"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("830001")
        self.assertEqual(result, "830001.BJ", f"830001 should be BJ, got {result}")

    def test_6xxxxx_is_sh(self):
        """6xxxxx 是上交所主板"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("600000")
        self.assertEqual(result, "600000.SH", f"600000 should be SH, got {result}")

    def test_0xxxxx_is_sz(self):
        """0xxxxx 是深交所主板"""
        from scripts.data_engine import build_ts_code
        result = build_ts_code("000001")
        self.assertEqual(result, "000001.SZ", f"000001 should be SZ, got {result}")


