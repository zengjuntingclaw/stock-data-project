"""DataEngine 核心模块测试

覆盖范围：
  1. 公共工具函数 (detect_board / detect_limit / build_ts_code)
  2. DataValidator (validate / cross_validate)
  3. DataEngine 核心方法 (init / query / execute / save_quotes / 技术指标计算)
  4. 边界条件与异常处理
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

from scripts.data_engine import (
    detect_board,
    detect_limit,
    build_ts_code,
    DataValidator,
    DataEngine,
    DEFAULT_START_DATE,
    DEFAULT_ADJ_TOLERANCE,
    DEFAULT_SAMPLE_RATIO,
    DEFAULT_MAX_WORKERS,
)


# ══════════════════════════════════════════════════════════════════
# 1. 公共工具函数测试
# ══════════════════════════════════════════════════════════════════

class TestDetectBoard(unittest.TestCase):
    """detect_board 板块识别测试"""

    def test_star_market(self):
        """科创板 688xxx"""
        self.assertEqual(detect_board("688001"), "科创板")
        self.assertEqual(detect_board("688999"), "科创板")
        # "068801".zfill(6) = "068801" → 不匹配 ^688\d{3}$ → 主板
        self.assertEqual(detect_board("068801"), "主板")

    def test_chinext(self):
        """创业板 30xxxx"""
        self.assertEqual(detect_board("300001"), "创业板")
        self.assertEqual(detect_board("301001"), "创业板")
        # "000300".zfill(6) = "000300" → 主板
        self.assertEqual(detect_board("000300"), "主板")

    def test_bse(self):
        """北交所 8xxxxx / 4xxxxx"""
        self.assertEqual(detect_board("830001"), "北交所")
        self.assertEqual(detect_board("830799"), "北交所")
        self.assertEqual(detect_board("430001"), "北交所")
        self.assertEqual(detect_board("400001"), "北交所")

    def test_main_board(self):
        """主板：60xxxx(沪), 00xxxx(深)"""
        self.assertEqual(detect_board("600000"), "主板")
        self.assertEqual(detect_board("601398"), "主板")
        self.assertEqual(detect_board("000001"), "主板")
        self.assertEqual(detect_board("002001"), "主板")
        self.assertEqual(detect_board("500001"), "主板")  # 基金类

    def test_numeric_input(self):
        """数字类型输入"""
        self.assertEqual(detect_board(688001), "科创板")
        self.assertEqual(detect_board(1), "主板")  # 000001
        self.assertEqual(detect_board(300001), "创业板")

    def test_short_code_padding(self):
        """不足6位自动补零"""
        self.assertEqual(detect_board("1"), "主板")      # 000001
        # "6881".zfill(6) = "006881" → 不匹配 ^688\d{3}$ → 主板
        self.assertEqual(detect_board("6881"), "主板")

    def test_empty_and_special(self):
        """空字符串等边界"""
        self.assertEqual(detect_board(""), "主板")   # "000000" → 主板
        self.assertEqual(detect_board("0"), "主板")


class TestDetectLimit(unittest.TestCase):
    """detect_limit 涨跌停幅度测试"""

    def test_star_limit(self):
        """科创板 20%"""
        self.assertEqual(detect_limit("688001"), 0.20)

    def test_chinext_limit(self):
        """创业板 20%"""
        self.assertEqual(detect_limit("300001"), 0.20)

    def test_bse_limit(self):
        """北交所 30%"""
        self.assertEqual(detect_limit("830001"), 0.30)
        self.assertEqual(detect_limit("430001"), 0.30)
        self.assertEqual(detect_limit("400001"), 0.30)

    def test_main_limit(self):
        """主板 10%"""
        self.assertEqual(detect_limit("600000"), 0.10)
        self.assertEqual(detect_limit("000001"), 0.10)

    def test_return_type_float(self):
        """返回值为浮点数"""
        result = detect_limit("600000")
        self.assertIsInstance(result, float)


class TestBuildTsCode(unittest.TestCase):
    """build_ts_code 构造ts_code测试"""

    def test_shanghai_codes(self):
        """上海证券交易所代码"""
        self.assertEqual(build_ts_code("600000"), "600000.SH")
        self.assertEqual(build_ts_code("601398"), "601398.SH")
        self.assertEqual(build_ts_code("688001"), "688001.SH")
        self.assertEqual(build_ts_code("500001"), "500001.SH")
        self.assertEqual(build_ts_code("900001"), "900001.SH")

    def test_shenzhen_codes(self):
        """深圳证券交易所代码"""
        self.assertEqual(build_ts_code("000001"), "000001.SZ")
        self.assertEqual(build_ts_code("002001"), "002001.SZ")
        self.assertEqual(build_ts_code("300001"), "300001.SZ")
        self.assertEqual(build_ts_code("830001"), "830001.BJ")  # 新三板→北交所
        self.assertEqual(build_ts_code("430001"), "430001.BJ")  # 新三板→北交所

    def test_padding(self):
        """不足6位自动补零"""
        self.assertEqual(build_ts_code("1"), "000001.SZ")
        self.assertEqual(build_ts_code("6005"), "006005.SZ")  # 006005匹配SZ_MAIN→.SZ

    def test_numeric_input(self):
        """数字类型输入"""
        self.assertEqual(build_ts_code(600000), "600000.SH")
        self.assertEqual(build_ts_code(1), "000001.SZ")


# ══════════════════════════════════════════════════════════════════
# 2. DataValidator 测试
# ══════════════════════════════════════════════════════════════════

class TestDataValidator(unittest.TestCase):
    """DataValidator 数据验证器测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.validator = DataValidator(
            error_log_path=str(Path(self.tmpdir) / "test_error.log")
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_df(self, **overrides):
        """构建测试 DataFrame"""
        base = {
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-01"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.5],
            "close": [10.2],
            "volume": [1000000],
        }
        base.update(overrides)
        return pd.DataFrame(base)

    # ---- validate 测试 ----

    def test_valid_data_passes(self):
        """正常数据通过验证"""
        df = self._make_df()
        result = self.validator.validate(df)
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 1)

    def test_empty_df_passes(self):
        """空 DataFrame 通过验证"""
        result = self.validator.validate(pd.DataFrame())
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 0)

    def test_negative_price_detected(self):
        """负价格被检测为异常"""
        df = self._make_df(open=-1.0)
        result = self.validator.validate(df)
        self.assertFalse(result["ok"])
        self.assertTrue(any(i["type"] == "invalid_price" for i in result["issues"]))

    def test_zero_price_detected(self):
        """零价格被检测为异常"""
        df = self._make_df(close=0)
        result = self.validator.validate(df)
        self.assertFalse(result["ok"])

    def test_negative_volume_detected(self):
        """负成交量被检测为异常"""
        df = self._make_df(volume=-100)
        result = self.validator.validate(df)
        self.assertFalse(result["ok"])
        self.assertTrue(any(i["type"] == "invalid_volume" for i in result["issues"]))

    def test_missing_columns_ok(self):
        """缺少列时不报错（优雅降级）"""
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["2024-01-01"]})
        result = self.validator.validate(df)
        self.assertTrue(result["ok"])

    # ---- cross_validate 测试 ----

    def test_cross_validate_identical(self):
        """完全相同数据交叉验证通过"""
        df = self._make_df()
        result = self.validator.cross_validate(df, df)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["mismatch_count"], 0)

    def test_cross_validate_empty(self):
        """空数据跳过交叉验证"""
        result = self.validator.cross_validate(pd.DataFrame(), pd.DataFrame())
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "empty_data")

    def test_cross_validate_no_overlap(self):
        """无重叠数据跳过"""
        df1 = self._make_df(ts_code=["000001.SZ"])
        df2 = self._make_df(ts_code=["600000.SH"])
        result = self.validator.cross_validate(df1, df2)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "no_overlap")

    def test_cross_validate_mismatch(self):
        """差异超容差被检测"""
        df1 = self._make_df(close=100.0)
        df2 = self._make_df(close=101.0)
        result = self.validator.cross_validate(df1, df2, tolerance=0.005)
        # 差异 1%，超过默认 0.5% 容差
        self.assertEqual(result["status"], "mismatch")
        self.assertGreater(result["mismatch_count"], 0)

    def test_cross_validate_within_tolerance(self):
        """差异在容差内通过"""
        df1 = self._make_df(close=100.0)
        df2 = self._make_df(close=100.003)
        result = self.validator.cross_validate(df1, df2, tolerance=0.005)
        # 差异 0.003%，低于 0.5% 容差
        self.assertEqual(result["status"], "ok")

    def test_cross_validate_logs_mismatches(self):
        """交叉验证错误写入日志"""
        df1 = self._make_df(close=100.0)
        df2 = self._make_df(close=200.0)
        self.validator.cross_validate(df1, df2)
        log_path = Path(self.tmpdir) / "test_error.log"
        self.assertTrue(log_path.exists())
        content = log_path.read_text(encoding="utf-8")
        self.assertIn("CROSS_VALIDATE_ERROR", content)


# ══════════════════════════════════════════════════════════════════
# 3. DataEngine 核心方法测试（使用临时 DuckDB）
# ══════════════════════════════════════════════════════════════════

class TestDataEngineInit(unittest.TestCase):
    """DataEngine 初始化测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_init_creates_dirs(self):
        """初始化时自动创建数据库和parquet目录"""
        db_path = str(Path(self.tmpdir) / "test.db")
        pq_dir = str(Path(self.tmpdir) / "pq")
        engine = DataEngine(db_path=db_path, parquet_dir=pq_dir)
        self.assertTrue(Path(db_path).exists())
        self.assertTrue(Path(pq_dir).exists())
        # 关闭连接
        engine.query("SELECT 1")  # 确保可用

    def test_init_with_env_var(self):
        """环境变量配置路径"""
        import os
        os.environ["STOCK_DB_PATH"] = str(Path(self.tmpdir) / "env.db")
        try:
            engine = DataEngine()
            self.assertTrue(Path(self.tmpdir, "env.db").exists())
        finally:
            os.environ.pop("STOCK_DB_PATH", None)

    def test_init_creates_tables(self):
        """初始化后表结构存在"""
        engine = DataEngine(db_path=str(Path(self.tmpdir) / "schema.db"))
        tables = engine.query("SHOW TABLES")
        table_names = tables.iloc[:, 0].tolist()
        for expected in ["daily_bar_adjusted", "stock_basic_history", "trading_calendar",
                         "financial_data", "valuation_daily",
                         "index_membership"]:
            self.assertIn(expected, table_names)


class TestDataEngineQuery(unittest.TestCase):
    """DataEngine query / execute 测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "query.db"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_simple_query(self):
        """简单 SELECT 查询"""
        df = self.engine.query("SELECT 1 as val")
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0, 0], 1)

    def test_query_returns_dataframe(self):
        """query 返回 DataFrame"""
        df = self.engine.query("SELECT 'hello' as msg")
        self.assertIsInstance(df, pd.DataFrame)

    def test_execute_insert_and_query(self):
        """execute 写入 + query 读取"""
        self.engine.execute(
            "INSERT INTO stock_basic (ticker, name, exchange, listed_date) VALUES (?, ?, ?, ?)",
            ("000001.SZ", "平安银行", "SZ", "2024-01-01")
        )
        df = self.engine.query("SELECT * FROM stock_basic WHERE ticker = '000001.SZ'")
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "平安银行")

    def test_query_with_params(self):
        """参数化查询（防SQL注入）"""
        self.engine.execute(
            "INSERT INTO stock_basic (ticker, name, exchange, listed_date) VALUES (?, ?, ?, ?)",
            ("600000.SH", "浦发银行", "SH", "2024-01-01")
        )
        df = self.engine.query(
            "SELECT * FROM stock_basic WHERE ticker LIKE ?", ("600000%",)
        )
        self.assertEqual(len(df), 1)


class TestDataEngineSaveQuotes(unittest.TestCase):
    """DataEngine save_quotes 测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "quotes.db"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_quotes(self):
        """创建测试行情数据（v5: market_daily 列）"""
        return pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],  # v5: save_quotes 自动映射 ts_code→ticker
            "trade_date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.5, 10.0],
            "close": [10.2, 10.8],
            "pre_close": [10.0, 10.2],
            "volume": [1000000, 1200000],
            "amount": [10200000.0, 12960000.0],
            "turnover_rate": [0.005, 0.006],  # v5: turnover_rate 而非 turnover
            "adj_factor": [1.0, 1.0],
            "suspended_flag": [False, False],
            "limit_up_flag": [False, False],
            "limit_down_flag": [False, False],
        })

    def test_save_and_query_quotes(self):
        """保存行情后能正确查询"""
        df = self._make_quotes()
        self.engine.save_quotes(df)
        result = self.engine.query(
            "SELECT * FROM daily_bar_adjusted WHERE ts_code = '000001.SZ' ORDER BY trade_date"
        )
        self.assertEqual(len(result), 2)
        # close = raw_close × adj_factor = 10.2 × 1.0
        adj = result.iloc[0]["adj_factor"]
        self.assertAlmostEqual(result.iloc[0]["close"] / adj, 10.2, places=2)
        self.assertAlmostEqual(result.iloc[1]["close"] / result.iloc[1]["adj_factor"], 10.8, places=2)

    def test_save_empty_df(self):
        """保存空 DataFrame 不报错"""
        self.engine.save_quotes(pd.DataFrame())
        result = self.engine.query("SELECT COUNT(*) FROM daily_bar_adjusted")
        self.assertEqual(result.iloc[0, 0], 0)

    def test_upsert_behavior(self):
        """UPSERT：重复写入不产生重复数据"""
        df = self._make_quotes()
        self.engine.save_quotes(df)
        self.engine.save_quotes(df)  # 重复写入
        result = self.engine.query("SELECT COUNT(*) FROM daily_bar_adjusted WHERE ts_code = '000001.SZ'")
        self.assertEqual(result.iloc[0, 0], 2)  # 仍然只有2条


class TestDataEngineApplyLimitFlags(unittest.TestCase):
    """DataEngine._apply_limit_flags 测试"""

    def test_main_board_limit_up(self):
        """主板涨停标记：pct_chg >= 9.99%"""
        df = pd.DataFrame({"pct_chg": [10.0]})
        result = DataEngine._apply_limit_flags(df, "600000")
        self.assertTrue(result.iloc[0]["limit_up"])
        self.assertFalse(result.iloc[0]["limit_down"])

    def test_main_board_limit_down(self):
        """主板跌停标记：pct_chg <= -9.99%"""
        df = pd.DataFrame({"pct_chg": [-10.0]})
        result = DataEngine._apply_limit_flags(df, "600000")
        self.assertFalse(result.iloc[0]["limit_up"])
        self.assertTrue(result.iloc[0]["limit_down"])

    def test_star_limit_normal(self):
        """科创板正常涨跌不标记"""
        df = pd.DataFrame({"pct_chg": [5.0]})
        result = DataEngine._apply_limit_flags(df, "688001")
        self.assertFalse(result.iloc[0]["limit_up"])
        self.assertFalse(result.iloc[0]["limit_down"])

    def test_star_limit_up(self):
        """科创板涨停：>= 19.99%"""
        df = pd.DataFrame({"pct_chg": [20.0]})
        result = DataEngine._apply_limit_flags(df, "688001")
        self.assertTrue(result.iloc[0]["limit_up"])

    def test_tolerance_boundary(self):
        """容差边界：主板 9.98% 不算涨停"""
        df = pd.DataFrame({"pct_chg": [9.98]})
        result = DataEngine._apply_limit_flags(df, "600000")
        self.assertFalse(result.iloc[0]["limit_up"])


class TestDataEngineGetLatestDate(unittest.TestCase):
    """DataEngine.get_latest_date 测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "latest.db"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_empty_db_returns_none(self):
        """空库返回 None"""
        result = self.engine.get_latest_date("000001.SZ")
        self.assertIsNone(result)

    def test_returns_latest_date(self):
        """返回最新交易日期"""
        quotes = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [10.0, 10.5], "high": [10.5, 11.0], "low": [9.5, 10.0],
            "close": [10.2, 10.8], "pre_close": [10.0, 10.2],
            "volume": [1000000, 1200000], "amount": [10200000.0, 12960000.0],
            "turnover_rate": [0.005, 0.006],
            "adj_factor": [1.0, 1.0], "suspended_flag": [False, False],
            "limit_up_flag": [False, False], "limit_down_flag": [False, False],
        })
        self.engine.save_quotes(quotes)
        result = self.engine.get_latest_date("000001.SZ")
        self.assertEqual(result, "2024-01-03")


class TestDataEngineRollingComputations(unittest.TestCase):
    """DataEngine 滚动计算测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "rolling.db"))

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _seed_daily_data(self, n=100):
        """插入模拟日线数据（v5: market_daily 列）"""
        dates = pd.date_range("2024-01-01", periods=n, freq="B")
        np.random.seed(42)
        pct = np.random.normal(0, 2, n)  # 日涨跌幅 ~N(0,2%)
        close = 10.0 * np.cumprod(1 + pct / 100)
        open_ = close * (1 + np.random.normal(0, 0.005, n))
        high = np.maximum(open_, close) * (1 + abs(np.random.normal(0, 0.003, n)))
        low = np.minimum(open_, close) * (1 - abs(np.random.normal(0, 0.003, n)))

        df = pd.DataFrame({
            "ts_code": ["000001.SZ"] * n,
            "trade_date": dates,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "pre_close": close / (1 + pct / 100),
            "volume": np.random.randint(100000, 10000000, n).astype(int),
            "amount": (close * np.random.randint(100000, 10000000, n)).astype(float),
            "turnover_rate": np.random.uniform(0.001, 0.01, n),
            "adj_factor": np.ones(n),
            "suspended_flag": [False] * n,
            "limit_up_flag": [False] * n,
            "limit_down_flag": [False] * n,
        })
        self.engine.save_quotes(df)
        return dates

    def test_rolling_returns(self):
        """滚动收益率计算"""
        self._seed_daily_data(100)
        result = self.engine.compute_rolling_returns(
            ["000001.SZ"], "2024-01-01", "2024-06-30",
            windows=[5, 10, 20]
        )
        self.assertFalse(result.empty)
        self.assertIn("window_5", result.columns)
        self.assertIn("window_10", result.columns)
        self.assertIn("window_20", result.columns)
        # 收益率是有限数值
        self.assertTrue(result["window_5"].notna().all())

    def test_rolling_ma(self):
        """移动平均线计算"""
        self._seed_daily_data(100)
        result = self.engine.compute_rolling_ma(
            ["000001.SZ"], "2024-01-01", "2024-06-30",
            windows=[5, 20], price_col="close"
        )
        self.assertFalse(result.empty)
        self.assertIn("ma_5", result.columns)
        self.assertIn("ma_20", result.columns)
        # MA 值应为有限数值
        self.assertTrue(result["ma_5"].dropna().notna().all())
        self.assertTrue(result["ma_20"].dropna().notna().all())
        # MA20 和 MA5 不应完全相同（不同窗口长度）
        self.assertFalse(result["ma_5"].dropna().equals(result["ma_20"].dropna()))

    def test_rolling_volatility(self):
        """滚动波动率计算"""
        self._seed_daily_data(100)
        result = self.engine.compute_rolling_volatility(
            ["000001.SZ"], "2024-01-01", "2024-06-30",
            windows=[20, 60]
        )
        self.assertFalse(result.empty)
        self.assertIn("vol_20", result.columns)
        self.assertIn("vol_60", result.columns)
        # 波动率非 NaN 的部分应 >= 0
        valid = result["vol_20"].dropna()
        self.assertTrue(len(valid) > 0)
        self.assertTrue((valid >= 0).all())

    def test_empty_ts_codes(self):
        """空代码列表返回空 DataFrame"""
        result = self.engine.compute_rolling_returns([], "2024-01-01", "2024-06-30")
        self.assertTrue(result.empty)

    def test_no_match_returns_empty(self):
        """不存在的代码返回空"""
        result = self.engine.compute_rolling_returns(
            ["999999.SZ"], "2024-01-01", "2024-06-30"
        )
        self.assertTrue(result.empty)


class TestDataEnginePreviousTradeDate(unittest.TestCase):
    """DataEngine.get_previous_trade_date 测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "prev.db"))
        # 插入交易日历
        dates = pd.bdate_range("2024-01-01", periods=10)
        for d in dates:
            self.engine.execute(
                "INSERT OR IGNORE INTO trading_calendar (cal_date, is_trading_day) VALUES (?, TRUE)",
                (d.strftime("%Y-%m-%d"),)
            )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_returns_previous(self):
        """返回前一个交易日"""
        # 2024-01-10 的前一个交易日
        result = self.engine.get_previous_trade_date("2024-01-10")
        self.assertIsNotNone(result)
        self.assertEqual(result, "2024-01-09")

    def test_first_date_returns_none(self):
        """第一个交易日之前返回 None"""
        result = self.engine.get_previous_trade_date("2023-12-31")
        self.assertIsNone(result)

    def test_weekend_gap(self):
        """跳过周末"""
        # 找到某个周一，确认前一天是上周五
        result = self.engine.get_previous_trade_date("2024-01-08")  # 周一
        self.assertIsNotNone(result)
        self.assertEqual(result, "2024-01-05")  # 周五


# ══════════════════════════════════════════════════════════════════
# 4. 全局常量测试
# ══════════════════════════════════════════════════════════════════

class TestGlobalConstants(unittest.TestCase):
    """全局常量完整性测试"""

    def test_start_date_format(self):
        """默认起始日期格式正确"""
        from datetime import datetime as dt
        dt.strptime(DEFAULT_START_DATE, "%Y-%m-%d")  # 不抛异常即可

    def test_tolerance_range(self):
        """容差值在合理范围"""
        self.assertGreater(DEFAULT_ADJ_TOLERANCE, 0)
        self.assertLess(DEFAULT_ADJ_TOLERANCE, 1)
        self.assertGreater(DEFAULT_SAMPLE_RATIO, 0)
        self.assertLessEqual(DEFAULT_SAMPLE_RATIO, 1)

    def test_max_workers_positive(self):
        """最大线程数为正整数"""
        self.assertGreater(DEFAULT_MAX_WORKERS, 0)
        self.assertIsInstance(DEFAULT_MAX_WORKERS, int)


# ══════════════════════════════════════════════════════════════════
# 5. 综合集成测试
# ══════════════════════════════════════════════════════════════════

class TestIntegration(unittest.TestCase):
    """端到端集成测试"""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = DataEngine(db_path=str(Path(self.tmpdir) / "integ.db"))
        self.validator = DataValidator(
            error_log_path=str(Path(self.tmpdir) / "integ_error.log")
        )

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_pipeline(self):
        """完整数据流水线：写入→验证→查询→计算"""
        # 1. 写入多只股票行情
        np.random.seed(123)
        for code in ["000001.SZ", "600000.SH", "688001.SH"]:
            dates = pd.date_range("2024-01-01", periods=60, freq="B")
            n = len(dates)
            pct = np.random.normal(0, 1.5, n)
            close = 10.0 * np.cumprod(1 + pct / 100)

            df = pd.DataFrame({
                "ts_code": [code] * n,
                "trade_date": dates,
                "open": close * (1 + np.random.normal(0, 0.003, n)),
                "high": np.maximum(close, close * (1 + np.random.normal(0, 0.003, n))),
                "low": np.minimum(close, close * (1 - np.random.normal(0, 0.003, n))),
                "close": close,
                "pre_close": close / (1 + pct / 100),
                "volume": np.random.randint(100000, 5000000, n).astype(int),
                "amount": (close * np.random.randint(100000, 5000000, n)).astype(float),
                "turnover_rate": np.random.uniform(0.001, 0.008, n),
                "adj_factor": np.ones(n),
                "suspended_flag": [False] * n,
                "limit_up_flag": [False] * n,
                "limit_down_flag": [False] * n,
            })
            self.engine.save_quotes(df)

        # 2. 查询验证
        all_data = self.engine.query(
            "SELECT * FROM daily_bar_adjusted ORDER BY ts_code, trade_date"
        )
        self.assertEqual(len(all_data), 3 * 60)

        # 3. 数据验证器
        result = self.validator.validate(all_data)
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 180)

        # 4. 滚动收益率计算
        ret = self.engine.compute_rolling_returns(
            ["000001.SZ", "600000.SH"], "2024-01-01", "2024-03-31",
            windows=[5, 20]
        )
        self.assertFalse(ret.empty)
        self.assertIn("window_5", ret.columns)

        # 5. 移动平均计算
        ma = self.engine.compute_rolling_ma(
            ["688001.SH"], "2024-01-01", "2024-03-31",
            windows=[5, 20]
        )
        self.assertFalse(ma.empty)

        # 6. 波动率计算
        vol = self.engine.compute_rolling_volatility(
            ["000001.SZ"], "2024-01-01", "2024-03-31",
            windows=[20]
        )
        self.assertFalse(vol.empty)
        valid_vol = vol["vol_20"].dropna()
        self.assertTrue(len(valid_vol) > 0)
        self.assertTrue((valid_vol >= 0).all())


# ========== Main ==========

if __name__ == "__main__":
    unittest.main(verbosity=2)
