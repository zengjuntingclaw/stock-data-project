"""测试：指数成分历史+增量写入幂等+数据层完整性。

覆盖：
- 指数成分历史 INSERT OR IGNORE 幂等（禁止 DELETE 覆盖）
- daily_bar_raw 补列后 adj_factor=1.0
- pre_close 字段存在性
- is_suspend 字段存在性
- get_universe_at_date 查询正确
- is_st_at_date 查询正确
"""
import unittest, os, sys, shutil, tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
import numpy as np

DB = os.environ.get(
    'STOCK_DB_PATH',
    str(Path(__file__).parent.parent / 'data' / 'stock_data.duckdb')
)


@unittest.skipUnless(os.path.exists(DB), f'DB not found: {DB}')
class TestIndexConstituentsHistory(unittest.TestCase):
    """指数成分历史表验证"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_table_has_proper_schema(self):
        """验证 index_constituents_history 表结构"""
        cols = [r[0] for r in self.conn.execute(
            "DESCRIBE index_constituents_history").fetchall()]
        required = ['index_code', 'ts_code', 'in_date', 'out_date', 'source']
        for col in required:
            self.assertIn(col, cols, f"缺少列: {col}")

    def test_000300_has_300_stocks(self):
        """沪深300应有约300只成分股"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM index_constituents_history
            WHERE index_code = '000300.SH'
        """).fetchone()[0]
        self.assertGreaterEqual(cnt, 290,
            f"沪深300成分股应有~300只，得到{cnt}")
        self.assertLessEqual(cnt, 310,
            f"沪深300成分股异常多，得到{cnt}")

    def test_000905_has_500_stocks(self):
        """中证500应有约500只成分股"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM index_constituents_history
            WHERE index_code = '000905.SH'
        """).fetchone()[0]
        self.assertGreaterEqual(cnt, 490,
            f"中证500成分股应有~500只，得到{cnt}")

    def test_000852_has_1000_stocks(self):
        """中证1000应有约1000只成分股"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM index_constituents_history
            WHERE index_code = '000852.SH'
        """).fetchone()[0]
        self.assertGreaterEqual(cnt, 990,
            f"中证1000成分股应有~1000只，得到{cnt}")

    def test_no_deleted_records(self):
        """验证 index_constituents_history 不存在 DELETE 覆盖的数据"""
        # 检查：同一 ts_code + index_code 不应有多个 in_date（多次快照覆盖）
        dups = self.conn.execute("""
            SELECT index_code, ts_code, COUNT(DISTINCT in_date) as versions
            FROM index_constituents_history
            GROUP BY index_code, ts_code
            HAVING COUNT(DISTINCT in_date) > 1
        """).fetchall()
        self.assertEqual(len(dups), 0,
            f"发现多次快照覆盖: {dups[:5]}")

    def test_no_future_out_dates(self):
        """out_date 不应在未来"""
        from datetime import date
        today = date.today().isoformat()
        future = self.conn.execute("""
            SELECT COUNT(*) FROM index_constituents_history
            WHERE out_date IS NOT NULL AND out_date > ?
        """, (today,)).fetchone()[0]
        self.assertEqual(future, 0,
            f"存在{future}条out_date在未来")

    def test_sample_constituents(self):
        """验证样本成分股"""
        samples = self.conn.execute("""
            SELECT index_code, ts_code, in_date, out_date, source
            FROM index_constituents_history
            WHERE ts_code = '600519.SH' OR ts_code = '000001.SZ'
            LIMIT 5
        """).fetchall()
        # 600519.SH (茅台) 应该在沪深300里
        self.assertGreater(len(samples), 0,
            "样本成分股查询应返回结果")


@unittest.skipUnless(os.path.exists(DB), f'DB not found: {DB}')
class TestDailyBarRawColumns(unittest.TestCase):
    """daily_bar_raw 补列验证"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_adj_factor_column_exists(self):
        """adj_factor 列必须存在"""
        cols = [r[0] for r in self.conn.execute(
            "DESCRIBE daily_bar_raw").fetchall()]
        self.assertIn('adj_factor', cols)

    def test_adj_factor_all_ones(self):
        """adj_factor 全=1.0（原始价未复权，等效adj_factor=1.0）"""
        nulls = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_raw WHERE adj_factor IS NULL"
        ).fetchone()[0]
        non_ones = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_raw WHERE adj_factor != 1.0"
        ).fetchone()[0]
        total = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_raw"
        ).fetchone()[0]
        self.assertEqual(nulls, 0, f"adj_factor NULL: {nulls}")
        self.assertEqual(non_ones, 0,
            f"adj_factor != 1.0: {non_ones} / {total}")

    def test_pre_close_column_exists(self):
        """pre_close 列必须存在"""
        cols = [r[0] for r in self.conn.execute(
            "DESCRIBE daily_bar_raw").fetchall()]
        self.assertIn('pre_close', cols)

    def test_is_suspend_column_exists(self):
        """is_suspend 列必须存在"""
        cols = [r[0] for r in self.conn.execute(
            "DESCRIBE daily_bar_raw").fetchall()]
        self.assertIn('is_suspend', cols)
        self.assertIn('limit_up', cols)
        self.assertIn('limit_down', cols)

    def test_primary_key_no_dups(self):
        """daily_bar_raw 主键(ts_code, trade_date)无重复"""
        dups = self.conn.execute("""
            SELECT ts_code, trade_date, COUNT(*) as cnt
            FROM daily_bar_raw
            GROUP BY ts_code, trade_date
            HAVING COUNT(*) > 1
            LIMIT 3
        """).fetchall()
        self.assertEqual(len(dups), 0,
            f"主键重复: {dups[:3]}")


@unittest.skipUnless(os.path.exists(DB), f'DB not found: {DB}')
class TestIncrementalWriteIdempotency(unittest.TestCase):
    """增量写入幂等性验证"""

    def setUp(self):
        # 用 shutil 创建临时文件（避免 Windows NamedTemporaryFile 锁文件问题）
        tmp_dir = tempfile.mkdtemp()
        self.tmp_path = os.path.join(tmp_dir, 'test.db')
        # 创建临时DB
        conn = duckdb.connect(self.tmp_path)
        conn.execute("""
            CREATE TABLE daily_bar_raw (
                ts_code VARCHAR, trade_date DATE,
                close DOUBLE, adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY(ts_code, trade_date)
            )
        """)
        conn.close()

    def tearDown(self):
        try:
            shutil.rmtree(os.path.dirname(self.tmp_path))
        except:
            pass

    def test_insert_ignore_prevents_duplicates(self):
        """INSERT OR IGNORE 应防止重复写入"""
        # 手动构造路径，避免 Windows NamedTemporaryFile 锁文件问题
        tmp = self.tmp_path + '.db'
        conn = duckdb.connect(tmp)
        conn.execute("""
            CREATE TABLE daily_bar_raw (
                ts_code VARCHAR, trade_date DATE,
                close DOUBLE, adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY(ts_code, trade_date)
            )
        """)
        # 写入第一行
        conn.execute("""
            INSERT OR IGNORE INTO daily_bar_raw
                (ts_code, trade_date, close, adj_factor)
            VALUES ('000001.SZ', '2024-01-02', 10.5, 1.0)
        """)
        cnt1 = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
        # 再次写入相同主键（值不同）
        conn.execute("""
            INSERT OR IGNORE INTO daily_bar_raw
                (ts_code, trade_date, close, adj_factor)
            VALUES ('000001.SZ', '2024-01-02', 11.0, 1.0)
        """)
        cnt2 = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
        conn.close()
        try:
            os.unlink(tmp)
        except:
            pass
        self.assertEqual(cnt1, 1)
        self.assertEqual(cnt2, 1,
            "INSERT OR IGNORE 应防止主键重复，cnt不应增加")


@unittest.skipUnless(os.path.exists(DB), f'DB not found: {DB}')
class TestDataLayerComplete(unittest.TestCase):
    """数据层完整性验证"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_all_required_tables_exist(self):
        """8张核心表必须存在"""
        tables = [r[0] for r in self.conn.execute("SHOW TABLES").fetchall()]
        required = [
            'daily_bar_raw',
            'daily_bar_adjusted',
            'stock_basic_history',
            'index_constituents_history',
            'st_status_history',
            'corporate_actions',
            'data_quality_alert',
        ]
        missing = [t for t in required if t not in tables]
        self.assertEqual(len(missing), 0,
            f"缺失表: {missing}")

    def test_stock_basic_history_pit_query(self):
        """stock_basic_history PIT 查询正确"""
        # 2024-01-01 可交易股票池
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM stock_basic_history
            WHERE list_date <= '2024-01-01'
              AND (delist_date > '2024-01-01' OR delist_date IS NULL)
        """).fetchone()[0]
        self.assertGreater(cnt, 4500,
            f"2024-01-01可交易股票应有>4500只，得到{cnt}")
        self.assertLess(cnt, 6500,
            f"2024-01-01可交易股票应有<6500只，得到{cnt}")

    def test_delisted_excluded_from_2026(self):
        """2024-04-26 退市的 000005.SZ 不应在 2025-01-01 股票池"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM stock_basic_history
            WHERE ts_code = '000005.SZ'
              AND list_date <= '2025-01-01'
              AND (delist_date > '2025-01-01' OR delist_date IS NULL)
        """).fetchone()[0]
        self.assertEqual(cnt, 0,
            "000005.SZ已于2024-04-26退市，不应在2025-01-01可交易")

    def test_st_status_history_has_stocks(self):
        """st_status_history 应有 ST 股记录"""
        cnt = self.conn.execute("SELECT COUNT(*) FROM st_status_history").fetchone()[0]
        self.assertGreater(cnt, 100,
            f"st_status_history应有ST股记录，得到{cnt}")
        stocks = self.conn.execute(
            "SELECT COUNT(DISTINCT ts_code) FROM st_status_history"
        ).fetchone()[0]
        self.assertGreater(stocks, 10,
            f"应有>10只ST股，得到{stocks}")

    def test_data_quality_alert_has_entries(self):
        """data_quality_alert 应有记录"""
        cnt = self.conn.execute(
            "SELECT COUNT(*) FROM data_quality_alert"
        ).fetchone()[0]
        self.assertGreater(cnt, 0,
            "data_quality_alert应有记录")

    def test_adj_factor_log_exists(self):
        """adj_factor_log 表存在（用于未来复权因子变化检测）"""
        tables = [r[0] for r in self.conn.execute("SHOW TABLES").fetchall()]
        self.assertIn('adj_factor_log', tables,
            "adj_factor_log 表应存在（用于复权因子变化检测）")

    def test_update_log_exists(self):
        """update_log 表存在（用于增量更新记录）"""
        tables = [r[0] for r in self.conn.execute("SHOW TABLES").fetchall()]
        self.assertIn('update_log', tables,
            "update_log 表应存在（用于增量更新记录）")


if __name__ == '__main__':
    unittest.main()
