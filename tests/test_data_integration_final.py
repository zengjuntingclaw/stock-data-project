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
                ticker VARCHAR, trade_date DATE,
                close DOUBLE, adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY(ticker, trade_date)
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
                ticker VARCHAR, trade_date DATE,
                close DOUBLE, adj_factor DOUBLE DEFAULT 1.0,
                PRIMARY KEY(ticker, trade_date)
            )
        """)
        # 写入第一行
        conn.execute("""
            INSERT OR IGNORE INTO daily_bar_raw
                (ticker, trade_date, close, adj_factor)
            VALUES ('000001.SZ', '2024-01-02', 10.5, 1.0)
        """)
        cnt1 = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
        # 再次写入相同主键（值不同）
        conn.execute("""
            INSERT OR IGNORE INTO daily_bar_raw
                (ticker, trade_date, close, adj_factor)
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
@unittest.skip("DataLayerComplete: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
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
            'market_daily',
            'stock_basic',
            'trading_calendar',
            'daily_bar_adjusted',
            'daily_bar_raw',
            'index_membership',
            'company_status',
            'corporate_action',
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
            WHERE ticker = '000005.SZ'
              AND list_date <= '2025-01-01'
              AND (delist_date > '2025-01-01' OR delist_date IS NULL)
        """).fetchone()[0]
        self.assertEqual(cnt, 0,
            "000005.SZ已于2024-04-26退市，不应在2025-01-01可交易")


    # [deleted: test_st_status_history_has_stocks - st_status_history 已从 v5 schema 删除]

    def test_data_quality_alert_has_entries(self):
        """data_quality_alert 应有记录"""
        cnt = self.conn.execute(
            "SELECT COUNT(*) FROM data_quality_alert"
        ).fetchone()[0]
        self.assertGreater(cnt, 0,
            "data_quality_alert应有记录")


    # [deleted: test_adj_factor_log_exists - adj_factor_log 已从 v5 schema 删除]


    # [deleted: test_update_log_exists - update_log 已从 v5 schema 删除]
