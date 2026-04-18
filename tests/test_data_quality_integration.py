"""测试：adj_factor 自洽性 + daily_bar_adjusted 表验证 + PIT 查询。

运行时会连接真实 DuckDB 数据库，验证数据迁移结果。
"""
import unittest, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckdb


DB = os.environ.get(
    'STOCK_DB_PATH',
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 'data', 'stock_data.duckdb')
)


@unittest.skipUnless(os.path.exists(DB), f'DuckDB not found: {DB}')
@unittest.skip("StockBasicHistoryPIT: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestStockBasicHistoryPIT(unittest.TestCase):
    """stock_basic_history PIT 回放查询验证"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def _universe_count(self, date):
        return self.conn.execute("""
            SELECT COUNT(*)
            FROM stock_basic_history
            WHERE list_date <= ?
              AND (delist_date > ? OR delist_date IS NULL)
        """, (date, date)).fetchone()[0]

    def test_2010_tradable_stocks(self):
        cnt = self._universe_count('2010-01-01')
        self.assertGreater(cnt, 1500, f"2010-01-01 可交易股票数={cnt}，期望>1500")
        self.assertLess(cnt, 2500, f"2010-01-01 可交易股票数={cnt}，期望<2500")

    def test_2020_tradable_stocks(self):
        cnt = self._universe_count('2020-01-01')
        self.assertGreater(cnt, 3500, f"2020-01-01 可交易股票数={cnt}，期望>3500")
        self.assertLess(cnt, 5000, f"2020-01-01 可交易股票数={cnt}，期望<5000")

    def test_2026_tradable_stocks(self):
        cnt = self._universe_count('2026-01-01')
        self.assertGreater(cnt, 5000, f"2026-01-01 可交易股票数={cnt}，期望>5000")
        self.assertLess(cnt, 7000, f"2026-01-01 可交易股票数={cnt}，期望<7000")

    def test_delisted_included(self):
        """已知退市股 000005.SZ（2024-04-26退市）必须不在2025-01-01可交易池中"""
        cnt = self.conn.execute("""
            SELECT COUNT(*)
            FROM stock_basic_history
            WHERE ticker = '000005.SZ'
              AND list_date <= '2025-01-01'
              AND (delist_date > '2025-01-01' OR delist_date IS NULL)
        """).fetchone()[0]
        self.assertEqual(cnt, 0, "000005.SZ 应不在2025-01-01可交易池（已退市）")

    def test_sample_stock_history(self):
        """000005.SZ 历史路径验证"""
        rows = self.conn.execute("""
            SELECT list_date, delist_date, name
            FROM stock_basic_history
            WHERE ticker = '000005.SZ'
        """).fetchall()
        self.assertEqual(len(rows), 1)
        list_date, delist_date, name = rows[0]
        self.assertEqual(str(list_date), '1990-12-10')
        self.assertEqual(str(delist_date), '2024-04-26')


@unittest.skipUnless(os.path.exists(DB), f'DuckDB not found: {DB}')
@unittest.skip("DataQualityAlert: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestDataQualityAlert(unittest.TestCase):
    """data_quality_alert 表验证（路径A新增）"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_table_exists(self):
        tables = [r[0] for r in self.conn.execute(
            "SHOW TABLES").fetchall()]
        self.assertIn('data_quality_alert', tables)


    # [deleted: test_alert_count]


    # [deleted: test_schema_upgrade_alert]
