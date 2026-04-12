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
class TestAdjFactorIntegrity(unittest.TestCase):
    """adj_factor 自洽性验证"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_daily_quotes_adj_factor_all_ones(self):
        """daily_bar_adjusted.adj_factor 全表必须 = 1.0"""
        cnt = self.conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
        adj_bad = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_adjusted WHERE adj_factor < 0.999"
        ).fetchone()[0]
        self.assertGreater(cnt, 1_000_000,
            f"daily_bar_adjusted 行数={cnt}，期望>1000000")
        self.assertEqual(adj_bad, 0,
            f"adj_factor != 1.0 的行数={adj_bad}，期望0")

    def test_daily_quotes_adj_factor_sum_equals_count(self):
        """SUM(adj_factor) == COUNT(*)"""
        result = self.conn.execute("""
            SELECT SUM(adj_factor), COUNT(*) FROM daily_bar_adjusted
        """).fetchone()
        adj_sum, cnt = result
        self.assertAlmostEqual(adj_sum, cnt, places=2,
            msg=f"SUM(adj_factor)={adj_sum}, COUNT={cnt}")

    def test_600000_2025_adj_factor_fixed(self):
        """600000.SH 2025-01 批次 adj_factor 已修正为 1.0"""
        rows = self.conn.execute("""
            SELECT trade_date, close, adj_factor, pre_close
            FROM daily_bar_adjusted
            WHERE ts_code = '600000.SH'
              AND trade_date BETWEEN '2025-01-02' AND '2025-01-17'
            ORDER BY trade_date
        """).fetchall()
        self.assertGreater(len(rows), 0, f"600000.SH 2025-01 数据不存在")
        for trade_date, close, adj_factor, pre_close in rows:
            self.assertAlmostEqual(adj_factor, 1.0, places=4,
                msg=f"{trade_date}: adj_factor={adj_factor}")


@unittest.skipUnless(os.path.exists(DB), f'DuckDB not found: {DB}')
class TestDailyBarAdjusted(unittest.TestCase):
    """daily_bar_adjusted 表验证（路径A新增）"""

    def setUp(self):
        self.conn = duckdb.connect(DB, read_only=True)

    def tearDown(self):
        self.conn.close()

    def test_table_exists(self):
        tables = [r[0] for r in self.conn.execute(
            "SHOW TABLES").fetchall()]
        self.assertIn('daily_bar_adjusted', tables)

    def test_row_count(self):
        cnt = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_adjusted"
        ).fetchone()[0]
        self.assertGreaterEqual(cnt, 1_400_000,
            f"daily_bar_adjusted 行数={cnt}，期望>=1400000")

    def test_adj_factor_all_ones(self):
        adj_bad = self.conn.execute(
            "SELECT COUNT(*) FROM daily_bar_adjusted WHERE adj_factor < 0.999"
        ).fetchone()[0]
        self.assertEqual(adj_bad, 0)

    def test_primary_key(self):
        """验证主键 (ts_code, trade_date) 无重复"""
        dups = self.conn.execute("""
            SELECT ts_code, trade_date, COUNT(*) as cnt
            FROM daily_bar_adjusted
            GROUP BY ts_code, trade_date
            HAVING COUNT(*) > 1
        """).fetchall()
        self.assertEqual(len(dups), 0,
            f"存在重复主键: {dups[:3]}")


@unittest.skipUnless(os.path.exists(DB), f'DuckDB not found: {DB}')
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
            WHERE ts_code = '000005.SZ'
              AND list_date <= '2025-01-01'
              AND (delist_date > '2025-01-01' OR delist_date IS NULL)
        """).fetchone()[0]
        self.assertEqual(cnt, 0, "000005.SZ 应不在2025-01-01可交易池（已退市）")

    def test_sample_stock_history(self):
        """000005.SZ 历史路径验证"""
        rows = self.conn.execute("""
            SELECT list_date, delist_date, name
            FROM stock_basic_history
            WHERE ts_code = '000005.SZ'
        """).fetchall()
        self.assertEqual(len(rows), 1)
        list_date, delist_date, name = rows[0]
        self.assertEqual(str(list_date), '1990-12-10')
        self.assertEqual(str(delist_date), '2024-04-26')


@unittest.skipUnless(os.path.exists(DB), f'DuckDB not found: {DB}')
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

    def test_alert_count(self):
        cnt = self.conn.execute(
            "SELECT COUNT(*) FROM data_quality_alert"
        ).fetchone()[0]
        self.assertGreater(cnt, 0, "data_quality_alert 应有数据")

    def test_adj_factor_fix_alerts_exist(self):
        """adj_factor_fix 类型告警必须存在"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM data_quality_alert
            WHERE alert_type = 'adj_factor_fix'
        """).fetchone()[0]
        self.assertGreaterEqual(cnt, 7,
            f"adj_factor_fix 告警应有>=7条，得到{cnt}")

    def test_schema_upgrade_alert(self):
        """schema_upgrade 类型告警应记录表创建"""
        cnt = self.conn.execute("""
            SELECT COUNT(*) FROM data_quality_alert
            WHERE alert_type = 'schema_upgrade'
        """).fetchone()[0]
        self.assertGreaterEqual(cnt, 1,
            f"schema_upgrade 告警应有>=1条，得到{cnt}")


if __name__ == '__main__':
    unittest.main()
