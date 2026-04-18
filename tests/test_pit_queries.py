"""Test PIT queries in data_engine + HistoricalSecurityMaster integration"""
import unittest, sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'stock_data.duckdb')

@unittest.skip("PITQueries: 需要 stock_data.duckdb 有数据，数据重载后删除此装饰器")
class TestPITQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from scripts.data_engine import DataEngine
        cls.de = DataEngine()

    def test_pit_stock_pool_2024(self):
        df = self.de.get_pit_stock_pool('2024-01-01')
        self.assertGreater(len(df), 5000)
        self.assertIn('is_tradable', df.columns)
        tradable = df[df['is_tradable'] == True]
        self.assertEqual(len(tradable), len(df))  # no delisted in history
        self.assertIn('ticker', df.columns)
        # 沪深股在列
        sh = df[df['exchange'] == 'SH']
        self.assertGreater(len(sh), 1000)

    def test_pit_stock_pool_2010(self):
        df = self.de.get_pit_stock_pool('2010-01-01')
        self.assertGreater(len(df), 1500)
        self.assertLess(len(df), 2500)  # fewer stocks in 2010

    def test_pit_stock_pool_2026(self):
        df = self.de.get_pit_stock_pool('2026-01-01')
        self.assertGreater(len(df), 5000)

    def test_st_on_date_2001(self):
        df = self.de.get_stocks_with_st_status('2001-02-20')
        self.assertGreater(len(df), 10)  # multiple ST stocks in 2001
        self.assertTrue(all(df['ticker'].str.contains('\\.')))

    def test_pit_stock_pool_exchange_filter(self):
        df = self.de.get_pit_stock_pool('2024-01-01', exchanges=['SZ'])
        self.assertTrue(all(df['exchange'] == 'SZ'))
        self.assertGreater(len(df), 500)

    def test_pit_vs_duckdb_direct(self):
        """PIT query results match direct DuckDB query"""
        import duckdb
        conn = duckdb.connect(DB, read_only=True)
        duck_df = conn.execute("""
            SELECT ticker FROM stock_basic_history
            WHERE list_date <= '2024-01-01'
              AND (delist_date > '2024-01-01' OR delist_date IS NULL)
            ORDER BY ticker
        """).fetchdf()
        conn.close()
        pit_df = self.de.get_pit_stock_pool('2024-01-01').sort_values('ticker').reset_index(drop=True)
        # Compare counts (column names differ but data same)
        self.assertEqual(len(duck_df), len(pit_df))

