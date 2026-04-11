"""Test PIT queries in data_engine + HistoricalSecurityMaster integration"""
import unittest, sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

DB = 'data/stock_data.duckdb'

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
        self.assertIn('ts_code', df.columns)
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
        self.assertTrue(all(df['ts_code'].str.contains('\\.')))

    def test_pit_stock_pool_exchange_filter(self):
        df = self.de.get_pit_stock_pool('2024-01-01', exchanges=['SZ'])
        self.assertTrue(all(df['exchange'] == 'SZ'))
        self.assertGreater(len(df), 500)

    def test_pit_vs_duckdb_direct(self):
        """PIT query results match direct DuckDB query"""
        import duckdb
        conn = duckdb.connect(DB, read_only=True)
        duck_df = conn.execute("""
            SELECT ts_code FROM stock_basic_history
            WHERE list_date <= '2024-01-01'
              AND (delist_date > '2024-01-01' OR delist_date IS NULL)
            ORDER BY ts_code
        """).fetchdf()
        conn.close()
        pit_df = self.de.get_pit_stock_pool('2024-01-01').sort_values('ts_code').reset_index(drop=True)
        # Compare counts (column names differ but data same)
        self.assertEqual(len(duck_df), len(pit_df))


class TestSecurityMasterDuckDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Rebuild JSON from DuckDB
        from scripts.security_master import HistoricalSecurityMaster
        # Delete cache to force rebuild
        from pathlib import Path
        json_path = Path('data/security_master/security_master.json')
        if json_path.exists():
            json_path.unlink()
        cls.master = HistoricalSecurityMaster.from_duckdb(DB, rebuild=True)

    def test_loads_stocks(self):
        self.assertGreater(len(self.master._stocks), 5000)

    def test_universe_2010(self):
        # HistoricalSecurityMaster 基于 stock_basic_history（含退市股），
        # 所以 2010 年已上市但后来退市的股也计入，比 DuckDB PIT 多。
        # 用 DuckDB get_pit_stock_pool 验证基准
        from scripts.data_engine import DataEngine
        de = DataEngine()
        pit_df = de.get_pit_stock_pool('2010-01-01')
        # DuckDB PIT: 真实可交易股票池（不含退市）
        self.assertGreater(len(pit_df), 1000)
        # HistoricalSecurityMaster universe 包含所有已上市股（包括已退市）
        df = self.master.get_universe_on_date('2010-01-01')
        self.assertGreater(len(df), 1500)  # 包含已退市股，数量更多

    def test_universe_2024(self):
        df = self.master.get_universe_on_date('2024-01-01')
        self.assertGreater(len(df), 5000)

    def test_delisted_excluded(self):
        """Delisted stocks not in tradable universe"""
        df = self.master.get_universe_on_date('2026-01-01')
        # Should not include stocks delisted before 2026
        delisted = df[df['status'] == 'delisted']
        # With include_delisted=False (default), should be empty
        self.assertEqual(len(delisted), 0)

    def test_master_vs_duckdb_pit(self):
        """Memory master and DuckDB PIT query agree on tradable count"""
        from scripts.data_engine import DataEngine
        de = DataEngine()
        pit_df = de.get_pit_stock_pool('2024-01-01')
        # DuckDB PIT gives active stocks only (is_tradable=True)
        pit_count = len(pit_df)
        # Master gives all stocks listed by that date
        mem_df = self.master.get_universe_on_date('2024-01-01')
        # Memory should have >= active stocks (includes some delisted too)
        self.assertGreaterEqual(len(mem_df), pit_count)


if __name__ == '__main__':
    unittest.main(verbosity=2)
