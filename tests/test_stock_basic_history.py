"""
测试证券主表时点版本功能（纯本地，不依赖网络）
"""
import unittest
import sys
import os
import tempfile
import shutil

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import duckdb
import pandas as pd


class TestStockBasicHistorySchema(unittest.TestCase):
    """测试证券主表历史版本Schema（纯本地）"""
    
    def setUp(self):
        """每个测试前创建临时数据库"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_stock_hist.duckdb")
        # 直接使用DuckDB测试Schema
        self.conn = duckdb.connect(self.db_path)
        self._create_schema()
    
    def tearDown(self):
        """每个测试后清理"""
        self.conn.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_schema(self):
        """创建stock_basic_history表"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic_history (
                ts_code       VARCHAR,
                snapshot_date DATE,
                symbol        VARCHAR,
                name          VARCHAR,
                area          VARCHAR,
                industry      VARCHAR,
                market        VARCHAR,
                list_date     DATE,
                delist_date   DATE,
                is_delisted   BOOLEAN DEFAULT FALSE,
                total_mv      DOUBLE,
                circ_mv       DOUBLE,
                PRIMARY KEY (ts_code, snapshot_date)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_hist_date 
            ON stock_basic_history(snapshot_date)
        """)
    
    def test_schema_created(self):
        """测试历史表已创建"""
        tables = self.conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema='main' AND table_name='stock_basic_history'
        """).fetchall()
        self.assertEqual(len(tables), 1)
    
    def test_insert_and_query_snapshot(self):
        """测试插入和查询快照"""
        # 插入测试数据
        test_data = pd.DataFrame({
            'ts_code': ['000001.SZ', '000002.SZ', '600000.SH'],
            'snapshot_date': pd.to_datetime(['2024-01-15', '2024-01-15', '2024-01-15']),
            'symbol': ['000001', '000002', '600000'],
            'name': ['平安银行', '万科A', '浦发银行'],
            'industry': ['银行', '地产', '银行'],
            'market': ['主板', '主板', '主板'],
            'is_delisted': [False, False, False],
            'total_mv': [1000.0, 2000.0, 3000.0],
            'circ_mv': [800.0, 1600.0, 2400.0]
        })
        
        self.conn.register("tmp_data", test_data)
        self.conn.execute("""
            INSERT INTO stock_basic_history 
            (ts_code, snapshot_date, symbol, name, industry, market, 
             is_delisted, total_mv, circ_mv)
            SELECT * FROM tmp_data
        """)
        
        # 查询验证
        result = self.conn.execute("""
            SELECT * FROM stock_basic_history 
            WHERE snapshot_date = '2024-01-15'
            ORDER BY ts_code
        """).fetchdf()
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]['ts_code'], '000001.SZ')
        self.assertEqual(result.iloc[0]['name'], '平安银行')
    
    def test_unique_constraint(self):
        """测试主键唯一性约束"""
        # 插入第一条
        self.conn.execute("""
            INSERT INTO stock_basic_history (ts_code, snapshot_date, symbol, name)
            VALUES ('000001.SZ', '2024-01-15', '000001', '平安银行')
        """)
        
        # 尝试插入重复数据应报错
        with self.assertRaises(Exception):
            self.conn.execute("""
                INSERT INTO stock_basic_history (ts_code, snapshot_date, symbol, name)
                VALUES ('000001.SZ', '2024-01-15', '000001', '平安银行')
            """)
    
    def test_index_usage(self):
        """测试索引是否存在"""
        indexes = self.conn.execute("""
            SELECT index_name FROM duckdb_indexes 
            WHERE table_name = 'stock_basic_history'
        """).fetchall()
        
        index_names = [idx[0] for idx in indexes]
        self.assertIn('idx_stock_hist_date', index_names)


if __name__ == '__main__':
    unittest.main()
