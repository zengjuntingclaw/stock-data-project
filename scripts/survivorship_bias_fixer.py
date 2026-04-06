"""
survivorship_bias_fix.py - 幸存者偏差深度治理模块
===============================================

消除幸存者偏差的核心问题：
1. 股票名称历史快照（判断ST状态）
2. 行业分类历史快照
3. 全市场股票历史快照

依赖表：
- stock_name_history (ts_code, name, is_st, start_date, end_date)
- industry_history (ts_code, industry, source, start_date, end_date)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, Optional, List
from loguru import logger
import duckdb

try:
    from scripts.data_classes import StockStatus
except ImportError:
    pass


class SurvivorshipBiasFixer:
    """
    幸存者偏差治理器
    
    核心原则：所有历史判断必须使用"交易当日"的快照数据，
    而非"当前"的静态数据。
    """
    
    def __init__(self, db_path: str = "data/stock_data.duckdb"):
        self.db_path = db_path
        self._ensure_history_tables()
    
    def _ensure_history_tables(self):
        """确保历史快照表存在"""
        conn = duckdb.connect(self.db_path)
        
        # 股票名称历史表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_name_history (
                ts_code VARCHAR,
                name VARCHAR,
                is_st BOOLEAN DEFAULT FALSE,
                start_date DATE,
                end_date DATE,
                PRIMARY KEY (ts_code, start_date)
            )
        """)
        
        # 行业历史分类表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS industry_history (
                ts_code VARCHAR,
                industry VARCHAR,
                industry_code VARCHAR,
                source VARCHAR DEFAULT 'sw',
                start_date DATE,
                end_date DATE,
                PRIMARY KEY (ts_code, start_date, source)
            )
        """)
        
        # 创建索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_name_code ON stock_name_history(ts_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_name_date ON stock_name_history(start_date, end_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ind_code ON industry_history(ts_code)")
        
        conn.close()
    
    def get_stock_name_at_date(self,
                                ts_code: str,
                                trade_date: str) -> Tuple[str, bool]:
        """
        获取"交易当日"的股票名称和ST状态
        
        Parameters
        ----------
        ts_code : str
            股票代码
        trade_date : str
            交易日期
            
        Returns
        -------
        (name, is_st) : 元组
            股票名称，是否ST
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            # 查历史名称表
            df = conn.execute(f"""
                SELECT name, is_st
                FROM stock_name_history
                WHERE ts_code = '{ts_code}'
                AND start_date <= '{trade_date}'
                AND (end_date IS NULL OR end_date >= '{trade_date}')
                ORDER BY start_date DESC
                LIMIT 1
            """).fetchdf()
            
            if not df.empty:
                return df["name"].iloc[0], df["is_st"].iloc[0]
            
            # 降级：从基础表获取（记录警告）
            logger.warning(f"[{trade_date}] 未找到 {ts_code} 的历史名称快照")
            
            basic_df = conn.execute(f"""
                SELECT name FROM stock_basic
                WHERE ts_code = '{ts_code}'
                LIMIT 1
            """).fetchdf()
            
            if not basic_df.empty:
                name = basic_df["name"].iloc[0]
                return name, "ST" in name
            
            return "未知", False
            
        finally:
            conn.close()
    
    def get_industry_at_date(self,
                              ts_code: str,
                              trade_date: str,
                              source: str = "sw") -> str:
        """
        获取"交易当日"的行业分类
        
        Parameters
        ----------
        ts_code : str
            股票代码
        trade_date : str
            交易日期
        source : str
            分类来源：sw=申万，zx=中信
            
        Returns
        -------
        industry : str
            行业名称
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            df = conn.execute(f"""
                SELECT industry
                FROM industry_history
                WHERE ts_code = '{ts_code}'
                AND source = '{source}'
                AND start_date <= '{trade_date}'
                AND (end_date IS NULL OR end_date >= '{trade_date}')
                ORDER BY start_date DESC
                LIMIT 1
            """).fetchdf()
            
            if not df.empty:
                return df["industry"].iloc[0]
            
            logger.warning(f"[{trade_date}] 未找到 {ts_code} 的历史行业分类")
            return "未知"
            
        finally:
            conn.close()
    
    def get_all_stocks_snapshot(self,
                                  trade_date: str,
                                  include_delisted: bool = True,
                                  use_pit: bool = True) -> pd.DataFrame:
        """
        获取"交易当日"的全市场股票快照
        
        Parameters
        ----------
        trade_date : str
            交易日期
        include_delisted : bool
            是否包含已退市股票
        use_pit : bool
            是否使用Point-in-Time数据
            
        Returns
        -------
        DataFrame
            包含：ts_code, symbol, name, is_st, industry, list_date, delist_date
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            # 基础股票列表
            sql = "SELECT ts_code, symbol, name, list_date, delist_date FROM stock_basic"
            if not include_delisted:
                sql += f" WHERE delist_date IS NULL OR delist_date > '{trade_date}'"
            
            stocks = conn.execute(sql).fetchdf()
            
            if stocks.empty:
                return pd.DataFrame()
            
            trade_dt = pd.Timestamp(trade_date)
            
            results = []
            for _, row in stocks.iterrows():
                ts_code = row["ts_code"]
                list_date = row.get("list_date")
                delist_date = row.get("delist_date")
                
                # 检查是否在交易期内
                if list_date and pd.notna(list_date):
                    if trade_dt < pd.Timestamp(list_date):
                        continue
                
                if delist_date and pd.notna(delist_date):
                    if trade_dt > pd.Timestamp(delist_date):
                        if not include_delisted:
                            continue
                
                # 获取PIT数据
                if use_pit:
                    name, is_st = self.get_stock_name_at_date(ts_code, trade_date)
                    industry = self.get_industry_at_date(ts_code, trade_date)
                else:
                    name = row.get("name", "未知")
                    is_st = "ST" in str(name)
                    industry = "未知"
                
                results.append({
                    "ts_code": ts_code,
                    "symbol": row.get("symbol", ts_code.split(".")[0]),
                    "name": name,
                    "is_st": is_st,
                    "industry": industry,
                    "list_date": list_date,
                    "delist_date": delist_date
                })
            
            return pd.DataFrame(results)
            
        finally:
            conn.close()
    
    def batch_check_st_status(self,
                                ts_codes: List[str],
                                trade_date: str) -> pd.Series:
        """
        批量获取ST状态（向量化）
        
        Parameters
        ----------
        ts_codes : List[str]
            股票代码列表
        trade_date : str
            交易日期
            
        Returns
        -------
        Series
            index=ts_code, value=is_st
        """
        if not ts_codes:
            return pd.Series(dtype=bool)
        
        conn = duckdb.connect(self.db_path, read_only=True)
        
        try:
            code_list = "','".join(ts_codes)
            
            # 使用窗口函数批量查询
            df = conn.execute(f"""
                WITH ranked AS (
                    SELECT 
                        ts_code,
                        is_st,
                        ROW_NUMBER() OVER (
                            PARTITION BY ts_code
                            ORDER BY start_date DESC
                        ) AS rn
                    FROM stock_name_history
                    WHERE ts_code IN ('{code_list}')
                    AND start_date <= '{trade_date}'
                    AND (end_date IS NULL OR end_date >= '{trade_date}')
                )
                SELECT ts_code, is_st
                FROM ranked
                WHERE rn = 1
            """).fetchdf()
            
            if df.empty:
                return pd.Series({code: False for code in ts_codes})
            
            return df.set_index("ts_code")["is_st"]
            
        finally:
            conn.close()
    
    def load_name_history_from_basic(self):
        """
        从stock_basic表加载历史名称数据
        
        初始化时使用：将当前的股票名称作为最新的历史记录
        """
        conn = duckdb.connect(self.db_path)
        
        try:
            # 获取当前所有股票
            stocks = conn.execute("""
                SELECT ts_code, name, list_date FROM stock_basic
            """).fetchdf()
            
            if stocks.empty:
                return
            
            # 插入历史名称表
            today = datetime.now().strftime("%Y-%m-%d")
            
            for _, row in stocks.iterrows():
                ts_code = row["ts_code"]
                name = row["name"]
                list_date = row.get("list_date", "1990-01-01")
                is_st = "ST" in name
                
                conn.execute(f"""
                    INSERT OR REPLACE INTO stock_name_history
                    (ts_code, name, is_st, start_date, end_date)
                    VALUES ('{ts_code}', '{name}', {is_st}, '{list_date}', NULL)
                """)
            
            logger.info(f"加载 {len(stocks)} 只股票的历史名称数据")
            
        finally:
            conn.close()


# 测试
if __name__ == "__main__":
    fixer = SurvivorshipBiasFixer()
    
    # 测试获取历史名称
    name, is_st = fixer.get_stock_name_at_date("000001.SZ", "2024-01-01")
    print(f"000001.SZ @ 2024-01-01: {name}, ST={is_st}")
    
    # 测试批量ST状态
    codes = ["000001.SZ", "000002.SZ", "600000.SH"]
    st_status = fixer.batch_check_st_status(codes, "2024-01-01")
    print(st_status)
