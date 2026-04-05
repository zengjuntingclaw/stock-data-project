"""向量化因子计算 + 长表管理"""
from datetime import datetime
from typing import List, Optional, Dict
import pandas as pd
import numpy as np
import duckdb
from loguru import logger

class VectorizedFactorEngine:
    def __init__(self, db_path: Optional[str] = None):
        self.conn = duckdb.connect(db_path) if db_path else duckdb.connect()
    
    def calc_momentum(self, table='stock_prices', windows=[5, 10, 20, 60]) -> pd.DataFrame:
        cols = [f"(close - lag(close, {w}) over (partition by symbol order by date)) / lag(close, {w}) over (partition by symbol order by date) as mom_{w}" for w in windows]
        return self.conn.execute(f"select symbol, date, {', '.join(cols)} from {table} order by symbol, date").fetchdf()
    
    def calc_volatility(self, table='stock_prices', windows=[20, 60]) -> pd.DataFrame:
        cols = [f"stddev_samp(ln(close/lag(close, 1) over (partition by symbol order by date))) over (partition by symbol order by date rows between {w-1} preceding and current row) * sqrt(252) as vol_{w}" for w in windows]
        return self.conn.execute(f"select symbol, date, {', '.join(cols)} from {table} order by symbol, date").fetchdf()
    
    def calc_all(self, table='stock_prices') -> pd.DataFrame:
        merged = self.calc_momentum(table).merge(self.calc_volatility(table), on=['symbol', 'date'])
        long = merged.melt(id_vars=['symbol', 'date'], var_name='factor', value_name='value')
        return long[long['value'].notna() & np.isfinite(long['value'])]
    
    def close(self): self.conn.close()

class LongTableManager:
    def __init__(self, db_path: Optional[str] = None):
        self.conn = duckdb.connect(db_path or ':memory:')
        self.conn.execute("create table if not exists factor_values (symbol varchar, date date, factor varchar, value double, primary key (symbol, date, factor))")
    
    def insert(self, df: pd.DataFrame):
        df = df[df['value'].notna() & np.isfinite(df['value'])]
        for i in range(0, len(df), 10000):
            batch = df.iloc[i:i+10000]
            self.conn.execute("insert or replace into factor_values select symbol, date::date, factor, value from batch", {'batch': batch})
    
    def get(self, factor: str, symbols: Optional[List[str]] = None, date: Optional[datetime] = None) -> pd.DataFrame:
        conds = [f"factor = '{factor}'"]
        if symbols: conds.append(f"symbol in ({','.join(repr(s) for s in symbols)})")
        if date: conds.append(f"date = '{date:%Y-%m-%d}'")
        return self.conn.execute(f"select * from factor_values where {' and '.join(conds)}").fetchdf()
    
    def close(self): self.conn.close()
