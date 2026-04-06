"""PIT数据对齐"""
from datetime import datetime
from typing import List, Optional
import pandas as pd
import numpy as np
from loguru import logger

class PITDataAligner:
    def __init__(self):
        self._data: Optional[pd.DataFrame] = None
    
    def load(self, df: pd.DataFrame):
        df = df.copy()
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['ann_date'] = pd.to_datetime(df['ann_date'])
        self._data = df.sort_values(['symbol', 'ann_date'])
        logger.info(f"PIT loaded: {len(df)} records")
    
    def get_factor(self, name: str, date: datetime, symbols: Optional[List[str]] = None) -> pd.Series:
        if self._data is None: raise ValueError("Data not loaded")
        mask = self._data['ann_date'] <= date
        if symbols: mask &= self._data['symbol'].isin(symbols)
        filtered = self._data[mask]
        if filtered.empty:
            return pd.Series(dtype=float, name=name)
        # 按每个 symbol 取 ann_date 最大的那一行（PIT 约束）
        idx = filtered.groupby('symbol')['ann_date'].idxmax()
        result = filtered.loc[idx].set_index('symbol')[name]
        return result
    
    def get_factors(self, names: List[str], date: datetime, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return pd.DataFrame({n: self.get_factor(n, date, symbols) for n in names})
    
    def validate(self, name: str, dates: List[datetime]) -> bool:
        for d in dates:
            for s in self.get_factor(name, d).index:
                data = self._data[(self._data['symbol'] == s) & (self._data['ann_date'] <= d)]
                if len(data) > 0 and data.iloc[-1]['ann_date'] > d:
                    return False
        return True
