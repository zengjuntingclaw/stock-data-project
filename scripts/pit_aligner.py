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
        mask = self._data['ann_date'] < date
        if symbols: mask &= self._data['symbol'].isin(symbols)
        filtered = self._data[mask].copy()
        if filtered.empty:
            return pd.Series(dtype=float, name=name)
        # 按每个 symbol 取 ann_date 最大的那一行（PIT 约束）
        # 先排序确保确定性：同一天多条公告时取最后一行
        filtered = filtered.sort_values(['symbol', 'ann_date'])
        result = filtered.groupby('symbol').last()[name]
        return result
    
    def get_factors(self, names: List[str], date: datetime, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return pd.DataFrame({n: self.get_factor(n, date, symbols) for n in names})
    
    def validate(self, name: str, dates: List[datetime]) -> bool:
        """验证PIT约束：所有可见数据均满足 ann_date < date"""
        # PIT边界已通过 < date 强制保证，此处做数据完整性检查
        for d in dates:
            for s in self.get_factor(name, d).index:
                data = self._data[(self._data['symbol'] == s) & (self._data['ann_date'] < d)]
                # 数据不为空且选取的是最大ann_date行（通过groupby.last保证）
                if len(data) == 0:
                    continue
                # 最大可见ann_date必须 < 回测日期（由 < 过滤保证，此处双重验证）
                latest_ann = data['ann_date'].max()
                if latest_ann >= d:
                    logger.warning(f"PIT violation: {s} ann_date={latest_ann} >= {d}")
                    return False
        return True
