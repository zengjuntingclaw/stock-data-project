"""动态股票池管理"""
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
import pandas as pd
from loguru import logger

@dataclass
class UniverseConfig:
    min_market_cap: float = 5e8
    min_listing_days: int = 60
    exclude_st: bool = True
    top_n: Optional[int] = None

class DynamicUniverse:
    def __init__(self, config: Optional[UniverseConfig] = None, data_engine=None):
        self.config = config or UniverseConfig()
        self.data_engine = data_engine
        self._cache: Dict[str, List[str]] = {}
    
    def get_universe(self, date: datetime, market: Optional[pd.DataFrame] = None) -> List[str]:
        key = date.strftime('%Y-%m-%d')
        if key in self._cache: return self._cache[key]
        if market is None and self.data_engine:
            market = self.data_engine.get_all_stocks_snapshot(date)
        result = self._filter(market, date)
        self._cache[key] = result
        return result
    
    def _filter(self, df: pd.DataFrame, date: datetime) -> List[str]:
        if self.config.exclude_st and 'is_st' in df.columns:
            df = df[~df['is_st']]
        if 'list_date' in df.columns:
            df['days'] = (date - pd.to_datetime(df['list_date'])).dt.days
            df = df[df['days'] >= self.config.min_listing_days]
        if 'total_mv' in df.columns:
            df = df[df['total_mv'] >= self.config.min_market_cap]
        if 'delist_date' in df.columns:
            df = df[df['delist_date'].isna() | (pd.to_datetime(df['delist_date']) > date)]
        if self.config.top_n and 'total_mv' in df.columns:
            df = df.nlargest(self.config.top_n, 'total_mv')
        return df['symbol'].tolist()
