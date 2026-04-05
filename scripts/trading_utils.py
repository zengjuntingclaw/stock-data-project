"""交易工具集：过滤、退市监控、ST监控"""
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
from loguru import logger
from scripts.data_classes import StockStatus

class TradingFilter:
    def __init__(self, exclude_st=True, exclude_suspended=True, min_listing_days=60, min_market_cap=1e8):
        self.exclude_st = exclude_st
        self.exclude_suspended = exclude_suspended
        self.min_days = min_listing_days
        self.min_mv = min_market_cap
    
    def filter(self, universe: List[str], market: pd.DataFrame, date: datetime) -> List[str]:
        result = []
        for s in universe:
            r = market[market['symbol'] == s]
            if r.empty: continue
            r = r.iloc[0]
            if self.exclude_st and r.get('is_st'): continue
            if self.exclude_suspended and r.get('is_suspended'): continue
            if self.min_days > 0:
                ld = r.get('list_date')
                if ld and pd.notna(ld) and (date - pd.to_datetime(ld)).days < self.min_days: continue
            if self.min_mv > 0 and r.get('total_mv', 0) < self.min_mv: continue
            result.append(s)
        return result

class DelistingMonitor:
    def __init__(self, warning_days=20):
        self.warning_days = warning_days
        self._calendar: Dict[str, datetime] = {}
    
    def load(self, df: pd.DataFrame):
        for _, row in df.iterrows():
            self._calendar[row['symbol']] = pd.to_datetime(row['delist_date'])
    
    def check(self, symbol: str, date: datetime) -> Tuple[bool, int]:
        if symbol not in self._calendar: return False, -1
        days = (self._calendar[symbol] - date).days
        return 0 <= days <= self.warning_days, days
    
    def get_force_sell(self, holdings: List[str], date: datetime) -> List[Dict]:
        result = []
        for s in holdings:
            ok, days = self.check(s, date)
            if ok:
                urgency = 'high' if days <= 5 else 'medium' if days <= 10 else 'low'
                result.append({'symbol': s, 'days': days, 'urgency': urgency})
        return sorted(result, key=lambda x: {'high': 0, 'medium': 1, 'low': 2}[x['urgency']])

class STMonitor:
    def __init__(self, allow_buy_st=False, allow_buy_star_st=False):
        self.allow_st = allow_buy_st
        self.allow_star = allow_buy_star_st
        self._history: Dict[str, List[Tuple[datetime, Optional[datetime]]]] = {}
    
    def load(self, df: pd.DataFrame):
        for _, row in df.iterrows():
            symbol = row['symbol']
            if symbol not in self._history:
                self._history[symbol] = []
            end = pd.to_datetime(row.get('end_date')) if pd.notna(row.get('end_date')) else None
            self._history[symbol].append((pd.to_datetime(row['start_date']), end))
    
    def get_status(self, symbol: str, date: datetime) -> StockStatus:
        if symbol not in self._history: return StockStatus.NORMAL
        for start, end in self._history[symbol]:
            if start <= date <= (end or datetime.max):
                return StockStatus.ST
        return StockStatus.NORMAL
    
    def can_buy(self, symbol: str, date: datetime) -> bool:
        s = self.get_status(symbol, date)
        if s == StockStatus.NORMAL: return True
        if s == StockStatus.ST: return self.allow_st
        return self.allow_star
