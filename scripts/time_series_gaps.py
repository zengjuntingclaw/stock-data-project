"""
TimeSeriesGaps - 时间序列数据对齐与断层补全
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger


class FillStrategy(Enum):
    FORWARD_FILL = "ffill"
    BACKWARD_FILL = "bfill"
    LINEAR_INTERPOLATE = "linear"
    NEAREST_FILL = "nearest"
    ZERO_FILL = "zero"


class GapType(Enum):
    TRADING_HALT = "trading_halt"
    WEEKEND = "weekend"
    HOLIDAY = "holiday"
    DATA_MISSING = "data_missing"


@dataclass
class GapInfo:
    start_date: str
    end_date: str
    gap_type: GapType
    trading_days: int
    severity: str  # minor/moderate/severe


@dataclass
class GapFillResult:
    original_rows: int
    filled_rows: int
    gaps_filled: List[GapInfo]
    strategy_used: FillStrategy
    warnings: List[str] = field(default_factory=list)


def get_trading_days(start_date: str, end_date: str) -> pd.DatetimeIndex:
    """获取交易日列表"""
    return pd.bdate_range(start=start_date, end=end_date)


def detect_gaps(df: pd.DataFrame, date_col: str = "trade_date") -> List[GapInfo]:
    """检测数据中的断层"""
    if df.empty or date_col not in df.columns:
        return []
    
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).reset_index(drop=True)
    
    gaps = []
    dates = df[date_col]
    
    for i in range(1, len(dates)):
        diff = (dates.iloc[i] - dates.iloc[i-1]).days
        if diff > 1:
            severity = "minor" if diff <= 5 else "moderate" if diff <= 10 else "severe"
            gaps.append(GapInfo(
                start_date=dates.iloc[i-1].strftime("%Y-%m-%d"),
                end_date=dates.iloc[i].strftime("%Y-%m-%d"),
                gap_type=GapType.WEEKEND if diff == 2 else GapType.HOLIDAY if diff <= 5 else GapType.DATA_MISSING,
                trading_days=diff - 1,
                severity=severity
            ))
    return gaps


def align_to_trading_calendar(df: pd.DataFrame, date_col: str = "trade_date", start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """将数据对齐到交易日历"""
    if df.empty:
        return df.copy()
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    start_date = start_date or df[date_col].min()
    end_date = end_date or df[date_col].max()
    full_dates = pd.bdate_range(start=start_date, end=end_date)
    return pd.DataFrame({date_col: full_dates}).merge(df, on=date_col, how="left")


def fill_gaps(df: pd.DataFrame, date_col: str = "trade_date", strategy: FillStrategy = FillStrategy.FORWARD_FILL, max_fill_distance: int = 30) -> Tuple[pd.DataFrame, GapFillResult]:
    """填充时间序列断层"""
    if df.empty:
        return df.copy(), GapFillResult(0, 0, [], strategy)
    
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    original_rows = len(df)
    
    numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c != date_col]
    
    if strategy == FillStrategy.FORWARD_FILL:
        df[numeric_cols] = df[numeric_cols].ffill(limit=max_fill_distance)
    elif strategy == FillStrategy.BACKWARD_FILL:
        df[numeric_cols] = df[numeric_cols].bfill(limit=max_fill_distance)
    elif strategy == FillStrategy.LINEAR_INTERPOLATE:
        for col in numeric_cols:
            df[col] = df[col].interpolate(method="linear", limit=max_fill_distance)
    elif strategy == FillStrategy.NEAREST_FILL:
        for col in numeric_cols:
            df[col] = df[col].interpolate(method="nearest", limit=max_fill_distance)
    elif strategy == FillStrategy.ZERO_FILL:
        df[numeric_cols] = df[numeric_cols].fillna(0)
    
    gaps = detect_gaps(df, date_col)
    result = GapFillResult(original_rows=original_rows, filled_rows=len(df), gaps_filled=gaps, strategy_used=strategy)
    return df, result


class GapAnalyzer:
    """断层分析器"""
    def __init__(self, df: pd.DataFrame, date_col: str = "trade_date"):
        self.df = df
        self.date_col = date_col
        self._gaps: Optional[List[GapInfo]] = None
    
    def detect_all(self) -> List[GapInfo]:
        self._gaps = detect_gaps(self.df, self.date_col)
        return self._gaps
    
    def auto_fill(self) -> Tuple[pd.DataFrame, GapFillResult]:
        gaps = self._gaps or self.detect_all()
        if not gaps:
            return self.df, GapFillResult(len(self.df), len(self.df), [], FillStrategy.FORWARD_FILL)
        max_gap = max(g.trading_days for g in gaps)
        strategy = FillStrategy.FORWARD_FILL if max_gap <= 5 else FillStrategy.LINEAR_INTERPOLATE
        self.df, result = fill_gaps(self.df, self.date_col, strategy=strategy, max_fill_distance=max_gap)
        return self.df, result
    
    def generate_report(self) -> Dict:
        gaps = self._gaps or self.detect_all()
        if not gaps:
            return {"total_gaps": 0, "summary": "No gaps detected"}
        by_severity = {"minor": 0, "moderate": 0, "severe": 0}
        for g in gaps:
            by_severity[g.severity] += 1
        return {"total_gaps": len(gaps), "by_severity": by_severity, "gaps": [g.__dict__ for g in gaps]}


__all__ = ["FillStrategy", "GapType", "GapInfo", "GapFillResult", "detect_gaps", "align_to_trading_calendar", "fill_gaps", "GapAnalyzer"]
