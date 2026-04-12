"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import QualityEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.time_series_gaps 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import QualityEngine
import pandas as pd
from enum import Enum

class GapType(Enum):
    """断层类型枚举"""
    TRADING_DAY = "trading_day"      # 非交易日
    SUSPENDED = "suspended"           # 停牌
    DELISTED = "delisted"            # 退市
    MINOR = "minor"                  # 小缺口 (<=5天)
    MODERATE = "moderate"            # 中等缺口 (6-10天)
    SEVERE = "severe"                # 严重缺口 (>10天)

class FillStrategy(Enum):
    """填充策略枚举"""
    FORWARD = "forward"              # 前向填充
    LINEAR = "linear"               # 线性插值
    DROP = "drop"                   # 删除记录

def detect_gaps(df: pd.DataFrame, date_col: str = "trade_date") -> list:
    """检测时间序列缺口（兼容旧接口）"""
    qe = QualityEngine()
    return qe.detect_gaps(df, date_col)

def fill_gaps(df: pd.DataFrame, date_col: str = "trade_date",
              strategy: str = "forward") -> tuple:
    """填充时间序列缺口（兼容旧接口）"""
    qe = QualityEngine()
    return qe.fill_gaps(df, date_col, strategy)

class GapAnalyzer:
    """向后兼容的缺口分析器"""
    def __init__(self, df: pd.DataFrame, date_col: str = "trade_date"):
        self.df = df
        self.date_col = date_col
        self._quality = QualityEngine()

    def detect_all(self) -> list:
        return self._quality.detect_gaps(self.df, self.date_col)

    def auto_fill(self, strategy="forward") -> tuple:
        return self._quality.fill_gaps(self.df, self.date_col, strategy)

    def generate_report(self) -> dict:
        gaps = self.detect_all()
        return {
            "total_gaps": len(gaps),
            "gaps": gaps,
            "has_severe": any(g["severity"] == "severe" for g in gaps),
        }

__all__ = ["GapType", "FillStrategy", "detect_gaps", "fill_gaps", "GapAnalyzer"]
