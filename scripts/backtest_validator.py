"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import BacktestValidator, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.backtest_validator 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import BacktestValidator as _BV, PipelineDataEngine
from enum import Enum

class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class BacktestValidator:
    """向后兼容的回测验证器"""
    def __init__(self, price_tolerance: float = 0.001):
        self._impl = _BV(price_tolerance)

    def validate(self, positions=None, trades=None, equity_curve=None) -> dict:
        return self._impl.validate(positions, trades, equity_curve)

__all__ = ["BacktestValidator", "ValidationSeverity"]
