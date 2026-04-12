"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import DataRouter, SourceStatus, SourceMetrics

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.baostock_enhanced 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import DataRouter, SourceStatus, SourceMetrics
from enum import Enum

SourceHealth = SourceStatus  # 别名兼容

class BaostockCircuitBreaker:
    """
    向后兼容的 Baostock 熔断器
    内部使用 DataRouter 的熔断机制
    """
    def __init__(self, failure_threshold=5, block_duration=300):
        self.failure_threshold = failure_threshold
        self.block_duration = block_duration
        self._router = DataRouter()

    def is_blocked(self, source="baostock") -> bool:
        return self._router._status.get(source) == SourceStatus.BLOCKED

    def record_success(self, latency_ms: float, source="baostock"):
        self._router._record_success(source, latency_ms)

    def record_failure(self, error="", is_empty=False, source="baostock"):
        self._router._record_failure(source, error)

__all__ = ["BaostockCircuitBreaker", "SourceStats", "SourceHealth", "SourceMetrics"]
