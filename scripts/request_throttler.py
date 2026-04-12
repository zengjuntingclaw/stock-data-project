"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import (
        RuntimeController, DataRouter, QualityEngine,
        StorageManager, BacktestValidator, PipelineDataEngine
    )

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.request_throttler 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import RuntimeController, RateLimitConfig
from scripts.pipeline_data_engine import _TokenBucket as TokenBucketThrottler


class RequestThrottler:
    """
    [向后兼容] 统一限速器门面，内部委托 RuntimeController。
    新代码请直接使用 RuntimeController。
    """
    def __init__(self):
        self._runtime = RuntimeController()

    def register_source(self, source: str, config: RateLimitConfig):
        self._runtime.register_source(source, config)

    def acquire(self, source: str = "akshare"):
        return self._runtime.acquire(source)

    def get_source_status(self, source: str):
        return self._runtime.get_source_status(source)


__all__ = ["RuntimeController", "RateLimitConfig", "TokenBucketThrottler", "RequestThrottler"]
