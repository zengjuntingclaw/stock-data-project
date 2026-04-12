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

__all__ = ["RuntimeController", "RateLimitConfig", "TokenBucketThrottler"]
