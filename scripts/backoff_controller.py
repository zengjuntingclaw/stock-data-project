"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.backoff_controller 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import RuntimeController

class RetryConfig:
    def __init__(self, max_retries=5, base_delay=1.0, max_delay=60.0,
                 multiplier=2.0, jitter=0.1):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter

class BackoffController:
    def __init__(self, config: RetryConfig = None):
        self._runtime = RuntimeController()
        self.config = config or RetryConfig()

class ExponentialBackoff:
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()

__all__ = ["RetryConfig", "BackoffController", "ExponentialBackoff"]
