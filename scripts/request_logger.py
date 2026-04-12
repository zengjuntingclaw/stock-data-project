"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.request_logger 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import RuntimeController

class RequestLogger:
    """向后兼容的请求日志接口"""
    def __init__(self):
        self._runtime = RuntimeController()

    def log_request(self, source, method, url_or_func, params=None,
                    status="success", latency_ms=0.0, error=None, record_count=None):
        self._runtime.log_request(source, method, url_or_func, params,
                                  status, latency_ms, error, record_count)

    def get_stats(self):
        return self._runtime.get_stats()

__all__ = ["RequestLogger"]
