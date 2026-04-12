"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import DataRouter

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.third_party_sources 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import DataRouter
from enum import Enum

class ErrorPattern(Enum):
    """错误类型枚举"""
    NETWORK_ERROR = "network_error"
    RATE_LIMIT = "rate_limit"
    FORMAT_CHANGE = "format_change"
    EMPTY_RESULT = "empty_result"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"

class FallbackStrategy:
    """备用源策略（内部使用 DataRouter）"""
    def __init__(self):
        self._router = DataRouter()

    def get_fallback_source(self, preferred: str = None) -> str:
        """返回当前可用的备用源"""
        for name, adapter in self._router._sources.items():
            if name != preferred and self._router._status.get(name) != "blocked":
                if adapter.is_available():
                    return name
        return preferred or "akshare"

class ErrorPatternDetector:
    """错误模式检测"""
    @staticmethod
    def detect(exception: Exception, source: str = None) -> ErrorPattern:
        err_str = str(exception).lower()
        if any(x in err_str for x in ["rate limit", "429", "too many"]):
            return ErrorPattern.RATE_LIMIT
        if any(x in err_str for x in ["timeout", "timed out"]):
            return ErrorPattern.TIMEOUT
        if any(x in err_str for x in ["network", "connection", "resolve"]):
            return ErrorPattern.NETWORK_ERROR
        if any(x in err_str for x in ["parse", "format", "column", "not found"]):
            return ErrorPattern.FORMAT_CHANGE
        if any(x in err_str for x in ["empty", "no data"]):
            return ErrorPattern.EMPTY_RESULT
        return ErrorPattern.UNKNOWN

__all__ = ["ErrorPattern", "FallbackStrategy", "ErrorPatternDetector"]
