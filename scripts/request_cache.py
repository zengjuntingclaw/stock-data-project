"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.request_cache 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import RuntimeController, _CacheEntry

class RequestCache:
    """向后兼容的缓存接口（内部使用 RuntimeController）"""
    def __init__(self):
        self._runtime = RuntimeController()

    def get(self, key):
        return self._runtime.get_cache(key)

    def set(self, key, value, ttl=None):
        self._runtime.set_cache(key, value, ttl)

    def get_or_fetch(self, key, fetch_func, ttl=None, *args, **kwargs):
        return self._runtime.get_or_fetch(key, fetch_func, ttl, *args, **kwargs)

class OfflinePlaybackMode:
    """向后兼容的离线回放接口"""
    def __init__(self):
        self._cache = RequestCache()

    def record(self, symbol, data, start, end):
        key = f"playback_{symbol}_{start}_{end}"
        self._cache.set(key, data)

    def fetch(self, symbol, start, end):
        key = f"playback_{symbol}_{start}_{end}"
        return self._cache.get(key)

__all__ = ["RequestCache", "OfflinePlaybackMode", "_CacheEntry"]
