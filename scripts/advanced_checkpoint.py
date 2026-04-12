"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import StorageManager, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.advanced_checkpoint 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import StorageManager, PipelineDataEngine
from typing import Optional

class SyncCoordinator:
    """向后兼容的同步协调器"""
    def __init__(self, engine=None):
        self._engine = engine or PipelineDataEngine()
        self._storage = self._engine.storage

    def sync_with_checkpoint(self, ts_code: str, start: str, end: str) -> tuple:
        """同步并记录断点"""
        result = self._engine.sync_stock(ts_code, start, end)
        return (
            result.get("success", False),
            result.get("error", ""),
            result.get("records", 0)
        )

    def get_checkpoint(self, ts_code: str, table: str = "daily_bar_raw") -> Optional[str]:
        return self._storage.get_checkpoint(ts_code, table)

__all__ = ["SyncCoordinator"]
