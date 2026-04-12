"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import DataRouter, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.multi_source_manager 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import DataRouter, RuntimeController, PipelineDataEngine
from dataclasses import dataclass
from typing import Optional

@dataclass
class SourceResult:
    success: bool
    data = None
    error: Optional[str] = None

_manager_instance: Optional[DataRouter] = None

def get_source_manager() -> DataRouter:
    """获取全局数据源管理器（单例）"""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = DataRouter()
    return _manager_instance

class MultiDataSourceManager:
    """向后兼容的多数据源管理器"""
    def __init__(self):
        self._router = DataRouter()
        self._runtime = RuntimeController()

    def fetch_stock_list(self) -> SourceResult:
        """获取股票列表"""
        # 通过 PipelineDataEngine 获取
        try:
            engine = PipelineDataEngine()
            df = engine.get_active_stocks()
            engine.close()
            return SourceResult(success=True, data=df)
        except Exception as e:
            return SourceResult(success=False, error=str(e))

    def fetch_daily(self, ts_code: str, start: str, end: str) -> SourceResult:
        """获取日线数据"""
        try:
            engine = PipelineDataEngine()
            df = engine.get_daily_raw(ts_code, start, end)
            engine.close()
            return SourceResult(success=True, data=df)
        except Exception as e:
            return SourceResult(success=False, error=str(e))

__all__ = ["MultiDataSourceManager", "get_source_manager", "SourceResult"]
