"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import PipelineDataEngine, StorageManager

本文件仅用于向后兼容，将在 v4.0 完全移除。

原核心功能已迁移到 PipelineDataEngine：
  - VersionedStorage     → StorageManager (pipeline_data_engine)
  - PartitionedStorage  → StorageManager (pipeline_data_engine)
  - DataQAPipeline       → QualityEngine (pipeline_data_engine)
  - SurvivorshipBias     → PipelineDataEngine.get_active_stocks() / get_index_constituents()
  - TradingFilter        → PipelineDataEngine.validate_and_repair()
"""
import warnings
warnings.warn(
    "scripts.production_data_system 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import (
    PipelineDataEngine as ProductionDataSystem,
    StorageManager,
    QualityEngine,
)
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd
from loguru import logger

# 向后兼容别名
VersionedStorage = StorageManager
PartitionedStorage = StorageManager
DataQAPipeline = QualityEngine


def _get_now():
    """获取当前时间（支持单测 mock）。"""
    return datetime.now()


def create_production_data_system(db_path: Optional[str] = None,
                                   parquet_root: Optional[str] = None) -> ProductionDataSystem:
    """创建生产级数据系统实例（向后兼容）"""
    return ProductionDataSystem(db_path=db_path)


__all__ = [
    'ProductionDataSystem',
    'create_production_data_system',
    'VersionedStorage',
    'PartitionedStorage',
    'DataQAPipeline',
    '_get_now',
]
