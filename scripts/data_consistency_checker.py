"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import QualityEngine, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.data_consistency_checker 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import QualityEngine
import pandas as pd

class DataConsistencyChecker:
    """向后兼容的数据一致性检查器"""
    def __init__(self):
        self._quality = QualityEngine()

    def check(self, df: pd.DataFrame, ts_code: str = None) -> dict:
        return self._quality.validate(df, ts_code)

__all__ = ["DataConsistencyChecker"]
