"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import PipelineDataEngine, QualityEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.enhanced_data_engine 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import PipelineDataEngine, QualityEngine, RuntimeController
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime
import pandas as pd

# 向后兼容别名
EnhancedDataEngine = PipelineDataEngine

class DownloadRecord:
    """下载记录"""
    def __init__(self, ts_code: str, start: str, end: str,
                 record_count: int, success: bool, error: str = None):
        self.ts_code = ts_code
        self.start = start
        self.end = end
        self.record_count = record_count
        self.success = success
        self.error = error
        self.timestamp = datetime.now()

class EnhancedDownloader:
    """向后兼容的增强下载器"""
    def __init__(self, engine=None):
        self._engine = engine or PipelineDataEngine()
        self._records: List[DownloadRecord] = []

    def download(self, ts_code: str, start: str, end: str) -> DownloadRecord:
        result = self._engine.sync_stock(ts_code, start, end)
        rec = DownloadRecord(
            ts_code=ts_code, start=start, end=end,
            record_count=result.get("records", 0),
            success=result.get("success", False),
            error=result.get("error", "")
        )
        self._records.append(rec)
        return rec

class EnhancedValidator:
    """向后兼容的增强验证器"""
    def __init__(self):
        self._quality = QualityEngine()

    def validate(self, df: pd.DataFrame, ts_code: str = None) -> dict:
        return self._quality.validate(df, ts_code)

    def validate_and_fix(self, df: pd.DataFrame) -> pd.DataFrame:
        result = self._quality.validate_and_repair(df)
        return df  # 修复后的 DataFrame（原地修改）

__all__ = [
    "EnhancedDataEngine", "EnhancedDownloader", "EnhancedValidator",
    "DownloadRecord"
]
