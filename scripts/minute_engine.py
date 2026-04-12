"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import StorageManager, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.minute_engine 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import PipelineDataEngine
import pandas as pd
from pathlib import Path
from datetime import datetime

class MinuteDataEngine:
    """
    向后兼容的分钟行情引擎
    存储使用 Parquet 文件（分钟频率独立存储）
    """
    SUPPORTED_FREQS = ("1min", "5min", "15min", "30min", "60min")

    def __init__(self, base_dir: str = "data/minute"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._engine = PipelineDataEngine()

    def fetch_and_save(self, ts_code: str, start: str, end: str,
                       freq: str = "5min") -> dict:
        """获取并保存分钟行情"""
        if freq not in self.SUPPORTED_FREQS:
            raise ValueError(f"不支持的频率: {freq}")

        # 分钟数据通过 PipelineDataEngine 获取
        # 实际实现需要 AkShare 分钟接口
        return {
            "success": False,
            "error": "minute_engine 已废弃，请使用 pipeline_data_engine 并通过 AkShare 接口获取分钟数据"
        }

__all__ = ["MinuteDataEngine"]
