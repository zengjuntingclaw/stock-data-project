"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import StorageManager, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.parquet_optimizer 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import StorageManager
import pandas as pd
from pathlib import Path

class ParquetOptimizer:
    """
    向后兼容的 Parquet 优化器
    DuckDB 内部已使用 Parquet 列式存储
    """
    def __init__(self, db_path: str = "data/stock_data.duckdb"):
        self._storage = StorageManager(db_path)

    def optimize_table(self, table_name: str) -> dict:
        """优化表（内部使用 DuckDB 列式存储）"""
        stats = self._storage.get_table_stats()
        return {"optimized": True, "table": table_name, "stats": stats.get(table_name, {})}

    def write_parquet(self, df: pd.DataFrame, path: str, partition_by: str = None):
        """导出为 Parquet"""
        if df.empty:
            return
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    def read_parquet(self, path: str) -> pd.DataFrame:
        """读取 Parquet"""
        return pd.read_parquet(path)

__all__ = ["ParquetOptimizer"]
