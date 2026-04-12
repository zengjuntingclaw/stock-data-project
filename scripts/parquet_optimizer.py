"""
ParquetOptimizer - Parquet分区压缩与DuckDB优化
"""
from __future__ import annotations

import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Literal
from datetime import datetime
from loguru import logger

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


class ParquetPartitionStrategy:
    """分区策略"""
    
    @staticmethod
    def by_date_year_month(df: pd.DataFrame, date_col: str = "trade_date") -> pd.DataFrame:
        """按年/月分区"""
        df = df.copy()
        df["_year"] = pd.to_datetime(df[date_col]).dt.year
        df["_month"] = pd.to_datetime(df[date_col]).dt.month
        return df
    
    @staticmethod
    def by_stock_and_date(df: pd.DataFrame, ts_code_col: str = "ts_code", date_col: str = "trade_date") -> pd.DataFrame:
        """按股票代码和日期分区"""
        df = df.copy()
        df["_stock_prefix"] = df[ts_code_col].str[:6]
        df["_year"] = pd.to_datetime(df[date_col]).dt.year
        df["_month"] = pd.to_datetime(df[date_col]).dt.month
        return df
    
    @staticmethod
    def by_date_only(df: pd.DataFrame, date_col: str = "trade_date") -> pd.DataFrame:
        """仅按日期分区"""
        df = df.copy()
        dt = pd.to_datetime(df[date_col])
        df["_year"] = dt.dt.year
        df["_month"] = dt.dt.month
        df["_day"] = dt.dt.day
        return df


class ParquetOptimizer:
    """
    Parquet 优化器
    
    功能：
    1. 按交易日/股票代码划分 parquet 分区
    2. ZSTD 压缩
    3. DuckDB 读写优化
    
    Usage:
        optimizer = ParquetOptimizer(base_path="data/parquet")
        
        # 写入压缩数据
        optimizer.write(df, partition_by=["year", "month"])
        
        # 读取分区范围
        df = optimizer.read_range(ts_code="000001.SZ", start="2024-01-01", end="2024-01-31")
    """
    
    COMPRESSION = "zstd"
    COMPRESSION_LEVEL = 3
    
    def __init__(self, base_path: str = "data/parquet"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_by: Optional[List[str]] = None,
        compression: str = COMPRESSION,
        use_threads: bool = True
    ) -> List[Path]:
        """
        写入 Parquet 文件
        
        Args:
            df: 数据 DataFrame
            table_name: 表名
            partition_by: 分区列（如 ["year", "month"]）
            compression: 压缩算法
            use_threads: 多线程写入
        
        Returns:
            写入的文件路径列表
        """
        if df.empty:
            return []
        
        # 准备分区
        if partition_by:
            df = self._prepare_partition_columns(df, partition_by)
        
        # 输出路径
        output_path = self.base_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 写入
        if partition_by:
            pq.write_to_dataset(
                pa.table_from_pandas(df),
                root_path=str(output_path),
                partition_cols=partition_by,
                compression=compression,
                use_threads=use_threads
            )
        else:
            file_path = output_path / f"{table_name}.parquet"
            pq.write_table(
                pa.table_from_pandas(df),
                str(file_path),
                compression=compression
            )
            return [file_path]
        
        # 返回写入的文件
        return list(output_path.rglob("*.parquet"))
    
    def _prepare_partition_columns(self, df: pd.DataFrame, partition_by: List[str]) -> pd.DataFrame:
        """准备分区列"""
        df = df.copy()
        
        for col in partition_by:
            if col not in df.columns:
                if col == "_year":
                    if "trade_date" in df.columns:
                        df["_year"] = pd.to_datetime(df["trade_date"]).dt.year
                elif col == "_month":
                    if "trade_date" in df.columns:
                        df["_month"] = pd.to_datetime(df["trade_date"]).dt.month
                elif col == "_day":
                    if "trade_date" in df.columns:
                        df["_day"] = pd.to_datetime(df["trade_date"]).dt.day
        
        return df
    
    def read(
        self,
        table_name: str,
        filters: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        读取 Parquet 文件
        
        Args:
            table_name: 表名
            filters: 过滤条件 {"column": value}
        
        Returns:
            DataFrame
        """
        file_path = self.base_path / table_name
        
        if not file_path.exists():
            return pd.DataFrame()
        
        # 构建过滤器
        pq_filters = None
        if filters:
            pq_filters = [(k, "=", v) for k, v in filters.items()]
        
        # 读取
        if file_path.is_dir():
            dataset = pq.ParquetDataset(str(file_path), filters=pq_filters)
            return dataset.read().to_pandas()
        else:
            return pq.read_table(str(file_path), filters=pq_filters).to_pandas()
    
    def read_range(
        self,
        table_name: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        ts_code: Optional[str] = None,
        date_col: str = "trade_date"
    ) -> pd.DataFrame:
        """按日期范围读取"""
        filters = {}
        if ts_code:
            filters["ts_code"] = ts_code
        
        df = self.read(table_name, filters if filters else None)
        
        if df.empty or date_col not in df.columns:
            return df
        
        df[date_col] = pd.to_datetime(df[date_col])
        
        if start:
            df = df[df[date_col] >= pd.to_datetime(start)]
        if end:
            df = df[df[date_col] <= pd.to_datetime(end)]
        
        return df.sort_values(date_col)
    
    def get_stats(self, table_name: str) -> Dict:
        """获取表统计"""
        path = self.base_path / table_name
        
        if not path.exists():
            return {"exists": False, "files": 0, "size_bytes": 0}
        
        files = list(path.rglob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        
        return {
            "exists": True,
            "files": len(files),
            "size_bytes": total_size,
            "size_mb": round(total_size / 1024 / 1024, 2)
        }


class DuckDBOptimizer:
    """
    DuckDB 优化器
    
    功能：
    1. 自动检测分区范围
    2. Parquet 联合查询
    3. 物化视图缓存
    
    Usage:
        optimizer = DuckDBOptimizer(db_path="data/stock_data.duckdb")
        
        # 查询
        df = optimizer.query("SELECT * FROM daily_bar WHERE trade_date >= '2024-01-01'")
        
        # 联合查询多个 Parquet
        df = optimizer.query_parquet_join(["data/parquet/daily_1", "data/parquet/daily_5"])
    """
    
    def __init__(self, db_path: str = "data/stock_data.duckdb"):
        if not HAS_DUCKDB:
            raise ImportError("duckdb not installed")
        
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
    
    def query(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """执行 SQL 查询"""
        try:
            if params:
                return self.con.execute(sql, params).df()
            return self.con.execute(sql).df()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()
    
    def register_parquet(self, name: str, path: str):
        """注册 Parquet 文件为表"""
        self.con.execute(f"CREATE VIEW IF NOT EXISTS {name} AS SELECT * FROM read_parquet('{path}')")
    
    def create_partitioned_table(self, table_name: str, parquet_dir: str, partition_cols: List[str]):
        """创建分区表"""
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet')
        """)
        logger.info(f"Created partitioned table: {table_name}")
    
    def analyze_table(self, table_name: str):
        """分析表统计"""
        try:
            self.con.execute(f"ANALYZE {table_name}")
            logger.debug(f"Analyzed table: {table_name}")
        except Exception as e:
            logger.warning(f"ANALYZE failed for {table_name}: {e}")
    
    def vacuum(self):
        """清理数据库"""
        self.con.execute("VACUUM")
        logger.info("Database vacuumed")
    
    def close(self):
        """关闭连接"""
        self.con.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()


# ══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ══════════════════════════════════════════════════════════════════════════════

def write_compressed_parquet(df: pd.DataFrame, path: str, **kwargs) -> None:
    """写入压缩 Parquet（便捷函数）"""
    pq.write_table(pa.table_from_pandas(df), path, compression="zstd", **kwargs)


def read_compressed_parquet(path: str, **kwargs) -> pd.DataFrame:
    """读取压缩 Parquet（便捷函数）"""
    return pq.read_table(path, **kwargs).to_pandas()


__all__ = ["ParquetPartitionStrategy", "ParquetOptimizer", "DuckDBOptimizer", "write_compressed_parquet", "read_compressed_parquet"]
