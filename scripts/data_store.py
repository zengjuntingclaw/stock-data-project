"""
DataStore - 底层存储抽象层
============================
职责：DuckDB连接管理、Schema初始化、基础CRUD
"""
from __future__ import annotations

import os
import threading
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator

from loguru import logger

from scripts.sql_config import SCHEMA_SQL, QUERY_SQL, DML_SQL, get_sql


class ConnectionPool:
    """线程安全的DuckDB连接池（读写分离）"""
    
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.max_connections = max_connections
        self._write_pool: deque = deque()
        self._read_pool: deque = deque()
        self._pool_lock = threading.RLock()
        self._local = threading.local()
        
    @contextmanager
    def get_connection(self, read_only: bool = False) -> Generator[Any, None, None]:
        """从连接池获取连接（上下文管理器）"""
        pool = self._read_pool if read_only else self._write_pool
        conn = None
        from_pool = False
        
        with self._pool_lock:
            if pool:
                conn = pool.popleft()
                from_pool = True
        
        if conn is None:
            import duckdb
            conn = duckdb.connect(self.db_path, read_only=read_only)
        
        try:
            yield conn
        finally:
            if from_pool and len(pool) < self.max_connections:
                pool.append(conn)
            else:
                conn.close()


class DataStore:
    """
    底层数据存储抽象
    
    职责：
    1. 连接池管理
    2. Schema初始化
    3. 基础CRUD操作
    """
    
    def __init__(self, db_path: str = "data/stock_data.duckdb"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.pool = ConnectionPool(str(self.db_path))
        self._init_schema()
        
    def _init_schema(self) -> None:
        """初始化数据库Schema（使用SQL配置中心）"""
        with self.pool.get_connection() as conn:
            # 使用SQL配置中心创建核心表（使用新口径）
            for table_name in ["stock_basic_history", "daily_bar_adjusted", "daily_bar_raw",
                               "financial_data", "trade_calendar", "index_constituents_history"]:
                try:
                    sql = get_sql("schema", table_name)
                    conn.execute(sql)
                except Exception as e:
                    logger.warning(f"Failed to init schema for {table_name}: {e}")
            
            conn.commit()
            logger.info(f"Schema initialized: {self.db_path}")
    
    def execute(self, sql: str, params: Optional[tuple] = None) -> Any:
        """执行SQL"""
        with self.pool.get_connection() as conn:
            return conn.execute(sql, params)
    
    def query(self, sql: str, params: Optional[tuple] = None) -> List[Dict]:
        """查询并返回字典列表"""
        with self.pool.get_connection(read_only=True) as conn:
            result = conn.execute(sql, params).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in result]
    
    def query_df(self, sql: str, params: Optional[tuple] = None) -> "pd.DataFrame":
        """查询并返回DataFrame"""
        import pandas as pd
        with self.pool.get_connection(read_only=True) as conn:
            return conn.execute(sql, params).fetchdf()


# =============================================================================
# Parquet 分层存储
# 支持 raw / processed 两层目录，按 ts_code + trade_date 分区压缩存储。
# =============================================================================

class ParquetStorage:
    """
    Parquet 分层存储层

    目录结构：
        data/
          parquet/
            raw/          ← 原始爬取数据（含原始字段）
            processed/    ← 清洗后的标准化数据

    每个文件按 ts_code / trade_date 分区，便于 DuckDB 直接查询。
    """

    LAYER_RAW = "raw"
    LAYER_PROCESSED = "processed"

    def __init__(self, base_dir: str = "data/parquet"):
        import pandas as pd

        self.base_dir = Path(base_dir)
        self._ensure_dir(self.LAYER_RAW)
        self._ensure_dir(self.LAYER_PROCESSED)
        logger.info(f"[ParquetStorage] initialized at {self.base_dir}")

    def _ensure_dir(self, layer: str) -> Path:
        p = self.base_dir / layer
        p.mkdir(parents=True, exist_ok=True)
        return p

    # -------------------------------------------------------------------------
    # 写入
    # -------------------------------------------------------------------------

    def write(
        self,
        df: "pd.DataFrame",
        layer: str = LAYER_RAW,
        partition_cols: Optional[List[str]] = None,
        compression: str = "zstd",
        overwrite: bool = False,
    ) -> List[Path]:
        """
        将 DataFrame 写入 Parquet（按 ts_code 分区）

        Args:
            df:             待写入数据
            layer:          raw 或 processed
            partition_cols: 分区列，默认 ['ts_code']
            compression:    压缩算法，默认 zstd（压缩率高、速度快）
            overwrite:      是否覆盖同名文件
        """
        import pandas as pd

        if df.empty:
            logger.warning("[ParquetStorage] 写入空 DataFrame，跳过")
            return []

        df = df.copy()
        layer_dir = self._ensure_dir(layer)
        partition_cols = partition_cols or ["ts_code"]
        written: List[Path] = []

        # 确保分区列存在
        for col in partition_cols:
            if col not in df.columns:
                logger.warning(f"[ParquetStorage] 分区列 {col} 不存在，跳过分区写入")
                partition_cols = []

        if partition_cols:
            # 按 ts_code 分组写入（避免单文件过大）
            for ts_code, group in df.groupby(partition_cols):
                ts_str = ts_code if isinstance(ts_code, str) else str(ts_code)
                file_path = layer_dir / f"ts_code={ts_str}" / "data.parquet"
                file_path.parent.mkdir(parents=True, exist_ok=True)

                if file_path.exists() and not overwrite:
                    logger.info(f"[ParquetStorage] 跳过已存在: {file_path}")
                    continue

                group.drop(columns=partition_cols, errors="ignore").to_parquet(
                    file_path, compression=compression, index=False
                )
                written.append(file_path)
                logger.debug(f"[ParquetStorage] written {file_path} ({len(group)} rows)")
        else:
            # 不分区：直接写入单个文件
            file_path = layer_dir / f"data_{pd.Timestamp.now():%Y%m%d%H%M%S}.parquet"
            df.to_parquet(file_path, compression=compression, index=False)
            written.append(file_path)

        logger.info(f"[ParquetStorage] 写入完成，共 {len(written)} 个文件")
        return written

    def write_incremental(
        self,
        df: "pd.DataFrame",
        layer: str = LAYER_RAW,
        dedup_cols: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Path]:
        """
        增量写入：自动去重后追加到已有分区

        Args:
            dedup_cols: 去重依据列，默认 ['ts_code', 'trade_date']
        """
        import pandas as pd

        if df.empty:
            return []

        dedup_cols = dedup_cols or ["ts_code", "trade_date"]
        layer_dir = self._ensure_dir(layer)

        # 读取已有文件
        existing = self.read_partition(layer=layer, ts_codes=df["ts_code"].unique().tolist())
        if existing is not None and not existing.empty:
            # 合并去重（保留新数据）
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=dedup_cols, keep="last")

        return self.write(df, layer=layer, overwrite=True, **kwargs)

    # -------------------------------------------------------------------------
    # 读取
    # -------------------------------------------------------------------------

    def read_partition(
        self,
        layer: str = LAYER_RAW,
        ts_codes: Optional[List[str]] = None,
        date_range: Optional[tuple] = None,
    ) -> Optional["pd.DataFrame"]:
        """
        读取 Parquet 分区数据（直接传给 DuckDB 查询）

        Args:
            ts_codes:   指定股票代码列表，不传则读全量
            date_range: (start, end) 日期范围过滤
        """
        layer_dir = self._ensure_dir(layer)
        if not layer_dir.exists():
            return None

        import pandas as pd

        # 构造 DuckDB SQL 读取 Parquet（支持分区裁剪）
        # 也可直接用 pd.read_parquet，但 SQL 方式对多文件更友好
        import duckdb

        conn = duckdb.connect()
        try:
            sql = f"SELECT * FROM read_parquet('{layer_dir}/**/*.parquet')"
            filters = []

            if ts_codes:
                codes_str = ", ".join(f"'{c}'" for c in ts_codes)
                filters.append(f"ts_code IN ({codes_str})")

            if date_range:
                start, end = date_range
                filters.append(f"trade_date >= '{start}' AND trade_date <= '{end}'")

            if filters:
                sql += " WHERE " + " AND ".join(filters)

            df = conn.execute(sql).fetchdf()
            return df if not df.empty else None
        finally:
            conn.close()

    def get_partition_info(self, layer: str = LAYER_RAW) -> Dict:
        """返回各分区的行数统计（用于监控）"""
        layer_dir = self._ensure_dir(layer)
        import pandas as pd

        info: Dict = {}
        if not layer_dir.exists():
            return info

        import duckdb

        conn = duckdb.connect()
        try:
            sql = f"""
                SELECT ts_code, COUNT(*) as row_count, MIN(trade_date) as first_date, MAX(trade_date) as last_date
                FROM read_parquet('{layer_dir}/**/*.parquet')
                GROUP BY ts_code
            """
            result = conn.execute(sql).fetchdf()
            return result.to_dict("records")
        finally:
            conn.close()
