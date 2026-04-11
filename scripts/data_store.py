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
