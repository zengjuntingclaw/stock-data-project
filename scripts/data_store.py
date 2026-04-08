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
        """初始化数据库Schema（精简版，仅核心表）"""
        with self.pool.get_connection() as conn:
            # 股票列表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_list (
                    symbol VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    ts_code VARCHAR,
                    list_date DATE,
                    delist_date DATE,
                    industry VARCHAR,
                    market VARCHAR
                )
            """)
            
            # 日线行情
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_quotes (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    amount DOUBLE,
                    adj_factor DOUBLE,
                    is_suspended BOOLEAN,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # 财务数据
            conn.execute("""
                CREATE TABLE IF NOT EXISTS financial_data (
                    symbol VARCHAR,
                    end_date DATE,
                    ann_date DATE,
                    revenue DOUBLE,
                    net_profit DOUBLE,
                    roe DOUBLE,
                    PRIMARY KEY (symbol, end_date, ann_date)
                )
            """)
            
            # 交易日历
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_calendar (
                    date DATE PRIMARY KEY,
                    is_open BOOLEAN
                )
            """)
            
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
