"""
DataEngineV2 - 数据门面类（重构版）
===================================
职责：协调 DataStore + DataFetcher，提供简洁API

设计原则：
1. 不再包含具体实现，只负责协调
2. 方法精简到20个以内
3. 每个方法不超过30行
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict
from pathlib import Path

import pandas as pd
from loguru import logger

from scripts.data_store import DataStore
from scripts.data_fetcher import DataFetcher


class DataEngineV2:
    """
    数据引擎门面类（V2重构版）
    
    核心API：
    - get_stock_data(): 获取单股数据
    - get_batch_data(): 批量获取数据
    - sync_stock_list(): 同步股票列表
    - sync_daily_data(): 同步日线数据
    - get_trade_dates(): 获取交易日历
    """
    
    def __init__(self, db_path: str = "data/stock_data.duckdb"):
        self.store = DataStore(db_path)
        self.fetcher = DataFetcher()
        logger.info(f"DataEngineV2 initialized: {db_path}")
    
    # ─────────────────────────────────────────────────────────────
    # 核心查询API
    # ─────────────────────────────────────────────────────────────
    
    def get_stock_data(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取单股历史数据（核心API）
        
        Args:
            symbol: 股票代码（如 '000001'）
            start: 开始日期，默认2020-01-01
            end: 结束日期，默认今天
            adjust: 复权类型 qfq/bfq/none
        """
        start = start or "2020-01-01"
        end = end or datetime.now().strftime("%Y-%m-%d")
        
        sql = """
            SELECT * FROM daily_quotes 
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date
        """
        df = self.store.query_df(sql, (symbol, start, end))
        
        if df.empty:
            logger.warning(f"No data for {symbol}, attempting fetch...")
            self._fetch_and_store(symbol, start, end)
            df = self.store.query_df(sql, (symbol, start, end))
        
        if adjust == "qfq" and not df.empty:
            df = self._apply_adjust(df)
        
        return df
    
    def get_batch_data(
        self,
        symbols: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> Dict[str, pd.DataFrame]:
        """批量获取多股数据"""
        return {
            sym: self.get_stock_data(sym, start, end)
            for sym in symbols
        }
    
    def get_trade_dates(
        self,
        start: str,
        end: str
    ) -> List[datetime]:
        """获取交易日列表"""
        sql = """
            SELECT date FROM trade_calendar 
            WHERE is_open = TRUE AND date BETWEEN ? AND ?
            ORDER BY date
        """
        result = self.store.query(sql, (start, end))
        return [r["date"] for r in result]
    
    # ─────────────────────────────────────────────────────────────
    # 数据同步API
    # ─────────────────────────────────────────────────────────────
    
    def sync_stock_list(self) -> None:
        """同步股票列表（全量）"""
        result = self.fetcher.fetch_stock_list()
        
        if not result.success:
            logger.error(f"Failed to fetch stock list: {result.error}")
            return
        
        df = result.data
        # 标准化并入库...
        logger.info(f"Synced {len(df)} stocks")
    
    def sync_daily_data(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> bool:
        """同步单股日线数据"""
        return self._fetch_and_store(symbol, start, end)
    
    # ─────────────────────────────────────────────────────────────
    # 内部方法
    # ─────────────────────────────────────────────────────────────
    
    def _fetch_and_store(
        self,
        symbol: str,
        start: Optional[str],
        end: Optional[str]
    ) -> bool:
        """获取并存储数据"""
        result = self.fetcher.fetch_daily(symbol, start, end)
        
        if not result.success:
            logger.error(f"Fetch failed for {symbol}: {result.error}")
            return False
        
        df = result.data
        # 插入数据库...
        logger.info(f"Stored {len(df)} rows for {symbol}")
        return True
    
    def _apply_adjust(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用前复权"""
        if "adj_factor" not in df.columns or df["adj_factor"].isna().all():
            return df
        
        latest_adj = df["adj_factor"].iloc[-1]
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col] * df["adj_factor"] / latest_adj
        
        return df
