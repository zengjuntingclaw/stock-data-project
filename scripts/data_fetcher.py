"""
DataFetcher - 数据获取抽象层
==============================
职责：多数据源获取（AkShare/Baostock）、重试机制、数据标准化
"""
from __future__ import annotations

import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Callable, Type

import pandas as pd
from loguru import logger


@dataclass
class FetchResult:
    """获取结果包装"""
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    source: str = ""


class DataSource(ABC):
    """数据源抽象基类"""
    
    name: str = ""
    
    @abstractmethod
    def fetch_daily(
        self, 
        symbol: str, 
        start: str, 
        end: str
    ) -> FetchResult:
        """获取日线数据"""
        pass
    
    @abstractmethod
    def fetch_stock_list(self) -> FetchResult:
        """获取股票列表"""
        pass


class AkShareSource(DataSource):
    """AkShare数据源"""
    
    name = "akshare"
    
    def __init__(self):
        self._import_akshare()
        
    def _import_akshare(self):
        try:
            import akshare as ak
            self.ak = ak
        except ImportError:
            raise ImportError("akshare not installed. Run: pip install akshare")
    
    def fetch_daily(
        self, 
        symbol: str, 
        start: str, 
        end: str
    ) -> FetchResult:
        """获取日线数据"""
        try:
            # 转换代码格式
            ts_code = self._to_ts_code(symbol)
            
            df = self.ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adjust=""
            )
            
            if df is None or df.empty:
                return FetchResult(False, error="No data returned", source=self.name)
            
            # 标准化列名
            df = self._standardize_columns(df)
            return FetchResult(True, data=df, source=self.name)
            
        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)
    
    def fetch_stock_list(self) -> FetchResult:
        """获取股票列表"""
        try:
            df = self.ak.stock_zh_a_spot_em()
            return FetchResult(True, data=df, source=self.name)
        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)
    
    def _to_ts_code(self, symbol: str) -> str:
        """转换为ts_code格式"""
        if symbol.startswith(("6", "5", "9")):
            return f"{symbol}.SH"
        else:
            return f"{symbol}.SZ"
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        column_map = {
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        }
        df = df.rename(columns=column_map)
        df["date"] = pd.to_datetime(df["date"])
        return df


class BaostockSource(DataSource):
    """Baostock数据源（备选）"""
    
    name = "baostock"
    
    def __init__(self):
        self._import_baostock()
        self._login()
        
    def _import_baostock(self):
        try:
            import baostock as bs
            self.bs = bs
        except ImportError:
            raise ImportError("baostock not installed. Run: pip install baostock")
    
    def _login(self):
        """登录Baostock"""
        self.bs.login()
        
    def fetch_daily(
        self, 
        symbol: str, 
        start: str, 
        end: str
    ) -> FetchResult:
        """获取日线数据"""
        try:
            bs_code = self._to_bs_code(symbol)
            
            rs = self.bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,adjustflag",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3"  # 不复权
            )
            
            data = []
            while (rs.error_code == '0') & rs.next():
                data.append(rs.get_row_data())
            
            if not data:
                return FetchResult(False, error="No data returned", source=self.name)
            
            df = pd.DataFrame(data, columns=rs.fields)
            df = self._standardize_columns(df)
            return FetchResult(True, data=df, source=self.name)
            
        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)
    
    def fetch_stock_list(self) -> FetchResult:
        """获取股票列表"""
        try:
            rs = self.bs.query_all_stock(day="2024-01-01")
            data = []
            while (rs.error_code == '0') & rs.next():
                data.append(rs.get_row_data())
            df = pd.DataFrame(data, columns=rs.fields)
            return FetchResult(True, data=df, source=self.name)
        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)
    
    def _to_bs_code(self, symbol: str) -> str:
        """转换为Baostock代码格式"""
        if symbol.startswith(("6", "5", "9")):
            return f"sh.{symbol}"
        else:
            return f"sz.{symbol}"
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        df.columns = [c.lower() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"])
        # 转换数值列
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df


class DataFetcher:
    """
    数据获取协调器
    
    职责：
    1. 多数据源管理
    2. 自动故障转移
    3. 重试机制
    4. 请求限流
    """
    
    def __init__(
        self,
        primary: str = "akshare",
        fallback: Optional[str] = "baostock",
        max_retries: int = 3,
        delay: float = 0.2
    ):
        self.sources: Dict[str, DataSource] = {}
        self.primary = primary
        self.fallback = fallback
        self.max_retries = max_retries
        self.delay = delay
        
        # 注册数据源
        self._register_source("akshare", AkShareSource)
        self._register_source("baostock", BaostockSource)
    
    def _register_source(self, name: str, cls: Type[DataSource]) -> None:
        """注册数据源（延迟初始化）"""
        self.sources[name] = None  # 延迟初始化
    
    def _get_source(self, name: str) -> DataSource:
        """获取数据源实例（懒加载）"""
        if self.sources[name] is None:
            if name == "akshare":
                self.sources[name] = AkShareSource()
            elif name == "baostock":
                self.sources[name] = BaostockSource()
        return self.sources[name]
    
    def fetch_daily(
        self,
        symbol: str,
        start: str,
        end: str,
        source: Optional[str] = None
    ) -> FetchResult:
        """
        获取日线数据（带重试和故障转移）
        
        Args:
            symbol: 股票代码
            start: 开始日期 YYYY-MM-DD
            end: 结束日期 YYYY-MM-DD
            source: 指定数据源，None则使用主备策略
        """
        sources_to_try = [source] if source else [self.primary, self.fallback]
        sources_to_try = [s for s in sources_to_try if s]
        
        for src_name in sources_to_try:
            for attempt in range(self.max_retries):
                try:
                    src = self._get_source(src_name)
                    result = src.fetch_daily(symbol, start, end)
                    
                    if result.success:
                        return result
                    
                    # 失败则重试
                    if attempt < self.max_retries - 1:
                        time.sleep(self.delay * (attempt + 1))
                        
                except Exception as e:
                    logger.warning(f"{src_name} attempt {attempt+1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.delay * (attempt + 1))
        
        return FetchResult(False, error="All sources failed")
    
    def fetch_stock_list(self, source: Optional[str] = None) -> FetchResult:
        """获取股票列表"""
        src_name = source or self.primary
        try:
            src = self._get_source(src_name)
            return src.fetch_stock_list()
        except Exception as e:
            return FetchResult(False, error=str(e))
