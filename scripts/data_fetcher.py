"""
DataFetcher - 数据获取抽象层
==============================
职责：多数据源获取（TuShare/AkShare/Baostock）、指数退避重试、数据标准化
"""
from __future__ import annotations

import os
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Callable, Type

import pandas as pd
from loguru import logger

# 复用统一交易所映射函数
from scripts.exchange_mapping import build_ts_code, build_bs_code
from scripts.field_specs import standardize_df, FieldNames


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
        """转换为ts_code格式（复用统一映射，支持沪深北三交易所）"""
        return build_ts_code(symbol)
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名（使用统一的field_specs模块）"""
        # 先进行中文列名映射
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
        
        # 使用field_specs进行标准化（统一字段名、类型、默认值）
        df = standardize_df(df)
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
        """转换为Baostock代码格式（使用统一映射，支持沪深北三交易所）"""
        # 统一使用exchange_mapping模块，确保所有入口一致
        return build_bs_code(symbol)
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名（使用统一的field_specs模块）"""
        df.columns = [c.lower() for c in df.columns]
        # 使用field_specs进行标准化（统一字段名、类型、默认值）
        df = standardize_df(df)
        return df


def _fetch_with_retry(
    fetch_fn: Callable,
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True,
    **kwargs,
) -> FetchResult:
    """
    通用指数退避重试函数

    策略：delay = min(base_delay * 2^attempt + random_jitter, max_delay)
    - 第 1 次重试：~1~3s
    - 第 2 次重试：~2~5s
    - 第 3 次重试：~4~8s
    - 超过 max_delay 则封顶
    """
    last_error = ""

    for attempt in range(max_retries):
        try:
            result = fetch_fn(*args, **kwargs)
            if result.success:
                return result
            last_error = result.error or "unknown error"
        except Exception as e:
            last_error = str(e)

        if attempt < max_retries - 1:
            delay = min(base_delay * (2**attempt), max_delay)
            if jitter:
                delay += random.uniform(0, delay * 0.5)
            logger.warning(
                f"[DataFetcher] attempt {attempt + 1}/{max_retries} failed: "
                f"{last_error}, retrying in {delay:.1f}s..."
            )
            time.sleep(delay)

    logger.error(f"[DataFetcher] All {max_retries} attempts failed: {last_error}")
    return FetchResult(False, error=last_error)


class DataFetcher:
    """
    数据获取协调器

    数据源优先级（默认）：TuShare Pro > AkShare > Baostock
    支持指数退避重试 + 多源故障转移。
    """

    def __init__(
        self,
        primary: str = "tushare",
        middle: str = "akshare",
        fallback: str = "baostock",
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
    ):
        """
        Args:
            primary:   主数据源（默认 TuShare Pro）
            middle:    中间数据源（AkShare）
            fallback:  兜底数据源（Baostock）
            max_retries:   单源最大重试次数
            base_delay:    初始重试延迟（秒）
            max_delay:     最大重试延迟（秒）
        """
        self.sources: Dict[str, Optional[DataSource]] = {}
        self.primary = primary
        self.middle = middle
        self.fallback = fallback
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

        # 注册数据源（延迟初始化）
        self._lazy_registry = {
            "tushare": self._init_tushare,
            "akshare": self._init_akshare,
            "baostock": self._init_baostock,
        }
        for name in self._lazy_registry:
            self.sources[name] = None

    def _init_tushare(self) -> DataSource:
        """TuShare 需要 token，优先读环境变量"""
        from scripts.tushare_source import TuShareSource

        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            logger.warning(
                "[DataFetcher] TUSHARE_TOKEN 未设置，TuShare 源不可用，"
                "将降级到 AkShare"
            )
            return None
        return TuShareSource(token=token)

    def _init_akshare(self) -> DataSource:
        return AkShareSource()

    def _init_baostock(self) -> DataSource:
        return BaostockSource()

    def _get_source(self, name: str) -> Optional[DataSource]:
        """懒加载获取数据源实例（失败返回 None，不抛异常）"""
        if name not in self._lazy_registry:
            return None
        if self.sources.get(name) is None:
            try:
                self.sources[name] = self._lazy_registry[name]()
            except Exception as e:
                logger.warning(f"[DataFetcher] 数据源 {name} 初始化失败: {e}")
                self.sources[name] = None
        return self.sources[name]

    def fetch_daily(
        self,
        symbol: str,
        start: str,
        end: str,
        source: Optional[str] = None,
    ) -> FetchResult:
        """
        获取日线数据（带指数退避重试 + 多源故障转移）

        策略链：TuShare → AkShare → Baostock
        每个数据源独立重试，全部失败才报错。
        """
        if source:
            # 指定数据源：直接用
            src = self._get_source(source)
            if src is None:
                return FetchResult(False, error=f"数据源 {source} 不可用", source=source)
            return _fetch_with_retry(
                src.fetch_daily, symbol, start, end,
                max_retries=self.max_retries,
                base_delay=self.base_delay,
                max_delay=self.max_delay,
            )

        # 多源故障转移
        for src_name in [self.primary, self.middle, self.fallback]:
            src = self._get_source(src_name)
            if src is None:
                continue
            result = _fetch_with_retry(
                src.fetch_daily, symbol, start, end,
                max_retries=self.max_retries,
                base_delay=self.base_delay,
                max_delay=self.max_delay,
            )
            if result.success:
                return result
            logger.warning(
                f"[DataFetcher] {src_name} 全部重试失败，尝试下一个数据源"
            )

        return FetchResult(False, error="所有数据源均失败")

    def fetch_daily_adjusted(
        self,
        symbol: str,
        start: str,
        end: str,
        adjust: str = "qfq",
    ) -> FetchResult:
        """
        获取复权日线数据（仅 TuShare 支持，返回失败则降级到普通日线）
        """
        src = self._get_source("tushare")
        if src is None:
            return FetchResult(False, error="TuShare 不可用", source="tushare")
        if not hasattr(src, "fetch_daily_adjusted"):
            return FetchResult(False, error="该数据源不支持复权数据", source="tushare")
        return _fetch_with_retry(
            src.fetch_daily_adjusted, symbol, start, end, adjust,
            max_retries=self.max_retries,
            base_delay=self.base_delay,
            max_delay=self.max_delay,
        )

    def fetch_stock_list(self, source: Optional[str] = None) -> FetchResult:
        """获取股票列表（优先用 TuShare，失败则降级）"""
        if source:
            src = self._get_source(source)
            if src is None:
                return FetchResult(False, error=f"数据源 {source} 不可用", source=source)
            try:
                return src.fetch_stock_list()
            except Exception as e:
                return FetchResult(False, error=str(e), source=source)

        for src_name in [self.primary, self.middle, self.fallback]:
            src = self._get_source(src_name)
            if src is None:
                continue
            try:
                result = src.fetch_stock_list()
                if result.success:
                    return result
            except Exception as e:
                logger.warning(f"[DataFetcher] {src_name} fetch_stock_list 失败: {e}")
        return FetchResult(False, error="所有数据源获取股票列表失败")

    def fetch_trade_cal(self, start: str, end: str) -> FetchResult:
        """获取交易日历（仅 TuShare 支持）"""
        src = self._get_source("tushare")
        if src is None or not hasattr(src, "fetch_trade_cal"):
            return FetchResult(False, error="TuShare 不可用", source="tushare")
        return _fetch_with_retry(
            src.fetch_trade_cal, start, end,
            max_retries=self.max_retries,
            base_delay=self.base_delay,
            max_delay=self.max_delay,
        )
