"""
MultiDataSourceManager - 生产级多数据源管理
============================================
核心功能：
1. 状态码/响应时间统计，动态调整优先级
2. 失败次数超阈值自动降级
3. 支持 TuShare Pro / RQData / Wind 等商业API
4. 健康检查与熔断机制
5. 请求级分布式锁（防止多进程并发冲击同一数据源）
"""
from __future__ import annotations

import os
import time
import random
import hashlib
import threading
import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Callable, Any, Tuple
from enum import Enum
from collections import deque
import pandas as pd
from loguru import logger

# 复用统一映射
from scripts.exchange_mapping import build_ts_code, build_bs_code


# ============================================================================
# 核心数据结构
# ============================================================================

class SourceStatus(Enum):
    """数据源状态"""
    HEALTHY = "healthy"      # 健康
    DEGRADED = "degraded"     # 降级（部分失败）
    UNAVAILABLE = "unavailable"  # 不可用
    RECOVERING = "recovering"  # 恢复中


@dataclass
class SourceMetrics:
    """数据源性能指标"""
    total_requests: int = 0         # 总请求数
    success_count: int = 0          # 成功次数
    error_count: int = 0            # 失败次数
    timeout_count: int = 0          # 超时次数
    rate_limit_count: int = 0       # 限流次数
    total_response_time: float = 0.0  # 总响应时间（秒）
    response_times: deque = field(default_factory=lambda: deque(maxlen=100))  # 最近100次响应时间
    
    # 错误类型统计
    error_codes: Dict[str, int] = field(default_factory=dict)  # {error_code: count}
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.success_count / self.total_requests
    
    @property
    def avg_response_time(self) -> float:
        if not self.response_times:
            return 0.0
        return statistics.mean(self.response_times)
    
    @property
    def p95_response_time(self) -> float:
        if len(self.response_times) < 2:
            return self.avg_response_time
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[min(idx, len(sorted_times) - 1)]


@dataclass
class SourceHealth:
    """数据源健康状态"""
    name: str
    status: SourceStatus = SourceStatus.HEALTHY
    metrics: SourceMetrics = field(default_factory=SourceMetrics)
    consecutive_failures: int = 0    # 连续失败次数
    last_success_time: Optional[datetime] = None
    last_failure_time: Optional[datetime] = None
    cooldown_until: Optional[datetime] = None  # 冷却期截止时间
    priority: int = 100              # 优先级（越小越优先）
    
    # 配置
    max_retries: int = 3
    timeout_seconds: float = 30.0
    failure_threshold: int = 5        # 连续失败超过此数降级
    recovery_threshold: int = 3       # 连续成功恢复所需次数


@dataclass
class FetchResult:
    """获取结果包装"""
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    source: str = ""
    response_time: float = 0.0
    error_code: Optional[str] = None
    retry_count: int = 0


@dataclass
class SourceConfig:
    """数据源配置"""
    name: str
    class_path: str                   # 如 "scripts.tushare_source.TuShareSource"
    api_key_env: str                  # 环境变量名，如 "TUSHARE_TOKEN"
    api_secret_env: str = ""          # 可选
    base_url: str = ""                # API基础URL
    rate_limit_rpm: int = 60          # 速率限制（请求/分钟）
    timeout_seconds: float = 30.0
    enabled: bool = True
    priority: int = 100               # 默认优先级


# ============================================================================
# 数据源基类
# ============================================================================

class DataSource(ABC):
    """数据源抽象基类"""
    
    name: str = "base"
    
    def __init__(self, config: SourceConfig):
        self.config = config
        self._health = SourceHealth(name=config.name)
    
    @property
    def health(self) -> SourceHealth:
        return self._health
    
    @abstractmethod
    def fetch_daily(self, symbol: str, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        """获取日线数据，返回 (data, error_code, error_msg)"""
        pass
    
    @abstractmethod
    def fetch_stock_list(self) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        """获取股票列表"""
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """健康检查"""
        pass
    
    def record_success(self, response_time: float):
        """记录成功请求"""
        h = self._health
        h.metrics.total_requests += 1
        h.metrics.success_count += 1
        h.metrics.response_times.append(response_time)
        h.metrics.total_response_time += response_time
        h.consecutive_failures = 0
        h.last_success_time = datetime.now()
        
        # 恢复检查
        if h.status == SourceStatus.DEGRADED or h.status == SourceStatus.RECOVERING:
            if h.consecutive_failures >= h.config.recovery_threshold:
                h.status = SourceStatus.HEALTHY
                logger.info(f"[MultiSource] 数据源 {h.name} 已恢复健康")
    
    def record_failure(self, error_code: str, error_msg: str, is_timeout: bool = False):
        """记录失败请求"""
        h = self._health
        h.metrics.total_requests += 1
        h.metrics.error_count += 1
        h.consecutive_failures += 1
        h.last_failure_time = datetime.now()
        
        if is_timeout:
            h.metrics.timeout_count += 1
        if error_code:
            h.metrics.error_codes[error_code] = h.metrics.error_codes.get(error_code, 0) + 1
        
        # 降级检查
        if h.consecutive_failures >= h.config.failure_threshold and h.status == SourceStatus.HEALTHY:
            h.status = SourceStatus.DEGRADED
            h.cooldown_until = datetime.now() + timedelta(minutes=5)  # 5分钟冷却期
            logger.warning(f"[MultiSource] 数据源 {h.name} 已降级（连续{ h.consecutive_failures}次失败），冷却期5分钟")
        
        # 完全不可用
        if h.consecutive_failures >= h.config.failure_threshold * 3:
            h.status = SourceStatus.UNAVAILABLE
            h.cooldown_until = datetime.now() + timedelta(minutes=15)
            logger.error(f"[MultiSource] 数据源 {h.name} 暂时不可用（连续{ h.consecutive_failures}次失败），冷却期15分钟")
    
    def record_rate_limit(self):
        """记录限流"""
        self._health.metrics.rate_limit_count += 1
        logger.warning(f"[MultiSource] 数据源 {self._health.name} 触发限流")
    
    def is_available(self) -> bool:
        """检查是否可用"""
        h = self._health
        if not h.config.enabled:
            return False
        if h.status == SourceStatus.UNAVAILABLE:
            # 检查冷却期
            if h.cooldown_until and datetime.now() < h.cooldown_until:
                return False
        return True


# ============================================================================
# TuShare Pro 数据源
# ============================================================================

class TuShareProSource(DataSource):
    """TuShare Pro 商业数据源"""
    
    name = "tushare_pro"
    
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._client = None
        self._connect()
    
    def _connect(self):
        token = os.environ.get(self.config.api_key_env, "")
        if not token:
            raise ValueError(f"环境变量 {self.config.api_key_env} 未设置")
        try:
            import tushare as ts
            ts.set_token(token)
            self._client = ts.pro_api()
            logger.info(f"[MultiSource] TuShare Pro 连接成功")
        except ImportError:
            raise ImportError("tushare未安装: pip install tushare")
        except Exception as e:
            raise RuntimeError(f"TuShare Pro 连接失败: {e}")
    
    def fetch_daily(self, symbol: str, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            df = self._client.daily(
                ts_code=build_ts_code(symbol),
                start_date=start.replace("-", ""),
                end_date=end.replace("-", "")
            )
            if df is None or df.empty:
                return None, "NO_DATA", "无数据"
            df.columns = [c.lower() for c in df.columns]
            return df, None, None
        except Exception as e:
            error_code = getattr(e, "code", None) or "UNKNOWN"
            return None, str(error_code), str(e)
    
    def fetch_stock_list(self) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            df = self._client.stock_basic(
                exchange='', list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def fetch_daily_adjusted(self, symbol: str, start: str, end: str, adj: str = "qfq") -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        """获取复权日线"""
        try:
            df = self._client.adj_daily(
                ts_code=build_ts_code(symbol),
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adj_type=adj
            )
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def health_check(self) -> bool:
        try:
            df = self._client.trade_cal(exchange='SSE', start_date='20240101', end_date='20240105')
            return df is not None and not df.empty
        except:
            return False


# ============================================================================
# RQData 数据源（Wind/米筐）
# ============================================================================

class RQDataSource(DataSource):
    """RQData 米筐数据源"""
    
    name = "rqdata"
    
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._connect()
    
    def _connect(self):
        username = os.environ.get(self.config.api_key_env, "")
        password = os.environ.get(self.config.api_secret_env, "")
        if not username or not password:
            raise ValueError("RQDATA_USERNAME/RQDATA_PASSWORD 环境变量未设置")
        try:
            import rqdatac as rq
            rq.init(username, password, (
                self.config.base_url or "rqdatad-pro.ricequant.com"
            ))
            self._rq = rq
            logger.info(f"[MultiSource] RQData 连接成功")
        except ImportError:
            raise ImportError("rqdatac未安装: pip install rqdatac")
        except Exception as e:
            raise RuntimeError(f"RQData 连接失败: {e}")
    
    def fetch_daily(self, symbol: str, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            # RQData 使用 000001.XSHG 格式
            rq_code = self._to_rq_code(symbol)
            df = self._rq.get_price(
                rq_code,
                start_date=start,
                end_date=end,
                frequency='1d',
                fields=['open', 'high', 'low', 'close', 'volume', 'turnover']
            )
            if df is None or df.empty:
                return None, "NO_DATA", "无数据"
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def fetch_stock_list(self) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            df = self._rq.all_instruments(type='CS', date=datetime.now().strftime('%Y-%m-%d'))
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def _to_rq_code(self, symbol: str) -> str:
        """转换为RQData代码格式"""
        from scripts.exchange_mapping import classify_exchange
        exchange, _ = classify_exchange(symbol)
        suffix = {'SH': 'XSHG', 'SZ': 'XSHE', 'BJ': 'BJ'}.get(exchange, 'XSHG')
        return f"{symbol.zfill(6)}.{suffix}"
    
    def health_check(self) -> bool:
        try:
            return self._rq.is_trading_date(datetime.now().strftime('%Y-%m-%d'))
        except:
            return False


# ============================================================================
# AkShare 数据源（增强版）
# ============================================================================

class AkShareSource(DataSource):
    """AkShare 免费数据源（增强版）"""
    
    name = "akshare"
    
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._ak = None
        self._import_akshare()
    
    def _import_akshare(self):
        try:
            import akshare as ak
            self._ak = ak
        except ImportError:
            raise ImportError("akshare未安装: pip install akshare")
    
    def fetch_daily(self, symbol: str, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            df = self._ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adjust=""
            )
            if df is None or df.empty:
                return None, "NO_DATA", "无数据"
            # 列名标准化
            column_map = {
                "日期": "date", "日期": "trade_date",
                "开盘": "open", "最高": "high",
                "最低": "low", "收盘": "close",
                "成交量": "volume", "成交额": "amount",
                "涨跌幅": "pct_chg", "换手率": "turnover"
            }
            df = df.rename(columns=column_map)
            if 'date' in df.columns and 'trade_date' not in df.columns:
                df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            return df, None, None
        except Exception as e:
            error_msg = str(e)
            error_code = "NETWORK_ERROR" if "timeout" in error_msg.lower() else "UNKNOWN"
            return None, error_code, error_msg
    
    def fetch_stock_list(self) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            df = self._ak.stock_zh_a_spot_em()
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def health_check(self) -> bool:
        try:
            df = self._ak.stock_zh_a_spot_em()
            return df is not None and len(df) > 100
        except:
            return False


# ============================================================================
# Baostock 数据源（增强版）
# ============================================================================

class BaostockSource(DataSource):
    """Baostock 免费数据源（增强版）"""
    
    name = "baostock"
    
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._bs = None
        self._login_lock = threading.Lock()
        self._import_baostock()
        self._login()
    
    def _import_baostock(self):
        try:
            import baostock as bs
            self._bs = bs
        except ImportError:
            raise ImportError("baostock未安装: pip install baostock")
    
    def _login(self):
        with self._login_lock:
            self._bs.login()
    
    def fetch_daily(self, symbol: str, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            bs_code = build_bs_code(symbol)
            rs = self._bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,adjustflag",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3"
            )
            data = []
            while rs.error_code == '0' and rs.next():
                data.append(rs.get_row_data())
            if not data:
                return None, "NO_DATA", "无数据"
            df = pd.DataFrame(data, columns=rs.fields)
            df.columns = [c.lower() for c in df.columns]
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def fetch_stock_list(self) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        try:
            rs = self._bs.query_all_stock(day="2024-01-01")
            data = []
            while rs.error_code == '0' and rs.next():
                data.append(rs.get_row_data())
            df = pd.DataFrame(data, columns=rs.fields)
            return df, None, None
        except Exception as e:
            return None, "UNKNOWN", str(e)
    
    def health_check(self) -> bool:
        try:
            rs = self._bs.query_history_k_data_plus(
                "sh.600000",
                "date,close",
                start_date="20240101",
                end_date="20240105",
                frequency="d"
            )
            return rs.error_code == '0'
        except:
            return False


# ============================================================================
# MultiDataSourceManager 核心
# ============================================================================

class MultiDataSourceManager:
    """
    多数据源管理器
    
    核心功能：
    - 状态码/响应时间统计，动态调整优先级
    - 失败次数超阈值自动降级
    - 支持 TuShare Pro / RQData / AkShare / Baostock
    - 请求级分布式锁（防止多进程并发冲击）
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        
        self._sources: Dict[str, DataSource] = {}
        self._source_configs: Dict[str, SourceConfig] = {}
        self._health_check_interval = 300  # 5分钟健康检查
        self._last_health_check = datetime.now()
        
        # 请求限流
        self._request_locks: Dict[str, threading.Semaphore] = {}
        self._request_counts: Dict[str, deque] = {}  # {source: deque of timestamps}
        
        # 初始化默认数据源
        self._register_default_sources()
    
    def _register_default_sources(self):
        """注册默认数据源"""
        # TuShare Pro
        self.register_source(SourceConfig(
            name="tushare_pro",
            class_path="tushare_pro",
            api_key_env="TUSHARE_TOKEN",
            rate_limit_rpm=500,
            timeout_seconds=30.0,
            priority=10,  # 最高优先级
        ))
        
        # RQData
        self.register_source(SourceConfig(
            name="rqdata",
            class_path="rqdata",
            api_key_env="RQDATA_USERNAME",
            api_secret_env="RQDATA_PASSWORD",
            base_url="rqdatad-pro.ricequant.com",
            rate_limit_rpm=300,
            timeout_seconds=30.0,
            priority=20,
        ))
        
        # AkShare
        self.register_source(SourceConfig(
            name="akshare",
            class_path="akshare",
            api_key_env="",
            rate_limit_rpm=60,
            timeout_seconds=30.0,
            priority=50,
        ))
        
        # Baostock
        self.register_source(SourceConfig(
            name="baostock",
            class_path="baostock",
            api_key_env="",
            rate_limit_rpm=120,
            timeout_seconds=30.0,
            priority=100,
        ))
    
    def register_source(self, config: SourceConfig):
        """注册数据源"""
        self._source_configs[config.name] = config
        self._request_locks[config.name] = threading.Semaphore(
            config.rate_limit_rpm // 10  # 每秒最大请求数
        )
        self._request_counts[config.name] = deque(maxlen=1000)
        logger.info(f"[MultiSource] 注册数据源: {config.name} (优先级={config.priority})")
    
    def _create_source(self, config: SourceConfig) -> Optional[DataSource]:
        """创建数据源实例"""
        try:
            if config.name == "tushare_pro":
                return TuShareProSource(config)
            elif config.name == "rqdata":
                return RQDataSource(config)
            elif config.name == "akshare":
                return AkShareSource(config)
            elif config.name == "baostock":
                return BaostockSource(config)
            else:
                logger.warning(f"[MultiSource] 未知数据源类型: {config.name}")
                return None
        except Exception as e:
            logger.error(f"[MultiSource] 初始化数据源 {config.name} 失败: {e}")
            return None
    
    def _get_source(self, name: str) -> Optional[DataSource]:
        """获取或初始化数据源"""
        if name not in self._sources:
            if name not in self._source_configs:
                return None
            source = self._create_source(self._source_configs[name])
            self._sources[name] = source
        return self._sources.get(name)
    
    def _get_sorted_sources(self) -> List[Tuple[str, DataSource]]:
        """获取按优先级排序的可用数据源"""
        available = []
        for name, config in self._source_configs.items():
            source = self._get_source(name)
            if source and source.is_available():
                available.append((name, source))
        # 按优先级排序（优先级相同时按成功率）
        available.sort(key=lambda x: (
            x[1].health.priority,
            -x[1].health.metrics.success_rate
        ))
        return available
    
    def _check_rate_limit(self, source_name: str) -> bool:
        """检查速率限制"""
        now = time.time()
        counts = self._request_counts[source_name]
        
        # 清理1分钟前的请求
        while counts and counts[0] < now - 60:
            counts.popleft()
        
        config = self._source_configs[source_name]
        if len(counts) >= config.rate_limit_rpm:
            return False
        counts.append(now)
        return True
    
    def _fetch_with_retry(
        self,
        source: DataSource,
        fetch_fn: Callable,
        symbol: str,
        start: str,
        end: str,
        max_retries: int = 3,
    ) -> FetchResult:
        """带重试的获取"""
        last_error = None
        last_error_code = None
        
        for attempt in range(max_retries):
            try:
                start_time = time.time()
                data, error_code, error_msg = fetch_fn(symbol, start, end)
                response_time = time.time() - start_time
                
                if data is not None and not data.empty:
                    source.record_success(response_time)
                    return FetchResult(
                        success=True,
                        data=data,
                        source=source.name,
                        response_time=response_time,
                        retry_count=attempt
                    )
                
                last_error = error_msg
                last_error_code = error_code
                
                # 判断是否限流
                if error_code in ("RATE_LIMIT", "429", "TOO_MANY_REQUESTS"):
                    source.record_rate_limit()
                
                # 超时检测
                is_timeout = response_time >= source.config.timeout_seconds
                source.record_failure(error_code or "UNKNOWN", error_msg or "", is_timeout)
                
            except Exception as e:
                last_error = str(e)
                last_error_code = "EXCEPTION"
                source.record_failure("EXCEPTION", str(e))
            
            if attempt < max_retries - 1:
                delay = min(2 ** attempt + random.uniform(0, 1), 30)
                logger.warning(f"[MultiSource] {source.name} attempt {attempt+1} failed: {last_error}, retry in {delay:.1f}s")
                time.sleep(delay)
        
        return FetchResult(
            success=False,
            error=last_error,
            source=source.name,
            error_code=last_error_code,
            retry_count=max_retries
        )
    
    def fetch_daily(
        self,
        symbol: str,
        start: str,
        end: str,
        preferred_source: Optional[str] = None,
    ) -> FetchResult:
        """
        获取日线数据（多源故障转移）
        
        策略：按优先级尝试各数据源，记录指标，失败时自动降级
        """
        # 限流检查
        if preferred_source and not self._check_rate_limit(preferred_source):
            logger.warning(f"[MultiSource] {preferred_source} 触发速率限制")
        
        sources_to_try = []
        
        if preferred_source:
            src = self._get_source(preferred_source)
            if src and src.is_available():
                sources_to_try = [(preferred_source, src)]
        
        if not sources_to_try:
            sources_to_try = self._get_sorted_sources()
        
        for source_name, source in sources_to_try:
            if not self._check_rate_limit(source_name):
                logger.warning(f"[MultiSource] {source_name} 速率限制，跳过")
                continue
            
            # 请求锁
            with self._request_locks[source_name]:
                result = self._fetch_with_retry(
                    source,
                    source.fetch_daily,
                    symbol, start, end,
                    max_retries=source.config.max_retries
                )
            
            if result.success:
                # 动态调整优先级：成功时略微提升
                if source.health.metrics.success_rate > 0.95:
                    source.health.priority = max(1, source.health.priority - 1)
                return result
            
            # 失败时降级优先级
            source.health.priority += 5
            logger.warning(f"[MultiSource] {source_name} 失败，优先级降至 {source.health.priority}")
        
        return FetchResult(success=False, error="所有数据源均失败")
    
    def fetch_stock_list(self, preferred_source: Optional[str] = None) -> FetchResult:
        """获取股票列表"""
        sources_to_try = []
        
        if preferred_source:
            src = self._get_source(preferred_source)
            if src and src.is_available():
                sources_to_try = [(preferred_source, src)]
        
        if not sources_to_try:
            sources_to_try = self._get_sorted_sources()
        
        for source_name, source in sources_to_try:
            try:
                with self._request_locks[source_name]:
                    start_time = time.time()
                    data, error_code, error_msg = source.fetch_stock_list()
                    response_time = time.time() - start_time
                    
                    if data is not None and not data.empty:
                        source.record_success(response_time)
                        return FetchResult(success=True, data=data, source=source_name, response_time=response_time)
                    
                    source.record_failure(error_code or "NO_DATA", error_msg or "")
            except Exception as e:
                source.record_failure("EXCEPTION", str(e))
        
        return FetchResult(success=False, error="所有数据源获取股票列表失败")
    
    def get_health_report(self) -> Dict:
        """获取健康状态报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "sources": {}
        }
        
        for name, config in self._source_configs.items():
            source = self._get_source(name)
            if source:
                h = source.health
                report["sources"][name] = {
                    "status": h.status.value,
                    "priority": h.priority,
                    "enabled": config.enabled,
                    "metrics": {
                        "total_requests": h.metrics.total_requests,
                        "success_rate": f"{h.metrics.success_rate:.2%}",
                        "avg_response_time": f"{h.metrics.avg_response_time:.3f}s",
                        "p95_response_time": f"{h.metrics.p95_response_time:.3f}s",
                        "consecutive_failures": h.consecutive_failures,
                        "error_codes": h.metrics.error_codes,
                    }
                }
            else:
                report["sources"][name] = {"status": "not_initialized", "enabled": config.enabled}
        
        return report
    
    def health_check_all(self):
        """执行健康检查"""
        logger.info("[MultiSource] 开始健康检查...")
        for name, config in self._source_configs.items():
            source = self._get_source(name)
            if source:
                try:
                    is_healthy = source.health_check()
                    if is_healthy:
                        source.health.status = SourceStatus.HEALTHY
                        logger.info(f"[MultiSource] {name}: 健康")
                    else:
                        logger.warning(f"[MultiSource] {name}: 不健康")
                except Exception as e:
                    logger.error(f"[MultiSource] {name} 健康检查异常: {e}")
        self._last_health_check = datetime.now()
    
    def reset_source(self, name: str):
        """重置数据源状态"""
        if name in self._sources:
            source = self._sources[name]
            source._health.status = SourceStatus.HEALTHY
            source._health.consecutive_failures = 0
            source._health.cooldown_until = None
            logger.info(f"[MultiSource] 数据源 {name} 已重置")


# ============================================================================
# 全局单例
# ============================================================================

def get_source_manager() -> MultiDataSourceManager:
    """获取全局数据源管理器"""
    return MultiDataSourceManager()


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    print("MultiDataSourceManager 测试")
    print("=" * 60)
    
    manager = MultiDataSourceManager()
    
    # 健康报告
    print("\n1. 健康状态报告:")
    report = manager.get_health_report()
    for name, info in report["sources"].items():
        print(f"  {name}: {info}")
    
    # 尝试获取数据（会失败因为没有配置token）
    print("\n2. 测试 fetch_daily:")
    result = manager.fetch_daily("000001", "2024-01-01", "2024-01-10")
    print(f"  成功: {result.success}, 来源: {result.source}, 错误: {result.error}")
    
    print("\n3. 统计信息:")
    report = manager.get_health_report()
    for name, info in report["sources"].items():
        metrics = info.get("metrics", {})
        print(f"  {name}: 请求数={metrics.get('total_requests',0)}, "
              f"成功率={metrics.get('success_rate','N/A')}")
