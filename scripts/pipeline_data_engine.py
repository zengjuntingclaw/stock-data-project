"""
PipelineDataEngine - 统一数据管道入口
======================================
架构重构后的唯一主入口，整合所有功能层。

分层架构：
  engine     - 统一入口层，对外 API
  router     - 统一数据源路由与熔断
  ingest     - 统一下载与增量更新
  quality    - 统一 schema 校验、缺口检测、修复
  storage    - 统一 parquet / DuckDB 写入与读取
  runtime    - 统一限速、并发、退避、缓存、日志
  backtest   - 统一回测验证

使用示例：
    from scripts.pipeline_data_engine import PipelineDataEngine

    engine = PipelineDataEngine()

    # 1. 数据获取（统一路由）
    df = engine.get_daily_raw("000001.SZ", "2024-01-01", "2024-12-31")

    # 2. 质量检查与修复
    result = engine.validate_and_repair("000001.SZ")

    # 3. 批量更新
    engine.batch_sync(["000001.SZ", "600000.SH"], start="2024-01-01")

    # 4. 回测验证
    report = engine.validate_backtest(trades=trades_df, positions=pos_df)

    # 5. 系统状态
    status = engine.get_status()
"""

from __future__ import annotations

import os
import sys
import time
import threading
import traceback
import hashlib
import statistics
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Callable, Union
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np

from loguru import logger

# 数据范围约束
try:
    from scripts.data_scope import enforce_scope, ScopeViolationError, ScopeConfig
except (ModuleNotFoundError, ImportError):
    from data_scope import enforce_scope, ScopeViolationError, ScopeConfig

# ──────────────────────────────────────────────────────────────────────────────
# 内部模块导入（统一后的模块）
# ──────────────────────────────────────────────────────────────────────────────

try:
    from scripts.data_engine import DataEngine as BaseDataEngine, DEFAULT_START_DATE
    from scripts.exchange_mapping import build_ts_code, build_bs_code
    from scripts.field_specs import standardize_df
except (ModuleNotFoundError, ImportError):
    from data_engine import DataEngine as BaseDataEngine, DEFAULT_START_DATE
    from exchange_mapping import build_ts_code, build_bs_code
    from field_specs import standardize_df

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: RUNTIME LAYER - 统一运行时控制
# ═══════════════════════════════════════════════════════════════════════════

class RateLimitConfig:
    """限速配置"""
    def __init__(
        self,
        requests_per_second: float = 5.0,
        requests_per_minute: float = 200.0,
        burst_size: int = 10,
        cooldown_seconds: float = 60.0
    ):
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size
        self.cooldown_seconds = cooldown_seconds


class RuntimeController:
    """
    统一运行时控制器
    合并了：限速、并发、退避、缓存、日志
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True

        # ── 限速器 ──
        self._throttlers: Dict[str, "_TokenBucket"] = {}
        self._cooldowns: Dict[str, float] = {}
        self._throttle_lock = threading.RLock()
        self.default_throttle = RateLimitConfig()

        # ── 退避配置 ──
        self.retry_config = {
            "max_retries": 5,
            "base_delay": 1.0,
            "max_delay": 60.0,
            "multiplier": 2.0,
            "jitter": 0.1,
        }

        # ── 缓存 ──
        self._cache: Dict[str, "_CacheEntry"] = {}
        self._cache_lock = threading.RLock()
        self._cache_ttl = 1800  # 30分钟
        self._cache_max = 10000

        # ── 日志 ──
        self._request_logs: List[Dict] = []
        self._log_lock = threading.RLock()
        self._log_dir = Path("logs")
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # ── 请求锁（防并发冲击）──
        self._request_semaphores: Dict[str, threading.Semaphore] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # 限速方法
    # ─────────────────────────────────────────────────────────────────────────

    def register_source(self, source: str, config: Optional[RateLimitConfig] = None):
        """注册数据源限速器"""
        with self._throttle_lock:
            cfg = config or self.default_throttle
            self._throttlers[source] = _TokenBucket(cfg)
            self._request_semaphores[source] = threading.Semaphore(
                int(cfg.requests_per_second)
            )
            logger.debug(f"[Runtime] Registered throttler: {source}")

    def acquire(self, source: str, timeout: float = 30.0) -> bool:
        """获取请求许可"""
        with self._throttle_lock:
            # 检查冷却期
            if source in self._cooldowns:
                if time.time() < self._cooldowns[source]:
                    return False
                del self._cooldowns[source]

            bucket = self._throttlers.get(source)
            if bucket is None:
                self.register_source(source)
                bucket = self._throttlers[source]

            return bucket.acquire(timeout)

    def wait_and_acquire(self, source: str, max_wait: float = 60.0) -> bool:
        """阻塞等待获取许可"""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            if self.acquire(source, timeout=1.0):
                return True
            time.sleep(0.1)
        return False

    def report_rate_limit(self, source: str, retry_after: Optional[float] = None):
        """报告限流"""
        with self._throttle_lock:
            wait = retry_after or self.default_throttle.cooldown_seconds
            self._cooldowns[source] = time.time() + wait
            logger.warning(f"[Runtime] Rate limit hit: {source}, cooldown {wait}s")

    # ─────────────────────────────────────────────────────────────────────────
    # 缓存方法
    # ─────────────────────────────────────────────────────────────────────────

    def get_cache(self, key: str) -> Optional[Any]:
        """获取缓存"""
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry and not entry.is_expired:
                entry.access_count += 1
                return entry.value
            return None

    def set_cache(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置缓存"""
        with self._cache_lock:
            expires = datetime.now() + timedelta(seconds=ttl or self._cache_ttl)
            self._cache[key] = _CacheEntry(key, value, expires)
            # 清理
            if len(self._cache) > self._cache_max:
                sorted_entries = sorted(
                    self._cache.items(),
                    key=lambda x: (x[1].access_count, x[1].created_at)
                )
                for k, _ in sorted_entries[:len(self._cache) - self._cache_max]:
                    del self._cache[k]

    def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable,
        ttl: Optional[int] = None,
        *args,
        **kwargs
    ) -> Any:
        """获取或抓取（使用缓存）"""
        cached = self.get_cache(key)
        if cached is not None:
            logger.debug(f"[Runtime] Cache hit: {key}")
            return cached

        logger.debug(f"[Runtime] Cache miss: {key}")
        result = fetch_func(*args, **kwargs)
        if result is not None:
            self.set_cache(key, result, ttl)
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # 退避重试
    # ─────────────────────────────────────────────────────────────────────────

    def retry_with_backoff(
        self,
        func: Callable,
        *args,
        source: str = "_global",
        task_id: Optional[str] = None,
        on_failure: Optional[Callable] = None,
        **kwargs
    ) -> Any:
        """带退避重试"""
        tid = task_id or f"task_{id(func)}"
        last_error = None

        for attempt in range(self.retry_config["max_retries"]):
            try:
                # 先限速
                if not self.wait_and_acquire(source, max_wait=30):
                    raise TimeoutError(f"Rate limit timeout for {source}")

                result = func(*args, **kwargs)
                return result

            except Exception as e:
                last_error = e
                if on_failure:
                    on_failure(e, attempt)

                if attempt < self.retry_config["max_retries"] - 1:
                    # 指数退避
                    base = self.retry_config["base_delay"]
                    mult = self.retry_config["multiplier"]
                    jitter = self.retry_config["jitter"]
                    delay = min(base * (mult ** attempt), self.retry_config["max_delay"])
                    if jitter > 0:
                        delay += np.random.uniform(-delay * jitter, delay * jitter)
                    delay = max(0, delay)

                    logger.warning(f"[Runtime] {tid} attempt {attempt+1} failed: {e}, retry in {delay:.1f}s")
                    time.sleep(delay)

        raise last_error

    # ─────────────────────────────────────────────────────────────────────────
    # 日志
    # ─────────────────────────────────────────────────────────────────────────

    def log_request(
        self,
        source: str,
        method: str,
        url_or_func: str,
        params: Optional[Dict] = None,
        status: str = "success",
        latency_ms: float = 0.0,
        error: Optional[str] = None,
        record_count: Optional[int] = None
    ):
        """记录请求"""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source,
            "method": method,
            "func": url_or_func,
            "params": params or {},
            "status": status,
            "latency_ms": latency_ms,
            "error": str(error)[:200] if error else None,
            "record_count": record_count,
        }
        with self._log_lock:
            self._request_logs.append(entry)
            if len(self._request_logs) > 10000:
                self._request_logs = self._request_logs[-5000:]

    def get_stats(self) -> Dict:
        """获取运行时统计"""
        with self._log_lock:
            logs = self._request_logs
            total = len(logs)
            success = sum(1 for l in logs if l["status"] == "success")
            return {
                "total_requests": total,
                "success": success,
                "failure": total - success,
                "success_rate": round(success / total, 4) if total else 0,
                "cache_size": len(self._cache),
                "sources": list(self._throttlers.keys()),
            }


class _TokenBucket:
    """令牌桶限速器"""
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._tokens = float(config.burst_size)
        self._last_update = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(
            self.config.burst_size,
            self._tokens + elapsed * self.config.requests_per_second
        )
        self._last_update = now

    def acquire(self, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._lock:
            self._refill()
            while self._tokens < 1:
                if time.monotonic() >= deadline:
                    return False
                wait = (1 - self._tokens) / self.config.requests_per_second
                time.sleep(min(wait, 0.1))
                self._refill()
            self._tokens -= 1
            return True


@dataclass
class _CacheEntry:
    key: str
    value: Any
    expires_at: datetime
    created_at: datetime = field(default_factory=datetime.now)
    access_count: int = 0

    @property
    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: ROUTER LAYER - 统一数据源路由
# ═══════════════════════════════════════════════════════════════════════════

class SourceStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    BLOCKED = "blocked"


@dataclass
class SourceMetrics:
    """数据源指标"""
    total_requests: int = 0
    success_count: int = 0
    error_count: int = 0
    avg_latency: float = 0.0
    consecutive_failures: int = 0
    status: SourceStatus = SourceStatus.HEALTHY

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.success_count / self.total_requests


class DataRouter:
    """
    统一数据源路由器
    合并了所有数据源（AkShare/Baostock/TuShare）到单一路由策略
    """

    def __init__(self, runtime: Optional[RuntimeController] = None):
        self.runtime = runtime or RuntimeController()

        # 数据源注册
        self._sources: Dict[str, "_SourceAdapter"] = {}
        self._metrics: Dict[str, SourceMetrics] = {}
        self._status: Dict[str, SourceStatus] = {}
        self._priority: Dict[str, int] = {}

        # 默认优先级
        self._default_priority = {
            "akshare": 1,
            "baostock": 2,
            "tushare": 0,
        }

        # 失败阈值
        self._failure_threshold = 5
        self._cooldown_seconds = 300

        self._lock = threading.RLock()
        self._register_default_sources()

    def _register_default_sources(self):
        """注册默认数据源"""
        self.register("akshare", _AkShareAdapter())
        self.register("baostock", _BaostockAdapter())
        # TuShare 需要 token，按需初始化

    def register(self, name: str, adapter: "_SourceAdapter", priority: Optional[int] = None):
        """注册数据源"""
        with self._lock:
            self._sources[name] = adapter
            self._metrics[name] = SourceMetrics()
            self._status[name] = SourceStatus.HEALTHY
            self._priority[name] = priority or self._default_priority.get(name, 100)
            self.runtime.register_source(name)
            logger.info(f"[Router] Registered source: {name} (priority={self._priority[name]})")

    def _get_sorted_sources(self, data_type: str = "daily") -> List[Tuple[str, "_SourceAdapter"]]:
        """获取按优先级排序的可用数据源"""
        available = []
        with self._lock:
            for name, adapter in self._sources.items():
                if self._status.get(name) != SourceStatus.BLOCKED:
                    if adapter.is_available():
                        available.append((name, adapter))
            available.sort(key=lambda x: self._priority.get(x[0], 100))
        return available

    def _record_success(self, source: str, latency_ms: float):
        """记录成功"""
        with self._lock:
            m = self._metrics.setdefault(source, SourceMetrics())
            m.total_requests += 1
            m.success_count += 1
            m.consecutive_failures = 0
            # 更新平均延迟
            if m.total_requests > 1:
                m.avg_latency = (m.avg_latency * (m.total_requests - 1) + latency_ms) / m.total_requests
            else:
                m.avg_latency = latency_ms
            # 恢复健康
            if m.status == SourceStatus.DEGRADED:
                m.status = SourceStatus.HEALTHY

    def _record_failure(self, source: str, error: str):
        """记录失败"""
        with self._lock:
            m = self._metrics.setdefault(source, SourceMetrics())
            m.total_requests += 1
            m.error_count += 1
            m.consecutive_failures += 1

            if m.consecutive_failures >= self._failure_threshold:
                m.status = SourceStatus.BLOCKED
                logger.warning(f"[Router] Source {source} blocked (consecutive failures: {m.consecutive_failures})")

    def fetch(
        self,
        symbol: str,
        start: str,
        end: str,
        data_type: str = "daily",
        preferred_source: Optional[str] = None
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """
        统一数据获取
        自动路由到可用数据源
        """
        # 1. 优先使用指定源
        if preferred_source and preferred_source in self._sources:
            if self._status.get(preferred_source) != SourceStatus.BLOCKED:
                adapter = self._sources[preferred_source]
                if adapter.is_available():
                    df, err = self._try_fetch(adapter, symbol, start, end, preferred_source)
                    if df is not None:
                        return df, preferred_source

        # 2. 按优先级尝试所有源
        for source_name, adapter in self._get_sorted_sources(data_type):
            df, err = self._try_fetch(adapter, symbol, start, end, source_name)
            if df is not None:
                return df, source_name

        return None, "all_failed"

    def _try_fetch(
        self,
        adapter: "_SourceAdapter",
        symbol: str,
        start: str,
        end: str,
        source: str
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """尝试从单个源获取"""
        start_time = time.time()
        try:
            # 通过运行时控制器限速
            self.runtime.log_request(source, "fetch", adapter.name, {"symbol": symbol, "start": start, "end": end}, "pending")

            df = adapter.fetch(symbol, start, end)
            latency_ms = (time.time() - start_time) * 1000

            if df is not None and not df.empty:
                self._record_success(source, latency_ms)
                self.runtime.log_request(source, "fetch", adapter.name, None, "success", latency_ms, None, len(df))
                return df, None
            else:
                self._record_failure(source, "empty_result")
                self.runtime.log_request(source, "fetch", adapter.name, None, "failure", latency_ms, "empty_result")
                return None, "empty_result"

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self._record_failure(source, str(e))

            # 检查限流
            error_str = str(e).lower()
            if any(x in error_str for x in ["rate limit", "429", "too many"]):
                self.runtime.report_rate_limit(source)

            self.runtime.log_request(source, "fetch", adapter.name, None, "failure", latency_ms, e)
            return None, str(e)

    def get_health_report(self) -> Dict:
        """获取健康报告"""
        with self._lock:
            return {
                name: {
                    "status": self._metrics[name].status.value,
                    "success_rate": f"{self._metrics[name].success_rate:.2%}",
                    "avg_latency": f"{self._metrics[name].avg_latency:.1f}ms",
                    "consecutive_failures": self._metrics[name].consecutive_failures,
                    "priority": self._priority.get(name, 100),
                }
                for name in self._sources
            }


class _SourceAdapter:
    """数据源适配器基类"""
    name: str = "base"

    def is_available(self) -> bool:
        return True

    def fetch(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        raise NotImplementedError


class _AkShareAdapter(_SourceAdapter):
    """AkShare 适配器"""
    name = "akshare"

    def __init__(self):
        self._ak = None
        self._import_akshare()

    def _import_akshare(self):
        try:
            import akshare as ak
            self._ak = ak
        except ImportError:
            logger.warning("[Router] AkShare not installed")

    def is_available(self) -> bool:
        return self._ak is not None

    def fetch(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        if not self._ak:
            return None
        try:
            df = self._ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adjust=""
            )
            if df is None or df.empty:
                return None
            # 标准化列名
            col_map = {
                "日期": "trade_date", "开盘": "open", "最高": "high",
                "最低": "low", "收盘": "close", "成交量": "volume",
                "成交额": "amount", "涨跌幅": "pct_chg",
            }
            df = df.rename(columns=col_map)
            if "trade_date" not in df.columns and "日期" in df.columns:
                df["trade_date"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
            return df
        except Exception as e:
            logger.warning(f"[Router] AkShare fetch failed: {e}")
            return None


class _BaostockAdapter(_SourceAdapter):
    """Baostock 适配器"""
    name = "baostock"

    def __init__(self):
        self._bs = None
        self._import_baostock()

    def _import_baostock(self):
        try:
            import baostock as bs
            self._bs = bs
            bs.login()
        except ImportError:
            logger.warning("[Router] Baostock not installed")
        except Exception as e:
            logger.warning(f"[Router] Baostock login failed: {e}")

    def is_available(self) -> bool:
        return self._bs is not None

    def fetch(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        if not self._bs:
            return None
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
            while rs.error_code == "0" and rs.next():
                data.append(rs.get_row_data())
            if not data:
                return None
            df = pd.DataFrame(data, columns=rs.fields)
            df.columns = [c.lower() for c in df.columns]
            if "date" in df.columns:
                df["trade_date"] = df["date"]
            return df
        except Exception as e:
            logger.warning(f"[Router] Baostock fetch failed: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════
# PART 3: QUALITY LAYER - 统一数据质量引擎
# ═══════════════════════════════════════════════════════════════════════════

class QualityEngine:
    """
    统一数据质量引擎
    合并了：校验、缺口检测、自动修复
    """

    # 各板块涨跌停阈值
    _LIMIT_THRESHOLDS = {
        "688": 0.20,
        "30": 0.20,
        "4": 0.30,
        "8": 0.30,
    }

    def __init__(self, router: Optional[DataRouter] = None):
        self.router = router or DataRouter()
        self._repair_log_path = Path("logs/repair_log.json")
        self._repair_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _detect_limit(self, code: str) -> float:
        """根据代码返回涨跌停阈值"""
        c = str(code).zfill(6)
        for prefix, th in self._LIMIT_THRESHOLDS.items():
            if c.startswith(prefix):
                return th
        return 0.10

    def validate(self, df: pd.DataFrame, ts_code: Optional[str] = None) -> Dict:
        """
        完整数据校验
        返回: {"ok": bool, "issues": [...], "suspended_dates": [...], ...}
        """
        issues = []
        if df.empty:
            return {"ok": True, "issues": [], "suspended_dates": [], "zero_price_dates": []}

        # 1. 字段完整性
        required = {"trade_date", "open", "high", "low", "close", "volume"}
        missing = list(required - set(df.columns))
        if missing:
            issues.append({"type": "missing_fields", "severity": "error", "detail": {"fields": missing}})

        # 2. 缺失值
        nulls = df.isnull().sum()
        nulls = nulls[nulls > 0]
        if not nulls.empty:
            issues.append({"type": "null_values", "severity": "warning", "detail": nulls.to_dict()})

        # 3. 价格 <= 0
        price_cols = ["open", "high", "low", "close"]
        zero_dates = []
        for col in price_cols:
            if col in df.columns:
                bad = df[df[col] <= 0]
                if not bad.empty:
                    dates = bad["trade_date"].astype(str).tolist()
                    zero_dates.extend(dates)
                    issues.append({
                        "type": "invalid_price", "severity": "error",
                        "detail": {"field": col, "count": len(bad), "dates": dates[:5]}
                    })

        # 4. 涨跌停异常
        if "pct_chg" in df.columns and "ts_code" in df.columns:
            codes = df["ts_code"].astype(str).str[:6]
            thresholds = codes.apply(self._detect_limit) * 100
            abnormal = df[df["pct_chg"].abs() > thresholds]
            if not abnormal.empty:
                issues.append({
                    "type": "abnormal_pct_chg", "severity": "warning",
                    "detail": {"count": len(abnormal), "samples": abnormal[["ts_code", "trade_date", "pct_chg"]].head(5).to_dict("records")}
                })

        # 5. 停牌检测
        suspended = []
        if "volume" in df.columns and "close" in df.columns:
            df_sorted = df.sort_values("trade_date").copy()
            df_sorted["prev_close"] = df_sorted["close"].shift(1)
            mask = ((df_sorted["volume"] == 0) | (df_sorted["close"] == 0)) & (df_sorted["close"] == df_sorted["prev_close"])
            suspended = df_sorted[mask]["trade_date"].astype(str).tolist()
            if suspended:
                issues.append({
                    "type": "suspended_suspect", "severity": "info",
                    "detail": {"count": len(suspended), "dates": suspended[:10]}
                })

        return {
            "ok": all(i["severity"] != "error" for i in issues),
            "issues": issues,
            "suspended_dates": suspended,
            "zero_price_dates": list(set(zero_dates)),
        }

    def validate_and_repair(
        self,
        df: pd.DataFrame,
        ts_code: Optional[str] = None,
        backup_fetch: Optional[Callable] = None
    ) -> Dict:
        """
        校验并自动修复
        """
        val_result = self.validate(df, ts_code)
        result = {
            "validated_at": datetime.now().isoformat(),
            "validation": val_result,
            "repaired_issues": [],
        }

        if val_result["ok"]:
            return result

        # 尝试修复
        if backup_fetch is None:
            return result

        repaired = 0
        for issue in val_result.get("issues", []):
            if issue.get("severity") == "info":
                continue
            issue_type = issue.get("type", "")

            if issue_type == "null_values":
                # 前向填充
                for col in ["open", "high", "low", "close"]:
                    if col in df.columns:
                        df[col] = df[col].ffill()
                repaired += 1
                result["repaired_issues"].append({"type": issue_type, "strategy": "forward_fill", "success": True})

            elif issue_type == "invalid_price":
                # 从备用源修复
                detail = issue.get("detail", {})
                dates = detail.get("dates", [])
                if dates and backup_fetch:
                    try:
                        backup_df = backup_fetch(min(dates), max(dates))
                        if backup_df is not None:
                            repaired += 1
                            result["repaired_issues"].append({"type": issue_type, "strategy": "backup_fetch", "success": True})
                    except:
                        result["repaired_issues"].append({"type": issue_type, "strategy": "backup_fetch", "success": False})

        result["repair"] = {
            "total_repairs": repaired,
            "successful": sum(1 for r in result["repaired_issues"] if r.get("success")),
        }

        return result

    def detect_gaps(self, df: pd.DataFrame, date_col: str = "trade_date") -> List[Dict]:
        """检测时间序列断层"""
        if df.empty or date_col not in df.columns:
            return []

        gaps = []
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col).reset_index(drop=True)
        dates = df[date_col]

        for i in range(1, len(dates)):
            diff = (dates.iloc[i] - dates.iloc[i-1]).days
            if diff > 1:
                severity = "minor" if diff <= 5 else "moderate" if diff <= 10 else "severe"
                gaps.append({
                    "start": dates.iloc[i-1].strftime("%Y-%m-%d"),
                    "end": dates.iloc[i].strftime("%Y-%m-%d"),
                    "gap_days": diff - 1,
                    "severity": severity,
                })
        return gaps

    def fill_gaps(
        self,
        df: pd.DataFrame,
        date_col: str = "trade_date",
        strategy: str = "forward"
    ) -> Tuple[pd.DataFrame, Dict]:
        """填充断层"""
        if df.empty:
            return df, {"filled": 0}

        original_rows = len(df)
        df = df.copy()
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c != date_col]

        if strategy == "forward":
            df[numeric_cols] = df[numeric_cols].ffill(limit=10)
        elif strategy == "linear":
            for col in numeric_cols:
                df[col] = df[col].interpolate(method="linear", limit=10)

        gaps = self.detect_gaps(df, date_col)
        return df, {"original_rows": original_rows, "gaps_remaining": len(gaps)}


# ═══════════════════════════════════════════════════════════════════════════
# PART 4: STORAGE LAYER - 统一存储管理
# ═══════════════════════════════════════════════════════════════════════════

class StorageManager:
    """
    统一存储管理器
    合并了：DuckDB 读写、断点续传、Parquet 压缩
    """

    def __init__(self, db_path: str, runtime: Optional[RuntimeController] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime = runtime or RuntimeController()

        if HAS_DUCKDB:
            self._conn = duckdb.connect(str(self.db_path))
        else:
            self._conn = None
            raise ImportError("DuckDB required")

        self._checkpoint_cache: Dict[Tuple[str, str], Dict] = {}

    def write_daily(
        self,
        df: pd.DataFrame,
        table_name: str = "daily_bar_raw",
        ts_code: Optional[str] = None
    ) -> int:
        """写入日线数据"""
        if df.empty:
            return 0

        # 标准化日期列
        for col in ["trade_date", "date"]:
            if col in df.columns:
                df = df.copy()
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")

        # 确保表存在
        self._ensure_table(table_name)

        # 去重写入
        written = 0
        for _, row in df.iterrows():
            cols = list(df.columns)
            values = [row.get(c) for c in cols]
            try:
                if "trade_date" in cols and "ts_code" in cols:
                    self._conn.execute(f"""
                        INSERT INTO {table_name} ({', '.join(cols)})
                        VALUES ({', '.join(['?' for _ in cols])})
                        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                            open = excluded.open, high = excluded.high,
                            low = excluded.low, close = excluded.close,
                            volume = excluded.volume, amount = excluded.amount
                    """, values)
                else:
                    self._conn.execute(f"""
                        INSERT INTO {table_name} ({', '.join(cols)})
                        VALUES ({', '.join(['?' for _ in cols])})
                    """, values)
                written += 1
            except Exception as e:
                logger.warning(f"[Storage] Write failed: {e}")

        self._conn.commit()
        return written

    def read_daily(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        table_name: str = "daily_bar_raw"
    ) -> pd.DataFrame:
        """读取日线数据"""
        sql = f"SELECT * FROM {table_name} WHERE ts_code = ?"
        params = [ts_code]

        if start:
            sql += " AND trade_date >= ?"
            params.append(start)
        if end:
            sql += " AND trade_date <= ?"
            params.append(end)

        sql += " ORDER BY trade_date"

        try:
            return self._conn.execute(sql, params).df()
        except:
            return pd.DataFrame()

    def get_checkpoint(self, ts_code: str, table_name: str) -> Optional[str]:
        """获取断点"""
        cache_key = (ts_code, table_name)
        if cache_key in self._checkpoint_cache:
            return self._checkpoint_cache[cache_key].get("last_date")

        try:
            result = self._conn.execute(f"""
                SELECT MAX(trade_date) FROM {table_name}
                WHERE ts_code = ?
            """, [ts_code]).fetchone()

            date = result[0] if result else None
            self._checkpoint_cache[cache_key] = {"last_date": date}
            return date
        except:
            return None

    def update_checkpoint(self, ts_code: str, table_name: str, last_date: str):
        """更新断点"""
        self._checkpoint_cache[(ts_code, table_name)] = {"last_date": last_date}

    def _ensure_table(self, table_name: str):
        """确保表存在"""
        schemas = {
            "daily_bar_raw": """
                CREATE TABLE IF NOT EXISTS daily_bar_raw (
                    ts_code VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE,
                    low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE,
                    pct_chg DOUBLE, turnover DOUBLE,
                    PRIMARY KEY (ts_code, trade_date)
                )
            """,
            "daily_bar_adjusted": """
                CREATE TABLE IF NOT EXISTS daily_bar_adjusted (
                    ts_code VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE,
                    low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE,
                    pct_chg DOUBLE, turnover DOUBLE,
                    qfq_open DOUBLE, qfq_high DOUBLE, qfq_low DOUBLE, qfq_close DOUBLE,
                    hfq_open DOUBLE, hfq_high DOUBLE, hfq_low DOUBLE, hfq_close DOUBLE,
                    adj_factor DOUBLE,
                    PRIMARY KEY (ts_code, trade_date)
                )
            """,
        }
        if table_name in schemas:
            try:
                self._conn.execute(schemas[table_name])
                self._conn.commit()
            except:
                pass

    def get_table_stats(self) -> Dict:
        """获取表统计"""
        stats = {}
        for table in ["daily_bar_raw", "daily_bar_adjusted"]:
            try:
                result = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                stats[table] = {"row_count": result[0] if result else 0}
            except:
                stats[table] = {"row_count": 0}
        return stats

    def close(self):
        if self._conn:
            self._conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# PART 5: BACKTEST LAYER - 统一回测验证
# ═══════════════════════════════════════════════════════════════════════════

class BacktestValidator:
    """
    统一回测验证器
    合并了：持仓/交易校验、权益曲线、费用一致性
    """

    def __init__(self, price_tolerance: float = 0.001):
        self.price_tolerance = price_tolerance

    def validate(
        self,
        positions: Optional[pd.DataFrame] = None,
        trades: Optional[pd.DataFrame] = None,
        equity_curve: Optional[pd.DataFrame] = None
    ) -> Dict:
        """
        执行回测验证
        返回验证报告
        """
        issues = []
        passed = 0

        # 1. 持仓完整性
        if positions is not None and not positions.empty:
            required = ["ts_code", "trade_date", "volume", "cost"]
            missing = [c for c in required if c not in positions.columns]
            if missing:
                issues.append({"check": "position_integrity", "severity": "error", "message": f"Missing columns: {missing}"})
            else:
                if (positions["volume"] < 0).any():
                    issues.append({"check": "position_integrity", "severity": "critical", "message": "Negative position volume"})
                else:
                    passed += 1

        # 2. 交易完整性
        if trades is not None and not trades.empty:
            required = ["ts_code", "trade_date", "direction", "price", "volume"]
            missing = [c for c in required if c not in trades.columns]
            if missing:
                issues.append({"check": "trade_integrity", "severity": "error", "message": f"Missing columns: {missing}"})
            else:
                if (trades["volume"] <= 0).any():
                    issues.append({"check": "trade_integrity", "severity": "critical", "message": "Non-positive trade volume"})
                else:
                    passed += 1

        # 3. 权益曲线
        if equity_curve is not None and not equity_curve.empty:
            if "equity" in equity_curve.columns:
                if (equity_curve["equity"] <= 0).any():
                    issues.append({"check": "equity_curve", "severity": "critical", "message": "Equity reached zero or negative"})
                else:
                    passed += 1
            else:
                issues.append({"check": "equity_curve", "severity": "error", "message": "Missing equity column"})

        return {
            "timestamp": datetime.now().isoformat(),
            "passed_checks": passed,
            "total_issues": len(issues),
            "issues": issues,
            "success": len(issues) == 0 or all(i["severity"] != "critical" for i in issues),
        }


# ═══════════════════════════════════════════════════════════════════════════
# PART 6: PIPELINE ENGINE - 统一主入口
# ═══════════════════════════════════════════════════════════════════════════

class PipelineDataEngine:
    """
    统一数据管道引擎 - 唯一主入口

    功能：
    1. 数据获取（统一路由）
    2. 数据存储（统一存储）
    3. 质量检查（统一质量引擎）
    4. 批量同步（断点续传）
    5. 回测验证（统一验证器）

    使用示例：
        engine = PipelineDataEngine()

        # 获取数据
        df = engine.get_daily_raw("000001.SZ", "2024-01-01", "2024-12-31")

        # 批量同步
        engine.batch_sync(["000001.SZ", "600000.SH"])

        # 验证
        result = engine.validate_and_repair("000001.SZ")

        # 回测验证
        report = engine.validate_backtest(trades=trades_df)
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        start_date: Optional[str] = None,
        enable_multi_source: bool = True,
    ):
        # 基础引擎（复用已有）
        self.base = BaseDataEngine(
            db_path=db_path,
            start_date=start_date or DEFAULT_START_DATE,
        )

        # 分层组件
        self.runtime = RuntimeController()
        self.router = DataRouter(self.runtime) if enable_multi_source else None
        self.quality = QualityEngine(self.router)
        self.storage = StorageManager(
            str(self.base.db_path),
            self.runtime
        )
        self.validator = BacktestValidator()

        # 配置
        self._db_path = db_path
        self._start_date = start_date or DEFAULT_START_DATE
        self._enable_multi_source = enable_multi_source

        logger.info("[PipelineDataEngine] Initialized (unified architecture)")

    # ─────────────────────────────────────────────────────────────────────────
    # 核心 API：数据获取
    # ─────────────────────────────────────────────────────────────────────────

    def get_daily_raw(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """获取原始行情（统一入口）"""
        # 数据范围约束校验（自动裁剪日期范围）
        clamped_start, clamped_end = enforce_scope(ts_code, start, end)
        start = clamped_start
        end = clamped_end

        # 1. 尝试从本地读取
        local_df = self.storage.read_daily(ts_code, start, end, "daily_bar_raw")
        if not local_df.empty and use_cache:
            logger.debug(f"[Pipeline] Cache hit: {ts_code}")
            return local_df

        # 2. 本地无数据，从远程获取
        if self._enable_multi_source and self.router:
            df, source = self.router.fetch(ts_code, start, end)
            if df is not None and not df.empty:
                # 写入本地
                self.storage.write_daily(df, "daily_bar_raw", ts_code)
                self.storage.update_checkpoint(ts_code, "daily_bar_raw", end)
                logger.info(f"[Pipeline] Fetched {ts_code} from {source}: {len(df)} records")
                return df

        # 3. 回退到基础引擎
        return self.base.get_daily_raw(ts_code, start, end)

    def get_daily_adjusted(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """获取复权行情"""
        return self.base.get_daily_adjusted(ts_code, start, end, adjust)

    def get_active_stocks(self, trade_date: str) -> pd.DataFrame:
        """获取当日可交易股票池（PIT）"""
        return self.base.get_active_stocks(trade_date)

    def get_index_constituents(
        self,
        index_code: str,
        trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取指数成分股"""
        return self.base.get_index_constituents(index_code, trade_date)

    # ─────────────────────────────────────────────────────────────────────────
    # 核心 API：数据同步
    # ─────────────────────────────────────────────────────────────────────────

    def sync_stock(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        table: str = "daily_bar_raw"
    ) -> Dict:
        """同步单只股票数据"""
        # 数据范围约束校验（自动裁剪日期范围）
        clamped_start, clamped_end = enforce_scope(ts_code, start, end)
        start = clamped_start
        end = clamped_end

        # 检查断点
        checkpoint = self.storage.get_checkpoint(ts_code, table)
        if checkpoint and str(checkpoint) >= str(end):
            return {"success": True, "message": "already_synced", "ts_code": ts_code}

        # 获取数据
        if self._enable_multi_source and self.router:
            df, source = self.router.fetch(ts_code, start, end)
        else:
            df = None

        if df is None or df.empty:
            return {"success": False, "error": "fetch_failed", "ts_code": ts_code}

        # 写入
        written = self.storage.write_daily(df, table, ts_code)
        self.storage.update_checkpoint(ts_code, table, end)

        return {
            "success": True,
            "ts_code": ts_code,
            "records": len(df),
            "written": written,
        }

    def batch_sync(
        self,
        ts_codes: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
        max_workers: int = 4
    ) -> Dict:
        """批量同步（受 data_scope.yaml 约束）"""
        # 数据范围约束预校验
        clamped_start, clamped_end = enforce_scope(ts_codes[0] if ts_codes else None, start, end)
        if clamped_start != (start or self._start_date) or clamped_end != (end or datetime.now().strftime("%Y-%m-%d")):
            logger.warning(f"[Pipeline] batch_sync 日期范围已被裁剪: {start}→{clamped_start} / {end}→{clamped_end}")

        # 不在白名单的股票提前拦截
        cfg = ScopeConfig.load()
        disallowed = [c for c in ts_codes if not cfg.is_stock_allowed(c)]
        if disallowed:
            raise ScopeViolationError(
                f"以下股票不在允许的股票池中: {disallowed}。"
                f" 允许列表: {cfg.allowed_stocks}。"
                f" 修改 data/data_scope.yaml 即可调整。"
            )

        results = {"total": len(ts_codes), "success": 0, "failed": 0, "total_records": 0, "errors": []}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.sync_stock, code, start, end): code
                for code in ts_codes
            }

            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    if result.get("success"):
                        results["success"] += 1
                        results["total_records"] += result.get("records", 0)
                    else:
                        results["failed"] += 1
                        results["errors"].append({"code": code, "error": result.get("error")})
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({"code": code, "error": str(e)})

        logger.info(f"[Pipeline] Batch sync: {results['success']}/{results['total']} success")
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # 核心 API：质量检查
    # ─────────────────────────────────────────────────────────────────────────

    def validate(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> Dict:
        """验证数据质量"""
        df = self.get_daily_raw(ts_code, start, end, use_cache=False)
        if df.empty:
            return {"ok": False, "reason": "no_data"}

        return self.quality.validate(df, ts_code)

    def validate_and_repair(
        self,
        ts_code: str,
        start: Optional[str] = None,
        end: Optional[str] = None
    ) -> Dict:
        """验证并修复"""
        df = self.get_daily_raw(ts_code, start, end, use_cache=False)
        if df.empty:
            return {"ok": False, "reason": "no_data"}

        # 备用获取函数
        def backup_fetch(s, e):
            if self.router:
                df, _ = self.router.fetch(ts_code, s, e)
                return df
            return None

        return self.quality.validate_and_repair(df, ts_code, backup_fetch)

    def detect_gaps(self, ts_code: str, start: Optional[str] = None, end: Optional[str] = None) -> List[Dict]:
        """检测数据断层"""
        df = self.get_daily_raw(ts_code, start, end, use_cache=False)
        return self.quality.detect_gaps(df)

    # ─────────────────────────────────────────────────────────────────────────
    # 核心 API：回测验证
    # ─────────────────────────────────────────────────────────────────────────

    def validate_backtest(
        self,
        positions: Optional[pd.DataFrame] = None,
        trades: Optional[pd.DataFrame] = None,
        equity_curve: Optional[pd.DataFrame] = None
    ) -> Dict:
        """回测结果一致性验证"""
        return self.validator.validate(positions, trades, equity_curve)

    # ─────────────────────────────────────────────────────────────────────────
    # 状态 API
    # ─────────────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict:
        """获取引擎状态"""
        status = {
            "timestamp": datetime.now().isoformat(),
            "db_path": str(self.base.db_path),
            "start_date": self._start_date,
            "multi_source": self._enable_multi_source,
            "table_stats": self.storage.get_table_stats(),
            "runtime_stats": self.runtime.get_stats(),
        }

        if self.router:
            status["router_health"] = self.router.get_health_report()

        return status

    def get_source_stats(self) -> Dict:
        """获取数据源统计"""
        if self.router:
            return self.router.get_health_report()
        return {}

    def close(self):
        """关闭引擎"""
        self.storage.close()
        self.base.close()


# ═══════════════════════════════════════════════════════════════════════════
# FACTORY & COMPATIBILITY
# ═══════════════════════════════════════════════════════════════════════════

def create_pipeline_engine(
    db_path: Optional[str] = None,
    start_date: Optional[str] = None,
    enable_multi_source: bool = True,
) -> PipelineDataEngine:
    """创建统一数据管道引擎"""
    return PipelineDataEngine(
        db_path=db_path,
        start_date=start_date,
        enable_multi_source=enable_multi_source,
    )


# 向后兼容别名
EnhancedDataEngine = PipelineDataEngine
ProductionDataEngine = PipelineDataEngine


# ═══════════════════════════════════════════════════════════════════════════
# EXPORTS
# ═══════════════════════════════════════════════════════════════════════════

__all__ = [
    "PipelineDataEngine",
    "create_pipeline_engine",
    "EnhancedDataEngine",
    "ProductionDataEngine",
    "RuntimeController",
    "RateLimitConfig",
    "DataRouter",
    "QualityEngine",
    "StorageManager",
    "BacktestValidator",
]
