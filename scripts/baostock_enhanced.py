"""
BaostockEnhanced - Baostock 数据源稳定性增强
============================================
功能：
  1. 健康检查：预请求测试连接和字段完整性
  2. 驱逐逻辑：连续失败/空数据时自动暂停
  3. 熔断器模式：保护性暂停机制
"""

from __future__ import annotations

import time
import threading
import statistics
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from loguru import logger

try:
    import baostock as bs
    HAS_BAOSTOCK = True
except ImportError:
    HAS_BAOSTOCK = False
    bs = None


class SourceHealth(Enum):
    """数据源健康状态"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    SUSPECTED = "suspected"
    BLOCKED = "blocked"


@dataclass
class HealthCheckResult:
    """健康检查结果"""
    is_healthy: bool
    latency_ms: float
    field_count: int
    expected_fields: int
    error_message: Optional[str] = None
    checked_at: datetime = field(default_factory=datetime.now)


@dataclass
class SourceStats:
    """数据源统计"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    empty_results: int = 0
    consecutive_failures: int = 0
    consecutive_empty: int = 0
    total_latency_ms: float = 0.0
    latencies: List[float] = field(default_factory=list)
    health: SourceHealth = SourceHealth.HEALTHY
    
    # 驱逐相关
    blocked_until: Optional[datetime] = None
    block_reason: Optional[str] = None
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests
    
    @property
    def avg_latency(self) -> float:
        if not self.latencies:
            return 0.0
        return statistics.mean(self.latencies)
    
    @property
    def p95_latency(self) -> float:
        if len(self.latencies) < 2:
            return 0.0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
    
    def record_success(self, latency_ms: float) -> None:
        self.total_requests += 1
        self.successful_requests += 1
        self.consecutive_failures = 0
        self.consecutive_empty = 0
        self.total_latency_ms += latency_ms
        self.latencies.append(latency_ms)
        if len(self.latencies) > 100:
            self.latencies = self.latencies[-100:]
    
    def record_failure(self, is_empty: bool = False) -> None:
        self.total_requests += 1
        self.failed_requests += 1
        self.consecutive_failures += 1
        if is_empty:
            self.consecutive_empty += 1
        else:
            self.consecutive_empty = 0
    
    def to_dict(self) -> Dict:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "empty_results": self.empty_results,
            "success_rate": round(self.success_rate, 4),
            "avg_latency_ms": round(self.avg_latency, 2),
            "p95_latency_ms": round(self.p95_latency, 2),
            "consecutive_failures": self.consecutive_failures,
            "consecutive_empty": self.consecutive_empty,
            "health": self.health.value,
            "blocked_until": self.blocked_until.isoformat() if self.blocked_until else None,
            "block_reason": self.block_reason
        }


class BaostockHealthChecker:
    """
    Baostock 健康检查器
    
    Usage:
        checker = BaostockHealthChecker()
        result = checker.check_health()
        if not result.is_healthy:
            logger.warning(f"Baostock unhealthy: {result.error_message}")
    """
    
    # 健康检查使用的测试股票
    TEST_STOCKS = ["sh.600000", "sz.000001"]
    
    # 期望的日线字段
    EXPECTED_FIELDS = [
        "date", "code", "open", "high", "low", "close",
        "preclose", "volume", "amount", "adjustflag",
        "turn", "tradestatus", "pctChg", "isST"
    ]
    
    def __init__(self):
        self._lock = threading.RLock()
        self._last_check: Optional[HealthCheckResult] = None
        self._check_count = 0
    
    def check_health(self, test_stock: Optional[str] = None) -> HealthCheckResult:
        """
        执行健康检查
        
        Args:
            test_stock: 测试用股票代码（格式如 "sh.600000"）
        
        Returns:
            HealthCheckResult
        """
        if not HAS_BAOSTOCK:
            return HealthCheckResult(
                is_healthy=False,
                latency_ms=0,
                field_count=0,
                expected_fields=len(self.EXPECTED_FIELDS),
                error_message="baostock not installed"
            )
        
        stock = test_stock or self.TEST_STOCKS[0]
        start_time = time.time()
        
        try:
            # 确保登录
            lg = bs.login()
            if lg.error_code != "0":
                return HealthCheckResult(
                    is_healthy=False,
                    latency_ms=(time.time() - start_time) * 1000,
                    field_count=0,
                    expected_fields=len(self.EXPECTED_FIELDS),
                    error_message=f"Login failed: {lg.error_msg}"
                )
            
            # 获取日线数据
            rs = bs.query_history_k_data_plus(
                stock,
                ",".join(self.EXPECTED_FIELDS),
                start_date="2024-01-01",
                end_date="2024-01-05",
                frequency="d",
                adjustflag="3"
            )
            
            latency_ms = (time.time() - start_time) * 1000
            bs.logout()
            
            # 检查结果
            if rs.error_code != "0":
                return HealthCheckResult(
                    is_healthy=False,
                    latency_ms=latency_ms,
                    field_count=0,
                    expected_fields=len(self.EXPECTED_FIELDS),
                    error_message=f"Query failed: {rs.error_msg}"
                )
            
            # 检查字段
            field_count = len(rs.fields) if hasattr(rs, "fields") else 0
            
            # 获取数据验证非空
            has_data = False
            while rs.next():
                row = rs.get_row_data()
                if row and any(row):
                    has_data = True
                    break
            
            if not has_data:
                return HealthCheckResult(
                    is_healthy=False,
                    latency_ms=latency_ms,
                    field_count=field_count,
                    expected_fields=len(self.EXPECTED_FIELDS),
                    error_message="No data returned"
                )
            
            result = HealthCheckResult(
                is_healthy=True,
                latency_ms=latency_ms,
                field_count=field_count,
                expected_fields=len(self.EXPECTED_FIELDS),
                error_message=None
            )
            
        except Exception as e:
            result = HealthCheckResult(
                is_healthy=False,
                latency_ms=(time.time() - start_time) * 1000,
                field_count=0,
                expected_fields=len(self.EXPECTED_FIELDS),
                error_message=str(e)
            )
        
        with self._lock:
            self._last_check = result
            self._check_count += 1
        
        return result
    
    def check_field_integrity(self, rs) -> tuple[bool, int, int]:
        """
        检查返回数据的字段完整性
        
        Returns:
            (is_valid, field_count, expected_count)
        """
        if not hasattr(rs, "fields"):
            return False, 0, len(self.EXPECTED_FIELDS)
        
        field_count = len(rs.fields)
        expected = len(self.EXPECTED_FIELDS)
        
        # 检查必需字段
        required = {"date", "code", "open", "high", "low", "close"}
        present = set(rs.fields)
        missing = required - present
        
        if missing:
            logger.warning(f"Baostock missing required fields: {missing}")
            return False, field_count, expected
        
        return True, field_count, expected
    
    @property
    def last_result(self) -> Optional[HealthCheckResult]:
        return self._last_check
    
    @property
    def check_count(self) -> int:
        return self._check_count


class BaostockCircuitBreaker:
    """
    Baostock 熔断器
    
    当连续失败达到阈值时，自动暂停 Baostock 使用。
    
    Usage:
        breaker = BaostockCircuitBreaker(
            failure_threshold=5,      # 连续5次失败
            empty_threshold=3,        # 连续3次空数据
            block_duration=300        # 暂停5分钟
        )
        
        if breaker.is_blocked():
            return备用源
        
        try:
            data = baostock_fetch(...)
            breaker.record_success()
        except Exception as e:
            breaker.record_failure(is_empty=is_empty_data(e))
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        empty_threshold: int = 3,
        block_duration: int = 300,  # 秒
        suspicion_threshold: int = 3,
        suspicion_decay: int = 180  # 秒
    ):
        self.failure_threshold = failure_threshold
        self.empty_threshold = empty_threshold
        self.block_duration = block_duration
        self.suspicion_threshold = suspicion_threshold
        self.suspicion_decay = suspicion_decay
        
        self._lock = threading.RLock()
        self._stats = SourceStats()
        self._last_failure_time: Optional[datetime] = None
        self._suspicion_score = 0
        self._last_activity: Optional[datetime] = None
    
    def is_blocked(self) -> bool:
        """检查是否被熔断"""
        with self._lock:
            if self._stats.blocked_until is None:
                return False
            
            if datetime.now() < self._stats.blocked_until:
                return True
            
            # 阻塞已过期，解除
            self._stats.blocked_until = None
            self._stats.block_reason = None
            logger.info("Baostock circuit breaker reset")
            return False
    
    def is_suspected(self) -> bool:
        """检查是否可疑"""
        with self._lock:
            if self.is_blocked():
                return True
            
            # 连续失败达到怀疑阈值
            if self._stats.consecutive_failures >= self.suspicion_threshold:
                return True
            
            # 嫌疑分超过阈值
            if self._suspicion_score >= self.suspicion_threshold:
                return True
            
            return False
    
    def record_success(self, latency_ms: float) -> None:
        """记录成功"""
        with self._lock:
            self._stats.record_success(latency_ms)
            self._suspicion_score = max(0, self._suspicion_score - 1)
            self._last_activity = datetime.now()
            self._update_health()
    
    def record_failure(self, is_empty: bool = False) -> None:
        """记录失败"""
        with self._lock:
            self._stats.record_failure(is_empty)
            self._last_failure_time = datetime.now()
            
            if is_empty:
                self._suspicion_score += 2  # 空数据嫌疑加倍
            else:
                self._suspicion_score += 1
            
            self._last_activity = datetime.now()
            self._check_circuit()
            self._update_health()
    
    def _check_circuit(self) -> None:
        """检查是否触发熔断"""
        # 连续失败超阈值
        if self._stats.consecutive_failures >= self.failure_threshold:
            self._trigger_block(f"Consecutive failures: {self._stats.consecutive_failures}")
            return
        
        # 连续空数据超阈值
        if self._stats.consecutive_empty >= self.empty_threshold:
            self._trigger_block(f"Consecutive empty results: {self._stats.consecutive_empty}")
            return
    
    def _trigger_block(self, reason: str) -> None:
        """触发熔断"""
        self._stats.blocked_until = datetime.now() + timedelta(seconds=self.block_duration)
        self._stats.block_reason = reason
        self._stats.health = SourceHealth.BLOCKED
        logger.warning(f"Baostock circuit breaker BLOCKED: {reason}. "
                     f"Will retry after {self._stats.blocked_until}")
    
    def _update_health(self) -> None:
        """更新健康状态"""
        if self._stats.blocked_until:
            self._stats.health = SourceHealth.BLOCKED
        elif self._suspicion_score >= self.suspicion_threshold:
            self._stats.health = SourceHealth.SUSPECTED
        elif self._stats.success_rate < 0.8:
            self._stats.health = SourceHealth.DEGRADED
        else:
            self._stats.health = SourceHealth.HEALTHY
    
    def force_block(self, reason: str, duration: Optional[int] = None) -> None:
        """强制熔断"""
        with self._lock:
            dur = duration or self.block_duration
            self._stats.blocked_until = datetime.now() + timedelta(seconds=dur)
            self._stats.block_reason = reason
            self._stats.health = SourceHealth.BLOCKED
            logger.warning(f"Baostock force blocked: {reason}")
    
    def force_unblock(self) -> None:
        """强制解除熔断"""
        with self._lock:
            self._stats.blocked_until = None
            self._stats.block_reason = None
            self._stats.consecutive_failures = 0
            self._stats.consecutive_empty = 0
            self._suspicion_score = 0
            self._stats.health = SourceHealth.HEALTHY
            logger.info("Baostock force unblocked")
    
    def reset(self) -> None:
        """重置熔断器"""
        with self._lock:
            self._stats = SourceStats()
            self._suspicion_score = 0
            self._last_failure_time = None
            self._last_activity = None
    
    @property
    def stats(self) -> SourceStats:
        return self._stats.copy() if hasattr(self._stats, "copy") else self._stats
    
    def get_status(self) -> Dict:
        """获取状态"""
        with self._lock:
            return {
                **self._stats.to_dict(),
                "is_blocked": self.is_blocked(),
                "is_suspected": self.is_suspected(),
                "suspicion_score": self._suspicion_score,
                "block_duration_remaining": (
                    (self._stats.blocked_until - datetime.now()).total_seconds()
                    if self._stats.blocked_until and datetime.now() < self._stats.blocked_until
                    else 0
                )
            }


class BaostockEnhancedManager:
    """
    增强版 Baostock 管理器
    
    整合健康检查和熔断器，提供稳定的 Baostock 访问。
    
    Usage:
        manager = BaostockEnhancedManager()
        
        if not manager.can_use():
            return备用源
        
        try:
            data = manager.fetch_with_protection(fetch_func, *args, **kwargs)
        except Exception as e:
            manager.record_failure(is_empty=is_empty_error(e))
    """
    
    def __init__(
        self,
        health_check_interval: int = 300,  # 5分钟检查一次
        auto_recovery: bool = True
    ):
        self.health_checker = BaostockHealthChecker()
        self.circuit_breaker = BaostockCircuitBreaker()
        self.health_check_interval = health_check_interval
        self.auto_recovery = auto_recovery
        
        self._lock = threading.RLock()
        self._last_health_check: Optional[datetime] = None
        self._enabled = True
    
    def can_use(self) -> bool:
        """是否可以使用的 Baostock"""
        with self._lock:
            if not self._enabled:
                return False
            
            if self.circuit_breaker.is_blocked():
                return False
            
            # 检查是否需要健康检查
            if self._should_health_check():
                self._run_health_check()
            
            return not self.circuit_breaker.is_blocked()
    
    def _should_health_check(self) -> bool:
        """是否应该运行健康检查"""
        if self._last_health_check is None:
            return True
        
        elapsed = (datetime.now() - self._last_health_check).total_seconds()
        return elapsed >= self.health_check_interval
    
    def _run_health_check(self) -> None:
        """运行健康检查"""
        try:
            result = self.health_checker.check_health()
            self._last_health_check = datetime.now()
            
            if result.is_healthy:
                self.circuit_breaker.force_unblock()
                logger.debug(f"Baostock health check OK (latency: {result.latency_ms:.1f}ms)")
            else:
                self.circuit_breaker._suspicion_score += 2
                logger.warning(f"Baostock health check FAILED: {result.error_message}")
                
        except Exception as e:
            logger.error(f"Health check error: {e}")
    
    def fetch_with_protection(
        self,
        fetch_func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        带保护的数据获取
        
        自动处理：
        1. 登录/登出
        2. 健康检查
        3. 错误处理
        """
        if not self.can_use():
            raise RuntimeError("Baostock is currently unavailable (circuit breaker open)")
        
        start_time = time.time()
        
        try:
            if HAS_BAOSTOCK:
                lg = bs.login()
                if lg.error_code != "0":
                    raise ConnectionError(f"Baostock login failed: {lg.error_msg}")
            
            # 执行请求
            result = fetch_func(*args, **kwargs)
            
            latency_ms = (time.time() - start_time) * 1000
            self.record_success(latency_ms)
            
            return result
            
        finally:
            if HAS_BAOSTOCK:
                try:
                    bs.logout()
                except:
                    pass
    
    def record_success(self, latency_ms: float) -> None:
        """记录成功"""
        self.circuit_breaker.record_success(latency_ms)
    
    def record_failure(self, is_empty: bool = False) -> None:
        """记录失败"""
        self.circuit_breaker.record_failure(is_empty)
    
    def record_response(self, result: Any, latency_ms: float) -> None:
        """
        记录响应（自动判断成功/失败）
        
        Args:
            result: 返回的数据
            latency_ms: 延迟(毫秒)
        """
        if result is None or (hasattr(result, "__len__") and len(result) == 0):
            self.record_failure(is_empty=True)
        else:
            self.record_success(latency_ms)
    
    def get_status(self) -> Dict:
        """获取状态"""
        return {
            "enabled": self._enabled,
            "can_use": self.can_use(),
            "health_checker": {
                "last_check": self._last_health_check.isoformat() if self._last_health_check else None,
                "check_count": self.health_checker.check_count,
                "last_result": {
                    "is_healthy": self.health_checker.last_result.is_healthy,
                    "latency_ms": self.health_checker.last_result.latency_ms,
                    "error": self.health_checker.last_result.error_message
                } if self.health_checker.last_result else None
            },
            "circuit_breaker": self.circuit_breaker.get_status()
        }
    
    def enable(self) -> None:
        """启用"""
        with self._lock:
            self._enabled = True
    
    def disable(self) -> None:
        """禁用"""
        with self._lock:
            self._enabled = False
    
    def reset(self) -> None:
        """重置"""
        with self._lock:
            self._enabled = True
            self._last_health_check = None
            self.circuit_breaker.reset()


# ══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ══════════════════════════════════════════════════════════════════════════════

_baostock_manager: Optional[BaostockEnhancedManager] = None


def get_baostock_manager() -> BaostockEnhancedManager:
    """获取全局 Baostock 管理器"""
    global _baostock_manager
    if _baostock_manager is None:
        _baostock_manager = BaostockEnhancedManager()
    return _baostock_manager


# ══════════════════════════════════════════════════════════════════════════════
# 导出
# ══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "SourceHealth",
    "HealthCheckResult",
    "SourceStats",
    "BaostockHealthChecker",
    "BaostockCircuitBreaker",
    "BaostockEnhancedManager",
    "get_baostock_manager",
]
