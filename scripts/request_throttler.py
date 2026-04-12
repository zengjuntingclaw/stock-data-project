"""
RequestThrottler & RequestQueue - 全局请求限速与队列管理
==========================================================
功能：
  1. 令牌桶限速器：支持每秒/每分钟请求次数上限
  2. 全局请求队列：统一调度所有 fetch 操作
  3. 数据源限流响应处理：自动冷却等待
  4. 线程安全：多线程并发安全
"""

from __future__ import annotations

import time
import threading
import queue
import hashlib
from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from loguru import logger
import collections


class RateLimitStrategy(Enum):
    """限速策略"""
    TOKEN_BUCKET = "token_bucket"       # 令牌桶
    SLIDING_WINDOW = "sliding_window"    # 滑动窗口
    FIXED_WINDOW = "fixed_window"        # 固定窗口


@dataclass
class RateLimitConfig:
    """限速配置"""
    requests_per_second: float = 5.0      # 每秒请求数
    requests_per_minute: float = 200.0    # 每分钟请求数
    burst_size: int = 10                  # 突发容量
    cooldown_on_limit: float = 60.0      # 触发限流后的冷却时间(秒)
    
    def __post_init__(self):
        # 计算令牌桶补充速率
        self.tokens_per_second = self.requests_per_second
        # 滑动窗口容量
        self.window_size_seconds = 60.0


@dataclass
class RateLimitStatus:
    """限速器状态"""
    total_requests: int = 0
    total_waits: int = 0
    total_rejections: int = 0
    total_cooldowns: int = 0
    last_request_time: Optional[datetime] = None
    last_limit_time: Optional[datetime] = None
    current_tokens: float = 0.0
    recent_errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "total_requests": self.total_requests,
            "total_waits": self.total_waits,
            "total_rejections": self.total_rejections,
            "total_cooldowns": self.total_cooldowns,
            "last_request_time": self.last_request_time.isoformat() if self.last_request_time else None,
            "last_limit_time": self.last_limit_time.isoformat() if self.last_limit_time else None,
            "current_tokens": round(self.current_tokens, 2),
            "recent_errors": self.recent_errors[-10:]  # 只保留最近10条
        }


class TokenBucketThrottler:
    """令牌桶限速器"""
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._lock = threading.RLock()
        self._tokens = float(self.config.burst_size)
        self._last_update = time.monotonic()
        self._cooldown_until: Optional[float] = None
        self.status = RateLimitStatus(current_tokens=self._tokens)
    
    def _refill(self) -> None:
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(
            self.config.burst_size,
            self._tokens + elapsed * self.config.tokens_per_second
        )
        self._last_update = now
    
    def acquire(self, timeout: float = 30.0, tokens: int = 1) -> bool:
        """
        获取令牌
        
        Args:
            timeout: 最大等待时间(秒)
            tokens: 需要获取的令牌数
        
        Returns:
            是否成功获取
        """
        deadline = time.monotonic() + timeout
        
        with self._lock:
            self._refill()
            
            # 检查是否在冷却期
            if self._cooldown_until and time.monotonic() < self._cooldown_until:
                remaining = self._cooldown_until - time.monotonic()
                self.status.total_cooldowns += 1
                self.status.last_limit_time = datetime.now()
                logger.debug(f"Rate limiter in cooldown, {remaining:.1f}s remaining")
                return False
            
            # 等待足够令牌
            while self._tokens < tokens:
                if time.monotonic() >= deadline:
                    self.status.total_rejections += 1
                    return False
                
                # 计算需要等待的时间
                wait_time = (tokens - self._tokens) / self.config.tokens_per_second
                sleep_time = min(wait_time, 0.1)  # 最多睡眠0.1秒再检查
                self.status.total_waits += 1
                
                self._lock.release()
                time.sleep(sleep_time)
                self._lock.acquire()
                self._refill()
            
            # 消耗令牌
            self._tokens -= tokens
            self.status.total_requests += 1
            self.status.last_request_time = datetime.now()
            self.status.current_tokens = self._tokens
            return True
    
    def report_limit_hit(self, source: str = "unknown") -> None:
        """报告触发限流"""
        with self._lock:
            self._cooldown_until = time.monotonic() + self.config.cooldown_on_limit
            error_msg = f"Rate limit hit from {source} at {datetime.now().isoformat()}"
            self.status.recent_errors.append(error_msg)
            self.status.last_limit_time = datetime.now()
            logger.warning(f"Rate limit triggered for {source}, cooling down for {self.config.cooldown_on_limit}s")
    
    def reset(self) -> None:
        """重置限速器"""
        with self._lock:
            self._tokens = float(self.config.burst_size)
            self._last_update = time.monotonic()
            self._cooldown_until = None
            self.status = RateLimitStatus(current_tokens=self._tokens)


class SlidingWindowThrottler:
    """滑动窗口限速器"""
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._lock = threading.RLock()
        self._request_times: collections.deque = collections.deque()
        self._cooldown_until: Optional[float] = None
        self.status = RateLimitStatus()
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """获取请求许可"""
        deadline = time.monotonic() + timeout
        window_size = self.config.window_size_seconds
        
        while True:
            with self._lock:
                now = time.monotonic()
                
                # 检查冷却期
                if self._cooldown_until and now < self._cooldown_until:
                    return False
                
                # 清理窗口外的请求
                cutoff = now - window_size
                while self._request_times and self._request_times[0] < cutoff:
                    self._request_times.popleft()
                
                # 检查是否超限
                if len(self._request_times) < self.config.requests_per_minute:
                    self._request_times.append(now)
                    self.status.total_requests += 1
                    self.status.last_request_time = datetime.now()
                    return True
                
                # 计算等待时间
                oldest = self._request_times[0]
                wait_time = oldest + window_size - now + 0.1
                
                if now + wait_time > deadline:
                    self.status.total_rejections += 1
                    return False
                
                self.status.total_waits += 1
            
            time.sleep(min(wait_time, 0.1))
    
    def report_limit_hit(self, source: str = "unknown") -> None:
        """报告触发限流"""
        with self._lock:
            self._cooldown_until = time.monotonic() + self.config.cooldown_on_limit
            self.status.total_cooldowns += 1
            self.status.last_limit_time = datetime.now()
            logger.warning(f"Sliding window limit hit for {source}")
    
    def reset(self) -> None:
        """重置"""
        with self._lock:
            self._request_times.clear()
            self._cooldown_until = None
            self.status = RateLimitStatus()


class RequestThrottler:
    """
    全局请求限速器（支持多数据源独立限速）
    
    Usage:
        throttler = RequestThrottler()
        throttler.register_source("akshare", requests_per_second=5.0)
        throttler.register_source("baostock", requests_per_second=2.0)
        
        with throttler.acquire("akshare"):
            data = akshare_fetch(...)
    """
    
    _instance: Optional["RequestThrottler"] = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, default_config: Optional[RateLimitConfig] = None):
        if self._initialized:
            return
        
        self._initialized = True
        self.default_config = default_config or RateLimitConfig()
        self._throttlers: Dict[str, TokenBucketThrottler] = {}
        self._source_cooldowns: Dict[str, float] = {}
        self._global_lock = threading.RLock()
        
        # 默认注册全局限速器
        self._throttlers["_global"] = TokenBucketThrottler(self.default_config)
    
    def register_source(
        self,
        source_name: str,
        config: Optional[RateLimitConfig] = None
    ) -> None:
        """注册数据源限速器"""
        with self._global_lock:
            cfg = config or self.default_config
            self._throttlers[source_name] = TokenBucketThrottler(cfg)
            logger.debug(f"Registered throttler for source: {source_name}")
    
    def acquire(
        self,
        source: str = "_global",
        timeout: float = 30.0,
        tokens: int = 1
    ) -> "ThrottlerContext":
        """
        获取请求许可（作为上下文管理器使用）
        
        Usage:
            with throttler.acquire("akshare"):
                data = fetch_data()
        """
        return ThrottlerContext(self, source, timeout, tokens)
    
    def _do_acquire(self, source: str, timeout: float, tokens: int) -> bool:
        """执行获取许可"""
        with self._global_lock:
            throttler = self._throttlers.get(source) or self._throttlers["_global"]
            return throttler.acquire(timeout, tokens)
    
    def wait_and_acquire(
        self,
        source: str = "_global",
        max_wait: float = 60.0
    ) -> bool:
        """
        等待直到获得许可（阻塞式）
        """
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            if self._do_acquire(source, timeout=1.0):
                return True
            time.sleep(0.1)
        return False
    
    def report_rate_limit(self, source: str, retry_after: Optional[float] = None) -> None:
        """报告收到限流响应"""
        with self._global_lock:
            throttler = self._throttlers.get(source) or self._throttlers["_global"]
            
            if retry_after:
                throttler._cooldown_until = time.monotonic() + retry_after
            else:
                throttler.report_limit_hit(source)
            
            self._source_cooldowns[source] = time.monotonic()
            logger.warning(f"Reported rate limit for {source}, cooldown: {retry_after}s")
    
    def is_cooling_down(self, source: str) -> bool:
        """检查数据源是否在冷却期"""
        with self._global_lock:
            if source not in self._source_cooldowns:
                return False
            cooldown_end = self._source_cooldowns[source] + 60  # 默认60秒冷却
            return time.monotonic() < cooldown_end
    
    def get_status(self, source: Optional[str] = None) -> Dict:
        """获取限速器状态"""
        with self._global_lock:
            if source:
                throttler = self._throttlers.get(source)
                return throttler.status.to_dict() if throttler else {}
            else:
                return {
                    name: throttler.status.to_dict()
                    for name, throttler in self._throttlers.items()
                }
    
    def reset(self, source: Optional[str] = None) -> None:
        """重置限速器"""
        with self._global_lock:
            if source:
                if source in self._throttlers:
                    self._throttlers[source].reset()
            else:
                for throttler in self._throttlers.values():
                    throttler.reset()


class ThrottlerContext:
    """限速器上下文管理器"""
    
    def __init__(
        self,
        throttler: RequestThrottler,
        source: str,
        timeout: float,
        tokens: int
    ):
        self.throttler = throttler
        self.source = source
        self.timeout = timeout
        self.tokens = tokens
        self._acquired = False
    
    def __enter__(self) -> bool:
        self._acquired = self.throttler._do_acquire(
            self.source, self.timeout, self.tokens
        )
        return self._acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # 不需要释放令牌（已在acquire时消耗）
        return False


# ══════════════════════════════════════════════════════════════════════════════
# 全局请求队列
# ══════════════════════════════════════════════════════════════════════════════

class RequestTask:
    """请求任务"""
    
    def __init__(
        self,
        task_id: str,
        fetch_func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        source: str = "_global",
        priority: int = 0,
        on_success: Optional[Callable] = None,
        on_failure: Optional[Callable] = None,
    ):
        self.task_id = task_id
        self.fetch_func = fetch_func
        self.args = args
        self.kwargs = kwargs or {}
        self.source = source
        self.priority = priority
        self.on_success = on_success
        self.on_failure = on_failure
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.status: str = "pending"  # pending/running/completed/failed
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
    
    def __lt__(self, other: "RequestTask"):
        """优先级队列比较"""
        return self.priority < other.priority


class RequestQueue:
    """
    全局请求队列
    
    所有 fetch 操作统一加入待执行队列，通过限速器控制执行节奏。
    
    Usage:
        q = RequestQueue(throttler=RequestThrottler())
        
        # 添加任务
        task_id = q.add_task(
            fetch_func=akshare_fetch,
            args=("stock_zh_a_hist",),
            kwargs={"symbol": "000001", "period": "daily"},
            source="akshare",
            priority=1
        )
        
        # 同步执行
        result = q.execute_pending(timeout=60)
        
        # 或异步执行
        q.start_worker(threads=3)
    """
    
    _instance: Optional["RequestQueue"] = None
    
    def __init__(
        self,
        throttler: Optional[RequestThrottler] = None,
        max_workers: int = 3,
        max_queue_size: int = 1000
    ):
        self.throttler = throttler or RequestThrottler()
        self.max_workers = max_workers
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=max_queue_size)
        self._pending_tasks: Dict[str, RequestTask] = {}
        self._completed_tasks: Dict[str, RequestTask] = {}
        self._lock = threading.Lock()
        self._workers: List[threading.Thread] = []
        self._running = False
        self._task_counter = 0
    
    @classmethod
    def get_instance(cls) -> "RequestQueue":
        """获取单例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def add_task(
        self,
        fetch_func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        source: str = "_global",
        priority: int = 0,
        task_id: Optional[str] = None,
        on_success: Optional[Callable] = None,
        on_failure: Optional[Callable] = None,
    ) -> str:
        """
        添加请求任务
        
        Returns:
            task_id 任务ID
        """
        with self._lock:
            self._task_counter += 1
            tid = task_id or f"task_{self._task_counter}_{int(time.time())}"
            
            task = RequestTask(
                task_id=tid,
                fetch_func=fetch_func,
                args=args,
                kwargs=kwargs,
                source=source,
                priority=priority,
                on_success=on_success,
                on_failure=on_failure
            )
            
            self._pending_tasks[tid] = task
            self._task_queue.put(task)
            
            logger.debug(f"Added task {tid} to queue (source: {source}, priority: {priority})")
            return tid
    
    def _execute_task(self, task: RequestTask) -> Any:
        """执行单个任务"""
        task.status = "running"
        task.started_at = datetime.now()
        
        try:
            # 先通过限速器
            if not self.throttler.acquire(task.source, timeout=60):
                raise TimeoutError(f"Throttler timeout for source {task.source}")
            
            # 执行请求
            result = task.fetch_func(*task.args, **task.kwargs)
            task.result = result
            task.status = "completed"
            task.completed_at = datetime.now()
            
            # 回调
            if task.on_success:
                task.on_success(result)
            
            return result
            
        except Exception as e:
            task.error = e
            task.status = "failed"
            task.completed_at = datetime.now()
            
            # 检查是否是限流错误
            error_msg = str(e).lower()
            if any(x in error_msg for x in ["rate limit", "429", "too many requests"]):
                self.throttler.report_rate_limit(task.source)
            
            # 回调
            if task.on_failure:
                task.on_failure(e)
            
            raise
    
    def execute_pending(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        同步执行所有待处理任务
        
        Args:
            timeout: 最大等待时间
        
        Returns:
            {task_id: result} 任务结果字典
        """
        results = {}
        deadline = time.monotonic() + timeout if timeout else None
        
        while True:
            # 检查超时
            if deadline and time.monotonic() >= deadline:
                break
            
            # 检查队列是否为空
            try:
                task = self._task_queue.get_nowait()
            except queue.Empty:
                break
            
            try:
                result = self._execute_task(task)
                results[task.task_id] = result
            except Exception as e:
                logger.error(f"Task {task.task_id} failed: {e}")
                results[task.task_id] = None
            
            with self._lock:
                self._completed_tasks[task.task_id] = task
        
        return results
    
    def start_worker(self, threads: Optional[int] = None) -> None:
        """启动工作线程"""
        if self._running:
            return
        
        self._running = True
        n = threads or self.max_workers
        
        for i in range(n):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            self._workers.append(t)
        
        logger.info(f"Started {n} request queue workers")
    
    def _worker_loop(self, worker_id: int) -> None:
        """工作线程主循环"""
        while self._running:
            try:
                # 阻塞获取任务
                task = self._task_queue.get(timeout=1.0)
                
                try:
                    self._execute_task(task)
                except Exception as e:
                    logger.error(f"Worker {worker_id} task {task.task_id} failed: {e}")
                finally:
                    with self._lock:
                        self._completed_tasks[task.task_id] = task
                    self._task_queue.task_done()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
    
    def stop_worker(self) -> None:
        """停止工作线程"""
        self._running = False
        for t in self._workers:
            t.join(timeout=2.0)
        self._workers.clear()
        logger.info("Request queue workers stopped")
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        with self._lock:
            task = self._pending_tasks.get(task_id) or self._completed_tasks.get(task_id)
            if not task:
                return None
            
            return {
                "task_id": task.task_id,
                "status": task.status,
                "source": task.source,
                "priority": task.priority,
                "created_at": task.created_at.isoformat(),
                "started_at": task.started_at.isoformat() if task.started_at else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "has_error": task.error is not None,
                "error_message": str(task.error) if task.error else None
            }
    
    def get_queue_stats(self) -> Dict:
        """获取队列统计"""
        with self._lock:
            pending = sum(1 for t in self._pending_tasks.values() if t.status == "pending")
            running = sum(1 for t in self._pending_tasks.values() if t.status == "running")
            return {
                "pending_tasks": pending,
                "running_tasks": running,
                "completed_tasks": len(self._completed_tasks),
                "queue_size": self._task_queue.qsize(),
                "workers": len(self._workers),
                "running": self._running,
                "throttler_status": self.throttler.get_status()
            }


# ══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ══════════════════════════════════════════════════════════════════════════════

def create_throttler(**kwargs) -> RequestThrottler:
    """创建限速器"""
    config = RateLimitConfig(**kwargs) if kwargs else None
    return RequestThrottler(config)


def create_request_queue(**kwargs) -> RequestQueue:
    """创建请求队列"""
    return RequestQueue(**kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# 导出
# ══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "RateLimitConfig",
    "RateLimitStatus",
    "RateLimitStrategy",
    "TokenBucketThrottler",
    "SlidingWindowThrottler",
    "RequestThrottler",
    "ThrottlerContext",
    "RequestTask",
    "RequestQueue",
    "create_throttler",
    "create_request_queue",
]
