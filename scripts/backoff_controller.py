"""
BackoffController - HTTP并发控制与弹性Backoff
"""
from __future__ import annotations

import time
import threading
import queue
import random
from typing import Callable, Any, Optional, List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, Future


class RetryStrategy(Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class RetryConfig:
    max_retries: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.1


class ExponentialBackoff:
    """指数退避计算器"""
    def __init__(self, config: RetryConfig):
        self.config = config
    
    def get_delay(self, attempt: int) -> float:
        delay = min(self.config.base_delay * (self.config.multiplier ** attempt), self.config.max_delay)
        if self.config.jitter > 0:
            delay += random.uniform(-delay * self.config.jitter, delay * self.config.jitter)
        return max(0, delay)


class BackoffController:
    """指数退避控制器"""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.backoff = ExponentialBackoff(self.config)
        self._retry_count: Dict[str, int] = {}
        self._lock = threading.Lock()
    
    def retry_with_backoff(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        task_id: Optional[str] = None,
        on_failure: Optional[Callable] = None,
        on_success: Optional[Callable] = None
    ) -> Any:
        tid = task_id or f"task_{id(func)}"
        kwargs = kwargs or {}
        last_error = None
        
        for attempt in range(self.config.max_retries):
            try:
                result = func(*args, **kwargs)
                if on_success:
                    on_success(result, attempt)
                return result
            except Exception as e:
                last_error = e
                if on_failure:
                    on_failure(e, attempt)
                if attempt < self.config.max_retries - 1:
                    delay = self.backoff.get_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {tid}: {e}. Retrying in {delay:.1f}s...")
                    time.sleep(delay)
        
        raise last_error
    
    async def async_retry_with_backoff(self, func: Callable, args: tuple = (), kwargs: dict = None, task_id: Optional[str] = None) -> Any:
        import asyncio
        tid = task_id or f"task_{id(func)}"
        kwargs = kwargs or {}
        last_error = None
        
        for attempt in range(self.config.max_retries):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                return result
            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries - 1:
                    delay = self.backoff.get_delay(attempt)
                    await asyncio.sleep(delay)
        
        raise last_error


class ConcurrentController:
    """并发控制器"""
    
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self._semaphore = threading.Semaphore(max_workers)
        self._active_count = 0
        self._lock = threading.Lock()
        self._executor: Optional[ThreadPoolExecutor] = None
    
    def __enter__(self):
        self._semaphore.acquire()
        with self._lock:
            self._active_count += 1
        return self
    
    def __exit__(self, *args):
        with self._lock:
            self._active_count -= 1
        self._semaphore.release()
    
    def create_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor
    
    def submit(self, func: Callable, *args, **kwargs) -> Future:
        return self.create_executor().submit(func, *args, **kwargs)
    
    def shutdown(self, wait: bool = True) -> None:
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None


class RetryQueue:
    """失败任务重试队列"""
    
    def __init__(self, backoff_controller: Optional[BackoffController] = None, max_queue_size: int = 1000):
        self.controller = backoff_controller or BackoffController()
        self.max_queue_size = max_queue_size
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._running = False
        self._workers: List[threading.Thread] = []
    
    def add_task(self, func: Callable, args: tuple = (), kwargs: dict = None, task_id: Optional[str] = None) -> str:
        tid = task_id or f"retry_{id(func)}"
        self._queue.put({"func": func, "args": args, "kwargs": kwargs or {}, "task_id": tid})
        return tid
    
    def start_worker(self, workers: int = 1):
        if self._running:
            return
        self._running = True
        for i in range(workers):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            self._workers.append(t)
    
    def _worker_loop(self, worker_id: int):
        while self._running:
            try:
                task = self._queue.get(timeout=1.0)
                try:
                    self.controller.retry_with_backoff(task["func"], task["args"], task["kwargs"], task["task_id"])
                except Exception as e:
                    logger.error(f"Retry task {task['task_id']} failed: {e}")
                self._queue.task_done()
            except queue.Empty:
                continue
    
    def stop_worker(self):
        self._running = False
        for t in self._workers:
            t.join(timeout=2.0)


def retry(func: Callable, *args, max_retries: int = 3, base_delay: float = 1.0, **kwargs) -> Any:
    controller = BackoffController(RetryConfig(max_retries=max_retries, base_delay=base_delay))
    return controller.retry_with_backoff(func, args, kwargs)


__all__ = ["RetryStrategy", "RetryConfig", "ExponentialBackoff", "BackoffController", "ConcurrentController", "RetryQueue", "retry"]
