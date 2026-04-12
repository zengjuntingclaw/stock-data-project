"""
ThirdPartySources - 第三备源与 ErrorPatternDetector
=================================================
功能：
  1. 第三备用源管理（备用免费源）
  2. 错误模式识别与智能回退
  3. 多级回退链路
"""

from __future__ import annotations

import time
import threading
import re
from typing import Optional, Dict, Any, Callable, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from loguru import logger


class ErrorPattern(Enum):
    """错误模式"""
    NETWORK_ERROR = "network_error"           # 网络错误
    TIMEOUT = "timeout"                        # 超时
    RATE_LIMIT = "rate_limit"                 # 限流
    SOURCE_ERROR = "source_error"              # 数据源错误
    FORMAT_CHANGE = "format_change"           # 格式变更
    FIELD_MISSING = "field_missing"           # 字段缺失
    EMPTY_RESULT = "empty_result"             # 空结果
    AUTH_ERROR = "auth_error"                 # 认证错误
    UNKNOWN = "unknown"                       # 未知错误


@dataclass
class ErrorInfo:
    """错误信息"""
    pattern: ErrorPattern
    message: str
    source: str
    timestamp: datetime = field(default_factory=datetime.now)
    retryable: bool = True
    severity: str = "medium"  # low/medium/high/critical


class ErrorPatternDetector:
    """
    错误模式检测器
    
    Usage:
        detector = ErrorPatternDetector()
        pattern = detector.detect(exception, source_name)
        
        if pattern == ErrorPattern.RATE_LIMIT:
            # 等待后重试
        elif pattern == ErrorPattern.NETWORK_ERROR:
            # 切换数据源
    """
    
    # 错误模式匹配规则
    NETWORK_PATTERNS = [
        r"connection.*refused",
        r"connection.*timeout",
        r"network.*unreachable",
        r"failed to connect",
        r"socket.*error",
        r"http.*error",
        r"ConnectionError",
        r"ConnectTimeout",
        r"NewConnectionError"
    ]
    
    RATE_LIMIT_PATTERNS = [
        r"rate.*limit",
        r"too many requests",
        r"429",
        r"503",
        r"server.*busy",
        r"请稍后",
        r"请求过于频繁",
    ]
    
    TIMEOUT_PATTERNS = [
        r"timeout",
        r"timed out",
        r"ReadTimeout",
        r"ConnectTimeout",
        r" deadline exceeded"
    ]
    
    FORMAT_PATTERNS = [
        r"keyerror",
        r"column.*not.*found",
        r"field.*not.*found",
        r"parse.*error",
        r"invalid.*format"
    ]
    
    FIELD_MISSING_PATTERNS = [
        r"'([^']+)'.*not.*in.*columns",
        r"missing.*field",
        r"required.*field.*none"
    ]
    
    def __init__(self):
        self._compiled_patterns: Dict[ErrorPattern, List[re.Pattern]] = {}
        self._init_patterns()
    
    def _init_patterns(self):
        """编译正则模式"""
        self._compiled_patterns = {
            ErrorPattern.NETWORK_ERROR: [re.compile(p, re.I) for p in self.NETWORK_PATTERNS],
            ErrorPattern.RATE_LIMIT: [re.compile(p, re.I) for p in self.RATE_LIMIT_PATTERNS],
            ErrorPattern.TIMEOUT: [re.compile(p, re.I) for p in self.TIMEOUT_PATTERNS],
            ErrorPattern.FORMAT_CHANGE: [re.compile(p, re.I) for p in self.FORMAT_PATTERNS],
            ErrorPattern.FIELD_MISSING: [re.compile(p, re.I) for p in self.FIELD_MISSING_PATTERNS],
        }
    
    def detect(self, error: Exception, source: str = "unknown") -> ErrorInfo:
        """
        检测错误模式
        
        Args:
            error: 异常对象
            source: 数据源名称
        
        Returns:
            ErrorInfo
        """
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # 按优先级检测
        if self._matches(error_str, ErrorPattern.RATE_LIMIT):
            return ErrorInfo(
                pattern=ErrorPattern.RATE_LIMIT,
                message=str(error),
                source=source,
                retryable=True,
                severity="medium"
            )
        
        if self._matches(error_str, ErrorPattern.NETWORK_ERROR):
            return ErrorInfo(
                pattern=ErrorPattern.NETWORK_ERROR,
                message=str(error),
                source=source,
                retryable=True,
                severity="medium"
            )
        
        if self._matches(error_str, ErrorPattern.TIMEOUT):
            return ErrorInfo(
                pattern=ErrorPattern.TIMEOUT,
                message=str(error),
                source=source,
                retryable=True,
                severity="low"
            )
        
        if self._matches(error_str, ErrorPattern.FORMAT_CHANGE) or "parse" in error_str:
            return ErrorInfo(
                pattern=ErrorPattern.FORMAT_CHANGE,
                message=str(error),
                source=source,
                retryable=False,  # 格式问题需要修复代码
                severity="high"
            )
        
        if self._matches(error_str, ErrorPattern.FIELD_MISSING) or "keyerror" in error_str:
            return ErrorInfo(
                pattern=ErrorPattern.FIELD_MISSING,
                message=str(error),
                source=source,
                retryable=False,
                severity="high"
            )
        
        # 认证错误
        if any(x in error_str for x in ["auth", "unauthorized", "403", "permission"]):
            return ErrorInfo(
                pattern=ErrorPattern.AUTH_ERROR,
                message=str(error),
                source=source,
                retryable=False,
                severity="critical"
            )
        
        # 空结果
        if "empty" in error_str or "no data" in error_str:
            return ErrorInfo(
                pattern=ErrorPattern.EMPTY_RESULT,
                message=str(error),
                source=source,
                retryable=True,
                severity="low"
            )
        
        return ErrorInfo(
            pattern=ErrorPattern.UNKNOWN,
            message=str(error),
            source=source,
            retryable=True,
            severity="medium"
        )
    
    def _matches(self, error_str: str, pattern: ErrorPattern) -> bool:
        """检查是否匹配模式"""
        patterns = self._compiled_patterns.get(pattern, [])
        for p in patterns:
            if p.search(error_str):
                return True
        return False


class FallbackStrategy:
    """回退策略"""
    
    # 主 -> 备 -> 备用备
    SOURCE_PRIORITY = {
        "daily": ["akshare", "baostock", "tushare"],
        "minute": ["akshare", "baostock"],
        "basic": ["akshare", "baostock"],
    }
    
    # 错误模式对应的回退行为
    ERROR_FALLBACK = {
        ErrorPattern.RATE_LIMIT: "switch_source",
        ErrorPattern.NETWORK_ERROR: "retry_or_switch",
        ErrorPattern.TIMEOUT: "retry",
        ErrorPattern.FORMAT_CHANGE: "switch_source",
        ErrorPattern.FIELD_MISSING: "switch_source",
        ErrorPattern.AUTH_ERROR: "alert_and_stop",
        ErrorPattern.EMPTY_RESULT: "retry_or_switch",
    }
    
    @classmethod
    def get_next_source(cls, data_type: str, current_source: str) -> Optional[str]:
        """获取下一个备用源"""
        priority = cls.SOURCE_PRIORITY.get(data_type, ["akshare", "baostock"])
        try:
            idx = priority.index(current_source)
            if idx < len(priority) - 1:
                return priority[idx + 1]
        except ValueError:
            return priority[0] if priority else None
        return None
    
    @classmethod
    def should_retry(cls, error_info: ErrorInfo) -> bool:
        """判断是否应该重试"""
        return error_info.retryable


class ThirdPartySourceManager:
    """
    第三备源管理器
    
    Usage:
        manager = ThirdPartySourceManager()
        
        # 获取第三备用源
        source = manager.get_fallback_source("daily", failed_sources=["akshare", "baostock"])
        
        # 执行带回退的数据获取
        result = await manager.fetch_with_fallback(
            data_type="daily",
            fetch_funcs={"akshare": akshare_fetch, "baostock": baostock_fetch},
            params={"ts_code": "000001.SZ", "start": "2024-01-01"}
        )
    """
    
    def __init__(self):
        self._detector = ErrorPatternDetector()
        self._fallback_stats: Dict[str, int] = {}
        self._lock = threading.Lock()
    
    def detect_error(self, error: Exception, source: str) -> ErrorInfo:
        """检测错误模式"""
        return self._detector.detect(error, source)
    
    def get_fallback_action(self, error_info: ErrorInfo) -> str:
        """获取回退动作"""
        return self._fallback_stats.get(error_info.pattern.value, self._fallback_stats.get("unknown", "switch_source"))
    
    def get_next_source(self, data_type: str, current_source: str, failed_sources: Optional[List[str]] = None) -> Optional[str]:
        """获取下一个可用源"""
        priority = FallbackStrategy.SOURCE_PRIORITY.get(data_type, ["akshare", "baostock"])
        failed = set(failed_sources or [])
        
        for source in priority:
            if source not in failed:
                return source
        return None
    
    async def fetch_with_fallback(
        self,
        data_type: str,
        fetch_funcs: Dict[str, Callable],
        params: Dict[str, Any],
        max_retries: int = 3
    ) -> Tuple[Any, str]:
        """
        带回退的数据获取
        
        Args:
            data_type: 数据类型
            fetch_funcs: {source_name: fetch_function}
            params: fetch 函数参数
            max_retries: 最大重试次数
        
        Returns:
            (result, source_name)
        """
        failed_sources: List[str] = []
        retry_count = 0
        
        while retry_count < max_retries:
            current_source = self.get_next_source(data_type, failed_sources[-1] if failed_sources else "", failed_sources)
            
            if current_source is None:
                raise RuntimeError(f"All sources failed for {data_type}")
            
            fetch_func = fetch_funcs.get(current_source)
            if fetch_func is None:
                failed_sources.append(current_source)
                continue
            
            try:
                result = await fetch_func(**params) if asyncio.iscoroutinefunction(fetch_func) else fetch_func(**params)
                
                if result is not None and not self._is_empty_result(result):
                    with self._lock:
                        self._fallback_stats[current_source] = self._fallback_stats.get(current_source, 0) + 1
                    return result, current_source
                    
            except Exception as e:
                error_info = self.detect_error(e, current_source)
                logger.warning(f"Source {current_source} failed: {error_info.pattern.value} - {error_info.message}")
                
                if not FallbackStrategy.should_retry(error_info):
                    raise
                
                failed_sources.append(current_source)
                retry_count += 1
                
                # 限流时等待
                if error_info.pattern == ErrorPattern.RATE_LIMIT:
                    await self._wait_for_cooldown()
        
        raise RuntimeError(f"Failed to fetch {data_type} after {max_retries} retries")
    
    def _is_empty_result(self, result: Any) -> bool:
        """检查结果是否为空"""
        if result is None:
            return True
        if hasattr(result, "__len__") and len(result) == 0:
            return True
        if hasattr(result, "empty") and result.empty:
            return True
        return False
    
    async def _wait_for_cooldown(self):
        """等待冷却"""
        import asyncio
        await asyncio.sleep(60)  # 1分钟冷却


# ══════════════════════════════════════════════════════════════════════════════
# 导出
# ══════════════════════════════════════════════════════════════════════════════

import asyncio

__all__ = [
    "ErrorPattern",
    "ErrorInfo",
    "ErrorPatternDetector",
    "FallbackStrategy",
    "ThirdPartySourceManager",
]
