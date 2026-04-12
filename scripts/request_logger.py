"""
RequestLogger - 增强日志与可追溯性
=================================
功能：
  1. 请求日志：记录每次 fetch 的时间/参数/响应/错误
  2. 修复日志增强：记录 local vs fetched 版本快照
  3. 可追溯性报告
"""

from __future__ import annotations

import json
import hashlib
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import deque
from loguru import logger


@dataclass
class RequestLog:
    """请求日志"""
    request_id: str
    timestamp: datetime
    source: str
    method: str                    # fetch/get/post
    url_or_function: str
    params: Dict[str, Any]
    response_status: str           # success/failure/partial
    status_code: Optional[int]
    latency_ms: float
    error_type: Optional[str]
    error_message: Optional[str]
    record_count: Optional[int]
    data_hash: Optional[str]
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


@dataclass
class RepairSnapshot:
    """修复快照"""
    repair_id: str
    timestamp: datetime
    ts_code: str
    table_name: str
    local_version: Optional[Dict]   # 本地版本摘要
    fetched_version: Optional[Dict] # 拉取版本摘要
    repair_action: str              # fix/replace/reject
    success: bool
    error: Optional[str]
    data_hash_before: Optional[str]
    data_hash_after: Optional[str]
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


class RequestLogger:
    """
    请求日志记录器
    
    Usage:
        logger = RequestLogger()
        
        # 记录请求
        logger.log_request(
            source="akshare",
            method="get",
            url_or_function="stock_zh_a_hist",
            params={"symbol": "000001", "period": "daily"},
            response_status="success",
            latency_ms=150.5
        )
        
        # 获取统计
        stats = logger.get_stats(source="akshare")
    """
    
    def __init__(
        self,
        max_logs: int = 10000,
        log_dir: Optional[Path] = None,
        persist_interval: int = 300
    ):
        self.max_logs = max_logs
        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.persist_interval = persist_interval
        
        self._logs: deque = deque(maxlen=max_logs)
        self._lock = threading.RLock()
        self._request_counter = 0
        self._last_persist = time.time()
        
        # 确保日志目录存在
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def _generate_id(self, prefix: str = "req") -> str:
        """生成唯一ID"""
        self._request_counter += 1
        return f"{prefix}_{self._request_counter}_{int(time.time() * 1000)}"
    
    def log_request(
        self,
        source: str,
        method: str,
        url_or_function: str,
        params: Optional[Dict[str, Any]] = None,
        response_status: str = "unknown",
        status_code: Optional[int] = None,
        latency_ms: float = 0.0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        record_count: Optional[int] = None,
        data: Optional[Any] = None
    ) -> str:
        """
        记录请求
        
        Args:
            source: 数据源名称
            method: 请求方法
            url_or_function: URL或函数名
            params: 请求参数
            response_status: 响应状态
            status_code: HTTP状态码
            latency_ms: 延迟(毫秒)
            error_type: 错误类型
            error_message: 错误消息
            record_count: 记录数
            data: 返回数据（用于计算哈希）
        
        Returns:
            request_id
        """
        request_id = self._generate_id("req")
        
        # 计算数据哈希
        data_hash = None
        if data is not None:
            try:
                if hasattr(data, "to_json"):
                    data_str = data.to_json()
                elif hasattr(data, "to_dict"):
                    data_str = json.dumps(data.to_dict(), sort_keys=True)
                else:
                    data_str = str(data)[:1000]
                data_hash = hashlib.md5(data_str.encode()).hexdigest()[:16]
            except:
                pass
        
        log = RequestLog(
            request_id=request_id,
            timestamp=datetime.now(),
            source=source,
            method=method,
            url_or_function=url_or_function,
            params=params or {},
            response_status=response_status,
            status_code=status_code,
            latency_ms=latency_ms,
            error_type=error_type,
            error_message=error_message,
            record_count=record_count,
            data_hash=data_hash
        )
        
        with self._lock:
            self._logs.append(log)
            
            # 定期持久化
            if time.time() - self._last_persist > self.persist_interval:
                self._persist_logs()
        
        return request_id
    
    def log_success(
        self,
        source: str,
        method: str,
        url_or_function: str,
        params: Optional[Dict] = None,
        latency_ms: float = 0.0,
        record_count: Optional[int] = None,
        data: Optional[Any] = None
    ) -> str:
        """记录成功请求"""
        return self.log_request(
            source=source, method=method, url_or_function=url_or_function,
            params=params, response_status="success", latency_ms=latency_ms,
            record_count=record_count, data=data
        )
    
    def log_failure(
        self,
        source: str,
        method: str,
        url_or_function: str,
        error: Exception,
        params: Optional[Dict] = None,
        latency_ms: float = 0.0
    ) -> str:
        """记录失败请求"""
        return self.log_request(
            source=source, method=method, url_or_function=url_or_function,
            params=params, response_status="failure", latency_ms=latency_ms,
            error_type=type(error).__name__, error_message=str(error)[:500]
        )
    
    def _persist_logs(self):
        """持久化日志到文件"""
        try:
            with self._lock:
                if not self._logs:
                    return
                
                filename = self.log_dir / f"request_log_{datetime.now().strftime('%Y%m%d_%H')}.jsonl"
                
                with open(filename, "a", encoding="utf-8") as f:
                    for log in list(self._logs):
                        f.write(json.dumps(log.to_dict(), ensure_ascii=False) + "\n")
                
                self._last_persist = time.time()
                logger.debug(f"Persisted {len(self._logs)} request logs")
                
        except Exception as e:
            logger.error(f"Failed to persist logs: {e}")
    
    def get_logs(
        self,
        source: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """获取日志"""
        with self._lock:
            logs = list(self._logs)
        
        if source:
            logs = [l for l in logs if l.source == source]
        if since:
            logs = [l for l in logs if l.timestamp >= since]
        
        return [l.to_dict() for l in logs[-limit:]]
    
    def get_stats(self, source: Optional[str] = None, hours: int = 24) -> Dict:
        """获取统计信息"""
        since = datetime.now() - timedelta(hours=hours)
        
        with self._lock:
            logs = [l for l in self._logs if l.timestamp >= since]
            if source:
                logs = [l for l in logs if l.source == source]
        
        total = len(logs)
        success = sum(1 for l in logs if l.response_status == "success")
        failure = sum(1 for l in logs if l.response_status == "failure")
        
        latencies = [l.latency_ms for l in logs if l.latency_ms > 0]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        
        by_source = {}
        for l in logs:
            by_source[l.source] = by_source.get(l.source, 0) + 1
        
        return {
            "total_requests": total,
            "success": success,
            "failure": failure,
            "success_rate": round(success / total, 4) if total else 0,
            "avg_latency_ms": round(avg_latency, 2),
            "by_source": by_source,
            "period_hours": hours
        }


class EnhancedRepairLogger:
    """
    增强修复日志
    
    Usage:
        repair_logger = EnhancedRepairLogger()
        
        # 记录修复
        repair_logger.log_repair(
            ts_code="000001.SZ",
            table_name="daily_bar_adjusted",
            local_version={"count": 100, "hash": "abc123"},
            fetched_version={"count": 105, "hash": "def456"},
            repair_action="fix",
            success=True
        )
    """
    
    def __init__(self, log_dir: Optional[Path] = None):
        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.repair_log_file = self.log_dir / "repair_log.json"
        self._snapshots: List[RepairSnapshot] = []
        self._repair_counter = 0
        self._lock = threading.RLock()
        
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._load_existing()
    
    def _generate_id(self) -> str:
        self._repair_counter += 1
        return f"repair_{self._repair_counter}_{int(time.time() * 1000)}"
    
    def _load_existing(self):
        """加载已存在的修复日志"""
        if self.repair_log_file.exists():
            try:
                with open(self.repair_log_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._snapshots = [RepairSnapshot(**s) for s in data.get("snapshots", [])[-1000:]]
            except Exception as e:
                logger.warning(f"Failed to load existing repair log: {e}")
    
    def log_repair(
        self,
        ts_code: str,
        table_name: str,
        local_version: Optional[Dict] = None,
        fetched_version: Optional[Dict] = None,
        repair_action: str = "fix",
        success: bool = True,
        error: Optional[str] = None,
        data_hash_before: Optional[str] = None,
        data_hash_after: Optional[str] = None
    ) -> str:
        """记录修复"""
        repair_id = self._generate_id()
        
        snapshot = RepairSnapshot(
            repair_id=repair_id,
            timestamp=datetime.now(),
            ts_code=ts_code,
            table_name=table_name,
            local_version=local_version,
            fetched_version=fetched_version,
            repair_action=repair_action,
            success=success,
            error=error,
            data_hash_before=data_hash_before,
            data_hash_after=data_hash_after
        )
        
        with self._lock:
            self._snapshots.append(snapshot)
            self._persist()
        
        return repair_id
    
    def _persist(self):
        """持久化到文件"""
        try:
            data = {
                "last_updated": datetime.now().isoformat(),
                "total_repairs": len(self._snapshots),
                "snapshots": [s.to_dict() for s in self._snapshots[-1000:]]
            }
            
            with open(self.repair_log_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to persist repair log: {e}")
    
    def get_repair_report(self, since: Optional[datetime] = None) -> Dict:
        """生成修复报告"""
        with self._lock:
            snapshots = list(self._snapshots)
        
        if since:
            snapshots = [s for s in snapshots if s.timestamp >= since]
        
        total = len(snapshots)
        success = sum(1 for s in snapshots if s.success)
        failure = total - success
        
        by_action = {}
        for s in snapshots:
            by_action[s.repair_action] = by_action.get(s.repair_action, 0) + 1
        
        by_table = {}
        for s in snapshots:
            by_table[s.table_name] = by_table.get(s.table_name, 0) + 1
        
        recent_failures = [s.to_dict() for s in snapshots[-100:] if not s.success]
        
        return {
            "total_repairs": total,
            "successful": success,
            "failed": failure,
            "success_rate": round(success / total, 4) if total else 0,
            "by_action": by_action,
            "by_table": by_table,
            "recent_failures": recent_failures
        }


# ══════════════════════════════════════════════════════════════════════════════
# 全局实例
# ══════════════════════════════════════════════════════════════════════════════

_request_logger: Optional[RequestLogger] = None
_repair_logger: Optional[EnhancedRepairLogger] = None


def get_request_logger() -> RequestLogger:
    """获取全局请求日志器"""
    global _request_logger
    if _request_logger is None:
        _request_logger = RequestLogger()
    return _request_logger


def get_repair_logger() -> EnhancedRepairLogger:
    """获取全局修复日志器"""
    global _repair_logger
    if _repair_logger is None:
        _repair_logger = EnhancedRepairLogger()
    return _repair_logger


# ══════════════════════════════════════════════════════════════════════════════
# 导出
# ══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "RequestLog",
    "RepairSnapshot",
    "RequestLogger",
    "EnhancedRepairLogger",
    "get_request_logger",
    "get_repair_logger",
]
