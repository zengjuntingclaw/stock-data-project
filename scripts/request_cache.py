"""
RequestCache - 缓存机制与本地脱机回放
"""
from __future__ import annotations

import json
import hashlib
import threading
import time
from pathlib import Path
from typing import Optional, Any, Dict, Callable, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd


@dataclass
class CacheEntry:
    """缓存条目"""
    key: str
    value: Any
    created_at: datetime
    expires_at: Optional[datetime]
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    
    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at


class RequestCache:
    """
    请求缓存
    
    Usage:
        cache = RequestCache(ttl_seconds=3600)
        
        # 缓存请求
        result = cache.get_or_fetch(
            key="daily_000001_2024",
            fetch_func=lambda: akshare_fetch(...),
            ttl=1800
        )
    """
    
    def __init__(
        self,
        ttl_seconds: int = 3600,
        max_entries: int = 10000,
        cache_dir: Optional[str] = None
    ):
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.cache_dir = Path(cache_dir) if cache_dir else None
        
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def _generate_key(self, *args, **kwargs) -> str:
        """生成缓存键"""
        key_str = str(args) + str(sorted(kwargs.items()))
        return hashlib.md5(key_str.encode()).hexdigest()[:16]
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._misses += 1
                return None
            
            if entry.is_expired:
                del self._cache[key]
                self._misses += 1
                return None
            
            entry.access_count += 1
            entry.last_accessed = datetime.now()
            self._hits += 1
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置缓存"""
        with self._lock:
            expires_at = None
            if ttl is not None:
                expires_at = datetime.now() + timedelta(seconds=ttl)
            elif self.ttl_seconds > 0:
                expires_at = datetime.now() + timedelta(seconds=self.ttl_seconds)
            
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                expires_at=expires_at
            )
            
            # 清理过期条目
            self._cleanup()
    
    def _cleanup(self):
        """清理过期和过多条目"""
        # 删除过期条目
        expired = [k for k, v in self._cache.items() if v.is_expired]
        for k in expired:
            del self._cache[k]
        
        # 如果还太多，删除最少使用的
        if len(self._cache) > self.max_entries:
            sorted_entries = sorted(
                self._cache.items(),
                key=lambda x: (x[1].access_count, x[1].created_at)
            )
            to_remove = len(self._cache) - self.max_entries
            for k, _ in sorted_entries[:to_remove]:
                del self._cache[k]
    
    def get_or_fetch(
        self,
        key: Optional[str] = None,
        fetch_func: Optional[Callable] = None,
        ttl: Optional[int] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        获取或抓取
        
        Args:
            key: 缓存键（如果不提供则自动生成）
            fetch_func: 抓取函数
            ttl: 缓存时间(秒)
            *args, **kwargs: 传给 fetch_func 的参数
        
        Returns:
            缓存或新获取的数据
        """
        if key is None:
            key = self._generate_key(*args, **kwargs)
        
        # 尝试从缓存获取
        cached = self.get(key)
        if cached is not None:
            logger.debug(f"Cache hit: {key}")
            return cached
        
        # 执行抓取
        if fetch_func is None:
            raise ValueError("fetch_func required for cache miss")
        
        logger.debug(f"Cache miss: {key}")
        result = fetch_func(*args, **kwargs)
        
        # 存入缓存
        if result is not None:
            self.set(key, result, ttl)
        
        return result
    
    def invalidate(self, key: str):
        """使缓存失效"""
        with self._lock:
            self._cache.pop(key, None)
    
    def clear(self):
        """清空缓存"""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
    
    def get_stats(self) -> Dict:
        """获取统计"""
        with self._lock:
            total = self._hits + self._misses
            return {
                "entries": len(self._cache),
                "max_entries": self.max_entries,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / total, 4) if total > 0 else 0
            }


class OfflinePlaybackMode:
    """
    脱机回放模式
    
    在无网或 emergency 情况下回放本地抓取过的数据。
    
    Usage:
        playback = OfflinePlaybackMode(data_dir="data/playback")
        
        # 检查是否可回放
        if playback.is_available("000001.SZ", "2024-01-01"):
            data = playback.fetch("000001.SZ", "2024-01-01", "2024-01-31")
    """
    
    def __init__(self, data_dir: str = "data/playback"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._index_file = self.data_dir / "playback_index.json"
        self._index: Dict[str, List[str]] = {}  # ts_code -> [date, ...]
        self._lock = threading.RLock()
        self._load_index()
    
    def _load_index(self):
        """加载索引"""
        if self._index_file.exists():
            try:
                with open(self._index_file, "r", encoding="utf-8") as f:
                    self._index = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load playback index: {e}")
                self._index = {}
    
    def _save_index(self):
        """保存索引"""
        try:
            with open(self._index_file, "w", encoding="utf-8") as f:
                json.dump(self._index, f)
        except Exception as e:
            logger.error(f"Failed to save playback index: {e}")
    
    def record(self, ts_code: str, data: Any, start: str, end: str):
        """
        记录数据用于回放
        
        Args:
            ts_code: 股票代码
            data: 数据（DataFrame 或其他）
            start: 起始日期
            end: 结束日期
        """
        with self._lock:
            # 生成文件名
            file_name = f"{ts_code.replace('.', '_')}_{start}_{end}.parquet"
            file_path = self.data_dir / file_name
            
            # 保存数据
            try:
                if isinstance(data, pd.DataFrame):
                    data.to_parquet(file_path, index=False)
                else:
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data if isinstance(data, dict) else str(data), f)
                
                # 更新索引
                if ts_code not in self._index:
                    self._index[ts_code] = []
                
                date_range = f"{start}_{end}"
                if date_range not in self._index[ts_code]:
                    self._index[ts_code].append(date_range)
                
                self._save_index()
                logger.debug(f"Recorded playback data: {ts_code} {start}-{end}")
                
            except Exception as e:
                logger.error(f"Failed to record playback data: {e}")
    
    def is_available(self, ts_code: str, start: str, end: str) -> bool:
        """检查是否可回放"""
        with self._lock:
            date_range = f"{start}_{end}"
            return ts_code in self._index and date_range in self._index[ts_code]
    
    def fetch(self, ts_code: str, start: str, end: str) -> Optional[Any]:
        """
        回放数据
        
        Returns:
            回放的数据，None 如果不可用
        """
        file_name = f"{ts_code.replace('.', '_')}_{start}_{end}.parquet"
        file_path = self.data_dir / file_name
        
        if not file_path.exists():
            logger.warning(f"Playback file not found: {file_name}")
            return None
        
        try:
            if file_path.suffix == ".parquet":
                return pd.read_parquet(file_path)
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to fetch playback data: {e}")
            return None
    
    def offline_fetch(
        self,
        ts_code: str,
        start: str,
        end: str,
        online_fetch_func: Optional[Callable] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        优先在线获取，失败时回放本地数据
        
        Args:
            ts_code: 股票代码
            start: 起始日期
            end: 结束日期
            online_fetch_func: 在线抓取函数
        
        Returns:
            数据
        """
        # 尝试在线获取
        if online_fetch_func is not None:
            try:
                data = online_fetch_func(*args, **kwargs)
                
                # 成功则记录用于回放
                if data is not None:
                    self.record(ts_code, data, start, end)
                
                return data
            except Exception as e:
                logger.warning(f"Online fetch failed: {e}, trying offline playback...")
        
        # 回放本地数据
        return self.fetch(ts_code, start, end)
    
    def get_available_range(self, ts_code: str) -> Optional[Dict]:
        """获取可用日期范围"""
        with self._lock:
            if ts_code not in self._index:
                return None
            
            ranges = self._index[ts_code]
            if not ranges:
                return None
            
            all_dates = []
            for r in ranges:
                start, end = r.split("_")
                all_dates.extend([start, end])
            
            return {
                "earliest": min(all_dates),
                "latest": max(all_dates),
                "ranges": ranges
            }


# ══════════════════════════════════════════════════════════════════════════════
# 全局实例
# ══════════════════════════════════════════════════════════════════════════════

_cache: Optional[RequestCache] = None
_playback: Optional[OfflinePlaybackMode] = None


def get_cache() -> RequestCache:
    """获取全局缓存"""
    global _cache
    if _cache is None:
        _cache = RequestCache()
    return _cache


def get_playback() -> OfflinePlaybackMode:
    """获取全局回放模式"""
    global _playback
    if _playback is None:
        _playback = OfflinePlaybackMode()
    return _playback


__all__ = ["CacheEntry", "RequestCache", "OfflinePlaybackMode", "get_cache", "get_playback"]
