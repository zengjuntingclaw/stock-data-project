"""版本化存储引擎 - Point-in-Time快照与状态同步

生产环境最怕的是"数据回溯"（Data Look-ahead）。
本模块实现：
1. 点对点快照 (Point-in-Time)：recorded_at字段标记数据生效时间点
2. 状态同步：财务数据使用ann_date（公告日）而非end_date（报表日）
3. 版本链：支持数据历史版本追溯
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque
import contextlib
import json
import hashlib
import threading
import pandas as pd
import numpy as np
from loguru import logger

# 延迟导入duckdb，避免循环依赖
duckdb = None


class DataVersionStatus(Enum):
    """数据版本状态"""
    ACTIVE = "active"           # 当前生效
    SUPERSEDED = "superseded"   # 被新版本替代
    CORRECTED = "corrected"     # 已修正（数据勘误）
    RETRACTED = "retracted"     # 已撤回


@dataclass(frozen=True)
class DataSnapshotKey:
    """数据快照唯一键"""
    ts_code: str
    data_type: str      # 'price', 'financial', 'valuation', 'corporate_action'
    trade_date: str     # YYYY-MM-DD
    
    def __str__(self) -> str:
        return f"{self.ts_code}:{self.data_type}:{self.trade_date}"
    
    def to_dict(self) -> Dict:
        return {
            'ts_code': self.ts_code,
            'data_type': self.data_type,
            'trade_date': self.trade_date
        }


@dataclass
class DataSnapshot:
    """数据快照 - 不可变版本记录"""
    key: DataSnapshotKey
    version: int                    # 版本号，从1开始递增
    recorded_at: datetime           # 数据记录时间（入库时间）
    effective_at: datetime          # 数据生效时间（公告日/交易日）
    data_hash: str                  # 数据内容哈希（完整性校验）
    data: Dict[str, Any]            # 实际数据内容
    source: str                     # 数据来源
    status: DataVersionStatus = DataVersionStatus.ACTIVE
    superseded_by: Optional[int] = None  # 被哪个版本替代
    correction_note: Optional[str] = None  # 修正说明
    
    def compute_hash(self) -> str:
        """计算数据哈希"""
        content = json.dumps(self.data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict:
        return {
            'key': self.key.to_dict(),
            'version': self.version,
            'recorded_at': self.recorded_at.isoformat(),
            'effective_at': self.effective_at.isoformat(),
            'data_hash': self.data_hash,
            'data': self.data,
            'source': self.source,
            'status': self.status.value,
            'superseded_by': self.superseded_by,
            'correction_note': self.correction_note
        }
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'DataSnapshot':
        return cls(
            key=DataSnapshotKey(**d['key']),
            version=d['version'],
            recorded_at=datetime.fromisoformat(d['recorded_at']),
            effective_at=datetime.fromisoformat(d['effective_at']),
            data_hash=d['data_hash'],
            data=d['data'],
            source=d['source'],
            status=DataVersionStatus(d['status']),
            superseded_by=d.get('superseded_by'),
            correction_note=d.get('correction_note')
        )


class VersionedStorage:
    """
    版本化存储引擎
    
    核心特性：
    1. 不可变快照：每个数据点都有完整版本历史
    2. PIT查询：查询指定时间点的数据视图
    3. 状态同步：财务数据使用ann_date约束
    4. 完整性校验：数据哈希防篡改
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Parameters
        ----------
        db_path : str, optional
            DuckDB数据库路径，默认使用环境变量或项目默认路径
        """
        import os
        from pathlib import Path
        
        if db_path is None:
            project_root = Path(__file__).resolve().parent.parent
            db_path = os.environ.get(
                'STOCK_DB_PATH', 
                str(project_root / 'data' / 'stock_data.duckdb')
            )
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 内存缓存（热数据）带TTL
        self._cache: Dict[str, Tuple[DataSnapshot, datetime]] = {}
        self._cache_max_size = 10000
        self._cache_ttl_seconds = 3600  # 1小时TTL
        self._cache_last_cleanup = datetime.now()
        
        # 连接池（读写分离，减少开销）
        # 延迟导入duckdb，避免循环依赖
        # 改进：使用 deque 替代 list（线程安全 pop/popleft），使用 RLock 替代 Lock（同线程可重入）
        global duckdb
        if duckdb is None:
            import duckdb as _duckdb
            duckdb = _duckdb
        self._write_pool: deque = deque()    # 写连接池（可复用）
        self._read_pool: deque = deque()     # 读连接池（可复用）
        self._pool_max = 5
        self._pool_lock = threading.RLock()  # RLock：同线程可重入，更安全
        
        self._init_schema()
        logger.info(f"VersionedStorage initialized: {self.db_path}")
    
    def _init_schema(self):
        """初始化版本化存储表结构"""
        with self._get_connection(read_only=False) as conn:
            # 版本化数据主表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS versioned_data (
                    ts_code        VARCHAR,
                    data_type      VARCHAR,      -- 'price', 'financial', 'valuation'
                    trade_date     DATE,
                    version        INTEGER,
                    recorded_at    TIMESTAMP,    -- 数据入库时间
                    effective_at   TIMESTAMP,    -- 数据生效时间（PIT关键）
                    data_hash      VARCHAR,      -- 完整性校验
                    data_json      JSON,         -- 实际数据
                    source         VARCHAR,      -- 数据来源
                    status         VARCHAR,      -- active/superseded/corrected/retracted
                    superseded_by  INTEGER,
                    correction_note VARCHAR,
                    PRIMARY KEY (ts_code, data_type, trade_date, version)
                )
            """)
            
            # 版本链索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_versioned_lookup 
                ON versioned_data(ts_code, data_type, trade_date, version DESC)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_versioned_effective 
                ON versioned_data(effective_at, ts_code, data_type)
            """)
            
            # 数据变更日志
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_changelog (
                    id             INTEGER PRIMARY KEY,
                    ts_code        VARCHAR,
                    data_type      VARCHAR,
                    trade_date     DATE,
                    old_version    INTEGER,
                    new_version    INTEGER,
                    change_type    VARCHAR,      -- 'insert', 'update', 'correct', 'retract'
                    changed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    changed_by     VARCHAR       -- 变更来源/操作人
                )
            """)
            
            # 数据质量审计表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_audit (
                    id             INTEGER PRIMARY KEY,
                    audit_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ts_code        VARCHAR,
                    data_type      VARCHAR,
                    trade_date     DATE,
                    audit_type     VARCHAR,      -- 'hash_mismatch', 'future_data', 'stale_data'
                    severity       VARCHAR,      -- 'warning', 'error', 'critical'
                    detail         VARCHAR
                )
            """)
    
    def store(self, 
              key: DataSnapshotKey, 
              data: Dict[str, Any],
              effective_at: datetime,
              source: str = "unknown") -> DataSnapshot:
        """
        存储数据快照
        
        Parameters
        ----------
        key : DataSnapshotKey
            数据唯一标识
        data : Dict
            实际数据内容
        effective_at : datetime
            数据生效时间（关键：财务数据用ann_date，价格数据用trade_date）
        source : str
            数据来源标识
            
        Returns
        -------
        DataSnapshot : 创建的快照对象
        """
        # 获取下一个版本号
        next_version = self._get_next_version(key)
        
        # 创建快照
        snapshot = DataSnapshot(
            key=key,
            version=next_version,
            recorded_at=datetime.now(),
            effective_at=effective_at,
            data_hash="",  # 临时，后面计算
            data=data,
            source=source
        )
        snapshot.data_hash = snapshot.compute_hash()
        
        # 如果有旧版本，标记为superseded
        if next_version > 1:
            self._supersede_previous(key, next_version)
        
        # 写入数据库 - 使用连接池
        with self._get_connection(read_only=False) as conn:
            conn.execute("""
                INSERT INTO versioned_data
                (ts_code, data_type, trade_date, version, recorded_at, effective_at,
                 data_hash, data_json, source, status, superseded_by, correction_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key.ts_code, key.data_type, key.trade_date, snapshot.version,
                snapshot.recorded_at, snapshot.effective_at, snapshot.data_hash,
                json.dumps(data, default=str), source, snapshot.status.value,
                snapshot.superseded_by, snapshot.correction_note
            ))
            
            # 记录变更日志
            conn.execute("""
                INSERT INTO data_changelog
                (ts_code, data_type, trade_date, old_version, new_version, 
                 change_type, changed_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                key.ts_code, key.data_type, key.trade_date,
                next_version - 1 if next_version > 1 else None,
                next_version, 'insert', source
            ))
        
        # 更新缓存 - 使用store:前缀区分存储操作缓存
        self._evict_cache_if_needed()
        cache_key = f"store:{key}:v{snapshot.version}"
        self._cache[cache_key] = (snapshot, datetime.now())
        
        logger.debug(f"Stored snapshot: {key} v{snapshot.version}")
        return snapshot
    
    def get_pit(self, 
                key: DataSnapshotKey, 
                as_of: datetime) -> Optional[DataSnapshot]:
        """
        Point-in-Time查询 - 获取指定时间点的数据视图
        
        这是防止未来函数的核心方法。
        
        Parameters
        ----------
        key : DataSnapshotKey
            数据标识
        as_of : datetime
            查询时间点（回测日期）
            
        Returns
        -------
        DataSnapshot or None : 该时间点可见的最新数据
        """
        # 缓存检查 - 使用key+as_of作为缓存键（不包含version，因为PIT查询只返回最新可见版本）
        self._evict_cache_if_needed()  # 先清理过期缓存
        cache_key = f"pit:{key}:{as_of.isoformat()}"
        if cache_key in self._cache:
            snapshot, _ = self._cache[cache_key]
            return snapshot
        
        # 使用连接池
        with self._get_connection(read_only=True) as conn:
            # PIT查询：effective_at < as_of 的最新版本（T日数据T+1才能见）
            # 关键：ann_date是公告日，T日收盘后公告，T+1日才能使用
            result = conn.execute("""
                SELECT * FROM versioned_data
                WHERE ts_code = ?
                  AND data_type = ?
                  AND trade_date = ?
                  AND effective_at < ?
                  AND status = 'active'
                ORDER BY version DESC
                LIMIT 1
            """, (key.ts_code, key.data_type, key.trade_date, as_of)).fetchdf()
            
            if result.empty:
                return None
            
            row = result.iloc[0]
            snapshot = DataSnapshot(
                key=DataSnapshotKey(
                    ts_code=row['ts_code'],
                    data_type=row['data_type'],
                    trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
                ),
                version=row['version'],
                recorded_at=row['recorded_at'],
                effective_at=row['effective_at'],
                data_hash=row['data_hash'],
                data=json.loads(row['data_json']),
                source=row['source'],
                status=DataVersionStatus(row['status']),
                superseded_by=row['superseded_by'],
                correction_note=row['correction_note']
            )
            
            # 缓存 - 带时间戳
            self._cache[cache_key] = (snapshot, datetime.now())
            
            return snapshot
    
    def get_pit_batch(self,
                      ts_codes: List[str],
                      data_type: str,
                      trade_date: str,
                      as_of: datetime) -> Dict[str, Dict]:
        """
        批量PIT查询
        
        Returns
        -------
        Dict[str, Dict] : {ts_code: data}
        """
        if not ts_codes:
            return {}
        
        # 使用连接池查询
        with self._get_connection(read_only=True) as conn:
            # 使用DuckDB的QUALIFY进行高效批量查询
            codes_df = pd.DataFrame({'ts_code': ts_codes})
            conn.register('tmp_codes', codes_df)
            
            result = conn.execute("""
                SELECT v.ts_code, v.data_json, v.version, v.effective_at
                FROM versioned_data v
                INNER JOIN tmp_codes c ON v.ts_code = c.ts_code
                WHERE v.data_type = ?
                  AND v.trade_date = ?
                  AND v.effective_at < ?
                  AND v.status = 'active'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ts_code ORDER BY v.version DESC) = 1
            """, (data_type, trade_date, as_of)).fetchdf()
            
            conn.execute("DROP VIEW IF EXISTS tmp_codes")
            
            if result.empty:
                return {}
            
            return {
                row['ts_code']: {
                    **json.loads(row['data_json']),
                    '_version': row['version'],
                    '_effective_at': row['effective_at'].isoformat() if hasattr(row['effective_at'], 'isoformat') else str(row['effective_at'])
                }
                for _, row in result.iterrows()
            }
    
    def correct(self,
                key: DataSnapshotKey,
                corrected_data: Dict[str, Any],
                correction_note: str,
                corrected_by: str = "system") -> DataSnapshot:
        """
        数据勘误 - 创建修正版本
        
        生产环境中数据错误不可避免，必须有修正机制。
        """
        # 标记旧版本为corrected
        self._mark_status(key, DataVersionStatus.CORRECTED, correction_note)
        
        # 创建新版本
        snapshot = self.store(
            key=key,
            data=corrected_data,
            effective_at=datetime.now(),  # 勘误立即生效
            source=f"{corrected_by}:correction"
        )
        snapshot.correction_note = correction_note
        
        logger.warning(f"Data corrected: {key} - {correction_note}")
        return snapshot
    
    def get_version_history(self, key: DataSnapshotKey) -> List[DataSnapshot]:
        """获取数据版本历史"""
        import duckdb
        
        with self._get_connection(read_only=True) as conn:
            result = conn.execute("""
                SELECT * FROM versioned_data
                WHERE ts_code = ? AND data_type = ? AND trade_date = ?
                ORDER BY version ASC
            """, (key.ts_code, key.data_type, key.trade_date)).fetchdf()
            
            snapshots = []
            for _, row in result.iterrows():
                snapshots.append(DataSnapshot(
                    key=DataSnapshotKey(
                        ts_code=row['ts_code'],
                        data_type=row['data_type'],
                        trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
                    ),
                    version=row['version'],
                    recorded_at=row['recorded_at'],
                    effective_at=row['effective_at'],
                    data_hash=row['data_hash'],
                    data=json.loads(row['data_json']),
                    source=row['source'],
                    status=DataVersionStatus(row['status']),
                    superseded_by=row['superseded_by'],
                    correction_note=row['correction_note']
                ))
            
            return snapshots
    
    def audit_data_integrity(self, 
                            ts_code: Optional[str] = None,
                            data_type: Optional[str] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> Dict:
        """
        数据完整性审计
        
        检查：
        1. 哈希一致性
        2. 未来数据（effective_at > recorded_at超过合理阈值）
        3. 数据缺失
        """
        issues = []
        
        # 构建查询条件
        conditions = ["status = 'active'"]
        params = []
        if ts_code:
            conditions.append("ts_code = ?")
            params.append(ts_code)
        if data_type:
            conditions.append("data_type = ?")
            params.append(data_type)
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)
        
        where_clause = " AND ".join(conditions)
        
        # 读操作：查询待审计数据
        with self._get_connection(read_only=True) as conn:
            result = conn.execute(f"""
                SELECT ts_code, data_type, trade_date, version, 
                       data_hash, data_json, effective_at, recorded_at
                FROM versioned_data
                WHERE {where_clause}
            """, params).fetchdf()
        
        # 计算哈希（内存操作）
        for _, row in result.iterrows():
            data = json.loads(row['data_json'])
            computed_hash = hashlib.sha256(
                json.dumps(data, sort_keys=True, default=str).encode()
            ).hexdigest()[:16]
            
            # 检查哈希
            if computed_hash != row['data_hash']:
                issues.append({
                    'type': 'hash_mismatch',
                    'ts_code': row['ts_code'],
                    'data_type': row['data_type'],
                    'trade_date': row['trade_date'],
                    'severity': 'critical',
                    'detail': f"Hash mismatch: stored={row['data_hash']}, computed={computed_hash}"
                })
            
            # 检查未来数据（财务数据公告日通常不会比报告日晚超过120天）
            if row['data_type'] == 'financial':
                effective = row['effective_at']
                recorded = row['recorded_at']
                if hasattr(effective, 'to_pydatetime'):
                    effective = effective.to_pydatetime()
                if hasattr(recorded, 'to_pydatetime'):
                    recorded = recorded.to_pydatetime()
                
                days_diff = (effective - recorded).days
                if days_diff > 120:
                    issues.append({
                        'type': 'suspicious_future_data',
                        'ts_code': row['ts_code'],
                        'data_type': row['data_type'],
                        'trade_date': row['trade_date'],
                        'severity': 'warning',
                        'detail': f"Effective date {days_diff} days after recorded"
                    })
        
        # 写操作：记录审计结果
        if issues:
            with self._get_connection(read_only=False) as conn:
                for issue in issues:
                    conn.execute("""
                        INSERT INTO data_audit
                        (ts_code, data_type, trade_date, audit_type, severity, detail)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        issue['ts_code'], issue['data_type'], issue['trade_date'],
                        issue['type'], issue['severity'], issue['detail']
                    ))
        
        return {
            'total_checked': len(result),
            'issues_found': len(issues),
            'critical': len([i for i in issues if i['severity'] == 'critical']),
            'warnings': len([i for i in issues if i['severity'] == 'warning']),
            'issues': issues[:10]  # 只返回前10个
        }
    
    def _get_next_version(self, key: DataSnapshotKey) -> int:
        """获取下一个版本号"""
        with self._get_connection(read_only=True) as conn:
            result = conn.execute("""
                SELECT MAX(version) FROM versioned_data
                WHERE ts_code = ? AND data_type = ? AND trade_date = ?
            """, (key.ts_code, key.data_type, key.trade_date)).fetchone()
            
            return (result[0] or 0) + 1
    
    def _supersede_previous(self, key: DataSnapshotKey, new_version: int):
        """标记旧版本为已替代"""
        with self._get_connection(read_only=False) as conn:
            conn.execute("""
                UPDATE versioned_data
                SET status = 'superseded', superseded_by = ?
                WHERE ts_code = ? AND data_type = ? AND trade_date = ?
                  AND version < ? AND status = 'active'
            """, (new_version, key.ts_code, key.data_type, key.trade_date, new_version))
    
    def _mark_status(self, 
                     key: DataSnapshotKey, 
                     status: DataVersionStatus,
                     note: Optional[str] = None):
        """标记数据状态"""
        with self._get_connection(read_only=False) as conn:
            conn.execute("""
                UPDATE versioned_data
                SET status = ?, correction_note = ?
                WHERE ts_code = ? AND data_type = ? AND trade_date = ?
                  AND status = 'active'
            """, (status.value, note, key.ts_code, key.data_type, key.trade_date))
    
    def _evict_cache_if_needed(self):
        """缓存淘汰 - 带TTL清理"""
        now = datetime.now()
        
        # 每5分钟执行一次TTL清理
        if (now - self._cache_last_cleanup).seconds > 300:
            expired_keys = [
                k for k, (snapshot, ts) in self._cache.items()
                if (now - ts).seconds > self._cache_ttl_seconds
            ]
            for k in expired_keys:
                del self._cache[k]
            self._cache_last_cleanup = now
            if expired_keys:
                logger.debug(f"Cache TTL cleanup: {len(expired_keys)} expired")
        
        # 容量超限清理（LRU）
        if len(self._cache) > self._cache_max_size:
            # 按时间排序，移除最早的10%
            sorted_items = sorted(self._cache.items(), key=lambda x: x[1][1])
            keys_to_remove = [k for k, _ in sorted_items[:self._cache_max_size // 10]]
            for k in keys_to_remove:
                del self._cache[k]
            logger.debug(f"Cache LRU cleanup: {len(keys_to_remove)} evicted")
    
    @contextlib.contextmanager
    def _get_connection(self, read_only: bool = False):
        """
        从连接池获取连接（上下文管理器）

        读写分离：写连接和读连接各自独立池，完全隔离，避免 read_only=True 的连接
        被错误地用于写入操作导致崩溃。

        线程安全改进：
        - 使用 RLock（同线程可重入）
        - 使用 deque 替代 list（线程安全 popleft）
        - 所有池操作在锁内完成
        """
        conn = None
        from_pool = False
        pool_key = 'read' if read_only else 'write'

        with self._pool_lock:
            pool = self._read_pool if read_only else self._write_pool
            if pool:
                conn = pool.popleft()  # deque popleft 是线程安全的 O(1) 操作
                from_pool = True

        # 池为空，创建新连接（在锁外创建，减少锁持有时间）
        if conn is None:
            conn = duckdb.connect(str(self.db_path), read_only=read_only)

        try:
            yield conn
        finally:
            with self._pool_lock:
                pool = self._read_pool if read_only else self._write_pool
                if from_pool and len(pool) < self._pool_max:
                    pool.append(conn)  # deque append 是线程安全的
                else:
                    conn.close()
    
    def close(self):
        """关闭所有连接池连接"""
        with self._pool_lock:
            while self._write_pool:
                try:
                    self._write_pool.popleft().close()
                except Exception:
                    pass
            while self._read_pool:
                try:
                    self._read_pool.popleft().close()
                except Exception:
                    pass
        logger.info("Connection pools closed")


class FinancialDataPITManager:
    """
    财务数据PIT管理器
    
    专门处理财务数据的Point-in-Time约束：
    - 使用ann_date（公告日）作为effective_at
    - 确保回测时只能看到已公告的数据
    """
    
    def __init__(self, storage: VersionedStorage):
        self.storage = storage
    
    def store_financial(self,
                        ts_code: str,
                        end_date: str,          # 报告期，如 '2025-12-31'
                        ann_date: str,          # 公告日，如 '2026-03-25'
                        report_type: str,       # 'Q1', 'Q2', 'Q3', '年报'
                        financials: Dict[str, float]):
        """
        存储财务数据（PIT约束）
        
        关键：effective_at = ann_date（公告日）
        这样回测到2026-01-01时，看不到2025年报（因为还没公告）
        """
        key = DataSnapshotKey(
            ts_code=ts_code,
            data_type='financial',
            trade_date=end_date  # 用报告期作为trade_date标识
        )
        
        data = {
            'report_type': report_type,
            **financials
        }
        
        return self.storage.store(
            key=key,
            data=data,
            effective_at=datetime.strptime(ann_date, '%Y-%m-%d'),
            source='financial_report'
        )
    
    def get_financial_pit(self,
                          ts_code: str,
                          as_of_date: str,        # 回测日期
                          lookback_days: int = 180) -> Optional[Dict]:
        """
        获取PIT财务数据
        
        返回as_of_date之前最新已公告的财务数据
        """
        as_of = datetime.strptime(as_of_date, '%Y-%m-%d')
        
        # 使用连接池查询
        with self.storage._get_connection(read_only=True) as conn:
            result = conn.execute("""
                SELECT trade_date, data_json, effective_at, version
                FROM versioned_data
                WHERE ts_code = ?
                  AND data_type = 'financial'
                  AND effective_at < ?
                  AND status = 'active'
                ORDER BY effective_at DESC, version DESC
                LIMIT 1
            """, (ts_code, as_of)).fetchdf()
            
            if result.empty:
                return None
            
            row = result.iloc[0]
            data = json.loads(row['data_json'])
            data['_report_period'] = row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
            data['_ann_date'] = row['effective_at'].isoformat() if hasattr(row['effective_at'], 'isoformat') else str(row['effective_at'])
            
            return data
    
    def get_batch_financial_pit(self,
                                 ts_codes: List[str],
                                 as_of_date: str) -> Dict[str, Dict]:
        """批量获取PIT财务数据"""
        as_of = datetime.strptime(as_of_date, '%Y-%m-%d')
        
        # 使用连接池查询
        with self.storage._get_connection(read_only=True) as conn:
            codes_df = pd.DataFrame({'ts_code': ts_codes})
            conn.register('tmp_codes', codes_df)
            
            result = conn.execute("""
                SELECT v.ts_code, v.trade_date, v.data_json, v.effective_at
                FROM versioned_data v
                INNER JOIN tmp_codes c ON v.ts_code = c.ts_code
                WHERE v.data_type = 'financial'
                  AND v.effective_at < ?
                  AND v.status = 'active'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ts_code ORDER BY v.effective_at DESC, v.version DESC) = 1
            """, (as_of,)).fetchdf()
            
            conn.execute("DROP VIEW IF EXISTS tmp_codes")
            
            if result.empty:
                return {}
            
            return {
                row['ts_code']: {
                    **json.loads(row['data_json']),
                    '_report_period': row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                    '_ann_date': row['effective_at'].isoformat() if hasattr(row['effective_at'], 'isoformat') else str(row['effective_at'])
                }
                for _, row in result.iterrows()
            }


# 便捷函数
def create_versioned_storage(db_path: Optional[str] = None) -> VersionedStorage:
    """创建版本化存储实例"""
    return VersionedStorage(db_path)
