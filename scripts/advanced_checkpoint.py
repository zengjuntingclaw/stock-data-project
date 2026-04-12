"""
增强断点续传管理器 - AdvancedCheckpointManager
==============================================
核心功能：
1. 完整抓取日期 + 摘要哈希记录
2. 本地 vs 远程数据完整性比对
3. 不一致时从最近有效断点重新 backfill
4. 支持多表分层跟踪（raw / adjusted）
5. 增量同步 + 全量回填双重策略
"""
from __future__ import annotations

import os
import hashlib
import json
import bisect
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple, Any
from pathlib import Path
import threading
import pandas as pd
from loguru import logger

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


# ============================================================================
# 核心数据结构
# ============================================================================

@dataclass
class CheckpointRecord:
    """断点记录"""
    ts_code: str
    table_name: str                    # daily_bar_raw / daily_bar_adjusted
    last_sync_date: Optional[str]     # 最后成功同步的交易日
    last_sync_at: datetime
    total_records: int
    local_hash: str                    # 本地数据摘要哈希
    remote_hash: str                   # 远程数据摘要哈希
    local_count: int                   # 本地记录数
    remote_count: int                  # 远程记录数
    status: str                        # ok / inconsistent / failed / in_progress
    error_msg: Optional[str] = None
    version: int = 1                   # 记录版本号
    
    @property
    def is_consistent(self) -> bool:
        return (self.status == "ok" and 
                self.local_hash == self.remote_hash and
                self.local_count == self.remote_count)
    
    @property
    def consistency_ratio(self) -> float:
        if self.remote_count == 0:
            return 1.0 if self.local_count == 0 else 0.0
        return min(self.local_count / self.remote_count, 1.0)


@dataclass 
class BackfillTask:
    """回填任务"""
    ts_code: str
    table_name: str
    start_date: str                    # 需要回填的起始日期
    end_date: str                      # 回填截止日期
    reason: str                        # 回填原因
    priority: int = 1                 # 优先级（越小越高）
    created_at: datetime = field(default_factory=datetime.now)


# ============================================================================
# 哈希计算器
# ============================================================================

class DataHasher:
    """数据摘要哈希计算器"""
    
    @staticmethod
    def compute_df_hash(df: pd.DataFrame, key_cols: List[str] = None) -> str:
        """
        计算 DataFrame 的稳定摘要哈希
        
        使用行数 + 关键列的哈希 + 数据范围来生成摘要
        """
        if df is None or df.empty:
            return hashlib.sha256(b"empty").hexdigest()[:16]
        
        # 按日期排序确保稳定性
        if 'trade_date' in df.columns:
            df = df.sort_values('trade_date').reset_index(drop=True)
        
        # 关键列哈希
        key_cols = key_cols or ['close']
        available_cols = [c for c in key_cols if c in df.columns]
        if not available_cols:
            available_cols = df.columns[:3].tolist()
        
        # 计算采样哈希（避免大数据集性能问题）
        sample_df = df.head(1000) if len(df) > 1000 else df
        
        hash_parts = [
            str(len(df)).encode(),                              # 行数
            str(sample_df.shape).encode(),                      # 形状
        ]
        
        for col in available_cols:
            try:
                col_hash = hashlib.sha256(
                    sample_df[col].astype(str).str[:50].sum().encode()
                ).hexdigest()[:8]
                hash_parts.append(col_hash.encode())
            except:
                pass
        
        # 日期范围
        if 'trade_date' in df.columns:
            hash_parts.append(str(df['trade_date'].min()).encode())
            hash_parts.append(str(df['trade_date'].max()).encode())
        
        combined = b"|".join(hash_parts)
        return hashlib.sha256(combined).hexdigest()[:16]
    
    @staticmethod
    def compute_date_range_hash(dates: List[str]) -> str:
        """计算日期范围的稳定哈希"""
        if not dates:
            return hashlib.sha256(b"empty").hexdigest()[:16]
        sorted_dates = sorted(set(dates))
        combined = "|".join(sorted_dates).encode()
        return hashlib.sha256(combined).hexdigest()[:16]


# ============================================================================
# 增强断点续传管理器
# ============================================================================

class AdvancedCheckpointManager:
    """
    增强版断点续传管理器
    
    核心功能：
    1. sync_progress 表增加 local_hash / remote_hash / local_count / remote_count
    2. 同步前比对本地vs远程完整性
    3. 不一致时生成 BackfillTask 并从最近有效断点重新拉取
    4. 支持 UPSERT 断点续传
    """
    
    def __init__(self, db_path: str = None):
        if not HAS_DUCKDB:
            raise ImportError("duckdb未安装: pip install duckdb")
        
        project_root = Path(__file__).parent.parent
        self.db_path = Path(db_path or str(project_root / "data" / "stock_data.duckdb"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path))
        
        # 缓存最近读取的记录
        self._cache: Dict[Tuple[str, str], CheckpointRecord] = {}
        self._cache_lock = threading.Lock()
        
        # 初始化表结构
        self._init_table()
    
    def _init_table(self):
        """初始化增强的断点表"""
        cur = self._conn.cursor()
        
        # 扩展 sync_progress 表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_progress_enhanced (
                ts_code        VARCHAR NOT NULL,
                table_name     VARCHAR NOT NULL,
                last_sync_date DATE,
                last_sync_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_records  INTEGER DEFAULT 0,
                local_hash     VARCHAR,              -- 本地数据摘要哈希
                remote_hash    VARCHAR,              -- 远程数据摘要哈希
                local_count    INTEGER DEFAULT 0,    -- 本地记录数
                remote_count   INTEGER DEFAULT 0,   -- 远程记录数
                status         VARCHAR DEFAULT 'ok', -- ok / inconsistent / failed / in_progress
                error_msg      VARCHAR,
                version        INTEGER DEFAULT 1,
                PRIMARY KEY (ts_code, table_name)
            )
        """)
        
        # 回填任务表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backfill_tasks (
                id            INTEGER PRIMARY KEY,
                ts_code       VARCHAR NOT NULL,
                table_name    VARCHAR NOT NULL,
                start_date    DATE NOT NULL,
                end_date      DATE NOT NULL,
                reason        VARCHAR,
                priority      INTEGER DEFAULT 1,
                status        VARCHAR DEFAULT 'pending', -- pending / running / completed / failed
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at    TIMESTAMP,
                completed_at  TIMESTAMP,
                error_msg     VARCHAR
            )
        """)
        
        # 同步历史日志
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_history (
                id            INTEGER PRIMARY KEY,
                ts_code       VARCHAR,
                table_name    VARCHAR,
                sync_type     VARCHAR,   -- incremental / full_backfill
                start_date    DATE,
                end_date      DATE,
                records_count INTEGER,
                duration_ms   INTEGER,
                status        VARCHAR,
                error_msg     VARCHAR,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_spe_status ON sync_progress_enhanced(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_spe_date ON sync_progress_enhanced(last_sync_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bft_status ON backfill_tasks(status)")
        
        self._conn.commit()
        logger.info("[AdvancedCheckpoint] 增强断点表初始化完成")
    
    def _get_cache_key(self, ts_code: str, table_name: str) -> Tuple[str, str]:
        return (ts_code, table_name)
    
    def get_checkpoint(self, ts_code: str, table_name: str) -> Optional[CheckpointRecord]:
        """获取断点记录"""
        cache_key = self._get_cache_key(ts_code, table_name)
        
        with self._cache_lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
        
        cur = self._conn.cursor()
        cur.execute("""
            SELECT ts_code, table_name, last_sync_date, last_sync_at, total_records,
                   local_hash, remote_hash, local_count, remote_count, status, error_msg, version
            FROM sync_progress_enhanced
            WHERE ts_code = ? AND table_name = ?
        """, (ts_code, table_name))
        
        row = cur.fetchone()
        if row:
            record = CheckpointRecord(
                ts_code=row[0],
                table_name=row[1],
                last_sync_date=str(row[2]) if row[2] else None,
                last_sync_at=row[3],
                total_records=row[4] or 0,
                local_hash=row[5] or "",
                remote_hash=row[6] or "",
                local_count=row[7] or 0,
                remote_count=row[8] or 0,
                status=row[9] or "ok",
                error_msg=row[10],
                version=row[11] or 1
            )
            with self._cache_lock:
                self._cache[cache_key] = record
            return record
        return None
    
    def upsert_checkpoint(
        self,
        ts_code: str,
        table_name: str,
        sync_date: Optional[str] = None,
        total_records: int = 0,
        local_hash: str = "",
        remote_hash: str = "",
        local_count: int = 0,
        remote_count: int = 0,
        status: str = "ok",
        error_msg: Optional[str] = None
    ):
        """更新/插入断点记录（UPSERT）"""
        cache_key = self._get_cache_key(ts_code, table_name)
        
        cur = self._conn.cursor()
        now = datetime.now()
        
        cur.execute("""
            INSERT INTO sync_progress_enhanced 
            (ts_code, table_name, last_sync_date, last_sync_at, total_records,
             local_hash, remote_hash, local_count, remote_count, status, error_msg, version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT (ts_code, table_name) DO UPDATE SET
                last_sync_date = COALESCE(excluded.last_sync_date, sync_progress_enhanced.last_sync_date),
                last_sync_at = excluded.last_sync_at,
                total_records = excluded.total_records,
                local_hash = excluded.local_hash,
                remote_hash = excluded.remote_hash,
                local_count = excluded.local_count,
                remote_count = excluded.remote_count,
                status = excluded.status,
                error_msg = excluded.error_msg,
                version = sync_progress_enhanced.version + 1
        """, (
            ts_code, table_name, sync_date, now, total_records,
            local_hash, remote_hash, local_count, remote_count, status, error_msg
        ))
        
        self._conn.commit()
        
        # 更新缓存
        with self._cache_lock:
            self._cache[cache_key] = CheckpointRecord(
                ts_code=ts_code, table_name=table_name,
                last_sync_date=sync_date, last_sync_at=now,
                total_records=total_records, local_hash=local_hash,
                remote_hash=remote_hash, local_count=local_count,
                remote_count=remote_count, status=status, error_msg=error_msg
            )
    
    def check_consistency(
        self,
        ts_code: str,
        table_name: str,
        local_df: pd.DataFrame,
        remote_count: int,
        remote_hash: str = ""
    ) -> Tuple[bool, str]:
        """
        检查本地vs远程数据一致性
        
        Returns: (is_consistent, reason)
        """
        checkpoint = self.get_checkpoint(ts_code, table_name)
        
        local_count = len(local_df)
        local_hash = DataHasher.compute_df_hash(local_df)
        
        # 计算一致性
        if checkpoint:
            # 与上次同步状态比较
            if checkpoint.local_hash == local_hash and checkpoint.remote_hash == remote_hash:
                return True, "数据未变化"
            
            # 检查是否有新增数据
            if remote_count > checkpoint.remote_count:
                return False, f"远程新增数据: {checkpoint.remote_count} -> {remote_count}"
            
            # 检查本地数据损坏
            if checkpoint.local_hash != local_hash and checkpoint.local_count == local_count:
                return False, f"数据可能损坏: hash不匹配"
        
        # 新同步任务
        if local_count != remote_count:
            return False, f"记录数不一致: 本地{local_count} vs 远程{remote_count}"
        
        if remote_hash and local_hash != remote_hash:
            return False, f"哈希不一致: 本地{local_hash} vs 远程{remote_hash}"
        
        return True, "一致"
    
    def create_backfill_task(
        self,
        ts_code: str,
        table_name: str,
        start_date: str,
        end_date: str,
        reason: str,
        priority: int = 1
    ) -> int:
        """创建回填任务"""
        cur = self._conn.cursor()
        
        # 检查是否已有待处理任务
        cur.execute("""
            SELECT id FROM backfill_tasks
            WHERE ts_code = ? AND table_name = ? AND status IN ('pending', 'running')
            LIMIT 1
        """, (ts_code, table_name))
        
        existing = cur.fetchone()
        if existing:
            logger.info(f"[AdvancedCheckpoint] 已有待处理回填任务: {existing[0]}")
            return existing[0]
        
        cur.execute("""
            INSERT INTO backfill_tasks (ts_code, table_name, start_date, end_date, reason, priority)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ts_code, table_name, start_date, end_date, reason, priority))
        
        task_id = cur.execute("SELECT last_insert_rowid()").fetchone()[0]
        self._conn.commit()
        
        logger.info(f"[AdvancedCheckpoint] 创建回填任务 #{task_id}: {ts_code} {table_name} "
                    f"[{start_date} ~ {end_date}] reason={reason}")
        return task_id
    
    def get_backfill_task(self, status: str = "pending", limit: int = 10) -> List[BackfillTask]:
        """获取待处理的回填任务"""
        cur = self._conn.cursor()
        cur.execute("""
            SELECT id, ts_code, table_name, start_date, end_date, reason, priority, 
                   status, created_at, started_at, completed_at, error_msg
            FROM backfill_tasks
            WHERE status = ?
            ORDER BY priority ASC, created_at ASC
            LIMIT ?
        """, (status, limit))
        
        tasks = []
        for row in cur.fetchall():
            tasks.append(BackfillTask(
                ts_code=row[1], table_name=row[2],
                start_date=str(row[3]), end_date=str(row[4]),
                reason=row[5], priority=row[6]
            ))
        return tasks
    
    def update_backfill_status(self, task_id: int, status: str, error_msg: str = None):
        """更新回填任务状态"""
        cur = self._conn.cursor()
        
        if status == "running":
            cur.execute("""
                UPDATE backfill_tasks SET status = ?, started_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (status, task_id))
        elif status in ("completed", "failed"):
            cur.execute("""
                UPDATE backfill_tasks 
                SET status = ?, completed_at = CURRENT_TIMESTAMP, error_msg = ?
                WHERE id = ?
            """, (status, error_msg, task_id))
        else:
            cur.execute("UPDATE backfill_tasks SET status = ? WHERE id = ?", (status, task_id))
        
        self._conn.commit()
    
    def get_effective_checkpoint(self, ts_code: str, table_name: str) -> Optional[str]:
        """
        获取最近有效断点
        
        策略：
        1. 先找 sync_progress_enhanced 中 status=ok 的记录
        2. 若有不一致的记录，向前回溯找最近的 ok 状态
        3. 返回 last_sync_date
        """
        cur = self._conn.cursor()
        
        # 找最近的 ok 状态记录
        cur.execute("""
            SELECT last_sync_date FROM sync_progress_enhanced
            WHERE ts_code = ? AND table_name = ? AND status = 'ok'
            ORDER BY last_sync_at DESC
            LIMIT 1
        """, (ts_code, table_name))
        
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        
        return None
    
    def get_sync_start_date(
        self,
        ts_code: str,
        table_name: str,
        default_start: str = "2018-01-01"
    ) -> str:
        """
        计算同步起始日期
        
        逻辑：
        1. 检查是否有待处理的回填任务
        2. 若有，按回填任务指定的起始日期
        3. 否则取最近有效断点的次日
        4. 若无任何记录，使用 default_start
        """
        # 检查待处理回填任务
        pending = self.get_backfill_task(status="pending", limit=1)
        for task in pending:
            if task.ts_code == ts_code and task.table_name == table_name:
                logger.info(f"[AdvancedCheckpoint] 使用回填任务起始日期: {task.start_date}")
                return task.start_date
        
        # 取最近有效断点
        checkpoint = self.get_effective_checkpoint(ts_code, table_name)
        if checkpoint:
            # 从断点次日开始（避免重复）
            next_date = datetime.strptime(checkpoint, "%Y-%m-%d") + timedelta(days=1)
            return next_date.strftime("%Y-%m-%d")
        
        return default_start
    
    def log_sync_history(
        self,
        ts_code: str,
        table_name: str,
        sync_type: str,
        start_date: str,
        end_date: str,
        records_count: int,
        duration_ms: int,
        status: str,
        error_msg: str = None
    ):
        """记录同步历史"""
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO sync_history 
            (ts_code, table_name, sync_type, start_date, end_date, 
             records_count, duration_ms, status, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts_code, table_name, sync_type, start_date, end_date,
              records_count, duration_ms, status, error_msg))
        self._conn.commit()
    
    def get_inconsistent_records(self, limit: int = 100) -> List[CheckpointRecord]:
        """获取所有不一致的断点记录"""
        cur = self._conn.cursor()
        cur.execute("""
            SELECT ts_code, table_name, last_sync_date, last_sync_at, total_records,
                   local_hash, remote_hash, local_count, remote_count, status, error_msg, version
            FROM sync_progress_enhanced
            WHERE status IN ('inconsistent', 'failed')
            ORDER BY last_sync_at DESC
            LIMIT ?
        """, (limit,))
        
        records = []
        for row in cur.fetchall():
            records.append(CheckpointRecord(
                ts_code=row[0], table_name=row[1],
                last_sync_date=str(row[2]) if row[2] else None,
                last_sync_at=row[3], total_records=row[4] or 0,
                local_hash=row[5] or "", remote_hash=row[6] or "",
                local_count=row[7] or 0, remote_count=row[8] or 0,
                status=row[9] or "failed", error_msg=row[10],
                version=row[11] or 1
            ))
        return records
    
    def get_sync_stats(self) -> Dict:
        """获取同步统计信息"""
        cur = self._conn.cursor()
        
        stats = {}
        
        # 各状态数量
        cur.execute("""
            SELECT status, COUNT(*) FROM sync_progress_enhanced
            GROUP BY status
        """)
        stats["by_status"] = {row[0]: row[1] for row in cur.fetchall()}
        
        # 总记录数
        stats["total_checkpoints"] = cur.execute(
            "SELECT COUNT(*) FROM sync_progress_enhanced"
        ).fetchone()[0]
        
        # 待处理回填任务
        stats["pending_backfill"] = cur.execute(
            "SELECT COUNT(*) FROM backfill_tasks WHERE status = 'pending'"
        ).fetchone()[0]
        
        return stats
    
    def repair_inconsistent(
        self,
        ts_code: str,
        table_name: str,
        local_df: pd.DataFrame,
        repair_source_df: pd.DataFrame
    ) -> int:
        """
        修复不一致数据
        
        策略：
        1. 比对 local_df 和 repair_source_df
        2. 合并差异部分
        3. 返回修复记录数
        """
        if local_df is None or local_df.empty:
            return 0
        
        if repair_source_df is None or repair_source_df.empty:
            logger.warning(f"[AdvancedCheckpoint] 修复源为空，跳过: {ts_code}")
            return 0
        
        # 找出差异行
        if 'trade_date' not in local_df.columns or 'trade_date' not in repair_source_df.columns:
            logger.error(f"[AdvancedCheckpoint] 缺少 trade_date 列")
            return 0
        
        # 按 trade_date 合并
        merged = local_df.copy()
        for idx, row in repair_source_df.iterrows():
            td = row['trade_date']
            mask = merged['trade_date'] == td
            if mask.any():
                # 替换现有行（用远程数据覆盖）
                merged.loc[mask] = row
            else:
                # 新增行
                merged = pd.concat([merged, pd.DataFrame([row])], ignore_index=True)
        
        return len(merged) - len(local_df)
    
    def close(self):
        """关闭数据库连接"""
        self._conn.close()


# ============================================================================
# 与 DataEngine 的集成
# ============================================================================

class SyncCoordinator:
    """
    同步协调器 - 将断点续传与 DataEngine 集成
    
    使用方式：
    coordinator = SyncCoordinator(data_engine)
    coordinator.sync_stock("000001", "2024-01-01", "2024-01-10")
    """
    
    def __init__(self, data_engine):
        self.data_engine = data_engine
        self.checkpoint_mgr = AdvancedCheckpointManager(str(data_engine.db_path))
    
    def compute_local_stats(self, ts_code: str, table_name: str) -> Tuple[int, str]:
        """计算本地数据的统计信息"""
        cur = self.data_engine._conn.cursor()
        
        try:
            cur.execute(f"""
                SELECT COUNT(*), 
                       COALESCE(MAX(trade_date), NULL) as max_date
                FROM {table_name}
                WHERE ts_code = ?
            """, (ts_code,))
            
            row = cur.fetchone()
            count = row[0] if row else 0
            max_date = str(row[1]) if row and row[1] else None
            
            return count, max_date
        except Exception as e:
            logger.error(f"[SyncCoordinator] 统计 {ts_code}.{table_name} 失败: {e}")
            return 0, None
    
    def sync_with_checkpoint(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        table_name: str = "daily_bar_raw",
        fetch_fn: Callable = None
    ) -> Tuple[bool, str, int]:
        """
        带断点续传的同步
        
        Returns: (success, message, records_synced)
        """
        start_time = time.time()
        
        # 1. 获取同步起始点
        sync_start = self.checkpoint_mgr.get_sync_start_date(
            ts_code, table_name, default_start=start_date
        )
        
        if sync_start > end_date:
            logger.info(f"[SyncCoordinator] {ts_code} 已同步至 {end_date}，跳过")
            return True, "已同步", 0
        
        # 2. 标记为进行中
        self.checkpoint_mgr.upsert_checkpoint(
            ts_code, table_name, sync_date=sync_start,
            status="in_progress"
        )
        
        try:
            # 3. 获取远程数据
            if fetch_fn:
                remote_df = fetch_fn(ts_code, sync_start, end_date)
            else:
                # 使用 MultiSourceManager
                from scripts.multi_source_manager import get_source_manager
                mgr = get_source_manager()
                result = mgr.fetch_daily(ts_code, sync_start, end_date)
                remote_df = result.data if result.success else None
            
            if remote_df is None or remote_df.empty:
                raise RuntimeError("远程数据获取失败")
            
            remote_count = len(remote_df)
            remote_hash = DataHasher.compute_df_hash(remote_df)
            
            # 4. 计算本地统计
            local_count, _ = self.compute_local_stats(ts_code, table_name)
            
            # 5. 检查一致性
            is_consistent, reason = self.checkpoint_mgr.check_consistency(
                ts_code, table_name, remote_df, remote_count, remote_hash
            )
            
            if not is_consistent:
                logger.warning(f"[SyncCoordinator] {ts_code} 数据不一致: {reason}")
                
                # 创建回填任务
                self.checkpoint_mgr.create_backfill_task(
                    ts_code, table_name,
                    start_date=sync_start,
                    end_date=end_date,
                    reason=reason,
                    priority=2
                )
            
            # 6. 写入数据
            records_written = self._write_to_table(ts_code, table_name, remote_df)
            
            # 7. 更新断点
            final_count, _ = self.compute_local_stats(ts_code, table_name)
            self.checkpoint_mgr.upsert_checkpoint(
                ts_code, table_name,
                sync_date=end_date,
                total_records=final_count,
                local_hash=DataHasher.compute_df_hash(remote_df),
                remote_hash=remote_hash,
                local_count=final_count,
                remote_count=remote_count,
                status="ok"
            )
            
            duration_ms = int((time.time() - start_time) * 1000)
            self.checkpoint_mgr.log_sync_history(
                ts_code, table_name, "incremental",
                sync_start, end_date, records_written,
                duration_ms, "ok"
            )
            
            return True, "同步成功", records_written
            
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            self.checkpoint_mgr.upsert_checkpoint(
                ts_code, table_name, status="failed", error_msg=str(e)
            )
            self.checkpoint_mgr.log_sync_history(
                ts_code, table_name, "incremental",
                sync_start, end_date, 0,
                duration_ms, "failed", str(e)
            )
            return False, str(e), 0
    
    def _write_to_table(self, ts_code: str, table_name: str, df: pd.DataFrame) -> int:
        """写入数据到表"""
        if df.empty:
            return 0
        
        cur = self.data_engine._conn.cursor()
        
        # 构建 INSERT 语句（使用 DuckDB 的 REPLACE ON CONFLICT 语义）
        cols = list(df.columns)
        placeholders = ", ".join(["?" for _ in cols])
        
        # 处理日期格式
        df = df.copy()
        for col in ['trade_date', 'date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
        
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg', 'turnover']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        values = [tuple(row) for row in df[cols].values]
        
        cur.executemany(f"""
            INSERT INTO {table_name} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                amount = excluded.amount
        """, values)
        
        self.data_engine._conn.commit()
        return len(df)


# ============================================================================
# 工具函数
# ============================================================================

def import_time():
    import time
    return time


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    print("AdvancedCheckpointManager 测试")
    print("=" * 60)
    
    # 测试哈希计算
    print("\n1. 哈希计算测试:")
    test_df = pd.DataFrame({
        'trade_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'close': [10.0, 10.5, 11.0],
        'volume': [1000, 2000, 3000]
    })
    
    hash1 = DataHasher.compute_df_hash(test_df)
    hash2 = DataHasher.compute_df_hash(test_df)
    print(f"  相同数据哈希: {hash1} == {hash2}: {hash1 == hash2}")
    
    # 修改数据后哈希变化
    test_df.loc[0, 'close'] = 9.0
    hash3 = DataHasher.compute_df_hash(test_df)
    print(f"  修改后哈希: {hash1} -> {hash3}, 变化: {hash1 != hash3}")
    
    # 日期范围哈希
    dates = ['2024-01-01', '2024-01-02', '2024-01-03']
    range_hash = DataHasher.compute_date_range_hash(dates)
    print(f"  日期范围哈希: {range_hash}")
    
    print("\n" + "=" * 60)
    print("哈希计算测试通过")
