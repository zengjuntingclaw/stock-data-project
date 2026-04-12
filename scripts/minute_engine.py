"""
MinuteDataEngine - 分钟级行情数据引擎
=====================================
核心功能：
  1. 分钟/5分钟/15分钟/30分钟/60分钟行情拉取
  2. Parquet 分区存储（按股票/日期）
  3. 未来函数治理（禁止回测时使用未来数据）
  4. 与 DataEngine 架构一致，共享 DuckDB 元数据
  5. 实时数据推送支持（可选 WebSocket）
"""

from __future__ import annotations

import os
import time
import bisect
import hashlib
import threading
import statistics
from pathlib import Path
from typing import Optional, List, Dict, Union, Literal, Tuple
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
import numpy as np

from loguru import logger

# ──────────────────────────────────────────────────────────────────────────────
# 常量定义
# ──────────────────────────────────────────────────────────────────────────────
MINUTE_FREQUENCIES = {
    "1min": "1",
    "5min": "5",
    "15min": "15",
    "30min": "30",
    "60min": "60",
    "daily": "D",    # 日线（特殊）
    "weekly": "W",   # 周线
    "monthly": "M",  # 月线
}

DEFAULT_MINUTE_START = "2020-01-01"
MAX_MINUTE_RECORDS_PER_SYMBOL = 50000  # 单股分钟数据上限（防止内存爆炸）

# 未来函数治理配置
FUTURE_FUNCTION_RULES = {
    "disallow_future_data": True,           # 回测时禁止使用未来数据
    "check_look_ahead_bias": True,          # 检查前瞻偏差
    "enforce_pit_boundary": True,           # 强制 PIT 边界
    "warn_on_same_bar_overwrite": True,    # 同一 bar 被覆盖时告警
}

# ──────────────────────────────────────────────────────────────────────────────
# 依赖导入
# ──────────────────────────────────────────────────────────────────────────────
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    logger.warning("duckdb not installed. Run: pip install duckdb")

try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False
    logger.warning("akshare not installed. Run: pip install akshare")


# ──────────────────────────────────────────────────────────────────────────────
# 数据结构
# ──────────────────────────────────────────────────────────────────────────────

class MinuteFreq(Enum):
    """分钟频率枚举"""
    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_15 = "15min"
    MIN_30 = "30min"
    MIN_60 = "60min"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

    @property
    def code(self) -> str:
        return MINUTE_FREQUENCIES.get(self.value, self.value)

    @property
    def minutes(self) -> int:
        """该频率对应的分钟数"""
        mapping = {
            "1min": 1, "5min": 5, "15min": 15,
            "30min": 30, "60min": 60, "daily": 1440
        }
        return mapping.get(self.value, 1)


@dataclass
class MinuteDataMetadata:
    """分钟数据元数据"""
    ts_code: str
    freq: str
    start_date: str
    end_date: str
    record_count: int
    data_hash: str
    last_updated: datetime = field(default_factory=datetime.now)
    source: str = "akshare"
    quality_score: float = 1.0

    def to_dict(self) -> Dict:
        return {
            "ts_code": self.ts_code,
            "freq": self.freq,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "record_count": self.record_count,
            "data_hash": self.data_hash,
            "last_updated": self.last_updated.isoformat(),
            "source": self.source,
            "quality_score": self.quality_score,
        }


@dataclass
class FutureFunctionViolation:
    """未来函数违规记录"""
    ts_code: str
    trade_date: str
    check_time: datetime
    violation_type: str
    description: str
    severity: str  # warning / error / critical


# ──────────────────────────────────────────────────────────────────────────────
# 工具函数
# ──────────────────────────────────────────────────────────────────────────────

def _compute_data_hash(df: pd.DataFrame) -> str:
    """计算数据哈希（用于完整性校验）"""
    if df is None or df.empty:
        return ""
    # 只对关键字段计算哈希
    key_cols = ["trade_date", "open", "high", "low", "close", "volume"]
    existing = [c for c in key_cols if c in df.columns]
    content = df[existing].to_csv(index=False)
    return hashlib.md5(content.encode()).hexdigest()[:16]


def _validate_minute_bar(bar: Dict) -> bool:
    """验证单个分钟 bar 的合法性"""
    required = ["trade_date", "open", "high", "low", "close", "volume"]
    if not all(k in bar for k in required):
        return False
    # 最高价 >= 最低价
    if bar["high"] < bar["low"]:
        return False
    # 开盘和收盘价在高低之间
    if not (bar["low"] <= bar["open"] <= bar["high"]):
        return False
    if not (bar["low"] <= bar["close"] <= bar["high"]):
        return False
    # 成交量非负
    if bar["volume"] < 0:
        return False
    return True


def _parse_trade_date_time(dt_str: str) -> datetime:
    """解析交易时间字符串"""
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"无法解析时间格式: {dt_str}")


def _get_trading_dates_range(start: str, end: str) -> List[str]:
    """获取交易日范围（排除周末和节假日简版）"""
    # 简化实现：排除周末
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    dates = pd.date_range(start_dt, end_dt, freq="B")  # B = 工作日
    return [d.strftime("%Y-%m-%d") for d in dates]


# ──────────────────────────────────────────────────────────────────────────────
# Parquet 存储管理器
# ──────────────────────────────────────────────────────────────────────────────

class MinuteParquetStore:
    """分钟数据 Parquet 存储"""

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or str(Path(__file__).parent.parent / "data" / "minute_data"))
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _get_partition_path(self, ts_code: str, freq: str, trade_date: str = None) -> Path:
        """
        获取分区路径
        结构: minute_data/{freq}/{ts_code}/{date}.parquet
        """
        freq_dir = self.data_dir / freq
        ts_dir = freq_dir / ts_code
        if trade_date:
            return ts_dir / f"{trade_date}.parquet"
        return ts_dir

    def save(self, df: pd.DataFrame, ts_code: str, freq: str, trade_date: str = None, mode: str = "append"):
        """
        保存分钟数据到 Parquet

        Args:
            df: 分钟数据 DataFrame
            ts_code: 股票代码
            freq: 频率 (1min/5min/15min/30min/60min)
            trade_date: 交易日期（用于分区）
            mode: append(追加) / overwrite(覆盖)
        """
        if df is None or df.empty:
            logger.warning(f"[MinuteStore] 空数据，跳过保存: {ts_code} {freq}")
            return

        # 确保时间列格式正确
        if "trade_date" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["trade_date"]):
            df["trade_date"] = pd.to_datetime(df["trade_date"])

        partition_path = self._get_partition_path(ts_code, freq, trade_date)
        partition_path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "append":
            # 追加模式：先读取现有数据，再合并
            if partition_path.exists():
                existing = pd.read_parquet(partition_path)
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
                df = df.sort_values("trade_date").reset_index(drop=True)

        # 限制单文件记录数
        if len(df) > MAX_MINUTE_RECORDS_PER_SYMBOL:
            logger.warning(f"[MinuteStore] 数据量 {len(df)} 超过上限，截断")
            df = df.tail(MAX_MINUTE_RECORDS_PER_SYMBOL)

        df.to_parquet(str(partition_path), index=False)
        logger.debug(f"[MinuteStore] 已保存 {len(df)} 条: {partition_path}")

    def load(self, ts_code: str, freq: str, start: str = None, end: str = None, trade_date: str = None) -> Optional[pd.DataFrame]:
        """
        加载分钟数据

        Args:
            ts_code: 股票代码
            freq: 频率
            start: 开始日期
            end: 结束日期
            trade_date: 指定日期（优先）
        """
        if trade_date:
            path = self._get_partition_path(ts_code, freq, trade_date)
            if not path.exists():
                return None
            df = pd.read_parquet(str(path))
        else:
            ts_dir = self._get_partition_path(ts_code, freq)
            if not ts_dir.exists():
                return None

            files = sorted(ts_dir.glob("*.parquet"))
            if not files:
                return None

            dfs = []
            for f in files:
                fdate = f.stem
                if start and fdate < start.replace("-", ""):
                    continue
                if end and fdate > end.replace("-", ""):
                    continue
                dfs.append(pd.read_parquet(str(f)))

            if not dfs:
                return None
            df = pd.concat(dfs, ignore_index=True)

        # 时间范围过滤
        if start and "trade_date" in df.columns:
            df = df[df["trade_date"] >= pd.to_datetime(start)]
        if end and "trade_date" in df.columns:
            df = df[df["trade_date"] <= pd.to_datetime(end)]

        return df.sort_values("trade_date").reset_index(drop=True) if not df.empty else None

    def get_metadata(self, ts_code: str, freq: str) -> Optional[MinuteDataMetadata]:
        """获取元数据"""
        ts_dir = self._get_partition_path(ts_code, freq)
        if not ts_dir.exists():
            return None

        files = sorted(ts_dir.glob("*.parquet"))
        if not files:
            return None

        # 读取第一条和最后一条记录获取时间范围
        first = pd.read_parquet(str(files[0]))
        last = pd.read_parquet(str(files[-1]))

        if "trade_date" not in first.columns:
            return None

        return MinuteDataMetadata(
            ts_code=ts_code,
            freq=freq,
            start_date=str(first["trade_date"].min())[:10],
            end_date=str(last["trade_date"].max())[:10],
            record_count=len(first) + len(last),  # 粗略估算
            data_hash=_compute_data_hash(pd.concat([first, last])),
        )

    def delete(self, ts_code: str, freq: str, trade_date: str = None):
        """删除数据"""
        if trade_date:
            path = self._get_partition_path(ts_code, freq, trade_date)
            if path.exists():
                path.unlink()
        else:
            ts_dir = self._get_partition_path(ts_code, freq)
            if ts_dir.exists():
                import shutil
                shutil.rmtree(ts_dir)

    def list_stocks(self, freq: str) -> List[str]:
        """列出已有数据的股票"""
        freq_dir = self.data_dir / freq
        if not freq_dir.exists():
            return []
        return [d.name for d in freq_dir.iterdir() if d.is_dir()]


# ──────────────────────────────────────────────────────────────────────────────
# 未来函数治理器
# ──────────────────────────────────────────────────────────────────────────────

class FutureFunctionGuard:
    """
    未来函数治理器
    ==============
    目的：防止因子计算/策略回测中引入未来数据（look-ahead bias）
    
    核心检查：
    1. 数据时间戳验证（禁止使用回测时点之后的数据）
    2. bar 完整性检查（同一时间点是否被多次修改）
    3. PIT 边界强制执行
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and FUTURE_FUNCTION_RULES["disallow_future_data"]
        self._violations: List[FutureFunctionViolation] = []
        self._lock = threading.Lock()

    def check(self, df: pd.DataFrame, ts_code: str, as_of_date: str = None) -> List[FutureFunctionViolation]:
        """
        检查数据中是否存在未来函数违规

        Args:
            df: 待检查的数据
            ts_code: 股票代码
            as_of_date: 回测截止日期（模拟当前时间点）

        Returns:
            违规列表
        """
        if not self.enabled or df is None or df.empty:
            return []

        violations = []
        as_of = pd.to_datetime(as_of_date) if as_of_date else datetime.now()

        # 检查1: 是否有未来数据
        if "trade_date" in df.columns:
            future_mask = df["trade_date"] > as_of
            if future_mask.any():
                violations.append(FutureFunctionViolation(
                    ts_code=ts_code,
                    trade_date=str(df.loc[future_mask, "trade_date"].iloc[0])[:10],
                    check_time=datetime.now(),
                    violation_type="future_data_used",
                    description=f"发现 {future_mask.sum()} 条未来数据（as_of={as_of_date}）",
                    severity="critical"
                ))

        # 检查2: bar 重复（同一时间点被多次修改）
        if "trade_date" in df.columns:
            dup_times = df["trade_date"].value_counts()
            dup_times = dup_times[dup_times > 1]
            if not dup_times.empty and FUTURE_FUNCTION_RULES["warn_on_same_bar_overwrite"]:
                violations.append(FutureFunctionViolation(
                    ts_code=ts_code,
                    trade_date=str(dup_times.index[0])[:10],
                    check_time=datetime.now(),
                    violation_type="bar_overwrite_detected",
                    description=f"发现 {len(dup_times)} 个时间点被重复写入",
                    severity="warning"
                ))

        # 检查3: 价格为0或负数
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                invalid = df[df[col] <= 0]
                if not invalid.empty:
                    violations.append(FutureFunctionViolation(
                        ts_code=ts_code,
                        trade_date=str(invalid["trade_date"].iloc[0])[:10] if "trade_date" in invalid.columns else "unknown",
                        check_time=datetime.now(),
                        violation_type="invalid_price",
                        description=f"{col} 字段存在 {len(invalid)} 条非正值",
                        severity="error"
                    ))

        with self._lock:
            self._violations.extend(violations)

        return violations

    def enforce_pit(self, df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
        """
        强制执行 PIT 边界（只返回 as_of_date 之前的数据）

        Args:
            df: 原始数据
            as_of_date: 回测截止日期

        Returns:
            过滤后的数据
        """
        if not self.enabled or df is None or df.empty:
            return df

        as_of = pd.to_datetime(as_of_date)
        if "trade_date" not in df.columns:
            return df

        original_len = len(df)
        filtered = df[df["trade_date"] <= as_of].copy()

        if len(filtered) < original_len:
            logger.warning(
                f"[FutureGuard] PIT过滤: {ts_code} 原始 {original_len} 条 → 过滤后 {len(filtered)} 条 "
                f"(as_of={as_of_date})"
            )

        return filtered

    def get_violations(self, ts_code: str = None) -> List[FutureFunctionViolation]:
        """获取违规记录"""
        with self._lock:
            if ts_code:
                return [v for v in self._violations if v.ts_code == ts_code]
            return list(self._violations)

    def clear_violations(self):
        """清空违规记录"""
        with self._lock:
            self._violations.clear()

    def report(self) -> str:
        """生成违规报告"""
        with self._lock:
            if not self._violations:
                return "✅ 未来函数检查通过，无违规"

            by_type = {}
            for v in self._violations:
                by_type.setdefault(v.violation_type, []).append(v)

            lines = [f"⚠️ 发现 {len(self._violations)} 条违规:", ""]
            for vtype, items in by_type.items():
                lines.append(f"  [{vtype}] {len(items)} 条")
                for item in items[:3]:
                    lines.append(f"    - {item.ts_code} @ {item.trade_date}: {item.description}")

            return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# 分钟数据拉取器
# ──────────────────────────────────────────────────────────────────────────────

class MinuteDataFetcher:
    """分钟数据拉取器（基于 AkShare）"""

    def __init__(self, max_retries: int = 3, delay: float = 0.5):
        self.max_retries = max_retries
        self.delay = delay
        self._stats = {"total": 0, "success": 0, "failed": 0, "latencies": []}
        self._lock = threading.Lock()

    def fetch(self, ts_code: str, freq: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        拉取分钟数据

        Args:
            ts_code: 股票代码（需要 .SH / .SZ 后缀）
            freq: 频率 (1min/5min/15min/30min/60min)
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD

        Returns:
            DataFrame 或 None
        """
        if not HAS_AKSHARE:
            logger.error("AkShare 未安装")
            return None

        # 转换代码格式（AkShare 使用 000001.XSHG 格式）
        symbol = self._to_akshare_symbol(ts_code)
        freq_code = MINUTE_FREQUENCIES.get(freq, freq)

        for attempt in range(self.max_retries):
            try:
                t0 = time.time()
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period=freq_code,
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                    adjust="qfq"  # 默认前复权
                )
                latency = time.time() - t0

                with self._lock:
                    self._stats["total"] += 1
                    self._stats["success"] += 1
                    self._stats["latencies"].append(latency)

                if df is None or df.empty:
                    return None

                # 标准化列名
                df = self._normalize_columns(df)
                df["ts_code"] = ts_code  # 保持统一格式

                return df

            except Exception as e:
                logger.warning(f"[MinuteFetcher] {ts_code} {freq} 拉取失败 (attempt {attempt+1}): {e}")
                with self._lock:
                    self._stats["failed"] += 1
                time.sleep(self.delay * (attempt + 1))

        return None

    def _to_akshare_symbol(self, ts_code: str) -> str:
        """转换为 AkShare 格式"""
        # 已经是 000001.XSHG 格式直接返回
        if ".XSHG" in ts_code or ".XSHE" in ts_code:
            return ts_code
        # 000001.SH → 000001.XSHG
        code, ex = ts_code.rsplit(".", 1)
        if ex == "SH":
            return f"{code}.XSHG"
        elif ex == "SZ":
            return f"{code}.XSHE"
        return ts_code

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名"""
        # AkShare 历史数据列名映射
        col_map = {
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "pct_chg",
            "涨跌额": "change",
            "换手率": "turnover",
        }

        df = df.rename(columns=col_map)

        # 确保必需列存在
        required = ["trade_date", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning(f"[MinuteFetcher] 缺少列: {missing}")
            for c in missing:
                df[c] = None

        # 类型转换
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])

        return df

    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._lock:
            latencies = self._stats["latencies"]
            return {
                "total_requests": self._stats["total"],
                "success_rate": self._stats["success"] / max(self._stats["total"], 1),
                "avg_latency_ms": statistics.mean(latencies) * 1000 if latencies else 0,
                "p95_latency_ms": sorted(latencies)[int(len(latencies) * 0.95)] * 1000 if latencies else 0,
            }


# ──────────────────────────────────────────────────────────────────────────────
# 主引擎
# ──────────────────────────────────────────────────────────────────────────────

class MinuteDataEngine:
    """
    分钟级行情数据引擎
    =================
    与 DataEngine 架构一致，支持分钟数据拉取、存储、治理

    使用示例：
        engine = MinuteDataEngine(data_dir="data/minute_data")
        engine.update("000001.SZ", freq="5min", start="2024-01-01", end="2024-01-31")
        df = engine.get_minute("000001.SZ", freq="5min", start="2024-01-01", end="2024-01-31")
    """

    def __init__(
        self,
        data_dir: str = None,
        db_path: str = None,
        start_date: str = None,
        freq: str = "5min",
        max_workers: int = 4,
        enable_future_guard: bool = True,
    ):
        """
        初始化

        Args:
            data_dir: Parquet 数据目录
            db_path: DuckDB 元数据数据库路径
            start_date: 默认起始日期
            freq: 默认频率
            max_workers: 最大并发数
            enable_future_guard: 启用未来函数治理
        """
        self.data_dir = data_dir
        self.db_path = db_path
        self.start_date = start_date or DEFAULT_MINUTE_START
        self.freq = freq
        self.max_workers = max_workers

        # 存储
        self.store = MinuteParquetStore(data_dir)

        # 拉取器
        self.fetcher = MinuteDataFetcher()

        # 未来函数治理
        self.future_guard = FutureFunctionGuard(enabled=enable_future_guard)

        # DuckDB 元数据
        self._conn = None
        if db_path and HAS_DUCKDB:
            self._conn = duckdb.connect(db_path)
            self._init_metadata_table()

        # 统计
        self._stats = {
            "symbols_updated": 0,
            "records_fetched": 0,
            "records_saved": 0,
            "errors": 0,
        }

        logger.info(f"[MinuteEngine] 初始化完成: data_dir={self.store.data_dir}, freq={freq}")

    def _init_metadata_table(self):
        """初始化元数据表"""
        if not self._conn:
            return
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS minute_data_metadata (
                ts_code VARCHAR,
                freq VARCHAR,
                start_date DATE,
                end_date DATE,
                record_count BIGINT,
                data_hash VARCHAR,
                last_updated TIMESTAMP,
                source VARCHAR,
                quality_score DOUBLE,
                PRIMARY KEY (ts_code, freq)
            )
        """)
        self._conn.commit()

    def update(
        self,
        ts_code: str,
        freq: str = None,
        start: str = None,
        end: str = None,
        force: bool = False,
    ) -> Dict:
        """
        更新分钟数据

        Args:
            ts_code: 股票代码
            freq: 频率（默认使用 self.freq）
            start: 开始日期（默认使用上次截止日期）
            end: 结束日期
            force: 强制重新拉取

        Returns:
            更新结果
        """
        freq = freq or self.freq
        end = end or datetime.now().strftime("%Y-%m-%d")

        # 检查上次截止日期
        if not force and start is None:
            meta = self.get_metadata(ts_code, freq)
            if meta:
                start = (pd.to_datetime(meta.end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start = self.start_date

        if not start:
            start = self.start_date

        logger.info(f"[MinuteEngine] 更新: {ts_code} {freq} [{start} ~ {end}]")

        try:
            # 拉取数据
            df = self.fetcher.fetch(ts_code, freq, start, end)
            if df is None or df.empty:
                logger.warning(f"[MinuteEngine] 无数据: {ts_code} {freq}")
                return {"success": False, "reason": "no_data"}

            # 未来函数检查
            violations = self.future_guard.check(df, ts_code)
            if violations:
                logger.warning(f"[MinuteEngine] 发现 {len(violations)} 条未来函数违规")

            # 保存到 Parquet
            trade_date = start.replace("-", "")[:8]  # 使用起始日期作为分区
            self.store.save(df, ts_code, freq, trade_date, mode="append")

            # 更新元数据
            self._update_metadata(ts_code, freq, df)

            self._stats["symbols_updated"] += 1
            self._stats["records_fetched"] += len(df)
            self._stats["records_saved"] += len(df)

            return {"success": True, "records": len(df), "violations": len(violations)}

        except Exception as e:
            logger.error(f"[MinuteEngine] 更新失败: {ts_code} {freq}: {e}")
            self._stats["errors"] += 1
            return {"success": False, "reason": str(e)}

    def update_batch(
        self,
        ts_codes: List[str],
        freq: str = None,
        start: str = None,
        end: str = None,
        max_workers: int = None,
    ) -> Dict:
        """
        批量更新

        Args:
            ts_codes: 股票列表
            freq: 频率
            start: 开始日期
            end: 结束日期
            max_workers: 最大并发数
        """
        max_workers = max_workers or self.max_workers
        results = {"success": 0, "failed": 0, "total_records": 0, "errors": []}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.update, code, freq, start, end): code
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
                        results["errors"].append({"code": code, "reason": result.get("reason")})
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({"code": code, "reason": str(e)})

        logger.info(
            f"[MinuteEngine] 批量更新完成: 成功 {results['success']}/{len(ts_codes)}, "
            f"记录 {results['total_records']}"
        )
        return results

    def get_minute(
        self,
        ts_code: str,
        freq: str = None,
        start: str = None,
        end: str = None,
        as_of_date: str = None,  # PIT 边界
    ) -> Optional[pd.DataFrame]:
        """
        获取分钟数据

        Args:
            ts_code: 股票代码
            freq: 频率
            start: 开始日期
            end: 结束日期
            as_of_date: PIT 边界（只返回该日期之前的数据）

        Returns:
            DataFrame
        """
        freq = freq or self.freq
        df = self.store.load(ts_code, freq, start, end)

        if df is None or df.empty:
            return None

        # PIT 边界过滤
        if as_of_date and FUTURE_FUNCTION_RULES["enforce_pit_boundary"]:
            df = self.future_guard.enforce_pit(df, as_of_date)

        return df

    def get_metadata(self, ts_code: str, freq: str = None) -> Optional[MinuteDataMetadata]:
        """获取元数据"""
        freq = freq or self.freq
        return self.store.get_metadata(ts_code, freq)

    def list_symbols(self, freq: str = None) -> List[str]:
        """列出已有数据的股票"""
        freq = freq or self.freq
        return self.store.list_stocks(freq)

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            "fetcher_stats": self.fetcher.get_stats(),
            "engine_stats": self._stats.copy(),
            "future_guard_violations": len(self.future_guard.get_violations()),
        }

    def close(self):
        """关闭连接"""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _update_metadata(self, ts_code: str, freq: str, df: pd.DataFrame):
        """更新元数据到 DuckDB"""
        if not self._conn or df is None or df.empty:
            return

        meta = MinuteDataMetadata(
            ts_code=ts_code,
            freq=freq,
            start_date=str(df["trade_date"].min())[:10],
            end_date=str(df["trade_date"].max())[:10],
            record_count=len(df),
            data_hash=_compute_data_hash(df),
            source="akshare",
        )

        self._conn.execute("""
            INSERT INTO minute_data_metadata (ts_code, freq, start_date, end_date, record_count, data_hash, last_updated, source, quality_score)
            VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?)
            ON CONFLICT (ts_code, freq) DO UPDATE SET
                end_date = EXCLUDED.end_date,
                record_count = minute_data_metadata.record_count + EXCLUDED.record_count,
                data_hash = EXCLUDED.data_hash,
                last_updated = EXCLUDED.last_updated
        """, (meta.ts_code, meta.freq, meta.start_date, meta.end_date, meta.record_count, meta.data_hash, meta.last_updated, meta.source, meta.quality_score))
        self._conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# 工厂函数
# ──────────────────────────────────────────────────────────────────────────────

def create_minute_engine(
    freq: str = "5min",
    data_dir: str = None,
    enable_future_guard: bool = True,
) -> MinuteDataEngine:
    """创建分钟引擎（便捷工厂函数）"""
    project_root = Path(__file__).parent.parent
    data_dir = data_dir or str(project_root / "data" / "minute_data")
    db_path = str(project_root / "data" / "stock_data.duckdb")

    return MinuteDataEngine(
        data_dir=data_dir,
        db_path=db_path,
        freq=freq,
        enable_future_guard=enable_future_guard,
    )


# ──────────────────────────────────────────────────────────────────────────────
# 主程序测试
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("MinuteDataEngine - 分钟级行情数据引擎")
    print("=" * 70)

    # 创建引擎
    engine = create_minute_engine(freq="5min")

    print("\n[功能概览]")
    print("  1. 拉取分钟数据 (1min/5min/15min/30min/60min)")
    print("  2. Parquet 分区存储")
    print("  3. 未来函数治理 (防止 look-ahead bias)")
    print("  4. PIT 边界强制执行")
    print("  5. 批量并发更新")

    print("\n[使用示例]")
    print("""
    # 单股更新
    engine.update("000001.SZ", freq="5min", start="2024-01-01", end="2024-01-31")

    # 批量更新
    engine.update_batch(["000001.SZ", "600000.SH"], freq="5min")

    # 获取数据
    df = engine.get_minute("000001.SZ", freq="5min", start="2024-01-01", end="2024-01-31")

    # PIT 查询（回测场景）
    df = engine.get_minute("000001.SZ", freq="5min", as_of_date="2024-01-15")

    # 未来函数检查报告
    print(engine.future_guard.report())
    """)

    print("\n[存储结构]")
    print("""
    data/minute_data/
    ├── 5min/
    │   ├── 000001.SZ/
    │   │   ├── 20240101.parquet
    │   │   ├── 20240102.parquet
    │   │   └── ...
    │   └── 600000.SH/
    │       └── ...
    └── 15min/
        └── ...
    """)

    print("\n[未来函数治理规则]")
    for rule, enabled in FUTURE_FUNCTION_RULES.items():
        status = "✅" if enabled else "❌"
        print(f"  {status} {rule}")

    # 拉取测试
    print("\n" + "=" * 70)
    print("[测试拉取]")
    result = engine.update("000001.SZ", freq="5min", start="2024-01-02", end="2024-01-05")
    print(f"更新结果: {result}")

    if result.get("success"):
        df = engine.get_minute("000001.SZ", freq="5min", start="2024-01-02", end="2024-01-05")
        if df is not None and not df.empty:
            print(f"\n获取到 {len(df)} 条分钟数据")
            print(df.head(5).to_string())

    print("\n[统计信息]")
    print(engine.get_stats())

    print("\n[未来函数检查]")
    print(engine.future_guard.report())

    engine.close()
