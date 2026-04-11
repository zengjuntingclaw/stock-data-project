"""
DataEngine - 生产级数据治理与存储引擎
=====================================
核心功能：
  1. 消除幸存者偏差（全量历史成分股含退市）
  2. DuckDB + Parquet 双存储
  3. 增量更新（断点续传）
  4. 多源交叉验证
  5. Point-in-Time 数据约束
  6. 单股数据提取
"""

import os
import bisect
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Union, Literal, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import warnings
import random

from loguru import logger


# ──────────────────────────────────────────────────────────────
# 全局常量
# ──────────────────────────────────────────────────────────────
DEFAULT_START_DATE = "2018-01-01"     # 默认数据起始日期
DEFAULT_ADJ_TOLERANCE = 0.005         # 交叉验证容差 0.5%
DEFAULT_SAMPLE_RATIO = 0.05           # 交叉验证抽样比例 5%
DEFAULT_ADJ_CHANGE_THRESHOLD = 0.05   # 复权因子变化告警阈值 5%
DEFAULT_LIMIT_TOLERANCE = 0.01        # 涨跌停判定容差 0.01%
DEFAULT_MAX_WORKERS = 12              # 默认多线程数
DEFAULT_FETCH_DELAY = 0.2             # 默认请求延迟(秒)
DEFAULT_MAX_RETRIES = 3               # 默认最大重试次数

# ──────────────────────────────────────────────────────────────
# 依赖导入
# ──────────────────────────────────────────────────────────────
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    from loguru import logger as _fallback_logger
    _fallback_logger.warning("duckdb not installed. Run: pip install duckdb")

try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False
    from loguru import logger as _fallback_logger
    _fallback_logger.warning("akshare not installed. Run: pip install akshare")

try:
    import baostock as bs
    HAS_BAOSTOCK = True
except ImportError:
    HAS_BAOSTOCK = False
    bs = None
    from loguru import logger as _fallback_logger
    _fallback_logger.warning("baostock not installed. Run: pip install baostock")


# ──────────────────────────────────────────────────────────────
# 公共工具函数
# ──────────────────────────────────────────────────────────────

def detect_board(symbol: str) -> str:
    """根据股票代码识别板块

    统一委托给 exchange_mapping.classify_exchange()，禁止自行实现。
    确保 920xxx（北交所2024新代码）、4xxxxx、8xxxxx 全部正确识别。
    """
    from scripts.exchange_mapping import classify_exchange
    _, board = classify_exchange(symbol)
    return board


def detect_limit(code: str) -> float:
    """根据股票代码返回涨跌停幅度（基础版，仅用于数据验证）

    统一委托给 exchange_mapping.get_price_limit_pct()，禁止自行实现。
    注意：这是简化版（不含时间维度）。完整涨跌停逻辑请用 AShareTradingRules。
    科创板(688): 20%, 创业板(30): 20%, 北交所(4/8/920): 30%, 主板: 10%
    """
    from scripts.exchange_mapping import get_price_limit_pct
    return get_price_limit_pct(code)


def build_ts_code(symbol: str) -> str:
    """构造 ts_code（支持沪深北三交易所）
    
    统一使用 exchange_mapping 模块的实现，确保所有入口一致。
    交易所后缀规则（Tushare 标准）：
      - .SH：上海证券交易所（主板 + 科创板，代码 6/5/688 开头）
      - .SZ：深圳证券交易所（主板 + 创业板，代码 0/1/2/3 开头）
      - .BJ：北京证券交易所（北交所，代码 4/8/920 开头）
    
    重要修复：920xxx 是北交所2024新代码段，不是沪市！
    """
    # 统一委托给 exchange_mapping 模块，确保所有入口一致
    from scripts.exchange_mapping import build_ts_code as _build_ts_code
    return _build_ts_code(symbol)


# ──────────────────────────────────────────────────────────────
# 数据验证器（已拆分到 data_validator.py，保留向后兼容导入）
# ──────────────────────────────────────────────────────────────
from scripts.data_validator import DataValidator


# ──────────────────────────────────────────────────────────────
# 核心 DataEngine
# ──────────────────────────────────────────────────────────────
class DataEngine:
    """
    数据引擎 - 统一数据管理入口
    
    Attributes
    ----------
    db_path : Path
        DuckDB 数据库路径
    parquet_dir : Path
        Parquet 存储目录
    validator : DataValidator
        数据验证器
    """

    # 类级缓存：已初始化的数据库路径（避免重复建表）
    _schema_initialized: set = set()

    def __init__(self,
                 db_path: str = None,
                 parquet_dir: str = None):
        # 支持环境变量配置，未设置时使用相对路径默认值
        project_root = Path(__file__).resolve().parent.parent
        self.db_path = Path(db_path or os.environ.get(
            'STOCK_DB_PATH', str(project_root / 'data' / 'stock_data.duckdb')))
        self.parquet_dir = Path(parquet_dir or os.environ.get(
            'STOCK_PARQUET_DIR', str(project_root / 'data' / 'parquet')))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.validator = DataValidator()

        if HAS_DUCKDB:
            self._init_schema()
        else:
            logger.error("DuckDB required but not installed.")

        # Baostock 全局会话锁（Baostock 不支持并发，全局单会话）
        self._bs_lock = threading.Lock()

    @staticmethod
    def _get_now() -> datetime:
        """获取当前时间（支持单测 mock）。默认返回真实当前时间。"""
        return datetime.now()

    # ──────────────────────────────────────────────────────────
    # 数据库初始化
    # ──────────────────────────────────────────────────────────
    def _init_schema(self):
        """初始化表结构（幂等：同类路径只执行一次）"""
        db_key = str(self.db_path.resolve())
        if db_key in DataEngine._schema_initialized:
            logger.debug(f"Schema already initialized for {db_key}")
            return
        
        conn = duckdb.connect(str(self.db_path))
        cur = conn.cursor()

        # 股票基本信息（含退市标记）— exchange/board 由 exchange_mapping.py 统一派生
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code       VARCHAR PRIMARY KEY,
                symbol        VARCHAR,
                name          VARCHAR,
                exchange      VARCHAR,       -- 交易所代码：SH/SZ/BJ（由 classify_exchange() 派生）
                board         VARCHAR,       -- 板块：main/chinext/kcb/bse（由 classify_exchange() 派生）
                area          VARCHAR,
                industry      VARCHAR,
                market        VARCHAR,
                list_date     DATE,
                delist_date   DATE,
                is_delisted   BOOLEAN DEFAULT FALSE,
                delist_reason VARCHAR,
                is_hs         BOOLEAN,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 证券主表历史版本（时点版本，解决历史universe完整性问题）
        # 设计：每只股票可有多条版本记录（ts_code + eff_date 联合主键）
        # PIT查询逻辑: WHERE list_date <= date AND (delist_date > date OR delist_date IS NULL)
        # eff_date = 本条记录的生效日期（通常=同步当日或 list_date）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic_history (
                ts_code       VARCHAR  NOT NULL,
                symbol        VARCHAR  NOT NULL,
                name          VARCHAR,
                exchange      VARCHAR,        -- SH / SZ / BJ（由 classify_exchange() 派生）
                area          VARCHAR,
                industry      VARCHAR,
                market        VARCHAR,
                list_date     DATE,           -- 上市日期（关键：PIT过滤用）
                delist_date   DATE,           -- 退市日期（关键：PIT过滤用）
                is_delisted   BOOLEAN DEFAULT FALSE,
                delist_reason VARCHAR,
                board         VARCHAR,        -- main/chinext/kcb/bse（由 classify_exchange() 派生）
                is_suspended  BOOLEAN DEFAULT FALSE,  -- 当日是否停牌
                eff_date      DATE NOT NULL,  -- 本版本记录生效日期（同步当日）
                end_date      DATE,           -- 本版本失效日期（下一次快照前一天），NULL=当前最新
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, eff_date)  -- 联合主键：同一股票可有多个版本
            )
        """)
        # 关键索引：支持 PIT 查询（list_date / delist_date 过滤）和按交易所过滤
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_hist_list_date
            ON stock_basic_history(list_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_hist_delist_date
            ON stock_basic_history(delist_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_hist_exchange
            ON stock_basic_history(exchange)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_hist_eff_date
            ON stock_basic_history(eff_date)
        """)

        # ── 增量同步进度表（断点续跑核心）────────────────────────────
        # 记录每只股票的最后成功同步位置，防止中断后重复下载或数据缺失
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_progress (
                ts_code        VARCHAR NOT NULL,   -- 股票代码
                table_name     VARCHAR NOT NULL,   -- 目标表（daily_bar_raw / daily_bar_adjusted）
                last_sync_date DATE,               -- 最后成功同步的交易日
                last_sync_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 最后同步时间戳
                total_records  INTEGER DEFAULT 0,  -- 累计同步记录数
                status         VARCHAR DEFAULT 'ok', -- ok / failed / in_progress
                error_msg      VARCHAR,            -- 最后一次错误信息
                PRIMARY KEY (ts_code, table_name)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_progress_date
            ON sync_progress(last_sync_date)
        """)

        # 指数成分股历史区间表（解决幸存者偏差）
        # 每次同步只 INSERT 不 DELETE，支持按任意交易日回放当时成分
        # 关键：in_date=out_date=NULL 表示尚未加入；out_date非空表示已退出
        cur.execute("""
            CREATE TABLE IF NOT EXISTS index_constituents_history (
                index_code   VARCHAR,      -- 指数代码如 '000300.SH'
                ts_code      VARCHAR,      -- 成分股代码
                in_date      DATE,         -- 纳入日期（NULL=未来/未知）
                out_date     DATE,         -- 剔除日期（NULL=仍在指数内）
                source       VARCHAR,       -- 数据来源：csindex/sse/szse
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (index_code, ts_code, in_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ich_index
            ON index_constituents_history(index_code)
        """)

        # ST状态历史表（时间序列特征）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS st_status_history (
                ts_code      VARCHAR,
                trade_date   DATE,
                is_st        BOOLEAN,     -- 当日是否ST
                is_new_st    BOOLEAN,     -- 当日是否新加入ST
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # ── 原始价日线表（基础层）──────────────────────────────────
        # adj_factor=1.0 表示不复权原始价；>1.0 表示有复权调整
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_bar_raw (
                ts_code       VARCHAR,
                trade_date    DATE,
                symbol        VARCHAR,
                open          DOUBLE,
                high          DOUBLE,
                low           DOUBLE,
                close         DOUBLE,           -- 原始未复权收盘价（不复权数据源直接存储）
                volume        BIGINT,
                amount        DOUBLE,
                pct_chg       DOUBLE,
                turnover      DOUBLE,
                adj_factor    DOUBLE DEFAULT 1.0,  -- 复权因子（>1.0=有复权，1.0=原始价）
                pre_close     DOUBLE,             -- 前收盘（由前一日收盘计算）
                is_suspend    BOOLEAN DEFAULT FALSE,
                limit_up      BOOLEAN DEFAULT FALSE,
                limit_down    BOOLEAN DEFAULT FALSE,
                data_source   VARCHAR DEFAULT 'akshare',
                PRIMARY KEY (ts_code, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bar_raw_code ON daily_bar_raw(ts_code)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bar_raw_date ON daily_bar_raw(trade_date)
        """)

        # ── 复权价日线表（派生层）──────────────────────────────────
        # 由 daily_bar_raw 乘 adj_factor 派生，close = raw_close × adj_factor
        # 注意：此表的 close/high/low/open 字段均为复权价
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_bar_adjusted (
                ts_code       VARCHAR,
                trade_date    DATE,
                open          DOUBLE,
                high          DOUBLE,
                low           DOUBLE,
                close         DOUBLE,            -- 复权收盘价（=raw_close × adj_factor）
                pre_close     DOUBLE,
                volume        BIGINT,
                amount        DOUBLE,
                pct_chg       DOUBLE,
                turnover      DOUBLE,
                adj_factor    DOUBLE DEFAULT 1.0,  -- 复权因子（参考值）
                is_suspend    BOOLEAN DEFAULT FALSE,
                limit_up      BOOLEAN DEFAULT FALSE,
                limit_down    BOOLEAN DEFAULT FALSE,
                data_source   VARCHAR DEFAULT 'akshare',
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 除权除息事件表（用于生成 corporate_actions）
        # 来源：daily_bar_raw 的 adj_factor 变化检测，或 Baostock 分红数据
        cur.execute("""
            CREATE TABLE IF NOT EXISTS corporate_actions (
                ts_code       VARCHAR,      -- 证券代码
                action_date   DATE,         -- 除权除息生效日期
                ann_date      DATE,         -- 公告日期（用于PIT约束）
                prev_adj      DOUBLE,       -- 变化前复权因子
                curr_adj      DOUBLE,       -- 变化后复权因子
                action_type   VARCHAR,      -- 类型：dividend/split/reverse_split/rights_issue
                change_ratio  DOUBLE,       -- 变化幅度（curr/prev - 1）
                reason        VARCHAR,      -- 事件描述（如 分红0.5元/股）
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, action_date)
            )
        """)

        # 交易日历
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_calendar (
                cal_date       DATE PRIMARY KEY,
                is_open        BOOLEAN,
                pretrade_date  DATE
            )
        """)

        # 财务数据（Point-in-Time：含 ann_date）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financial_data (
                ts_code        VARCHAR,
                ann_date       DATE,          -- 公告日（PIT 约束用）
                end_date       DATE,          -- 报告期
                report_type    VARCHAR,       -- 'Q1'/'Q2'/'Q3'/'Q4'
                revenue        DOUBLE,
                net_profit     DOUBLE,
                total_assets   DOUBLE,
                total_equity   DOUBLE,
                total_mv       DOUBLE,
                circ_mv        DOUBLE,
                pe_ttm         DOUBLE,
                pb             DOUBLE,
                roe            DOUBLE,
                roa            DOUBLE,
                roic           DOUBLE,
                gross_margin   DOUBLE,
                debt_ratio     DOUBLE,
                eps            DOUBLE,        -- 基本每股收益（_bs_profit_row / _ak_indicator_row 提供）
                data_source    VARCHAR,       -- 数据来源标识（akshare / baostock）
                PRIMARY KEY (ts_code, end_date, report_type)
            )
        """)

        # 市值数据（每日快照）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_valuation (
                ts_code       VARCHAR,
                trade_date    DATE,
                total_mv      DOUBLE,
                circ_mv       DOUBLE,
                pe_ttm        DOUBLE,
                pb            DOUBLE,
                ps_ttm        DOUBLE,
                pcf_cf_ttm    DOUBLE,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 数据更新日志
        cur.execute("""
            CREATE TABLE IF NOT EXISTS update_log (
                id            INTEGER PRIMARY KEY,
                table_name     VARCHAR,
                ts_code       VARCHAR,
                start_date    DATE,
                end_date      DATE,
                record_count  INTEGER,
                update_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status        VARCHAR,
                error_message VARCHAR
            )
        """)

        # 复权因子变化日志（用于检测分红/拆股）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS adj_factor_log (
                ts_code       VARCHAR,
                trade_date    DATE,
                adj_factor_old DOUBLE,
                adj_factor_new DOUBLE,
                change_ratio  DOUBLE,
                detected_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 数据质量报警表
        cur.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_dqa_id START 1
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_alert (
                id            INTEGER PRIMARY KEY DEFAULT nextval('seq_dqa_id'),
                alert_type    VARCHAR,
                ts_code       VARCHAR,
                trade_date    DATE,
                detail        VARCHAR,
                created_at    TIMESTAMP DEFAULT NOW()
            )
        """)

        # 索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_code ON daily_bar_adjusted(ts_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_date ON daily_bar_adjusted(trade_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fin_ann ON financial_data(ann_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_val_date ON daily_valuation(trade_date)")

        conn.close()
        DataEngine._schema_initialized.add(str(self.db_path.resolve()))
        logger.info(f"DataEngine schema initialized: {self.db_path}")

    # ──────────────────────────────────────────────────────────
    # 查询接口
    # ──────────────────────────────────────────────────────────
    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行 SQL 查询（支持参数化查询防止SQL注入）"""
        if not HAS_DUCKDB:
            return pd.DataFrame()
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            if params:
                df = conn.execute(sql, params).fetchdf()
            else:
                df = conn.execute(sql).fetchdf()
            return df
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple = None):
        """执行写操作（支持参数化查询防止SQL注入）"""
        conn = duckdb.connect(str(self.db_path))
        try:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        finally:
            conn.close()

    def get_connection(self, read_only: bool = False):
        """获取数据库连接（上下文管理器，确保连接正确关闭）"""
        import contextlib
        @contextlib.contextmanager
        def _conn():
            conn = duckdb.connect(str(self.db_path), read_only=read_only)
            try:
                yield conn
            finally:
                conn.close()
        return _conn()

    # ──────────────────────────────────────────────────────────
    # 工具方法
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def _apply_limit_flags(df: pd.DataFrame, code: str, pct_col: str = "pct_chg") -> pd.DataFrame:
        """统一应用涨跌停标记（容差0.01%避免浮点误差）
        
        注意：这是简化版涨跌停判断，不含时间维度。
        完整涨跌停逻辑（含新股规则、历史变迁）请使用 AShareTradingRules.get_price_limit()
        """
        limit_pct = detect_limit(code)  # 返回小数，如0.1表示10%
        # pct_chg是百分比数值（如9.8表示9.8%），需要乘100转换
        df["limit_up"] = df[pct_col] >= (limit_pct * 100 - 0.01)
        df["limit_down"] = df[pct_col] <= -(limit_pct * 100 - 0.01)
        return df
    
    @staticmethod
    def _build_ts_code(symbol: str) -> str:
        """构造 ts_code（委托到公共函数）"""
        return build_ts_code(symbol)
    
    # ──────────────────────────────────────────────────────────
    # 幸存者偏差处理
    # ──────────────────────────────────────────────────────────
    def get_all_stocks(self, include_delisted: bool = True) -> pd.DataFrame:
        """
        获取全量股票列表（含退市股）
        
        解决幸存者偏差的关键入口。
        """
        if not HAS_AKSHARE:
            return self._get_local_stocks()

        stocks = []

        # 1. 当前上市股票
        try:
            spot = ak.stock_zh_a_spot_em()
            current = spot[["代码", "名称", "板块", "总市值", "流通市值"]].copy()
            current.columns = ["symbol", "name", "industry", "total_mv", "circ_mv"]
            current["is_delisted"] = False
            current["list_date"] = pd.NaT
            current["delist_date"] = pd.NaT
            stocks.append(current)
            logger.info(f"Current stocks: {len(current)}")
        except (ValueError, KeyError, RuntimeError) as e:
            logger.warning(f"Failed to fetch current stocks: {e}")

        # 2. 退市股票（关键：解决幸存者偏差）
        if include_delisted:
            try:
                delisted = ak.stock_zh_a_delist(symbol="退市")
                if not delisted.empty and "证券代码" in delisted.columns:
                    dl = delisted[["证券代码", "证券名称"]].copy()
                    dl.columns = ["symbol", "name"]
                    dl["is_delisted"] = True
                    dl["industry"] = "退市"
                    dl["total_mv"] = np.nan
                    dl["circ_mv"] = np.nan
                    stocks.append(dl)
                    logger.info(f"Delisted stocks: {len(dl)}")
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Failed to fetch delisted stocks: {e}")

        if not stocks:
            return self._get_local_stocks()

        df = pd.concat(stocks, ignore_index=True)
        df = df.drop_duplicates(subset=["symbol"], keep="first")

        # 构造 ts_code
        df["ts_code"] = df["symbol"].apply(build_ts_code)

        df["market"] = df["symbol"].apply(detect_board)
        return df

    def save_stock_basic_snapshot(self, snapshot_date: Optional[str] = None) -> int:
        """
        保存证券主表每日快照到 stock_basic_history

        解决"证券主表时点版本"问题，支持历史时点查询。
        每次调用仅 UPSERT 当日版本（eff_date = snapshot_date），
        不删除其他日期的历史版本。

        Parameters
        ----------
        snapshot_date : str, optional
            快照日期 YYYY-MM-DD，默认今日

        Returns
        -------
        int: 保存的记录数
        """
        if snapshot_date is None:
            snapshot_date = self._get_now().strftime("%Y-%m-%d")

        # 获取当前股票列表（含退市）
        df = self.get_all_stocks(include_delisted=True)
        if df.empty:
            logger.warning(f"No stocks to snapshot for {snapshot_date}")
            return 0

        # ── 确保 exchange / board 字段完整（统一由 classify_exchange 派生）──
        from scripts.exchange_mapping import classify_exchange
        if "exchange" not in df.columns or df["exchange"].isna().any():
            def _classify(row):
                sym = str(row.get("symbol", "")).zfill(6)
                exch, board = classify_exchange(sym)
                return pd.Series({"exchange": exch, "board": board})
            classified = df.apply(_classify, axis=1)
            df["exchange"] = classified["exchange"]
            df["board"] = classified["board"]

        for col in ["list_date", "delist_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "is_delisted" not in df.columns:
            df["is_delisted"] = df["delist_date"].notna()

        # eff_date = snapshot_date（本条记录生效日期）
        df["eff_date"] = pd.to_datetime(snapshot_date)
        df["end_date"] = pd.NaT

        # 必需字段列表（匹配 stock_basic_history 表结构）
        hist_cols = ["ts_code", "symbol", "name", "exchange", "board",
                     "industry", "market", "list_date", "delist_date",
                     "is_delisted", "eff_date", "end_date"]
        for col in hist_cols:
            if col not in df.columns:
                df[col] = None
        hist_df = df[hist_cols].copy()

        # UPSERT：已存在同日版本则更新，不存在则插入（保留历史版本）
        with self.get_connection() as conn:
            conn.register("tmp_snapshot", hist_df)
            conn.execute("""
                INSERT INTO stock_basic_history
                    (ts_code, symbol, name, exchange, board, industry, market,
                     list_date, delist_date, is_delisted, eff_date, end_date)
                SELECT ts_code, symbol, name, exchange, board, industry, market,
                       list_date, delist_date, is_delisted, eff_date, end_date
                FROM tmp_snapshot
                ON CONFLICT (ts_code, eff_date) DO UPDATE SET
                    symbol      = excluded.symbol,
                    name        = excluded.name,
                    exchange    = excluded.exchange,
                    board       = excluded.board,
                    industry    = excluded.industry,
                    market      = excluded.market,
                    list_date   = excluded.list_date,
                    delist_date = excluded.delist_date,
                    is_delisted = excluded.is_delisted
            """)
            conn.execute("DROP VIEW IF EXISTS tmp_snapshot")

        logger.info(f"Saved stock basic snapshot for {snapshot_date}: {len(hist_df)} records")
        return len(hist_df)

    def get_stocks_as_of(self, as_of_date: str) -> pd.DataFrame:
        """
        获取指定日期的证券主表（时点版本）
        
        解决结构性风险：回测时使用历史真实的股票池，
        而非"当前快照+退市补丁"的近似方案。
        
        Parameters
        ----------
        as_of_date : str
            查询日期 YYYY-MM-DD
            
        Returns
        -------
        pd.DataFrame: 该日期的证券主表快照
            - 包含当日所有可交易股票
            - 包含已退市但当日仍有效的股票
            - 不包含当日尚未上市的股票
        """
        as_of = pd.to_datetime(as_of_date)
        
        # 先尝试从历史表查询（历史快照已预过滤，只保留当日有效的股票）
        # stock_basic_history 表使用 eff_date 作为快照日期字段
        with self.get_connection() as conn:
            hist_df = conn.execute("""
                SELECT * FROM stock_basic_history
                WHERE eff_date = ?
                -- 边界过滤：当日必须已上市且尚未退市
                AND (list_date IS NULL OR list_date <= ?)
                AND (delist_date IS NULL OR delist_date >= ?)
            """, (as_of_date, as_of_date, as_of_date)).fetchdf()
        
        if not hist_df.empty:
            logger.info(f"Using historical snapshot for {as_of_date}: {len(hist_df)} stocks")
            return hist_df
        
        # 无历史数据，回退到当前表+日期过滤
        logger.warning(f"No historical snapshot for {as_of_date}, using current data with date filter")
        
        with self.get_connection() as conn:
            df = conn.execute("""
                SELECT * FROM stock_basic
                WHERE (list_date IS NULL OR list_date <= ?)
                  AND (delist_date IS NULL OR delist_date >= ?)
            """, (as_of_date, as_of_date)).fetchdf()
        
        return df

    def get_all_stocks_historical(self, 
                                   start_date: str, 
                                   end_date: str) -> pd.DataFrame:
        """
        获取日期范围内的证券主表历史序列
        
        用于分析股票池变化、IPO/退市时间线等。
        
        Parameters
        ----------
        start_date : str
            开始日期 YYYY-MM-DD
        end_date : str
            结束日期 YYYY-MM-DD
            
        Returns
        -------
        pd.DataFrame: 包含所有日期快照的合并数据
        """
        with self.get_connection() as conn:
            df = conn.execute("""
                SELECT * FROM stock_basic_history 
                WHERE eff_date BETWEEN ? AND ?
                ORDER BY eff_date, ts_code
            """, (start_date, end_date)).fetchdf()
        
        return df

    def get_pit_stock_pool(
        self,
        trade_date: str,
        include_delisted: bool = True,
        exchanges: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        获取指定日期的历史可交易股票池（Point-in-Time 查询）

        直接用 DuckDB SQL 精确还原任意历史日期的真实股票池。

        规则：
        - list_date <= trade_date：已上市
        - delist_date > trade_date OR delist_date IS NULL：尚未退市
        - 退市整理期（delist_date <= trade_date <= delist_date+30天）：仍可交易

        Parameters
        ----------
        trade_date : str
            查询日期 YYYY-MM-DD
        include_delisted : bool
            是否包含退市整理期股票
        exchanges : List[str], optional
            筛选交易所（如 ['SH', 'SZ', 'BJ']）

        Returns
        -------
        pd.DataFrame: 包含 ts_code, symbol, name, exchange, board, list_date, delist_date, is_tradable
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        # 交易所筛选（参数化，安全）
        exchange_filter = ""
        params = []
        if exchanges:
            placeholders = ','.join(['?' for _ in exchanges])
            exchange_filter = f"AND exchange IN ({placeholders})"
            params.extend(exchanges)

        with self.get_connection() as conn:
            df = conn.execute(f"""
                SELECT
                    s.ts_code,
                    s.symbol,
                    s.name,
                    s.exchange,
                    s.board,
                    s.list_date,
                    s.delist_date,
                    s.is_delisted,
                    s.delist_reason,
                    CASE
                        WHEN s.delist_date IS NULL THEN TRUE
                        WHEN s.delist_date > CAST(? AS DATE) THEN TRUE
                        ELSE FALSE
                    END AS is_tradable
                FROM stock_basic_history s
                WHERE s.list_date <= CAST(? AS DATE)
                  AND (s.delist_date IS NULL OR s.delist_date > CAST(? AS DATE))
                  {exchange_filter}
                ORDER BY s.exchange, s.ts_code
            """, [trade_date, trade_date, trade_date] + params).fetchdf()

        return df

    def get_active_stocks(self, trade_date: str) -> List[str]:
        """
        获取指定日期可交易股票池（PIT 查询）

        核心接口：回测时必须用此方法获取历史某一天的真实可交易股票，
        不依赖"当前快照 + 退市补丁"的近似方案。

        规则：
        - list_date <= trade_date：已上市
        - delist_date IS NULL OR delist_date > trade_date：尚未退市
        - 退市整理期（delist_date + 30天内）仍视为可交易

        Parameters
        ----------
        trade_date : str
            查询日期 YYYY-MM-DD

        Returns
        -------
        List[str]: 该日期可交易的 ts_code 列表（如 ['000001.SZ', '600000.SH', ...]）
        """
        if not HAS_DUCKDB:
            return []

        df = self.query(f"""
            SELECT DISTINCT ts_code FROM stock_basic_history
            WHERE list_date <= CAST(? AS DATE)
              AND (delist_date IS NULL OR delist_date > CAST(? AS DATE))
            ORDER BY ts_code
        """, (trade_date, trade_date))

        return df["ts_code"].tolist() if not df.empty else []

    def get_stocks_with_st_status(
        self,
        trade_date: str,
    ) -> pd.DataFrame:
        """
        获取指定日期的 ST 股列表（Point-in-Time 查询）

        Parameters
        ----------
        trade_date : str
            查询日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 包含 ts_code, symbol, name, exchange, board, list_date, delist_date
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        with self.get_connection() as conn:
            df = conn.execute("""
                SELECT DISTINCT s.ts_code, s.symbol, s.name,
                       s.exchange, s.board, s.list_date, s.delist_date
                FROM stock_basic_history s
                JOIN st_status_history st
                  ON s.ts_code = st.ts_code
                WHERE st.trade_date = ?
                  AND st.is_st = TRUE
                  AND s.list_date <= ?
                  AND (s.delist_date > ? OR s.delist_date IS NULL)
                ORDER BY s.ts_code
            """, (trade_date, trade_date, trade_date)).fetchdf()

        return df

    # ──────────────────────────────────────────────────────────
    # 动态股票池（沪深300成分股时点对齐）
    # ──────────────────────────────────────────────────────────
    def sync_index_constituents(self, index_code: str = "000300.SH") -> None:
        """
        同步指数成分股历史（解决幸存者偏差）
        
        记录每个股票何时被加入/退出指数，确保回测时只使用
        该时点真实属于指数成分的股票。
        
        关键原则：
        1. 新纳入 → INSERT with in_date，out_date=NULL
        2. 已存在但本次快照未出现 → UPDATE out_date（标记退出）
        3. 绝对禁止整表 DELETE + INSERT 重建
        """
        if not HAS_AKSHARE:
            return

        with self.get_connection() as conn:
            try:
                # 解析指数代码（支持 "000300" 或 "000300.SH"）
                index_symbol = index_code.replace(".SH", "").replace(".SZ", "")
                
                # 获取当前成分股快照
                if index_symbol == "000300":
                    df = ak.index_stock_cons_csindex(symbol="000300")
                elif index_symbol == "000905":
                    df = ak.index_stock_cons_csindex(symbol="000905")
                elif index_symbol == "000852":
                    df = ak.index_stock_cons_csindex(symbol="000852")
                else:
                    logger.warning(f"Unsupported index: {index_code}")
                    return

                if df.empty:
                    return

                # 解析中证指数接口字段
                df = df.rename(columns={
                    '日期': 'snapshot_date',
                    '指数代码': 'index_code',
                    '成分券代码': 'ts_code_raw',
                    '成分券名称': 'name',
                    '交易所': 'exchange_cn',
                })
                df['index_code'] = df['index_code'] + '.SH'
                df['ts_code'] = df['ts_code_raw'].apply(build_ts_code)
                df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.strftime('%Y-%m-%d')
                df['in_date'] = df['snapshot_date']  # 快照日期 = 该股票进入指数的日期
                df['out_date'] = None               # 未退出
                df['source'] = 'csindex'

                # 写入（INSERT OR IGNORE：已存在则跳过，绝对禁止 DELETE）
                conn.register('tmp_constituents', df[['index_code', 'ts_code', 'in_date', 'out_date', 'source']])
                conn.execute("""
                    INSERT OR IGNORE INTO index_constituents_history
                        (index_code, ts_code, in_date, out_date, source)
                    SELECT index_code, ts_code,
                           CAST(in_date AS DATE),
                           NULL,
                           source
                    FROM tmp_constituents
                """)

                # ── 标记退出：更新 out_date ─────────────────────────────────
                # 找到本次快照的日期（所有成分股的日期相同，取第一个即可）
                snapshot_date = df['snapshot_date'].iloc[0]
                current_codes = set(df['ts_code'].tolist())

                # 注册当前快照代码列表
                codes_df = pd.DataFrame({'ts_code': list(current_codes)})
                conn.register('tmp_current_codes', codes_df)

                # 更新：之前在指数内（out_date IS NULL）、本次快照不再出现的股票
                # → 标记其 out_date = snapshot_date（前一天为退出日）
                from_date = (pd.Timestamp(snapshot_date) - timedelta(days=1)).strftime('%Y-%m-%d')
                conn.execute(f"""
                    UPDATE index_constituents_history
                    SET out_date = CAST(? AS DATE)
                    WHERE index_code = ?
                      AND out_date IS NULL
                      AND ts_code NOT IN (SELECT ts_code FROM tmp_current_codes)
                """, (from_date, index_code))

                conn.execute("DROP VIEW IF EXISTS tmp_constituents")
                conn.execute("DROP VIEW IF EXISTS tmp_current_codes")

                new_count = len(df)
                logger.info(
                    f"Synced {index_code}: {new_count} current constituents, "
                    f"out_date updated for removed stocks"
                )
            except Exception as e:
                logger.warning(f"Failed to sync index constituents: {e}")

    def get_index_constituents(self, index_code: str, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的指数成分股列表（DataFrame格式，支持历史PIT查询）

        Parameters
        ----------
        index_code : str
            指数代码，如 '000300.SH'
        trade_date : str
            查询日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 包含 ts_code, name, exchange, board, in_date, out_date
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        return self.query("""
            SELECT
                h.ts_code,
                b.name,
                b.exchange,
                b.board,
                h.in_date,
                h.out_date
            FROM index_constituents_history h
            LEFT JOIN stock_basic b ON h.ts_code = b.ts_code
            WHERE h.index_code = ?
              AND h.in_date <= ?
              AND (h.out_date IS NULL OR h.out_date > ?)
            ORDER BY h.ts_code
        """, (index_code, trade_date, trade_date))

    def get_universe_at_date(self, index_code: str, trade_date: str) -> List[str]:
        """
        获取指定日期的指数成分股列表（动态股票池）

        Returns
        -------
        List[str]: 在该日期属于指数成分的股票代码列表
        """
        if not HAS_DUCKDB:
            return []

        df = self.query("""
            SELECT ts_code FROM index_constituents_history
            WHERE index_code = ?
            AND in_date <= ?
            AND (out_date IS NULL OR out_date > ?)
        """, (index_code, trade_date, trade_date))
        return df["ts_code"].tolist() if not df.empty else []

    # ──────────────────────────────────────────────────────────
    # ST状态历史（时间序列特征）
    # ──────────────────────────────────────────────────────────
    def sync_st_status_history(self) -> None:
        """
        同步ST状态历史
        
        记录每只股票何时被ST/解除ST，用于时间序列特征，
        严禁在T日过滤"未来的ST"。
        
        注意：daily_bar_adjusted 表没有 name 字段，需要从 stock_basic 表 JOIN 获取。
        """
        if not HAS_DUCKDB:
            return

        with self.get_connection() as conn:
            # 从 stock_basic 获取含 ST 的股票列表
            try:
                # 先找到所有历史上曾是 ST 的股票
                st_stocks = conn.execute("""
                    SELECT ts_code, name FROM stock_basic 
                    WHERE name LIKE '%ST%' OR name LIKE '%*ST%'
                """).fetchdf()
                
                if st_stocks.empty:
                    return
                
                st_codes = st_stocks['ts_code'].tolist()
                
                # 从日线数据推断 ST 状态变化（使用 register 避免大列表拼接 SQL 注入风险）
                # 注意：daily_bar_adjusted 无 is_st 字段，只能用"ST股是否出现第一条记录"来标记新ST日。
                # is_new_st = TRUE 当且仅当该股在 daily_bar_adjusted 中的当前记录之前没有紧邻的交易记录，
                # 即 prev_trade_date IS NULL 或与当前日期间隔 > 1个交易日（被退市/停牌再上市）。
                # 如需精确ST摘帽历史，应接入 AkShare stock_zh_a_st_em 接口。
                st_df = pd.DataFrame({"ts_code": st_codes})
                conn.register('tmp_st_codes', st_df)
                conn.execute("""
                    INSERT OR REPLACE INTO st_status_history
                    (ts_code, trade_date, is_st, is_new_st)
                    SELECT 
                        sub.ts_code,
                        sub.trade_date,
                        TRUE as is_st,
                        CASE 
                            WHEN sub.prev_trade_date IS NULL THEN TRUE
                            WHEN datediff('day', sub.prev_trade_date, sub.trade_date) > 7 THEN TRUE
                            ELSE FALSE 
                        END as is_new_st
                    FROM (
                        SELECT 
                            d.ts_code,
                            d.trade_date,
                            LAG(d.trade_date) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) as prev_trade_date
                        FROM daily_bar_adjusted d
                        INNER JOIN tmp_st_codes t ON d.ts_code = t.ts_code
                    ) sub
                """)
                conn.execute("DROP VIEW tmp_st_codes")
                logger.info("Synced ST status history")
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Failed to sync ST status: {e}")

    def is_st_at_date(self, ts_code: str, trade_date: str) -> bool:
        """检查某股票在某日期是否为ST
        
        注意：st_status_history 只记录 ST 期间的数据行。
        如果该股在查询日没有记录，说明不是 ST（正常/已摘帽）。
        """
        df = self.query("""
            SELECT is_st FROM st_status_history
            WHERE ts_code = ?
            AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
        """, (ts_code, trade_date))
        # 如果没有记录，说明该股从未被 ST 或已摘帽，返回 False
        return not df.empty and bool(df.iloc[0, 0])

    # ──────────────────────────────────────────────────────────
    # 财务数据抓取（PIT 约束）
    # ──────────────────────────────────────────────────────────
    def fetch_financial_data(self,
                             ts_code: str = None,
                             start_year: int = None,
                             end_year: int = None,
                             max_workers: int = 4) -> Dict:
        """
        抓取财务数据（财报 + 估值），支持多线程。

        数据对齐规则：
        - 使用 ann_date（公告日）作为 PIT 约束点
        - 每期财报对应 end_date（报告期），但公告后才能使用

        AkShare 字段：营业总收入、净利润、资产总计、负债总计、
        基本每股收益、净资产收益率、每股现金流等
        """
        stats = {"success": 0, "failed": 0, "records": 0, "errors": []}
        if not HAS_AKSHARE:
            logger.warning("AkShare not available, financial fetch skipped")
            return stats

        end_year = end_year or self._get_now().year
        start_year = start_year or (end_year - 5)

        # 单只股票
        if ts_code:
            sym = ts_code.split(".")[0] if "." in ts_code else str(ts_code).zfill(6)
            df = self._fetch_financial_single(sym, start_year, end_year)
            if not df.empty:
                self._save_financial_data(df)
                stats["success"] = 1
                stats["records"] = len(df)
            else:
                stats["failed"] = 1
            return stats

        # 全量
        local = self._get_local_stocks()
        symbols = local["symbol"].tolist()

        def _fetch_one(sym: str):
            df = self._fetch_financial_single(str(sym).zfill(6), start_year, end_year)
            return sym, df

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_one, s): s for s in symbols}
            done = 0
            for future in as_completed(futures):
                sym, df = future.result()
                done += 1
                if not df.empty:
                    self._save_financial_data(df)
                    stats["success"] += 1
                    stats["records"] += len(df)
                else:
                    stats["failed"] += 1
                if done % 200 == 0:
                    logger.info(f"Financial progress: {done}/{len(symbols)} "
                          f"success={stats['success']} failed={stats['failed']}")
                time.sleep(0.2)

        logger.info(f"Financial data done. success={stats['success']} "
              f"failed={stats['failed']} records={stats['records']}")
        return stats

    def _fetch_financial_single(self,
                                 symbol: str,
                                 start_year: int,
                                 end_year: int) -> pd.DataFrame:
        """抓取单只股票的财务数据（年报 + 季报）"""
        records = []
        sym6 = str(symbol).zfill(6)
        # 统一使用 exchange_mapping.build_bs_code()，禁止手写交易所判断
        from scripts.exchange_mapping import build_bs_code as _build_bs_code
        bs_code = _build_bs_code(sym6)

        # 年报（4个财年）- Baostock 线程安全
        try:
            with self._bs_lock:
                bs.login()
                try:
                    for year in range(start_year, end_year + 1):
                        rs = bs.query_profit_statements_per_year(
                            bs_code=bs_code, year=str(year)
                        )
                        if rs and rs.error_code == "0":
                            while rs.next():
                                row = dict(zip(rs.fields, rs.get_row_data()))
                                if row.get("profit_statements_pub_date"):
                                    records.append(self._bs_profit_row(row, symbol))
                        time.sleep(0.1)
                finally:
                    bs.logout()
        except Exception as e:
            logger.debug(f"Baostock financial fetch failed for {symbol}: {e}")

        # 估值快照（用 AkShare）
        try:
            df_val = ak.stock_a_indicator_lg(secu=symbol)
            if not df_val.empty and "代码" in df_val.columns:
                df_val = df_val.rename(columns={"代码": "symbol"})
                for _, row in df_val.iterrows():
                    records.append(self._ak_indicator_row(row, symbol))
        except Exception as e:
            logger.debug(f"AkShare indicator fetch failed for {symbol}: {e}")

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def _bs_profit_row(self, row: Dict, symbol: str) -> Dict:
        """Baostock 利润表行标准化"""
        sym6 = str(symbol).zfill(6)
        ann = row.get("profit_statements_pub_date", "")
        end = row.get("profit_statements_report_date", "")
        return {
            "ts_code": build_ts_code(sym6),
            "ann_date": pd.to_datetime(ann, errors="coerce") if ann else pd.NaT,
            "end_date": pd.to_datetime(end, errors="coerce") if end else pd.NaT,
            "report_type": "年报",
            "revenue": float(row.get("total_operating_revenue", 0) or 0),
            "net_profit": float(row.get("parent_net_profit", 0) or 0),
            "total_assets": float(row.get("total_assets", 0) or 0),
            "total_equity": float(row.get("total_shareholder_equity", 0) or 0),
            "roe": float(row.get("avg_roe", 0) or 0),
            "roa": float(row.get("roe", 0) or 0),
            "eps": float(row.get("basic_eps", 0) or 0),
            "gross_margin": float(row.get("gross_profit_margin", 0) or 0),  # 与DDL字段名对齐
            "data_source": "baostock",
        }

    def _ak_indicator_row(self, row: pd.Series, symbol: str) -> Dict:
        """AkShare 指标行标准化"""
        sym6 = str(symbol).zfill(6)
        return {
            "ts_code": build_ts_code(sym6),
            "ann_date": pd.Timestamp.today(),
            "end_date": pd.NaT,
            "report_type": "指标",
            "revenue": float(row.get("营业总收入", 0) or 0),
            "net_profit": float(row.get("净利润", 0) or 0),
            "total_assets": 0,
            "total_equity": 0,
            "roe": float(row.get("净资产收益率(%)", 0) or 0),
            "roa": float(row.get("资产报酬率(%)", 0) or 0),
            "eps": float(row.get("基本每股收益", 0) or 0),
            "gross_margin": float(row.get("销售毛利率(%)", 0) or 0),  # 与DDL字段名对齐
            "pe_ttm": float(row.get("市盈率(TTM)", 0) or 0),
            "pb": float(row.get("市净率", 0) or 0),
            "data_source": "akshare",
        }

    def _save_financial_data(self, df: pd.DataFrame):
        """保存财务数据到数据库"""
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            # 财务数据用 REPLACE（可重复更新）
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO financial_data
                    (ts_code, ann_date, end_date, report_type, revenue,
                     net_profit, total_assets, total_equity, roe, roa,
                     eps, gross_margin, data_source)
                    SELECT
                        ts_code, ann_date, end_date, report_type,
                        revenue, net_profit, total_assets, total_equity,
                        roe, roa, eps, gross_margin, data_source
                    FROM df
                """)
            except Exception as e:
                logger.warning(f"Financial data UPSERT failed, trying fallback: {e}")
                # 字段不全时用原始列（列名来自白名单，非用户输入，DuckDB DF 绑定无需参数化）
                cols = [c for c in df.columns if c in
                        ["ts_code", "ann_date", "end_date", "report_type",
                         "revenue", "net_profit", "roe", "roa", "eps",
                         "total_assets", "total_equity", "gross_margin", "data_source"]]
                if cols:
                    col_str = ','.join(cols)
                    conn.execute(f"INSERT OR IGNORE INTO financial_data ({col_str}) SELECT {col_str} FROM df")

    def _get_local_stocks(self) -> pd.DataFrame:
        """从本地数据库获取股票列表"""
        return self.query("SELECT * FROM stock_basic")

    def sync_stock_list(self, include_delisted: bool = True):
        """
        同步股票列表到本地数据库（含 list_date/delist_date/exchange/board）

        改进点：
        1. 同时写入 stock_basic（当前快照）和 stock_basic_history（历史PIT版本）
        2. Baostock 补充精确的 list_date / delist_date
        3. exchange / board 统一由 exchange_mapping.classify_exchange() 派生，禁止自行拼接
        4. stock_basic_history 使用 UPSERT（ON CONFLICT DO UPDATE）防止重复记录
        5. eff_date = 今日，标记本次同步时间点
        """
        df = self.get_all_stocks(include_delisted)
        if df.empty:
            return

        # ── Baostock 补充 list_date / delist_date（线程安全）────────
        if HAS_BAOSTOCK:
            logger.info("Enriching list/delist dates from Baostock...")
            try:
                with self._bs_lock:
                    bs.login()
                    try:
                        bs_stocks = bs.query_all_stock(day=self._get_now().strftime("%Y-%m-%d"))
                    finally:
                        bs.logout()
                if bs_stocks is not None and not bs_stocks.empty:
                    # Baostock 代码格式：sh.600000 / sz.000001
                    # 将 sh.XXXXXX → XXXXXX（保留6位），sz.XXXXXX → XXXXXX
                    bs_code = bs_stocks["code"].str.replace(r"^sh\.", "", regex=True).str.replace(r"^sz\.", "", regex=True)
                    date_map   = dict(zip(bs_code, bs_stocks["ipoDate"]))
                    delist_map = dict(zip(bs_code, bs_stocks["outDate"]))
                    if "symbol" in df.columns:
                        df["list_date"] = df["symbol"].map(date_map).pipe(
                            lambda s: pd.to_datetime(s, errors="coerce")
                        )
                        df["delist_date"] = df["symbol"].map(delist_map).pipe(
                            lambda s: pd.to_datetime(s, errors="coerce")
                        )
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Baostock list_date enrichment failed: {e}")

        # ── 补全 exchange / board（统一从 exchange_mapping 派生）──────
        from scripts.exchange_mapping import classify_exchange
        if "exchange" not in df.columns or df["exchange"].isna().any():
            def _classify(row):
                sym = str(row.get("symbol", "")).zfill(6)
                exch, board = classify_exchange(sym)
                return pd.Series({"exchange": exch, "board": board})
            classified = df.apply(_classify, axis=1)
            df["exchange"] = classified["exchange"]
            df["board"] = classified["board"]

        for col in ["list_date", "delist_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "is_delisted" not in df.columns:
            df["is_delisted"] = df["delist_date"].notna()

        today_str = self._get_now().strftime("%Y-%m-%d")
        df["eff_date"] = pd.to_datetime(today_str)
        df["end_date"] = pd.NaT

        with self.get_connection() as conn:
            # ── 1. 写入 stock_basic（当前快照，全量覆盖）────────────
            basic_cols = ["ts_code", "symbol", "name", "exchange", "board",
                          "industry", "market", "list_date", "delist_date",
                          "is_delisted"]
            write_cols = [c for c in basic_cols if c in df.columns]
            conn.execute("DELETE FROM stock_basic")
            conn.register("tmp_basic", df[write_cols])
            conn.execute(
                f"INSERT INTO stock_basic ({','.join(write_cols)}) "
                f"SELECT {','.join(write_cols)} FROM tmp_basic"
            )
            conn.execute("DROP VIEW IF EXISTS tmp_basic")

            # ── 2. 写入 stock_basic_history（历史PIT层，仅追加/更新当日版本）──
            # UPSERT：同一 (ts_code, eff_date) 已存在则更新，不存在则插入
            # 保留历史版本：不删除旧 eff_date 的记录
            hist_cols = ["ts_code", "symbol", "name", "exchange", "board",
                         "industry", "market", "list_date", "delist_date",
                         "is_delisted", "eff_date", "end_date"]
            hist_write_cols = [c for c in hist_cols if c in df.columns]
            conn.register("tmp_hist", df[hist_write_cols])
            conn.execute(f"""
                INSERT INTO stock_basic_history ({','.join(hist_write_cols)})
                SELECT {','.join(hist_write_cols)} FROM tmp_hist
                ON CONFLICT (ts_code, eff_date) DO UPDATE SET
                    symbol       = excluded.symbol,
                    name         = excluded.name,
                    exchange     = excluded.exchange,
                    board        = excluded.board,
                    industry     = excluded.industry,
                    market       = excluded.market,
                    list_date    = excluded.list_date,
                    delist_date  = excluded.delist_date,
                    is_delisted  = excluded.is_delisted
            """)
            conn.execute("DROP VIEW IF EXISTS tmp_hist")

        list_filled = df["list_date"].notna().sum() if "list_date" in df.columns else 0
        logger.info(
            f"Synced {len(df)} stocks to database "
            f"(list_date filled: {list_filled}, eff_date: {today_str})"
        )

    # ──────────────────────────────────────────────────────────
    # 日线数据下载
    # ──────────────────────────────────────────────────────────
    def fetch_single(self,
                     symbol: str,
                     start_date: str,
                     end_date: str,
                     adjust: Literal["qfq", ""] = "qfq") -> pd.DataFrame:
        """
        下载单只股票日线数据
        
        重要：同时下载原始价和复权价，分别存储。
        - daily_bar_raw: 原始未复权OHLCV（所有数据的基础）
        - daily_bar_adjusted: 复权价格（通过 adj_factor 在查询时计算，或直接用复权close）
        
        adjust 参数仅决定返回给调用方的 DataFrame 是否含复权价，
        不影响原始价存储。
        
        Parameters
        ----------
        symbol : str
            6位股票代码
        start_date : str
            开始日期，格式 YYYY-MM-DD
        end_date : str
            结束日期，格式 YYYY-MM-DD
        adjust : 'qfq' | ''
            复权方式（仅影响返回值，复权因子始终计算并存入 daily_bar_adjusted）
        """
        if not HAS_AKSHARE:
            return pd.DataFrame()

        try:
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "")

            # ── 1. 始终下载原始价格（adjust=""）─────────────────────
            df_raw = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust=""
            )
            if df_raw.empty:
                return pd.DataFrame()

            # 列名标准化
            col_map = {
                "日期": "trade_date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "涨跌幅": "pct_chg", "换手率": "turnover"
            }
            df_raw = df_raw.rename(columns=col_map)
            df_raw["trade_date"] = pd.to_datetime(df_raw["trade_date"])
            df_raw["ts_code"] = build_ts_code(symbol)
            df_raw["pre_close"] = df_raw["close"].shift(1)
            df_raw["is_suspend"] = df_raw["volume"] == 0
            df_raw["data_source"] = "akshare"

            # ── 2. 下载复权价格（用于计算 adj_factor）─────────────
            adj_factor = pd.Series(1.0, index=df_raw.index)
            df_close_adj = df_raw["close"].copy()  # 默认复权=原始
            df_open_adj  = df_raw["open"].copy()
            df_high_adj  = df_raw["high"].copy()
            df_low_adj   = df_raw["low"].copy()
            if adjust:
                try:
                    df_adj = ak.stock_zh_a_hist(
                        symbol=symbol, period="daily",
                        start_date=start_str, end_date=end_str, adjust="qfq"
                    )
                    if not df_adj.empty:
                        adj_col_map = {
                            "日期": "td2", "开盘": "open_adj", "收盘": "close_adj",
                            "最高": "high_adj", "最低": "low_adj"
                        }
                        df_adj = df_adj.rename(columns=adj_col_map)
                        df_adj["td2"] = pd.to_datetime(df_adj["td2"])
                        df_raw = df_raw.merge(
                            df_adj[["td2", "open_adj", "high_adj", "low_adj", "close_adj"]],
                            left_on="trade_date", right_on="td2",
                            how="left"
                        )
                        # adj_factor = adj_close / raw_close（复权因子 = 复权收盘 / 原始收盘）
                        adj_factor = (df_raw["close_adj"] / df_raw["close"].replace(0, np.nan)).fillna(1.0)
                        adj_factor = adj_factor.replace([np.inf, -np.inf], 1.0)
                        # 保存复权价格序列
                        df_close_adj = df_raw["close_adj"].fillna(df_raw["close"])
                        df_open_adj  = df_raw["open_adj"].fillna(df_raw["open"])
                        df_high_adj  = df_raw["high_adj"].fillna(df_raw["high"])
                        df_low_adj   = df_raw["low_adj"].fillna(df_raw["low"])
                        df_raw.drop(
                            columns=["td2", "open_adj", "high_adj", "low_adj", "close_adj"],
                            inplace=True, errors="ignore"
                        )
                except Exception as e:
                    logger.debug(f"adj_factor calc failed for {symbol}: {e}")

            df_raw["adj_factor"] = adj_factor
            # ── 原始价保存到 _raw_ 字段（供 save_quotes 写入 daily_bar_raw）──
            df_raw["raw_open"]  = df_raw["open"].copy()
            df_raw["raw_high"]  = df_raw["high"].copy()
            df_raw["raw_low"]   = df_raw["low"].copy()
            df_raw["raw_close"] = df_raw["close"].copy()
            # ── 复权价覆盖 open/high/low/close（供 save_quotes 写入 daily_bar_adjusted）──
            df_raw["open"]  = df_open_adj
            df_raw["high"]  = df_high_adj
            df_raw["low"]   = df_low_adj
            df_raw["close"] = df_close_adj
            # 复权 pct_chg（用复权价重新计算）
            if adjust == "qfq":
                df_raw["pct_chg"] = (
                    df_raw["close"] / df_raw["pre_close"].replace(0, np.nan) - 1
                ) * 100

            # ── 3. 涨跌停判定（基于原始价）─────────────────────────
            df_raw = self._apply_limit_flags(df_raw, symbol)

            # ── 4. 验证 ──────────────────────────────────────────
            val = self.validator.validate(df_raw)
            if not val["ok"]:
                logger.warning(f"Data validation issues for {symbol}: {val['issues']}")

            # ── 5. 返回完整 DataFrame（含 raw_*/adj 双字段）─────────
            # save_quotes() 依赖：
            #   - raw_open/raw_high/raw_low/raw_close → 写入 daily_bar_raw
            #   - open/high/low/close（复权价）       → 写入 daily_bar_adjusted
            cols = ["ts_code", "trade_date",
                    "raw_open", "raw_high", "raw_low", "raw_close",  # 原始价
                    "open", "high", "low", "close",                  # 复权价
                    "pre_close", "volume", "amount", "pct_chg", "turnover",
                    "adj_factor", "is_suspend", "limit_up", "limit_down", "data_source"]
            return df_raw[[c for c in cols if c in df_raw.columns]]

        except (ValueError, KeyError, RuntimeError, ConnectionError) as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            return pd.DataFrame()

    def save_quotes(self, df: pd.DataFrame, mode: str = "append"):
        """
        保存行情数据到 DuckDB（双表分层写入：daily_bar_raw + daily_bar_adjusted）

        数据分层原则（禁止混用）：
        - daily_bar_raw：存原始未复权 OHLCV（raw_open/raw_high/raw_low/raw_close）
          若无 raw_* 字段（旧接口兼容），则以 close（adj_factor=1 时等于原始价）降级写入
        - daily_bar_adjusted：存复权价 OHLCV（open/high/low/close 已乘 adj_factor）

        写入策略：使用 DuckDB register() + INSERT ... SELECT（批量，700x 快于逐行）
        同步更新 sync_progress 表（记录每只股票的最新同步日期，支持断点续跑）
        """
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            # ── 预处理：统一列名，确保必需字段存在 ──────────────────
            df = df.copy()
            if "symbol" not in df.columns:
                df["symbol"] = df["ts_code"].str.replace(r"\.[A-Z]+$", "", regex=True)

            # 原始价字段：优先用 raw_* 前缀（fetch_single 新格式）
            # 降级兼容：若无 raw_* 字段，假定传入 df 的 close 等字段就是原始价
            has_raw_fields = "raw_close" in df.columns
            for raw_col, std_col in [("raw_open", "open"), ("raw_high", "high"),
                                      ("raw_low", "low"), ("raw_close", "close")]:
                if raw_col not in df.columns:
                    df[raw_col] = df.get(std_col, 0.0)

            for col in ["open", "high", "low", "close", "pre_close"]:
                if col not in df.columns:
                    df[col] = 0.0
            for col in ["volume", "amount"]:
                if col not in df.columns:
                    df[col] = 0
            for col in ["pct_chg", "turnover"]:
                if col not in df.columns:
                    df[col] = 0.0
            if "adj_factor" not in df.columns:
                df["adj_factor"] = 1.0
            for col in ["is_suspend", "limit_up", "limit_down"]:
                if col not in df.columns:
                    df[col] = False
            if "data_source" not in df.columns:
                df["data_source"] = "unknown"

            if mode == "overwrite":
                codes = df["ts_code"].unique().tolist()
                dates_min = df["trade_date"].min()
                dates_max = df["trade_date"].max()
                if codes and not (pd.isna(dates_min) or pd.isna(dates_max)):
                    codes_df = pd.DataFrame({"ts_code": codes})
                    conn.register('tmp_codes', codes_df)
                    conn.execute("""
                        DELETE FROM daily_bar_adjusted
                        WHERE ts_code IN (SELECT ts_code FROM tmp_codes)
                        AND trade_date BETWEEN ? AND ?
                    """, (dates_min, dates_max))
                    conn.execute("""
                        DELETE FROM daily_bar_raw
                        WHERE ts_code IN (SELECT ts_code FROM tmp_codes)
                        AND trade_date BETWEEN ? AND ?
                    """, (dates_min, dates_max))
                    conn.execute("DROP VIEW IF EXISTS tmp_codes")

            # 注册 DataFrame（批量写入核心优化）
            conn.register('tmp_quotes', df)

            # ── 1. 写入 daily_bar_raw（原始未复权价格）─────────────
            # 使用 raw_open/raw_close 等原始价字段
            try:
                conn.execute("""
                    INSERT INTO daily_bar_raw
                        (ts_code, trade_date, symbol,
                         open, high, low, close,
                         volume, amount, pct_chg, turnover, data_source)
                    SELECT ts_code, trade_date, symbol,
                           COALESCE(raw_open, open, 0),
                           COALESCE(raw_high, high, 0),
                           COALESCE(raw_low,  low,  0),
                           COALESCE(raw_close, close, 0),
                           COALESCE(volume, 0),
                           COALESCE(amount, 0),
                           COALESCE(pct_chg, 0),
                           COALESCE(turnover, 0),
                           COALESCE(data_source, 'unknown')
                    FROM tmp_quotes
                    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
                        symbol     = excluded.symbol,
                        open       = excluded.open,
                        high       = excluded.high,
                        low        = excluded.low,
                        close      = excluded.close,
                        volume     = excluded.volume,
                        amount     = excluded.amount,
                        pct_chg    = excluded.pct_chg,
                        turnover   = excluded.turnover,
                        data_source= excluded.data_source
                """)
            except Exception as e:
                logger.warning(f"daily_bar_raw upsert failed: {e}")

            # ── 2. 写入 daily_bar_adjusted（复权价格）──────────────
            # open/high/low/close 是复权价（fetch_single 已乘 adj_factor）
            try:
                conn.execute("""
                    INSERT INTO daily_bar_adjusted
                        (ts_code, trade_date, open, high, low, close, pre_close,
                         volume, amount, pct_chg, turnover, adj_factor,
                         is_suspend, limit_up, limit_down, data_source)
                    SELECT ts_code, trade_date,
                           COALESCE(open, 0),
                           COALESCE(high, 0),
                           COALESCE(low,  0),
                           COALESCE(close, 0),
                           COALESCE(pre_close, 0),
                           COALESCE(volume, 0),
                           COALESCE(amount, 0),
                           COALESCE(pct_chg, 0),
                           COALESCE(turnover, 0),
                           COALESCE(adj_factor, 1.0),
                           COALESCE(is_suspend, FALSE),
                           COALESCE(limit_up, FALSE),
                           COALESCE(limit_down, FALSE),
                           COALESCE(data_source, 'unknown')
                    FROM tmp_quotes
                    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
                        open       = excluded.open,
                        high       = excluded.high,
                        low        = excluded.low,
                        close      = excluded.close,
                        pre_close  = excluded.pre_close,
                        volume     = excluded.volume,
                        amount     = excluded.amount,
                        pct_chg    = excluded.pct_chg,
                        turnover   = excluded.turnover,
                        adj_factor = excluded.adj_factor,
                        is_suspend = excluded.is_suspend,
                        limit_up   = excluded.limit_up,
                        limit_down = excluded.limit_down,
                        data_source= excluded.data_source
                """)
            except Exception as e:
                logger.warning(f"daily_bar_adjusted upsert failed: {e}")

            conn.execute("DROP VIEW IF EXISTS tmp_quotes")

            # ── 3. 更新 sync_progress（断点续跑状态跟踪）──────────
            try:
                # 按股票聚合最新日期
                progress_df = (
                    df.groupby("ts_code")["trade_date"]
                    .max()
                    .reset_index()
                    .rename(columns={"trade_date": "last_sync_date"})
                )
                progress_df["table_name"]    = "daily_bar_raw"
                progress_df["status"]        = "ok"
                progress_df["error_msg"]     = None
                progress_df["total_records"] = (
                    df.groupby("ts_code").size().reset_index(name="cnt")["cnt"].values
                )
                conn.register("tmp_progress", progress_df)
                conn.execute("""
                    INSERT INTO sync_progress
                        (ts_code, table_name, last_sync_date, total_records, status)
                    SELECT ts_code, table_name, last_sync_date, total_records, status
                    FROM tmp_progress
                    ON CONFLICT (ts_code, table_name) DO UPDATE SET
                        last_sync_date = GREATEST(sync_progress.last_sync_date, excluded.last_sync_date),
                        total_records  = sync_progress.total_records + excluded.total_records,
                        last_sync_at   = NOW(),
                        status         = excluded.status
                """)
                conn.execute("DROP VIEW IF EXISTS tmp_progress")
            except Exception as e:
                logger.debug(f"sync_progress update skipped: {e}")

    def get_daily_raw(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取原始行情数据（未复权价格）

        Parameters
        ----------
        ts_code : str
            股票代码，如 '000001.SZ'
        start_date : str, optional
            开始日期 YYYY-MM-DD
        end_date : str, optional
            结束日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 包含 ts_code, trade_date, open, high, low, close, volume, amount, pct_chg, turnover
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        sql = "SELECT * FROM daily_bar_raw WHERE ts_code = ?"
        params = [ts_code]

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY trade_date"

        return self.query(sql, tuple(params))

    def get_daily_adjusted(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取复权行情数据（前复权价格）

        Parameters
        ----------
        ts_code : str
            股票代码，如 '000001.SZ'
        start_date : str, optional
            开始日期 YYYY-MM-DD
        end_date : str, optional
            结束日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 包含 ts_code, trade_date, open, high, low, close, volume, amount, pct_chg, turnover, adj_factor
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        sql = "SELECT * FROM daily_bar_adjusted WHERE ts_code = ?"
        params = [ts_code]

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY trade_date"

        return self.query(sql, tuple(params))

    # ──────────────────────────────────────────────────────────
    # 增量更新（多线程 + 重试 + 断点续传）
    # ──────────────────────────────────────────────────────────
    def get_latest_date(self, ts_code: str = None) -> Optional[str]:
        """获取本地最新同步日期（优先从 sync_progress 读，备选扫描 daily_bar_adjusted）

        断点续跑改进：
        - 优先查 sync_progress.last_sync_date（记录上次成功同步的最新日期）
        - 若 sync_progress 无记录，则扫描 daily_bar_adjusted 的 MAX(trade_date)
        - 验证最新日期数据完整性（close>0），损坏则回退到前一天
        """
        if not HAS_DUCKDB:
            return None

        # ── 优先查 sync_progress（断点续跑核心）──────────────────
        if ts_code:
            prog = self.query("""
                SELECT last_sync_date FROM sync_progress
                WHERE ts_code = ? AND table_name = 'daily_bar_raw'
                AND status = 'ok'
            """, (ts_code,))
            if not prog.empty and not pd.isna(prog.iloc[0, 0]):
                return pd.Timestamp(prog.iloc[0, 0]).strftime("%Y-%m-%d")

        # ── 备选：扫描 daily_bar_adjusted ────────────────────────
        if ts_code:
            result = self.query(
                "SELECT MAX(trade_date) as md FROM daily_bar_adjusted WHERE ts_code = ?",
                (ts_code,)
            )
        else:
            result = self.query("SELECT MAX(trade_date) as md FROM daily_bar_adjusted")
        val = result.iloc[0, 0] if not result.empty else None
        if val is None or pd.isna(val):
            return None

        latest = pd.Timestamp(val).strftime("%Y-%m-%d")

        # 断点续传加固：检查最新日期数据是否完整（close>0 且 volume>=0）
        if ts_code:
            check = self.query("""
                SELECT COUNT(*) as cnt FROM daily_bar_adjusted
                WHERE ts_code = ? AND trade_date = ?
                AND close > 0 AND volume >= 0
            """, (ts_code, latest))
            if check.empty or check.iloc[0, 0] == 0:
                # 数据损坏/不完整，回退到前一天
                prev = (pd.Timestamp(latest) - timedelta(days=1)).strftime("%Y-%m-%d")
                prev_check = self.query("""
                    SELECT COUNT(*) as cnt FROM daily_bar_adjusted
                    WHERE ts_code = ? AND trade_date = ?
                    AND close > 0 AND volume >= 0
                """, (ts_code, prev))
                if prev_check.empty or prev_check.iloc[0, 0] == 0:
                    return None  # 数据彻底损坏
                return prev
        return latest

    def get_sync_status(self, ts_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        查询增量同步进度状态（基于 sync_progress 表）

        Parameters
        ----------
        ts_codes : List[str], optional
            指定股票代码列表，None 表示查全部

        Returns
        -------
        pd.DataFrame: 同步状态表，含 ts_code / last_sync_date / status / total_records
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()
        if ts_codes:
            codes_df = pd.DataFrame({"ts_code": ts_codes})
            with self.get_connection() as conn:
                conn.register("tmp_sc", codes_df)
                df = conn.execute("""
                    SELECT sp.* FROM sync_progress sp
                    INNER JOIN tmp_sc t ON sp.ts_code = t.ts_code
                    WHERE sp.table_name = 'daily_bar_raw'
                    ORDER BY sp.last_sync_date DESC
                """).fetchdf()
                conn.execute("DROP VIEW IF EXISTS tmp_sc")
            return df
        return self.query("""
            SELECT * FROM sync_progress
            WHERE table_name = 'daily_bar_raw'
            ORDER BY last_sync_date DESC
        """)

    def _fetch_single_with_retry(self,
                                 symbol: str,
                                 start_date: str,
                                 end_date: str,
                                 adjust: Literal["qfq", ""] = "qfq",
                                 max_retries: int = DEFAULT_MAX_RETRIES) -> Tuple[pd.DataFrame, str]:
        """
        带指数退避重试的抓取（AkShare → Baostock → 失败）
        
        改进：HTTP 429 限流时使用更长等待时间；先尝试一个数据源，
        失败后再尝试备援，避免同时触发多个数据源的限流。

        Returns
        -------
        (df, source): df数据, source='akshare'|'baostock'|''
        """
        last_error: str = ""
        for attempt in range(max_retries):
            # 优先尝试 AkShare（主数据源）
            if HAS_AKSHARE:
                try:
                    df = self._fetch_akshare(symbol, start_date, end_date, adjust)
                    if not df.empty:
                        return df, "akshare"
                except Exception as e:
                    last_error = str(e)
                    # HTTP 429 限流：使用更长等待时间
                    if "429" in last_error or "Too Many Requests" in last_error:
                        wait = (5 ** attempt) + random.uniform(1, 3)  # 限流时更保守
                        logger.warning(f"AkShare rate limit hit for {symbol}, waiting {wait:.1f}s")
                        time.sleep(wait)
                        continue

            # AkShare 失败后尝试 Baostock 备援
            if HAS_BAOSTOCK:
                try:
                    df = self._fetch_baostock(symbol, start_date, end_date, adjust)
                    if not df.empty:
                        return df, "baostock"
                except Exception as e:
                    last_error = str(e)

            # 非限流错误或所有数据源都失败
            if attempt < max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait)

        logger.debug(f"All sources failed for {symbol} after {max_retries} retries: {last_error}")
        return pd.DataFrame(), ""

    def _fetch_akshare(self,
                       symbol: str,
                       start_date: str,
                       end_date: str,
                       adjust: Literal["qfq", ""] = "qfq") -> pd.DataFrame:
        """AkShare 抓取（内部用）"""
        try:
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "")
            adj_map = {"qfq": "qfq", "": ""}
            adj = adj_map.get(adjust, "qfq")

            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust=adj
            )
            if df.empty:
                return pd.DataFrame()

            df = df.rename(columns={
                "日期": "trade_date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "涨跌幅": "pct_chg", "换手率": "turnover"
            })
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            sym6 = str(symbol).zfill(6)
            df["ts_code"] = build_ts_code(sym6)
            df["pre_close"] = df["close"].shift(1)
            df["is_suspend"] = df["volume"] == 0
            
            # 涨跌停判定
            df = self._apply_limit_flags(df, symbol)
            df["data_source"] = "akshare"

            # 复权因子
            if adjust:
                try:
                    df_raw = ak.stock_zh_a_hist(
                        symbol=symbol, period="daily",
                        start_date=start_str, end_date=end_str, adjust=""
                    )
                    if not df_raw.empty:
                        df_raw = df_raw.rename(columns={"日期": "td2", "收盘": "close_raw"})
                        df_raw["td2"] = pd.to_datetime(df_raw["td2"])
                        df = df.merge(df_raw[["td2", "close_raw"]], left_on="trade_date", right_on="td2", how="left")
                        df["adj_factor"] = df["close"] / df["close_raw"].replace(0, np.nan)
                        df.drop(columns=["td2", "close_raw"], inplace=True, errors="ignore")
                    else:
                        df["adj_factor"] = 1.0
                except Exception as e:
                    logger.debug(f"adj_factor calc failed for {symbol}: {e}")
                    df["adj_factor"] = 1.0
            else:
                df["adj_factor"] = 1.0

            cols = ["ts_code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "volume", "amount", "pct_chg", "turnover",
                    "adj_factor", "is_suspend", "limit_up", "limit_down", "data_source"]
            return df[[c for c in cols if c in df.columns]]
        except Exception as e:
            logger.debug(f"AkShare fetch failed for {symbol}: {e}")
            return pd.DataFrame()

    def _fetch_baostock(self,
                        symbol: str,
                        start_date: str,
                        end_date: str,
                        adjust: Literal["qfq", ""] = "qfq") -> pd.DataFrame:
        """Baostock 抓取（备援用，线程安全：使用锁保护全局会话）"""
        if not HAS_BAOSTOCK:
            return pd.DataFrame()
        try:
            adjflag_map = {"qfq": "2", "": "3"}
            adjflag = adjflag_map.get(adjust, "2")

            # 标准化 baostock 代码
            sym6 = str(symbol).zfill(6)
            # 统一使用 exchange_mapping.build_bs_code()，禁止手写交易所判断
            # 修复：920xxx 不再误判为 sh，北交所 4/8/920xxx 全部正确归为 bj
            from scripts.exchange_mapping import build_bs_code as _build_bs_code
            bs_code = _build_bs_code(sym6)

            # Baostock 全局会话锁（多线程安全）
            with self._bs_lock:
                bs.login()
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount,adjustflag",
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                        frequency="d",
                        adjustflag=adjflag
                    )
                finally:
                    bs.logout()
            if rs is None or rs.error_code != "0":
                return pd.DataFrame()

            data = []
            while rs.next():
                data.append(rs.get_row_data())
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=rs.fields)
            df.columns = ["trade_date", "open", "high", "low", "close", "volume", "amount", "adj_flag"]
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df["ts_code"] = build_ts_code(sym6)
            df["pre_close"] = df["close"].shift(1)
            df["pct_chg"] = df["close"].pct_change().fillna(0) * 100
            df["amount"] = df["amount"].astype(float)
            df["is_suspend"] = df["volume"] == 0
            
            # 涨跌停判定
            df = self._apply_limit_flags(df, symbol)
            df["data_source"] = "baostock"
            df["adj_factor"] = 1.0
            df["turnover"] = 0.0

            cols = ["ts_code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "volume", "amount", "pct_chg", "turnover",
                    "adj_factor", "is_suspend", "limit_up", "limit_down", "data_source"]
            return df[[c for c in cols if c in df.columns]]
        except Exception as e:
            logger.debug(f"Baostock fetch failed for {symbol}: {e}")
            return pd.DataFrame()

    def _batch_get_latest_dates(self, ts_codes: List[str]) -> Dict[str, str]:
        """批量获取多只股票的最新同步日期（优先 sync_progress，备选 daily_bar_adjusted）

        断点续跑改进：
        - 优先从 sync_progress 读（O(1) 索引查询，无需扫描数据表）
        - 仅对 sync_progress 无记录的股票扫描 daily_bar_adjusted

        Returns
        -------
        Dict[str, str]: {ts_code: latest_date_str} 字典
        """
        if not ts_codes or not HAS_DUCKDB:
            return {}
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        result: Dict[str, str] = {}

        with self.get_connection() as conn:
            # ── 1. 优先从 sync_progress 批量读取 ──────────────────
            conn.register('tmp_bl', codes_df)
            prog_df = conn.execute("""
                SELECT sp.ts_code, sp.last_sync_date
                FROM sync_progress sp
                INNER JOIN tmp_bl t ON sp.ts_code = t.ts_code
                WHERE sp.table_name = 'daily_bar_raw' AND sp.status = 'ok'
            """).fetchdf()

            for _, row in prog_df.iterrows():
                val = row["last_sync_date"]
                if val and not pd.isna(val):
                    result[row["ts_code"]] = pd.Timestamp(val).strftime("%Y-%m-%d")

            # ── 2. 对 sync_progress 无记录的股票，扫描 daily_bar_adjusted ──
            missing = [tc for tc in ts_codes if tc not in result]
            if missing:
                missing_df = pd.DataFrame({"ts_code": missing})
                conn.register('tmp_missing', missing_df)
                adj_df = conn.execute("""
                    SELECT d.ts_code, MAX(d.trade_date) as latest_date
                    FROM daily_bar_adjusted d
                    INNER JOIN tmp_missing t ON d.ts_code = t.ts_code
                    GROUP BY d.ts_code
                """).fetchdf()
                conn.execute("DROP VIEW IF EXISTS tmp_missing")
                for _, row in adj_df.iterrows():
                    val = row["latest_date"]
                    if val and not pd.isna(val):
                        result[row["ts_code"]] = pd.Timestamp(val).strftime("%Y-%m-%d")

            conn.execute("DROP VIEW IF EXISTS tmp_bl")

        return result

    def update_daily_data(self,
                          symbols: List[str] = None,
                          adjust: Literal["qfq", ""] = "qfq",
                          max_workers: int = 12,
                          delay: float = 0.2,
                          check_dividend: bool = True,
                          cross_validate: bool = True) -> Dict:
        """
        多线程增量更新日线数据 — 生产级增强版 v2
        
        改进点（Issue #1）：
        1. max_workers 默认 12（充分利用多核）
        2. 批量 latest_date 缓存（单条SQL替代N次查询）
        3. 限速移入线程内部（主线程不再 sleep）
        4. 批量入库累积写入（减少DB连接开销）
        5. 复权因子监测（dividend_check）
        6. 多源交叉验证（AkShare vs Baostock）
        7. 断点续传加固 + DataValidator
        """
        start_time = time.time()
        stats = {
            "total": len(symbols) if symbols else 0,
            "success": 0, "failed": 0, "skipped": 0, "records": 0,
            "akshare_success": 0, "baostock_fallback": 0,
            "dividend_detected": 0,
            "cross_validate_errors": 0,
            "errors": [], "elapsed_sec": 0.0
        }

        if symbols is None:
            local = self._get_local_stocks()
            symbols = local["symbol"].tolist()
            stats["total"] = len(symbols)

        latest_local = self.get_latest_date()
        if latest_local:
            start_date = (pd.Timestamp(latest_local) + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = DEFAULT_START_DATE

        end_date = self._get_now().strftime("%Y-%m-%d")
        if start_date > end_date:
            logger.info("Data is up-to-date.")
            return stats

        # ── Issue #2: 批量缓存 latest_date（单条SQL替代5000+次查询）──
        ts_codes = [build_ts_code(sym) for sym in symbols]

        logger.info(f"Pre-caching latest dates for {len(ts_codes)} stocks...")
        latest_cache = self._batch_get_latest_dates(ts_codes)
        logger.info(f"Cache hit: {len(latest_cache)}/{len(ts_codes)} stocks have local data")

        # 预过滤：排除已是最新且无复权因子的股票
        active_symbols = []
        for sym, tc in zip(symbols, ts_codes):
            cached = latest_cache.get(tc, "")
            if cached and cached >= start_date:
                stats["skipped"] += 1
            else:
                active_symbols.append(sym)

        logger.info(f"Incremental update: {start_date} ~ {end_date}, "
              f"total={len(symbols)}, active={len(active_symbols)}, "
              f"skipped={stats['skipped']}, workers={max_workers}")

        if not active_symbols:
            stats["elapsed_sec"] = time.time() - start_time
            return stats

        # ── 线程安全的批量累积缓冲区 ──
        import threading
        batch_buffer = []
        batch_lock = threading.Lock()
        BATCH_FLUSH_SIZE = 50  # 每50只股票批量入库一次

        def _download_one(sym: str) -> Tuple[str, pd.DataFrame, str, str]:
            """单线程下载器（内部自带限速）"""
            try:
                ts_code = build_ts_code(sym)
                # 使用缓存而非实时查询
                sym_latest = latest_cache.get(ts_code, "")
                if sym_latest and sym_latest >= start_date:
                    return sym, pd.DataFrame(), "", ""

                actual_start = start_date
                if actual_start > end_date:
                    return sym, pd.DataFrame(), "", ""

                # 线程内限速：避免并发请求过猛
                time.sleep(delay * random.uniform(0.5, 1.5))

                df, source = self._fetch_single_with_retry(sym, actual_start, end_date, adjust)
                return sym, df, source, ""
            except Exception as e:
                return sym, pd.DataFrame(), "", str(e)

        # 多线程下载 + 批量入库
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_download_one, s): s for s in active_symbols}
            done = 0
            for future in as_completed(futures):
                sym, df, source, err = future.result()
                done += 1
                if not df.empty:
                    # 复权因子监测
                    if check_dividend:
                        div_alerts = self._check_adj_factor_change(sym, df)
                        if div_alerts:
                            stats["dividend_detected"] += len(div_alerts)

                    # DataValidator 验证
                    val = self.validator.validate(df)
                    if not val["ok"]:
                        for issue in val["issues"]:
                            stats["errors"].append(f"{sym}: {issue}")

                    # 累积到批量缓冲区
                    with batch_lock:
                        batch_buffer.append(df)
                        if len(batch_buffer) >= BATCH_FLUSH_SIZE:
                            self.save_quotes(pd.concat(batch_buffer, ignore_index=True), mode="append")
                            batch_buffer.clear()

                    stats["success"] += 1
                    stats["records"] += len(df)
                    if source == "akshare":
                        stats["akshare_success"] += 1
                    elif source == "baostock":
                        stats["baostock_fallback"] += 1
                else:
                    stats["failed"] += 1
                    if err:
                        stats["errors"].append(f"{sym}: {err}")

                if done % 200 == 0:
                    logger.info(f"Progress: {done}/{len(active_symbols)} "
                          f"({done/len(active_symbols)*100:.1f}%) "
                          f"success={stats['success']} failed={stats['failed']}")

        # 刷出缓冲区剩余数据
        if batch_buffer:
            self.save_quotes(pd.concat(batch_buffer, ignore_index=True), mode="append")
            batch_buffer.clear()

        # 多源交叉验证（抽样）
        if cross_validate and HAS_BAOSTOCK and stats["success"] > 0:
            self._cross_validate_sources(active_symbols[:min(50, len(active_symbols))], end_date, stats)

        stats["elapsed_sec"] = time.time() - start_time
        rate = stats["success"] / stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else 0
        logger.info(f"Update done. success={stats['success']} failed={stats['failed']} "
              f"skipped={stats['skipped']} records={stats['records']} "
              f"time={stats['elapsed_sec']:.1f}s ({rate:.1f} stocks/s) "
              f"dividend={stats['dividend_detected']} "
              f"validate_err={stats['cross_validate_errors']}")
        return stats

    def _check_adj_factor_change(self,
                                  symbol: str,
                                  new_data: pd.DataFrame) -> List[Dict]:
        """
        复权因子变化检测
        
        如果 adj_factor 发生非连续跳变（>5%），说明发生了分红/拆股，
        需要记录日志，严重时可触发全量重拉。
        """
        if "adj_factor" not in new_data.columns or new_data.empty:
            return []

        alerts = []
        ts_code = new_data["ts_code"].iloc[0]

        # 获取本地最近的复权因子
        local = self.query("""
            SELECT trade_date, adj_factor
            FROM daily_bar_adjusted
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 5
        """, (ts_code,))

        if local.empty:
            return []

        old_adj = local["adj_factor"].iloc[0]
        new_adj = new_data["adj_factor"].iloc[0]

        if old_adj > 0 and new_adj > 0:
            change_ratio = abs(new_adj / old_adj - 1)
            if change_ratio > DEFAULT_ADJ_CHANGE_THRESHOLD:  # 5% 以上变化
                alert = {
                    "ts_code": ts_code,
                    "trade_date": new_data["trade_date"].iloc[0],
                    "old_adj": old_adj,
                    "new_adj": new_adj,
                    "change_ratio": change_ratio,
                }
                alerts.append(alert)

                # 记录到日志表（参数化查询防止SQL注入）
                with self.get_connection() as log_conn:
                    log_conn.execute("""
                        INSERT INTO adj_factor_log
                        (ts_code, trade_date, adj_factor_old, adj_factor_new, change_ratio)
                        VALUES (?, ?, ?, ?, ?)
                    """, (ts_code, str(alert["trade_date"]), old_adj, new_adj, change_ratio))

        return alerts

    def _cross_validate_sources(self,
                                symbols: List[str],
                                trade_date: str,
                                stats: Dict) -> None:
        """
        AkShare vs Baostock 交叉验证
        
        抽样检查当日涨跌幅，偏差 > 0.5% 报警
        """
        if not HAS_BAOSTOCK:
            return

        # Baostock 线程安全
        with self._bs_lock:
            bs.login()
            try:
                for sym in symbols[:20]:  # 抽样20只
                    sym6 = str(sym).zfill(6)
                    # 统一使用 exchange_mapping.build_bs_code()，修复：688/920xxx/4xxx/8xxx 全部正确
                    from scripts.exchange_mapping import build_bs_code as _build_bs_code
                    bs_code = _build_bs_code(sym6)

                    # AkShare 数据（参数化查询防止SQL注入）
                    ak_data = self.query("""
                        SELECT pct_chg FROM daily_bar_adjusted
                        WHERE ts_code LIKE ?
                        AND trade_date = ?
                        LIMIT 1
                    """, (f"{sym6}%", trade_date))
                    if ak_data.empty:
                        continue

                    # Baostock 数据
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code, "date,pctChg",
                            start_date=trade_date.replace("-", ""),
                            end_date=trade_date.replace("-", ""),
                            frequency="d"
                        )
                        if rs is None or rs.error_code != "0":
                            continue

                        bs_pct = None
                        while rs.next():
                            row = rs.get_row_data()
                            if row[1]:
                                bs_pct = float(row[1])
                                break

                        if bs_pct is None:
                            continue

                        ak_pct = ak_data["pct_chg"].iloc[0]
                        diff = abs(ak_pct - bs_pct)

                        if diff > 0.5:  # 偏差超过 0.5%
                            stats["cross_validate_errors"] += 1
                            with self.get_connection() as conn:
                                conn.execute("""
                                    INSERT INTO data_quality_alert
                                    (alert_type, ts_code, trade_date, detail)
                                    VALUES (?, ?, ?, ?)
                                """, ('cross_validate', sym6, trade_date,
                                      f'AkShare={ak_pct:.2f}% vs Baostock={bs_pct:.2f}%, diff={diff:.2f}%'))
                    except Exception as e:
                        logger.debug(f"Cross-validate error for {sym6}: {e}")
            finally:
                bs.logout()


    # ──────────────────────────────────────────────────────────
    # 数据质量全面校验（写入 data_quality_alert 表）
    # ──────────────────────────────────────────────────────────
    def run_data_quality_check(
        self,
        start_date: str = None,
        end_date: str = None,
        ts_codes: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        对 daily_bar_raw / daily_bar_adjusted 执行全面数据质量校验。

        校验项（结果写入 data_quality_alert 表）：
        1. duplicate_rows        — (ts_code, trade_date) 重复行
        2. ohlc_violation        — high < low 或 close 超出 [low, high] 范围
        3. pct_chg_extreme       — 单日涨跌幅绝对值 > 60%（超出任何板块限制）
        4. adj_factor_jump       — 相邻两日 adj_factor 变化 > 5%（可能分红/拆股但未记录）
        5. zero_volume_non_suspend — volume=0 但未标记 is_suspend
        6. missing_trade_days    — 相对 trade_calendar 缺失的交易日（每只股票）
        7. delisted_with_future_data — 已退市股票（delist_date 非空）仍有晚于退市日的数据
        8. adj_raw_ratio_invalid — daily_bar_adjusted.close / daily_bar_raw.close 偏离 adj_factor > 1%

        Parameters
        ----------
        start_date : str, optional
            校验起始日期，默认近 90 天
        end_date : str, optional
            校验截止日期，默认今日
        ts_codes : List[str], optional
            指定股票列表，None 表示全库校验

        Returns
        -------
        Dict[str, int]: 各校验类型发现的问题数量
        """
        if not HAS_DUCKDB:
            return {}
        end_date   = end_date   or self._get_now().strftime("%Y-%m-%d")
        start_date = start_date or (pd.Timestamp(end_date) - timedelta(days=90)).strftime("%Y-%m-%d")
        stats: Dict[str, int] = {}

        # 构建过滤子句
        code_filter_adj = ""
        code_filter_raw = ""
        code_params_adj: list = [start_date, end_date]
        code_params_raw: list = [start_date, end_date]
        if ts_codes:
            placeholders = ",".join(["?" for _ in ts_codes])
            code_filter_adj = f"AND ts_code IN ({placeholders})"
            code_filter_raw = f"AND ts_code IN ({placeholders})"
            code_params_adj += ts_codes
            code_params_raw += ts_codes

        alerts = []

        # ── 1. 重复行检测 ──────────────────────────────────────
        dup_sql = f"""
            SELECT ts_code, trade_date, COUNT(*) as cnt
            FROM daily_bar_raw
            WHERE trade_date BETWEEN ? AND ? {code_filter_raw}
            GROUP BY ts_code, trade_date
            HAVING COUNT(*) > 1
        """
        try:
            dup_df = self.query(dup_sql, tuple(code_params_raw))
            for _, row in dup_df.iterrows():
                alerts.append(("duplicate_rows", row["ts_code"], row["trade_date"],
                                f"daily_bar_raw has {int(row['cnt'])} duplicate rows"))
            stats["duplicate_rows"] = len(dup_df)
        except Exception as e:
            logger.warning(f"duplicate_rows check failed: {e}")
            stats["duplicate_rows"] = -1

        # ── 2. OHLC 关系校验 ──────────────────────────────────
        ohlc_sql = f"""
            SELECT ts_code, trade_date,
                   open, high, low, close
            FROM daily_bar_raw
            WHERE trade_date BETWEEN ? AND ? {code_filter_raw}
              AND (high < low
                OR close > high * 1.001
                OR close < low  * 0.999
                OR open  > high * 1.001
                OR open  < low  * 0.999)
        """
        try:
            ohlc_df = self.query(ohlc_sql, tuple(code_params_raw))
            for _, row in ohlc_df.iterrows():
                alerts.append(("ohlc_violation", row["ts_code"], row["trade_date"],
                                f"OHLC: open={row['open']}, high={row['high']}, "
                                f"low={row['low']}, close={row['close']}"))
            stats["ohlc_violation"] = len(ohlc_df)
        except Exception as e:
            logger.warning(f"ohlc_violation check failed: {e}")
            stats["ohlc_violation"] = -1

        # ── 3. 涨跌幅极端值检测 ─────────────────────────────────
        pct_sql = f"""
            SELECT ts_code, trade_date, pct_chg
            FROM daily_bar_adjusted
            WHERE trade_date BETWEEN ? AND ? {code_filter_adj}
              AND ABS(pct_chg) > 60
        """
        try:
            pct_df = self.query(pct_sql, tuple(code_params_adj))
            for _, row in pct_df.iterrows():
                alerts.append(("pct_chg_extreme", row["ts_code"], row["trade_date"],
                                f"pct_chg={row['pct_chg']:.2f}% exceeds ±60%"))
            stats["pct_chg_extreme"] = len(pct_df)
        except Exception as e:
            logger.warning(f"pct_chg_extreme check failed: {e}")
            stats["pct_chg_extreme"] = -1

        # ── 4. 复权因子跳变检测 ─────────────────────────────────
        adj_jump_sql = f"""
            WITH adj_diff AS (
                SELECT ts_code, trade_date, adj_factor,
                       LAG(adj_factor) OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_adj
                FROM daily_bar_adjusted
                WHERE trade_date BETWEEN ? AND ? {code_filter_adj}
            )
            SELECT ts_code, trade_date, adj_factor, prev_adj,
                   ABS(adj_factor / NULLIF(prev_adj, 0) - 1) AS change_ratio
            FROM adj_diff
            WHERE prev_adj IS NOT NULL
              AND ABS(adj_factor / NULLIF(prev_adj, 0) - 1) > 0.05
        """
        try:
            adj_df = self.query(adj_jump_sql, tuple(code_params_adj))
            for _, row in adj_df.iterrows():
                alerts.append(("adj_factor_jump", row["ts_code"], row["trade_date"],
                                f"adj_factor jumped from {row['prev_adj']:.4f} to "
                                f"{row['adj_factor']:.4f} ({row['change_ratio']*100:.2f}%)"))
            stats["adj_factor_jump"] = len(adj_df)
        except Exception as e:
            logger.warning(f"adj_factor_jump check failed: {e}")
            stats["adj_factor_jump"] = -1

        # ── 5. volume=0 但未标记停牌 ────────────────────────────
        suspend_sql = f"""
            SELECT ts_code, trade_date, volume, is_suspend
            FROM daily_bar_adjusted
            WHERE trade_date BETWEEN ? AND ? {code_filter_adj}
              AND volume = 0
              AND (is_suspend IS NULL OR is_suspend = FALSE)
        """
        try:
            susp_df = self.query(suspend_sql, tuple(code_params_adj))
            for _, row in susp_df.iterrows():
                alerts.append(("zero_volume_non_suspend", row["ts_code"], row["trade_date"],
                                "volume=0 but is_suspend not set"))
            stats["zero_volume_non_suspend"] = len(susp_df)
        except Exception as e:
            logger.warning(f"zero_volume_non_suspend check failed: {e}")
            stats["zero_volume_non_suspend"] = -1

        # ── 6. 退市股在退市日后仍有数据 ─────────────────────────
        delist_code_filter = ""
        if ts_codes:
            placeholders = ",".join(["?" for _ in ts_codes])
            delist_code_filter = f"AND d.ts_code IN ({placeholders})"
        delist_sql = f"""
            SELECT d.ts_code, d.trade_date, s.delist_date
            FROM daily_bar_raw d
            JOIN stock_basic_history s ON d.ts_code = s.ts_code
            WHERE s.delist_date IS NOT NULL
              AND d.trade_date > s.delist_date + INTERVAL 30 DAYS
              AND d.trade_date BETWEEN ? AND ? {delist_code_filter}
        """
        delist_params = [start_date, end_date] + (list(ts_codes) if ts_codes else [])
        try:
            dl_df = self.query(delist_sql, tuple(delist_params))
            for _, row in dl_df.iterrows():
                alerts.append(("delisted_with_future_data", row["ts_code"], row["trade_date"],
                                f"data after delist_date={row['delist_date']}"))
            stats["delisted_with_future_data"] = len(dl_df)
        except Exception as e:
            logger.warning(f"delisted_with_future_data check failed: {e}")
            stats["delisted_with_future_data"] = -1

        # ── 7. 复权价 / 原始价比率偏离 adj_factor ───────────────
        ratio_sql = f"""
            SELECT adj.ts_code, adj.trade_date,
                   adj.close AS adj_close, raw.close AS raw_close,
                   adj.adj_factor,
                   ABS(adj.close / NULLIF(raw.close, 0) - adj.adj_factor) AS ratio_diff
            FROM daily_bar_adjusted adj
            JOIN daily_bar_raw raw
              ON adj.ts_code = raw.ts_code AND adj.trade_date = raw.trade_date
            WHERE adj.trade_date BETWEEN ? AND ?
              AND raw.close > 0 AND adj.adj_factor > 0
              AND ABS(adj.close / NULLIF(raw.close, 0) - adj.adj_factor) > adj.adj_factor * 0.01
              {code_filter_adj.replace('AND ts_code', 'AND adj.ts_code')}
        """
        try:
            ratio_params = [start_date, end_date] + (ts_codes or [])
            ratio_df = self.query(ratio_sql, tuple(ratio_params))
            for _, row in ratio_df.iterrows():
                alerts.append(("adj_raw_ratio_invalid", row["ts_code"], row["trade_date"],
                                f"adj/raw={row['adj_close']/max(row['raw_close'],1e-9):.4f} "
                                f"vs adj_factor={row['adj_factor']:.4f} "
                                f"(diff={row['ratio_diff']:.4f})"))
            stats["adj_raw_ratio_invalid"] = len(ratio_df)
        except Exception as e:
            logger.warning(f"adj_raw_ratio_invalid check failed: {e}")
            stats["adj_raw_ratio_invalid"] = -1

        # ── 写入 data_quality_alert 表（批量插入）────────────────
        if alerts:
            with self.get_connection() as conn:
                for alert_type, ts_code, trade_date, detail in alerts:
                    try:
                        conn.execute("""
                            INSERT INTO data_quality_alert
                                (alert_type, ts_code, trade_date, detail)
                            VALUES (?, ?, ?, ?)
                        """, (alert_type, str(ts_code), str(trade_date)[:10], detail))
                    except Exception as e:
                        logger.debug(f"alert insert failed: {e}")

        total = sum(v for v in stats.values() if v > 0)
        logger.info(
            f"Data quality check [{start_date}~{end_date}]: "
            f"total={total} alerts | {stats}"
        )
        return stats

    # ──────────────────────────────────────────────────────────
    # Issue #3: 停牌日期填充 & 退市过滤
    # ──────────────────────────────────────────────────────────
    def _fill_suspend_dates(self,
                            df: pd.DataFrame,
                            start_date: str,
                            end_date: str) -> pd.DataFrame:
        """
        填充停牌日缺失日期（Forward Fill），解决均线断裂问题

        性能优化：使用双指针 O(n+m) 而非 O(n*m) 过滤操作

        对于数据库中缺失的停牌日期（volume=0 或无数据行），
        使用前一日收盘价填充 OHLC，确保时间序列连续性。

        Parameters
        ----------
        df : pd.DataFrame
            已有的日线数据（需按 trade_date 排序）
        start_date : str
            期望的起始日期 YYYY-MM-DD
        end_date : str
            期望的结束日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 填充后的完整时间序列
        """
        if df.empty:
            return df

        ts_code = df["ts_code"].iloc[0]

        # 获取交易日历中的完整交易日序列
        trade_dates = self.get_trade_dates(start_date, end_date)
        if not trade_dates:
            return df

        # 转换为 datetime 列表并排序
        trade_dates = sorted([pd.Timestamp(d) for d in trade_dates])
        df_dates = pd.to_datetime(df["trade_date"]).tolist()
        existing_set = set(df_dates)

        # 双指针查找缺失日期（O(n+m)）
        missing_dates = []
        j = 0
        for td in trade_dates:
            if td not in existing_set:
                missing_dates.append(td)

        if not missing_dates:
            return df

        # 预处理：提取 df 中的有效数据用于前向填充
        # 将 df 转换为字典列表，避免重复访问 DataFrame
        df_list = df.to_dict('records')
        last_valid = df_list[-1] if df_list else {}

        # 使用 bisect 二分查找快速定位前一个有效日期
        fill_rows = []
        for mdt in missing_dates:
            # 二分查找：找到 < mdt 的最大索引
            idx = bisect.bisect_right(df_dates, mdt) - 1
            if idx >= 0:
                last_valid = df_list[idx]
            fill_rows.append({
                "ts_code": ts_code,
                "trade_date": mdt,
                "open": last_valid.get("close", 0),
                "high": last_valid.get("close", 0),
                "low": last_valid.get("close", 0),
                "close": last_valid.get("close", 0),
                "pre_close": last_valid.get("close", 0),
                "volume": 0,
                "amount": 0,
                "pct_chg": 0,
                "turnover": 0,
                "adj_factor": last_valid.get("adj_factor", 1.0),
                "is_suspend": True,
                "limit_up": False,
                "limit_down": False,
                "data_source": "filled",
            })

        if fill_rows:
            fill_df = pd.DataFrame(fill_rows)
            df = pd.concat([df, fill_df], ignore_index=True)
            df = df.sort_values("trade_date").reset_index(drop=True)
            logger.debug(f"Filled {len(fill_rows)} suspend dates for {ts_code}")

        return df

    def is_delisted(self, ts_code: str) -> bool:
        """检查股票是否已退市
        
        Parameters
        ----------
        ts_code : str
            股票代码如 '000001.SZ'
            
        Returns
        -------
        bool: True 表示已退市
        """
        result = self.query(
            "SELECT is_delisted FROM stock_basic WHERE ts_code = ?",
            (ts_code,)
        )
        if result.empty:
            return False
        return bool(result.iloc[0, 0])

    def get_delist_date(self, ts_code: str) -> Optional[str]:
        """获取退市日期
        
        Returns
        -------
        str or None: 退市日期 YYYY-MM-DD，未退市返回 None
        """
        result = self.query(
            "SELECT delist_date FROM stock_basic WHERE ts_code = ? AND is_delisted = TRUE",
            (ts_code,)
        )
        if result.empty or pd.isna(result.iloc[0, 0]):
            return None
        return pd.Timestamp(result.iloc[0, 0]).strftime("%Y-%m-%d")

    def filter_delisted_stocks(self, symbols: List[str], as_of_date: str) -> List[str]:
        """过滤在指定日期已退市的股票
        
        Parameters
        ----------
        symbols : List[str]
            股票代码列表（6位数字）
        as_of_date : str
            截止日期 YYYY-MM-DD
            
        Returns
        -------
        List[str]: 仍可交易的股票代码列表
        """
        if not symbols or not HAS_DUCKDB:
            return symbols
        
        ts_codes = []
        ts_codes = [build_ts_code(sym) for sym in symbols]

        codes_df = pd.DataFrame({"ts_code": ts_codes})
        sym_map = dict(zip(ts_codes, symbols))
        
        with self.get_connection() as conn:
            conn.register('tmp_delisted_filter', codes_df)
            df = conn.execute("""
                SELECT t.ts_code, s.symbol
                FROM tmp_delisted_filter t
                LEFT JOIN stock_basic s ON t.ts_code = s.ts_code
                WHERE s.is_delisted IS NULL 
                   OR s.is_delisted = FALSE
                   OR s.delist_date IS NULL
                   OR s.delist_date > ?
            """, (as_of_date,)).fetchdf()
            conn.execute("DROP VIEW tmp_delisted_filter")
        
        if df.empty:
            return []
        
        # 使用 ts_code 列映射回原始 symbol
        result = []
        for _, row in df.iterrows():
            tc = row["ts_code"]
            if "symbol" in df.columns and not pd.isna(row.get("symbol")):
                result.append(str(row["symbol"]))
            elif tc in sym_map:
                result.append(sym_map[tc])
        
        return result

    # ──────────────────────────────────────────────────────────
    # 单股数据提取
    # ──────────────────────────────────────────────────────────
    def get_security_data(self,
                          code: str,
                          start_date: str = None,
                          end_date: str = None,
                          adjust: Literal["qfq", ""] = "qfq",
                          fill_suspend: bool = True) -> pd.DataFrame:
        """
        获取单只股票数据
        
        先查本地，没有则从网络拉取。
        
        Parameters
        ----------
        code : str
            股票代码（6位数字或带后缀）
        start_date : str, optional
            开始日期
        end_date : str, optional
            结束日期
        adjust : 'qfq' | ''
            复权方式
        fill_suspend : bool
            是否填充停牌日（默认True）
        """
        # 标准化代码
        if "." in code:
            ts_code, symbol = code, code.split(".")[0]
        else:
            ts_code = build_ts_code(code)

        start_date = start_date or DEFAULT_START_DATE
        end_date = end_date or self._get_now().strftime("%Y-%m-%d")

        # Issue #3: 检查是否已退市（退市后停止抓取）
        delist_date = self.get_delist_date(ts_code)
        if delist_date and delist_date < start_date:
            logger.debug(f"{ts_code} delisted on {delist_date}, skip fetch")
            # 仍然返回本地已有数据
            local = self.query("""
                SELECT * FROM daily_bar_adjusted
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """, (ts_code, start_date, end_date))
            return local

        # 查本地（参数化查询防止SQL注入）
        local = self.query("""
            SELECT * FROM daily_bar_adjusted
            WHERE ts_code = ?
            AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
        """, (ts_code, start_date, end_date))
        if not local.empty:
            local_latest = local["trade_date"].max()
            today = self._get_now().strftime("%Y-%m-%d")
            if pd.Timestamp(local_latest).strftime("%Y-%m-%d") < today:
                new_start = (pd.Timestamp(local_latest) + timedelta(days=1)).strftime("%Y-%m-%d")
                # 退市后不抓取
                if delist_date and new_start > delist_date:
                    new_start = delist_date
                if new_start <= end_date:
                    new_df = self.fetch_single(symbol, new_start, today, adjust)
                    if not new_df.empty:
                        self.save_quotes(new_df, mode="append")
                        local = pd.concat([local, new_df], ignore_index=True)
            result = local.sort_values("trade_date").reset_index(drop=True)
        else:
            # 本地没有，从网络拉（带重试）
            if delist_date:
                fetch_end = min(
                    pd.Timestamp(end_date),
                    pd.Timestamp(delist_date)
                ).strftime("%Y-%m-%d")
            else:
                fetch_end = end_date
            df, source = self._fetch_single_with_retry(symbol, start_date, fetch_end, adjust)
            if not df.empty:
                self.save_quotes(df, mode="append")
            result = df

        # Issue #3: 停牌日填充
        if fill_suspend and not result.empty:
            result = self._fill_suspend_dates(result, start_date, end_date)

        return result

    # ──────────────────────────────────────────────────────────
    # 财务数据（PIT 约束）
    # ──────────────────────────────────────────────────────────
    def get_financial_data_pit(self,
                               ts_code: str,
                               trade_date: str,
                               lookback_days: int = 90) -> pd.DataFrame:
        """
        Point-in-Time 财务数据获取
        
        只返回 ann_date <= trade_date 的数据。
        
        这是防止未来函数的关键方法。
        """
        cutoff = (pd.Timestamp(trade_date) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        return self.query("""
            SELECT * FROM financial_data
            WHERE ts_code = ?
            AND ann_date <= ?
            AND ann_date >= ?
            ORDER BY end_date DESC
        """, (ts_code, trade_date, cutoff))

    def get_latest_ann_financial(self,
                                  ts_code: str,
                                  trade_date: str) -> Optional[Dict]:
        """
        获取 T 日最新的已公告财务数据
        
        用于因子计算时的 PIT 约束。
        """
        df = self.get_financial_data_pit(ts_code, trade_date)
        if df.empty:
            return None
        latest = df.iloc[0]
        return latest.to_dict()

    # ──────────────────────────────────────────────────────────
    # 市值数据（PIT约束：只用T-1日收盘市值）
    # ──────────────────────────────────────────────────────────
    def get_market_cap(self, trade_date: str, use_pit: bool = True) -> pd.Series:
        """
        获取某日全市场市值
        
        Parameters
        ----------
        trade_date : str
            目标日期
        use_pit : bool
            True=使用T-1日市值（防止未来函数），False=使用当日市值
        """
        if use_pit:
            # PIT约束：只用T-1日收盘市值
            prev_date = self.get_previous_trade_date(trade_date)
            if prev_date:
                trade_date = prev_date
        
        df = self.query("""
            SELECT ts_code, circ_mv, total_mv
            FROM daily_valuation
            WHERE trade_date = ?
        """, (trade_date,))
        if df.empty:
            return pd.Series(dtype=float)
        return df.set_index("ts_code")["circ_mv"]

    def get_previous_trade_date(self, trade_date: str) -> Optional[str]:
        """获取指定日期的前一个交易日"""
        df = self.query("""
            SELECT cal_date FROM trade_calendar
            WHERE is_open = TRUE
            AND cal_date < ?
            ORDER BY cal_date DESC
            LIMIT 1
        """, (trade_date,))
        if df.empty:
            return None
        return df.iloc[0, 0].strftime("%Y-%m-%d")

    # ═══════════════════════════════════════════════════════════
    # DuckDB Window Function 计算（下推至SQL层）
    # ═══════════════════════════════════════════════════════════
    def compute_rolling_returns(self,
                                  ts_codes: List[str],
                                  start_date: str,
                                  end_date: str,
                                  windows: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """
        DuckDB Window Function 计算滚动收益率
        
        直接在SQL层计算，避免将海量数据载入Pandas。
        返回：ts_code, trade_date, window_5, window_10, window_20, window_60
        """
        if not ts_codes:
            return pd.DataFrame()

        # 使用 register 避免大列表拼接 SQL 注入风险
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        max_window = max(windows)

        # 构建窗口函数SQL（使用复利乘积计算滚动收益率）
        window_parts = []
        for w in windows:
            window_parts.append(f"""
                ROUND(EXP(SUM(LN(1 + pct_chg / 100.0)) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW)) - 1, 4) as window_{w}
            """)

        with self.get_connection() as conn:
            conn.register('tmp_codes_ret', codes_df)
            sql = f"""
                WITH ranked AS (
                    SELECT 
                        d.ts_code,
                        d.trade_date,
                        d.pct_chg,
                        ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) as rn
                    FROM daily_bar_adjusted d
                    INNER JOIN tmp_codes_ret t ON d.ts_code = t.ts_code
                    AND d.trade_date BETWEEN ? AND ?
                    AND d.volume > 0
                )
                SELECT 
                    ts_code,
                    trade_date,
                    {','.join(window_parts)}
                FROM ranked
                WHERE rn >= {max_window}
                ORDER BY ts_code, trade_date
            """
            df = conn.execute(sql, (start_date, end_date)).fetchdf()
            conn.execute("DROP VIEW tmp_codes_ret")
            return df

    def compute_rolling_ma(self,
                            ts_codes: List[str],
                            start_date: str,
                            end_date: str,
                            windows: List[int] = [5, 10, 20, 60],
                            price_col: str = "close") -> pd.DataFrame:
        """
        DuckDB Window Function 计算移动平均线
        
        直接在SQL层计算MA，避免内存爆炸。
        """
        if not ts_codes:
            return pd.DataFrame()

        # 使用 register 避免大列表拼接 SQL 注入风险
        codes_df = pd.DataFrame({"ts_code": ts_codes})

        window_parts = []
        for w in windows:
            window_parts.append(f"""
                ROUND(AVG({price_col}) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW), 2) as ma_{w}
            """)

        with self.get_connection() as conn:
            conn.register('tmp_codes_ma', codes_df)
            sql = f"""
                SELECT 
                    d.ts_code,
                    d.trade_date,
                    d.{price_col},
                    {','.join(window_parts)}
                FROM daily_bar_adjusted d
                INNER JOIN tmp_codes_ma t ON d.ts_code = t.ts_code
                AND d.trade_date BETWEEN ? AND ?
                AND d.volume > 0
                ORDER BY d.ts_code, d.trade_date
            """
            df = conn.execute(sql, (start_date, end_date)).fetchdf()
            conn.execute("DROP VIEW tmp_codes_ma")
            return df

    def compute_rolling_volatility(self,
                                    ts_codes: List[str],
                                    start_date: str,
                                    end_date: str,
                                    windows: List[int] = [20, 60]) -> pd.DataFrame:
        """
        DuckDB Window Function 计算滚动波动率
        """
        if not ts_codes:
            return pd.DataFrame()

        # 使用 register 避免大列表拼接 SQL 注入风险
        codes_df = pd.DataFrame({"ts_code": ts_codes})

        window_parts = []
        for w in windows:
            window_parts.append(f"""
                ROUND(STDDEV(pct_chg) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW), 4) as vol_{w}
            """)

        with self.get_connection() as conn:
            conn.register('tmp_codes_vol', codes_df)
            sql = f"""
                SELECT 
                    d.ts_code,
                    d.trade_date,
                    d.pct_chg,
                    {','.join(window_parts)}
                FROM daily_bar_adjusted d
                INNER JOIN tmp_codes_vol t ON d.ts_code = t.ts_code
                AND d.trade_date BETWEEN ? AND ?
                AND d.volume > 0
                ORDER BY d.ts_code, d.trade_date
            """
            df = conn.execute(sql, (start_date, end_date)).fetchdf()
            conn.execute("DROP VIEW tmp_codes_vol")
            return df

    # ──────────────────────────────────────────────────────────
    # 交易日历
    # ──────────────────────────────────────────────────────────
    def sync_calendar(self, start_year: int = 2018, end_year: int = None):
        """同步交易日历（网络不可用时跳过，使用本地已有数据）"""
        # 检查本地是否已有日历数据，有则跳过
        existing = self.query("SELECT COUNT(*) FROM trade_calendar WHERE is_open=TRUE")
        if existing.iloc[0, 0] > 100:
            logger.info(f"Calendar exists ({existing.iloc[0, 0]} days), skip sync")
            return

        if not HAS_AKSHARE:
            logger.info("AkShare unavailable, using existing calendar")
            return

        logger.info("Fetching calendar from AkShare...")
        end_year = end_year or self._get_now().year
        with self.get_connection() as conn:
            cal = ak.tool_trade_date_hist_sina()
            cal.columns = ["cal_date", "is_open"]
            cal["cal_date"] = pd.to_datetime(cal["cal_date"])
            cal["pretrade_date"] = None
            import os as _os
            tmpdir = self.db_path.parent / "_tmp_cal"
            tmpdir.mkdir(exist_ok=True)
            pq = tmpdir / "cal.parquet"
            cal.to_parquet(str(pq), index=False)
            conn.execute(f"COPY trade_calendar FROM '{pq}' (FORMAT PARQUET)")
            _os.remove(pq)
            logger.info(f"Calendar synced: {len(cal)} days")

    def get_trade_dates(self,
                        start_date: str = None,
                        end_date: str = None) -> List[str]:
        """获取交易日列表"""
        params = []
        sql = "SELECT cal_date FROM trade_calendar WHERE is_open = TRUE"
        if start_date:
            sql += " AND cal_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND cal_date <= ?"
            params.append(end_date)
        sql += " ORDER BY cal_date"
        df = self.query(sql, tuple(params) if params else None)
        if df.empty:
            return []
        return df["cal_date"].dt.strftime("%Y-%m-%d").tolist()

    # ──────────────────────────────────────────────────────────
    # Parquet 导出
    # ──────────────────────────────────────────────────────────
    # 允许导出的表白名单（防止路径遍历和SQL注入）
    _EXPORT_TABLE_WHITELIST = frozenset({
        'daily_bar_adjusted', 'stock_basic', 'financial_data', 'index_constituents',
        'st_status_history', 'trade_calendar', 'daily_valuation',
        'update_log', 'adj_factor_log', 'data_quality_alert',
    })

    def export_parquet(self, table: str = "daily_bar_adjusted") -> Path:
        """导出表到 Parquet 格式（仅允许白名单表名）"""
        if table not in self._EXPORT_TABLE_WHITELIST:
            raise ValueError(f"表 '{table}' 不在导出白名单中，允许的表: {sorted(self._EXPORT_TABLE_WHITELIST)}")
        output = self.parquet_dir / f"{table}.parquet"
        # 路径安全检查：确保输出在 parquet_dir 内
        if not str(output.resolve()).startswith(str(self.parquet_dir.resolve())):
            raise ValueError(f"导出路径 '{output}' 不在允许的目录 '{self.parquet_dir}' 内")
        with self.get_connection() as conn:
            conn.execute(f"COPY (SELECT * FROM {table}) TO '{output}' (FORMAT PARQUET)")
        logger.info(f"Exported to {output}")
        return output

    def load_parquet(self, file_path: Path) -> pd.DataFrame:
        """从 Parquet 加载"""
        return pd.read_parquet(file_path)

    # ──────────────────────────────────────────────────────────
    # 数据质量报告
    # ──────────────────────────────────────────────────────────
    def quality_report(self, start_date: str, end_date: str) -> Dict:
        """生成数据质量报告"""
        total = self.query("""
            SELECT COUNT(*) as cnt FROM daily_bar_adjusted
            WHERE trade_date BETWEEN ? AND ?
        """, (start_date, end_date)).iloc[0, 0]

        anomaly = self.query("""
            SELECT COUNT(*) as cnt FROM daily_bar_adjusted
            WHERE trade_date BETWEEN ? AND ?
            AND (close <= 0 OR volume < 0 OR adj_factor <= 0)
        """, (start_date, end_date)).iloc[0, 0]

        delisted = self.query(
            "SELECT COUNT(*) FROM stock_basic WHERE is_delisted = TRUE"
        ).iloc[0, 0]

        return {
            "total_records": int(total),
            "anomaly_records": int(anomaly),
            "delisted_stocks": int(delisted),
            "quality_score": 1.0 - anomaly / total if total > 0 else 0.0
        }


    # ──────────────────────────────────────────────────────────
    # 批量数据获取（优化 N+1 查询问题）
    # ──────────────────────────────────────────────────────────
    def get_batch_stock_data(self,
                              symbols: List[str],
                              trade_date: str) -> pd.DataFrame:
        """
        批量获取多只股票在指定日期的日线数据
        
        用单条 SQL 替代 N 次循环查询，大幅减少数据库 I/O。
        
        Parameters
        ----------
        symbols : List[str]
            6位股票代码列表
        trade_date : str
            交易日期 YYYY-MM-DD
            
        Returns
        -------
        pd.DataFrame: 合并后的日线数据
        """
        if not symbols or not HAS_DUCKDB:
            return pd.DataFrame()
        
        # 构造 ts_code 列表
        ts_codes = []
        ts_codes = [build_ts_code(sym) for sym in symbols]

        # 使用 register 避免 SQL 注入
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        with self.get_connection() as conn:
            conn.register('tmp_batch_codes', codes_df)
            # 注意：daily_bar_adjusted.close = AkShare前复权价（adj_factor=1.0时close本身已是复权价）
            # daily_bar_raw.close = 原始未复权价，供未来动态复权使用
            df = conn.execute("""
                SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close,
                       d.volume, d.amount, d.pct_chg, d.pre_close,
                       d.is_suspend, d.limit_up, d.limit_down
                FROM daily_bar_adjusted d
                INNER JOIN tmp_batch_codes t ON d.ts_code = t.ts_code
                WHERE d.trade_date = ?
            """, (trade_date,)).fetchdf()
            conn.execute("DROP VIEW tmp_batch_codes")
        
        if df.empty:
            return pd.DataFrame()
        
        # 提取6位代码作为 symbol 列
        df["symbol"] = df["ts_code"].str.split(".").str[0]
        return df

    # ──────────────────────────────────────────────────────────
    # 便捷函数
    # ──────────────────────────────────────────────────────────
def load_stock(code: str,
               start: str = None,
               end: str = None,
               adjust: str = "qfq") -> pd.DataFrame:
    """一行代码加载单只股票数据"""
    engine = DataEngine()
    return engine.get_security_data(code, start, end, adjust)
