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
# 数据起始日期配置优先级（由高到低）：
#   1. DataEngine(start_date=...) 构造函数参数
#   2. 环境变量 STOCK_START_DATE
#   3. 配置文件 data/config.toml 的 [data] start_date
#   4. 硬编码默认值 "2018-01-01"
# 默认 2018-01-01：合理回测起点（2015年股灾后市场充分调整，
# 覆盖2016-2018供给侧改革周期，确保有足够历史数据用于因子计算）
_DEFAULT_START_DATE_FALLBACK = "2018-01-01"
DEFAULT_START_DATE = os.environ.get("STOCK_START_DATE", _DEFAULT_START_DATE_FALLBACK)
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
try:
    from scripts.data_validator import DataValidator
except ModuleNotFoundError:
    from data_validator import DataValidator


# ──────────────────────────────────────────────────────────────
# 统一配置层
# ──────────────────────────────────────────────────────────────
# 配置文件路径：data/config.toml
_CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "config.toml"


def _load_config() -> dict:
    """
    从 data/config.toml 加载配置（若不存在则返回空字典）。

    优先级由 DataEngine 构造函数统一决定，此函数仅负责读文件。
    """
    if not _CONFIG_PATH.exists():
        return {}
    try:
        import configparser
        cp = configparser.ConfigParser()
        cp.read(str(_CONFIG_PATH), encoding="utf-8")
        return {s: dict(cp.items(s)) for s in cp.sections()}
    except Exception:
        return {}


def _resolve_start_date(param_value: Optional[str]) -> str:
    """
    解析最终生效的起始日期。

    优先级（由高到低）：
      1. 构造函数参数 start_date
      2. 环境变量 STOCK_START_DATE
      3. 配置文件 data/config.toml → [data] → start_date
      4. 硬编码默认值 "2018-01-01"

    Parameters
    ----------
    param_value : str or None
        构造函数传入的 start_date 参数

    Returns
    -------
    str: 最终生效的 YYYY-MM-DD 日期字符串
    """
    # 1. 构造函数参数（最高优先）
    if param_value:
        return param_value
    # 2. 环境变量
    env_val = os.environ.get("STOCK_START_DATE", "")
    if env_val:
        return env_val
    # 3. 配置文件
    config = _load_config()
    cfg_val = config.get("data", {}).get("start_date", "")
    if cfg_val:
        return cfg_val
    # 4. 硬编码默认值
    return _DEFAULT_START_DATE_FALLBACK


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
                 parquet_dir: str = None,
                 start_date: str = None):
        """
        初始化 DataEngine

        Parameters
        ----------
        db_path : str, optional
            DuckDB 数据库路径，默认使用 data/stock_data.duckdb
        parquet_dir : str, optional
            Parquet 存储目录，默认使用 data/parquet
        start_date : str, optional
            默认数据起始日期，格式 YYYY-MM-DD
            默认值：2018-01-01（DEFAULT_START_DATE）
            可通过环境变量 STOCK_START_DATE 覆盖
        """
        # 支持环境变量配置，未设置时使用相对路径默认值
        project_root = Path(__file__).resolve().parent.parent
        self.db_path = Path(db_path or os.environ.get(
            'STOCK_DB_PATH', str(project_root / 'data' / 'stock_data.duckdb')))
        self.parquet_dir = Path(parquet_dir or os.environ.get(
            'STOCK_PARQUET_DIR', str(project_root / 'data' / 'parquet')))
        # 统一配置入口：参数 > 环境变量 > 配置文件 > 硬编码默认值
        # 优先级由 _resolve_start_date() 统一管理
        self.start_date = _resolve_start_date(start_date)
        logger.info(f"[DataEngine] start_date resolved: {self.start_date} "
                    f"(param={start_date}, "
                    f"env={os.environ.get('STOCK_START_DATE','<unset>')}, "
                    f"config={_CONFIG_PATH})")
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
        """初始化 v5 表结构（幂等：同类路径只执行一次）

        v5 Schema（10张表）：
          核心8表: stock_basic / trading_calendar / market_daily /
                   corporate_action / financial_statement / financial_factor /
                   share_structure / company_status
          辅助2表: index_membership / valuation_daily
          工具表:  sync_progress / data_quality_alert
        """
        db_key = str(self.db_path.resolve())
        if db_key in DataEngine._schema_initialized:
            logger.debug(f"Schema already initialized for {db_key}")
            return

        conn = duckdb.connect(str(self.db_path))
        cur = conn.cursor()

        # ── 清理旧口径残留表（v4 及之前）──────────────────────────────
        old_tables = [
            'stock_basic_history', 'daily_bar_raw', 'daily_bar_adjusted',
            'financial_data', 'daily_valuation', 'corporate_actions',
            'index_constituents_history', 'st_status_history',
            'adj_factor_log', 'update_log',
        ]
        for tbl in old_tables:
            cur.execute(f"DROP VIEW IF EXISTS {tbl}")
        logger.debug(f"旧口径残留表已清理: {old_tables}")

        # ── 1. stock_basic 证券主表 ─────────────────────────────────
        # 记录每只股票的基础信息（上市/退市/行业/交易所等）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic (
                ticker           VARCHAR  PRIMARY KEY,
                name             VARCHAR,
                full_name        VARCHAR,
                exchange         VARCHAR,          -- SH / SZ / BJ
                market           VARCHAR,          -- A / HK / US
                industry_level1  VARCHAR,          -- 申万一级行业（30个）
                industry_level2  VARCHAR,
                industry_level3  VARCHAR,
                listed_date      DATE,
                delisted_date    DATE,
                list_status      VARCHAR,          -- listed / delisted / suspended
                is_financial     BOOLEAN DEFAULT FALSE,  -- 金融股（毛利率/负债率不适用）
                currency         VARCHAR DEFAULT 'CNY',
                country          VARCHAR DEFAULT 'CN',
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sb_exchange ON stock_basic(exchange)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sb_industry1 ON stock_basic(industry_level1)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sb_status ON stock_basic(list_status)")

        # ── 2. trading_calendar 交易日历 ───────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trading_calendar (
                cal_date       DATE PRIMARY KEY,
                is_trading_day BOOLEAN,          -- TRUE=交易日
                prev_trade_date DATE              -- 最近前一个交易日
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tc_is_trading_day ON trading_calendar(is_trading_day)")

        # ── 3. market_daily 日行情（原始价层）─────────────────────────
        # 存储 raw_close + adj_factor，qfq 由 market_daily.adj_factor × raw_close 实时计算
        # adj_factor=1.0 表示不复权原始价；>1.0 表示有复权调整
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_daily (
                ticker           VARCHAR,
                trade_date       DATE,
                open             DOUBLE,
                high             DOUBLE,
                low              DOUBLE,
                close            DOUBLE,           -- 原始未复权收盘价
                pre_close        DOUBLE,           -- 前收盘
                volume           BIGINT,
                amount           DOUBLE,
                turnover_rate    DOUBLE,           -- Baostock turn 字段（%）
                total_market_cap DOUBLE,           -- 总市值（万元）
                float_market_cap DOUBLE,           -- 流通市值（万元）
                adj_factor       DOUBLE DEFAULT 1.0,  -- 复权因子：qfq_close = close × adj_factor
                suspended_flag   BOOLEAN DEFAULT FALSE,
                limit_up_flag   BOOLEAN DEFAULT FALSE,
                limit_down_flag BOOLEAN DEFAULT FALSE,
                is_new_stock    BOOLEAN DEFAULT FALSE,  -- 上市首日（无涨跌停）
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, trade_date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_md_ticker ON market_daily(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_md_date ON market_daily(trade_date)")

        # ── 4. corporate_action 分红送股事件 ─────────────────────────
        # 来源：Baostock 分红送股数据
        cur.execute("""
            CREATE TABLE IF NOT EXISTS corporate_action (
                ticker            VARCHAR,
                action_date        DATE,            -- 除权除息生效日
                announcement_date  DATE,            -- 公告日期（PIT约束：T日公告T日可见）
                report_period_end  DATE,            -- 报告期
                action_type        VARCHAR,         -- dividend/split/rights_issue/bonus
                dividend_per_share DOUBLE,           -- 每股分红（元）
                split_ratio        DOUBLE,           -- 拆股比例（如10送10=2.0）
                bonus_ratio        DOUBLE,           -- 送股比例（如10送5=0.5）
                rights_ratio       DOUBLE,           -- 配股比例
                rights_price       DOUBLE,           -- 配股价格
                prev_adj_factor    DOUBLE,           -- 调整前复权因子
                curr_adj_factor    DOUBLE,           -- 调整后复权因子
                detail             VARCHAR,         -- 事件描述
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, action_date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ca_ticker ON corporate_action(ticker)")

        # ── 5. financial_statement 年度财务报表原始数据 ───────────────
        # 数据来源：Baostock query_profit_data / query_balance_data / query_dupont_data
        # PIT约束：announcement_date <= available_date（公告日T日生效，T+1可查）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financial_statement (
                ticker               VARCHAR,
                report_period_end     DATE,          -- 报告期（如 2024-12-31）
                announcement_date     DATE,          -- 公告日期
                available_date       DATE,          -- PIT生效日 = announcement_date + 1
                report_type          VARCHAR,       -- FY / Q1 / Q2 / Q3
                revenue              DOUBLE,        -- 营业收入（万元）
                cogs                 DOUBLE,        -- 营业成本
                gross_profit         DOUBLE,        -- 毛利润
                operating_profit     DOUBLE,        -- 营业利润
                net_profit           DOUBLE,        -- 净利润
                net_profit_parent    DOUBLE,        -- 归母净利润
                total_assets         DOUBLE,        -- 总资产
                total_liabilities    DOUBLE,        -- 总负债
                equity               DOUBLE,        -- 所有者权益
                asset_to_equity      DOUBLE,        -- 资产负债率推导值（= total_assets / equity）
                operating_cash_flow  DOUBLE,        -- 经营活动现金流
                investing_cash_flow  DOUBLE,        -- 投资活动现金流
                financing_cash_flow  DOUBLE,        -- 筹资活动现金流
                capex               DOUBLE,        -- 资本支出
                roe_avg             DOUBLE,        -- 加权平均净资产收益率（Baostock roeAvg）
                roa                 DOUBLE,        -- 总资产报酬率
                roic                DOUBLE,        -- 投资资本回报率
                gross_margin         DOUBLE,        -- 毛利率（gpMargin，金融机构为NULL）
                net_margin           DOUBLE,        -- 净利率（npMargin）
                created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, report_period_end)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_ticker ON financial_statement(ticker)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_ann ON financial_statement(announcement_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fs_avail ON financial_statement(available_date)")

        # ── 6. financial_factor 财务因子（选股用派生数据）───────────
        # 由 financial_statement 派生，标准化为因子值
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financial_factor (
                ticker                 VARCHAR,
                report_period_end       DATE,
                announcement_date       DATE,
                available_date         DATE,
                roe                   DOUBLE,         -- roe_avg（同比用前一年值）
                roic                  DOUBLE,
                gross_margin          DOUBLE,
                asset_liability_ratio DOUBLE,         -- 资产负债率（= 1 - 1/asset_to_equity）
                net_profit            DOUBLE,
                operating_cash_flow   DOUBLE,
                revenue_yoy           DOUBLE,         -- 营收同比增速
                net_profit_yoy        DOUBLE,         -- 净利润同比增速
                quality_score         DOUBLE,         -- 质量因子综合分（待实现）
                growth_score          DOUBLE,         -- 成长因子综合分（待实现）
                created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, report_period_end)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ff_ticker ON financial_factor(ticker)")

        # ── 7. share_structure 股本结构 ─────────────────────────────
        # 来源：Baostock query_stock_struct
        cur.execute("""
            CREATE TABLE IF NOT EXISTS share_structure (
                ticker             VARCHAR,
                announcement_date  DATE,
                report_period_end  DATE,
                total_shares       DOUBLE,        -- 总股本（万股）
                float_shares       DOUBLE,        -- 流通股本（万股）
                float_ratio        DOUBLE,        -- 流通比例
                share_type         VARCHAR,       -- share_type 类别：share_total / share_float
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, announcement_date, share_type)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ss_ticker ON share_structure(ticker)")

        # ── 8. company_status 公司状态（时间序列）────────────────────
        # 记录公司状态变化时间点（上市/退市/停牌/ ST等）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS company_status (
                ticker              VARCHAR,
                status_date         DATE,           -- 状态变化日期
                listed_flag         BOOLEAN,         -- 是否上市
                delisted_flag      BOOLEAN,         -- 是否退市
                suspended_flag      BOOLEAN,         -- 是否停牌
                st_status           BOOLEAN,          -- 是否ST
                risk_warning_flag   BOOLEAN,         -- 是否风险警示
                tradable_flag       BOOLEAN,         -- 是否可交易
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, status_date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cs_ticker ON company_status(ticker)")

        # ── 9. index_membership 指数成分 ────────────────────────────
        # 记录各指数当前成分股列表
        cur.execute("""
            CREATE TABLE IF NOT EXISTS index_membership (
                index_code     VARCHAR,
                ticker         VARCHAR,
                in_date        DATE,
                out_date       DATE,
                source         VARCHAR,           -- csindex / sse / szse
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (index_code, ticker)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_im_ticker ON index_membership(ticker)")

        # ── 10. valuation_daily 每日估值 ────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS valuation_daily (
                ticker         VARCHAR,
                trade_date     DATE,
                pe_ttm         DOUBLE,
                pb             DOUBLE,
                ps_ttm         DOUBLE,
                pcf_cf_ttm     DOUBLE,
                total_mv       DOUBLE,
                circ_mv        DOUBLE,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, trade_date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_vd_ticker ON valuation_daily(ticker)")

        # ── 工具表：sync_progress（断点续传）─────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_progress (
                ticker          VARCHAR NOT NULL,
                table_name      VARCHAR NOT NULL,
                last_sync_date  DATE,
                last_sync_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_records   INTEGER DEFAULT 0,
                status          VARCHAR DEFAULT 'ok',
                error_msg       VARCHAR,
                PRIMARY KEY (ticker, table_name)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sp_date ON sync_progress(last_sync_date)")

        # ── 工具表：data_quality_alert ───────────────────────────────
        cur.execute("CREATE SEQUENCE IF NOT EXISTS seq_dqa_id START 1")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_alert (
                id           INTEGER PRIMARY KEY DEFAULT nextval('seq_dqa_id'),
                alert_type   VARCHAR,
                ticker       VARCHAR,
                trade_date   DATE,
                detail       VARCHAR,
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)


        # ── v5 READ COMPATIBILITY VIEWS ─────────────────────────────
        # 让旧读取代码（get_daily_raw/get_daily_adjusted等）无需修改即可工作
        cur.execute("DROP VIEW IF EXISTS daily_bar_adjusted")
        cur.execute("DROP VIEW IF EXISTS daily_bar_raw")
        cur.execute("DROP VIEW IF EXISTS financial_data")
        cur.execute("DROP VIEW IF EXISTS stock_basic_history")
        cur.execute("DROP VIEW IF EXISTS st_status_history")

        # daily_bar_adjusted VIEW：market_daily 列映射 + qfq 实时计算
        cur.execute("""
            CREATE VIEW daily_bar_adjusted AS
            SELECT
                ticker        AS ts_code,
                trade_date,
                open  * adj_factor  AS open,
                high  * adj_factor  AS high,
                low   * adj_factor  AS low,
                close * adj_factor  AS close,
                pre_close * adj_factor AS pre_close,
                volume,
                amount,
                turnover_rate AS turnover,
                adj_factor,
                suspended_flag  AS is_suspend,
                limit_up_flag   AS limit_up,
                limit_down_flag AS limit_down,
                CASE WHEN pre_close IS NOT NULL AND pre_close != 0
                     THEN (close - pre_close) / pre_close * 100.0
                     ELSE 0.0 END AS pct_chg
            FROM market_daily
        """)

        # daily_bar_raw VIEW：market_daily 列映射
        cur.execute("""
            CREATE VIEW daily_bar_raw AS
            SELECT
                ticker        AS ts_code,
                trade_date,
                ticker        AS symbol,
                open,
                high,
                low,
                close,
                volume,
                amount,
                turnover_rate AS turnover,
                adj_factor,
                pre_close,
                suspended_flag  AS is_suspend,
                limit_up_flag   AS limit_up,
                limit_down_flag AS limit_down,
                'market_daily'  AS data_source,
                CASE WHEN pre_close IS NOT NULL AND pre_close != 0
                     THEN (close - pre_close) / pre_close * 100.0
                     ELSE 0.0 END AS pct_chg
            FROM market_daily
        """)

        # financial_data VIEW：financial_statement 列映射
        cur.execute("""
            CREATE VIEW financial_data AS
            SELECT
                ticker               AS ts_code,
                announcement_date   AS ann_date,
                report_period_end   AS end_date,
                report_type,
                revenue,
                net_profit,
                total_assets,
                equity              AS total_equity,
                roe_avg             AS roe,
                NULL                AS eps,
                gross_margin,
                roic,
                NULL                AS pe_ttm,
                NULL                AS pb,
                NULL                AS debt_ratio,
                'financial_statement' AS data_source
            FROM financial_statement
        """)

        # stock_basic_history VIEW：stock_basic 映射
        cur.execute("""
            CREATE VIEW stock_basic_history AS
            SELECT
                ticker       AS ts_code,
                ticker       AS symbol,
                name,
                exchange,
                NULL            AS area,
                industry_level1 AS industry,
                NULL            AS market,
                listed_date  AS list_date,
                delisted_date AS delist_date,
                (delisted_date IS NOT NULL) AS is_delisted,
                NULL            AS delist_reason,
                NULL            AS board,
                FALSE           AS is_suspend,
                listed_date  AS eff_date,
                NULL            AS end_date,
                NOW() AS created_at
            FROM stock_basic
        """)

        DataEngine._schema_initialized.add(str(self.db_path.resolve()))
        logger.info(f"DataEngine v5 schema initialized: {self.db_path}")

        conn.close()

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
    def _fetch_remote_stocks(self, include_delisted: bool = True) -> pd.DataFrame:
        """
        从远程 API（AkShare）获取股票列表（内部同步用）。

        仅用于同步操作（sync_stock_list / save_snapshot）的远程数据获取。
        不依赖 AkShare 时返回空 DataFrame，由调用方决定降级策略。

        Returns
        -------
        pd.DataFrame: 包含 symbol/name/industry 等字段的股票列表
        """
        if not HAS_AKSHARE:
            return pd.DataFrame()

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
            logger.info(f"Current stocks from remote: {len(current)}")
        except (ValueError, KeyError, RuntimeError) as e:
            logger.warning(f"Failed to fetch current stocks: {e}")

        # 2. 退市股票（解决幸存者偏差）
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
                    logger.info(f"Delisted stocks from remote: {len(dl)}")
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Failed to fetch delisted stocks: {e}")

        if not stocks:
            return pd.DataFrame()

        df = pd.concat(stocks, ignore_index=True)
        df = df.drop_duplicates(subset=["symbol"], keep="first")
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        df["ts_code"] = df["symbol"].apply(build_ts_code)
        df["market"] = df["symbol"].apply(detect_board)
        return df

    def get_all_stocks(self, include_delisted: bool = True) -> pd.DataFrame:
        """
        [废弃] 请使用 get_active_stocks(trade_date) 获取历史股票池。

        此方法仅用于外部同步股票列表，不再参与回测主链路。
        主流程应调用 get_active_stocks() 以支持 PIT 查询。

        获取全量股票列表（含退市股）。优先从远程 API 获取，
        失败时直接抛出 RuntimeError（旧表 stock_basic 已废弃，不支持静默回退）。
        """
        remote_df = self._fetch_remote_stocks(include_delisted)
        if not remote_df.empty:
            return remote_df

        # 远程获取失败 → 明确报错，不静默回退
        raise RuntimeError(
            "无法从 AkShare 获取股票列表，且 stock_basic_history 表为空。"
            "请先调用 sync_stock_list() 同步数据。"
            "旧表 stock_basic 已废弃，不支持回退。"
        )

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

        # 从远程 API 获取股票列表（直接调用，不走 get_all_stocks 回退逻辑）
        df = self._fetch_remote_stocks(include_delisted=True)
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
        # 使用 stock_basic_history 进行 PIT 查询
        with self.get_connection() as conn:
            df = conn.execute("""
                SELECT h.*
                FROM (
                    SELECT ts_code, MAX(eff_date) as latest_eff
                    FROM stock_basic_history
                    WHERE eff_date <= ?
                    GROUP BY ts_code
                ) latest
                JOIN stock_basic_history h
                  ON h.ts_code = latest.ts_code
                 AND h.eff_date = latest.latest_eff
                WHERE h.list_date <= ?
                  AND (h.delist_date IS NULL OR h.delist_date > ?)
            """, (as_of_date, as_of_date, as_of_date)).fetchdf()

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
        - 取每个 ts_code 的 eff_date <= trade_date 的最新记录
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

        # PIT 查询：对每个 ts_code 取 eff_date <= trade_date 的最新记录
        df = self.query(f"""
            SELECT h.ts_code
            FROM (
                SELECT ts_code, MAX(eff_date) as latest_eff
                FROM stock_basic_history
                WHERE eff_date <= CAST(? AS DATE)
                GROUP BY ts_code
            ) latest
            JOIN stock_basic_history h
              ON h.ts_code = latest.ts_code
             AND h.eff_date = latest.latest_eff
            WHERE h.list_date <= CAST(? AS DATE)
              AND (h.delist_date IS NULL OR h.delist_date > CAST(? AS DATE))
            ORDER BY h.ts_code
        """, (trade_date, trade_date, trade_date))

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
    @staticmethod
    def _normalize_index_code(index_code: str) -> str:
        """
        标准化指数代码格式。

        转换规则：
          - .XSHG / .XSH → .SH
          - .XSHE / .XSZ → .SZ
          - 纯数字 → 默认加 .SH（中证系列指数均为上交所）
          - .SH / .SZ → 保持不变

        用于：sync_index_constituents / get_index_constituents / get_universe_at_date
        """
        if not index_code:
            return index_code
        normalized = (index_code
                      .replace('.XSHG', '.SH')
                      .replace('.XSHE', '.SZ')
                      .replace('.XSH', '.SH')
                      .replace('.XSZ', '.SZ')
                      .replace('.SZ', '.SZ')
                      .replace('.SH', '.SH'))
        if '.' not in normalized:
            normalized = normalized + '.SH'
        return normalized

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
                # 标准化 index_code（统一格式：000300.SH / 000905.SH / 000852.SH）
                norm_code = self._normalize_index_code(index_code)
                # 取纯数字部分用于 API 调用
                index_symbol = norm_code.replace(".SH", "").replace(".SZ", "")
                
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
                # 使用标准化后的 index_code（统一存储格式：000300.SH / 000905.SH / 000852.SH）
                df['index_code'] = norm_code
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
                """, (from_date, norm_code))

                conn.execute("DROP VIEW IF EXISTS tmp_constituents")
                conn.execute("DROP VIEW IF EXISTS tmp_current_codes")

                new_count = len(df)
                logger.info(
                    f"Synced {norm_code}: {new_count} current constituents, "
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
            指数代码，如 '000300.SH' 或 '000300.XSHG'（两种格式都支持）
        trade_date : str
            查询日期 YYYY-MM-DD

        Returns
        -------
        pd.DataFrame: 包含 ts_code, name, exchange, board, in_date, out_date
        """
        if not HAS_DUCKDB:
            return pd.DataFrame()

        # 标准化 index_code 格式（委托给 _normalize_index_code）
        norm_code = self._normalize_index_code(index_code)

        return self.query("""
            SELECT
                h.ts_code,
                s.name,
                s.exchange,
                s.board,
                h.in_date,
                h.out_date
            FROM index_constituents_history h
            LEFT JOIN (
                SELECT ts_code, name, exchange, board,
                       MAX(eff_date) as latest_eff
                FROM stock_basic_history
                GROUP BY ts_code, name, exchange, board
            ) s ON h.ts_code = s.ts_code
            WHERE h.index_code = ?
              AND h.in_date <= ?
              AND (h.out_date IS NULL OR h.out_date >= ?)
            ORDER BY h.ts_code
        """, (norm_code, trade_date, trade_date))

    def get_universe_at_date(self, index_code: str, trade_date: str) -> List[str]:
        """
        获取指定日期的指数成分股列表（动态股票池）

        Returns
        -------
        List[str]: 在该日期属于指数成分的股票代码列表
        """
        if not HAS_DUCKDB:
            return []

        # 标准化 index_code（委托给统一方法）
        norm_code = self._normalize_index_code(index_code)

        df = self.query("""
            SELECT ts_code FROM index_constituents_history
            WHERE index_code = ?
              AND in_date <= ?
              AND (out_date IS NULL OR out_date >= ?)
        """, (norm_code, trade_date, trade_date))
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
            # 从 stock_basic_history 获取含 ST 的股票列表（当前快照）
            try:
                # 先找到所有历史上曾是 ST 的股票（取最新 eff_date 的记录）
                st_stocks = conn.execute("""
                    SELECT h.ts_code, h.name FROM (
                        SELECT ts_code, MAX(eff_date) as latest_eff
                        FROM stock_basic_history
                        GROUP BY ts_code
                    ) latest
                    JOIN stock_basic_history h
                      ON h.ts_code = latest.ts_code
                     AND h.eff_date = latest.latest_eff
                    WHERE h.name LIKE '%ST%' OR h.name LIKE '%*ST%'
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

        # 全量：使用 get_active_stocks() PIT 查询获取当前可交易股票池
        # 不再依赖 _get_local_stocks()（旧表 stock_basic 已废弃）
        today = self._get_now().strftime("%Y-%m-%d")
        ts_codes = self.get_active_stocks(today)
        if not ts_codes:
            raise RuntimeError(
                "get_active_stocks() 返回空列表，stock_basic_history 表可能为空。"
                "请先调用 sync_stock_list() 同步数据。"
            )
        # get_active_stocks 返回 ts_code 列表（如 '000001.SZ'），需提取纯数字部分
        symbols = [tc.split(".")[0].lstrip("0") or tc.split(".")[0]
                   for tc in ts_codes]

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

        # 估值快照（用 Baostock K 线 PE/PB，已验证可用）
        # Bug Fix (2026-04-14): 原 AkShare stock_a_indicator_lg 早已不可用，
        # 替换为 Baostock query_history_k_data_plus 自带 pe/pb 列
        try:
            from exchange_mapping import build_bs_code as _build_bs_code
            bs_code_val = _build_bs_code(sym6)
            end_date = self._get_now().strftime("%Y-%m-%d")
            start_date = self._get_now().replace(year=self._get_now().year - 1).strftime("%Y-%m-%d")
            rs = bs.query_history_k_data_plus(
                bs_code_val,
                "date,totalMv,turnover,pe,pb",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3",  # 不复权，保持 raw
            )
            if rs and rs.error_code == "0":
                while rs.next():
                    row = dict(zip(rs.fields, rs.get_row_data()))
                    records.append(self._bs_kline_valuation_row(row, sym6))
            time.sleep(0.1)
        except Exception as e:
            logger.debug(f"Baostock K-line PE/PB fetch failed for {symbol}: {e}")

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def _bs_profit_row(self, row: Dict, symbol: str) -> Dict:
        """
        Baostock 利润表行标准化。

        Bug Fix (2026-04-14):
        - ROA: Baostock 利润表无 ROA 字段，留空（0.0），由 daily_valuation 表提供
        - EPS: Baostock 提供 epsTTM（非 basic_eps）
        - gross_margin: 对应 gpMargin 字段
        """
        sym6 = str(symbol).zfill(6)
        ann = row.get("profit_statements_pub_date", "")
        end = row.get("profit_statements_report_date", "")
        return {
            "ts_code": build_ts_code(sym6),
            "ann_date": pd.to_datetime(ann, errors="coerce") if ann else pd.NaT,
            "end_date": pd.to_datetime(end, errors="coerce") if end else pd.NaT,
            "report_type": "年报",
            "revenue": float(row.get("total_operating_revenue", 0) or 0),
            "net_profit": float(row.get("parent_netprofit", 0) or 0),
            "total_assets": float(row.get("total_assets", 0) or 0),
            "total_equity": float(row.get("total_shareholder_equity", 0) or 0),
            "roe": float(row.get("roeAvg", 0) or 0),
            "roa": 0.0,                                        # 修复：Baostock 利润表无 ROA 字段，留空
            "eps": float(row.get("epsTTM", 0) or 0),
            "gross_margin": float(row.get("gpMargin", 0) or 0),
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

    def _bs_kline_valuation_row(self, row: Dict, symbol: str) -> Dict:
        """
        Baostock K 线自带 PE/PB 估值行 → 存入 financial_data。

        Bug Fix (2026-04-14): 原 _ak_indicator_row 调用的 AkShare stock_a_indicator_lg
        早已不可用（akshare 1.18.50 无此接口），导致 pe_ttm/pb 永远为空。
        替换为 Baostock query_history_k_data_plus 的 pe/pb 列（已验证可用）。
        totalMv = 总市值（万元），转为亿元：/ 10000
        """
        sym6 = str(symbol).zfill(6)
        return {
            "ts_code": build_ts_code(sym6),
            "ann_date": pd.to_datetime(row.get("date", ""), errors="coerce") or pd.NaT,
            "end_date": pd.NaT,
            "report_type": "估值",
            "revenue": 0.0,
            "net_profit": 0.0,
            "total_assets": 0.0,
            "total_equity": 0.0,
            "roe": 0.0,
            "roa": 0.0,
            "eps": 0.0,
            "gross_margin": 0.0,
            "pe_ttm": float(row.get("pe", "") or 0) or 0.0,
            "pb": float(row.get("pb", "") or 0) or 0.0,
            "data_source": "baostock_kline",
        }

    def _save_financial_data(self, df: pd.DataFrame):
        """
        保存财务数据到 v5 financial_statement 表（v5 重构）

        字段映射（旧 → v5）：
          ts_code → ticker
          ann_date → announcement_date
          end_date → report_period_end
          roe → roe_avg
          debt_ratio → (不写入 financial_statement，由 financial_factor 派生)
        同时写入 financial_factor 派生表（asset_liability_ratio = 1 - 1/asset_to_equity）
        """
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            # ── 列名兼容映射 ─────────────────────────────────────────
            df = df.copy()
            if "ts_code" in df.columns and "ticker" not in df.columns:
                df.rename(columns={"ts_code": "ticker"}, inplace=True)
            if "ann_date" in df.columns and "announcement_date" not in df.columns:
                df.rename(columns={"ann_date": "announcement_date"}, inplace=True)
            if "end_date" in df.columns and "report_period_end" not in df.columns:
                df.rename(columns={"end_date": "report_period_end"}, inplace=True)
            if "roe" in df.columns and "roe_avg" not in df.columns:
                df.rename(columns={"roe": "roe_avg"}, inplace=True)

            # available_date = announcement_date（PIT：公告日当天可见）
            if "available_date" not in df.columns:
                df["available_date"] = df.get("announcement_date")

            # report_type 默认 FY
            if "report_type" not in df.columns:
                df["report_type"] = "FY"

            # 补充 v5 新字段默认值
            for col in ["cogs", "gross_profit", "operating_profit", "net_profit_parent",
                        "total_liabilities", "asset_to_equity", "operating_cash_flow",
                        "investing_cash_flow", "financing_cash_flow", "capex",
                        "roa", "roic", "gross_margin", "net_margin"]:
                if col not in df.columns:
                    df[col] = None

            conn.register("tmp_fin", df)
            try:
                conn.execute("""
                    INSERT INTO financial_statement
                        (ticker, report_period_end, announcement_date, available_date,
                         report_type, revenue, net_profit, total_assets, equity,
                         roe_avg, roa, roic, gross_margin, net_margin,
                         asset_to_equity, created_at, updated_at)
                    SELECT
                        ticker, report_period_end, announcement_date, available_date,
                        COALESCE(report_type, 'FY'),
                        COALESCE(revenue, 0),
                        COALESCE(net_profit, 0),
                        COALESCE(total_assets, 0),
                        COALESCE(total_equity, 0),
                        COALESCE(roe_avg, 0),
                        COALESCE(roa, 0),
                        COALESCE(roic, 0),
                        gross_margin,
                        net_margin,
                        asset_to_equity,
                        NOW(),
                        NOW()
                    FROM tmp_fin
                    ON CONFLICT(ticker, report_period_end) DO UPDATE SET
                        revenue       = excluded.revenue,
                        net_profit    = excluded.net_profit,
                        total_assets  = excluded.total_assets,
                        equity        = excluded.equity,
                        roe_avg       = excluded.roe_avg,
                        roa           = excluded.roa,
                        roic          = excluded.roic,
                        gross_margin  = excluded.gross_margin,
                        net_margin    = excluded.net_margin,
                        asset_to_equity = excluded.asset_to_equity,
                        updated_at    = NOW()
                """)
            except Exception as e:
                logger.warning(f"financial_statement upsert failed: {e}")

            # ── 同步写入 financial_factor（派生因子）─────────────────
            try:
                conn.execute("""
                    INSERT INTO financial_factor
                        (ticker, report_period_end, announcement_date, available_date,
                         roe, roic, gross_margin, asset_liability_ratio, net_profit,
                         created_at)
                    SELECT
                        ticker, report_period_end, announcement_date, available_date,
                        roe_avg,
                        roic,
                        gross_margin,
                        CASE WHEN asset_to_equity IS NOT NULL AND asset_to_equity > 1
                             THEN 1.0 - 1.0 / asset_to_equity
                             ELSE NULL END,
                        net_profit,
                        NOW()
                    FROM tmp_fin
                    ON CONFLICT(ticker, report_period_end) DO UPDATE SET
                        roe           = excluded.roe,
                        roic          = excluded.roic,
                        gross_margin  = excluded.gross_margin,
                        asset_liability_ratio = excluded.asset_liability_ratio,
                        net_profit    = excluded.net_profit
                """)
            except Exception as e:
                logger.debug(f"financial_factor sync skipped: {e}")

            conn.execute("DROP VIEW IF EXISTS tmp_fin")

    def _get_local_stocks(self) -> pd.DataFrame:
        """
        从本地数据库获取股票列表。

        只使用 stock_basic_history（PIT 历史表）获取当前有效股票池。
        旧表 stock_basic 已废弃，不再使用。

        Returns
        -------
        pd.DataFrame: 包含当前有效股票池（最新 eff_date 版本）
        """
        # 使用 PIT 历史表：取最新 eff_date 的记录作为当前状态
        pit_result = self.query("""
            SELECT * FROM stock_basic_history
            WHERE eff_date = (SELECT MAX(eff_date) FROM stock_basic_history)
        """)
        if pit_result.empty:
            raise RuntimeError(
                "stock_basic_history 表为空，请先调用 sync_stock_list() 同步数据。"
                "旧表 stock_basic 已废弃，不支持回退。"
            )
        return pit_result

    def sync_stock_list(self, include_delisted: bool = True):
        """
        同步股票列表到本地数据库（含 list_date/delist_date/exchange/board）

        改进点：
        1. 只写入 stock_basic_history（历史PIT版本），旧表 stock_basic 已废弃
        2. Baostock 补充精确的 list_date / delist_date
        3. exchange / board 统一由 exchange_mapping.classify_exchange() 派生，禁止自行拼接
        4. stock_basic_history 使用 UPSERT（ON CONFLICT DO UPDATE）防止重复记录
        5. eff_date = 今日，标记本次同步时间点
        """
        df = self._fetch_remote_stocks(include_delisted)
        if df.empty:
            raise RuntimeError(
                "无法从 AkShare 获取股票列表（网络可能不可用）。"
                "请检查网络连接后重试。"
            )

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
            # ── 写入 v5 stock_basic 表 ──────────────────────────────
            # UPSERT：同一 ticker 已存在则更新，不存在则插入
            # 列名映射：ts_code → ticker, industry → industry_level1
            df_write = df.copy()
            if "ts_code" in df_write.columns and "ticker" not in df_write.columns:
                df_write.rename(columns={"ts_code": "ticker"}, inplace=True)
            if "industry" in df_write.columns and "industry_level1" not in df_write.columns:
                df_write.rename(columns={"industry": "industry_level1"}, inplace=True)
            if "delist_date" in df_write.columns and "delisted_date" not in df_write.columns:
                df_write.rename(columns={"delist_date": "delisted_date"}, inplace=True)
            if "list_date" in df_write.columns and "listed_date" not in df_write.columns:
                df_write.rename(columns={"list_date": "listed_date"}, inplace=True)
            if "list_status" not in df_write.columns:
                df_write["list_status"] = df_write["delisted_date"].apply(
                    lambda d: "delisted" if pd.notna(d) else "listed"
                )
            if "is_financial" not in df_write.columns:
                df_write["is_financial"] = False

            sb_cols = ["ticker", "name", "exchange", "market", "industry_level1",
                       "listed_date", "delisted_date", "list_status", "is_financial"]
            sb_write = [c for c in sb_cols if c in df_write.columns]
            conn.register("tmp_sb", df_write[sb_write])
            conn.execute(f"""
                INSERT INTO stock_basic ({','.join(sb_write)}, created_at, updated_at)
                SELECT {','.join(sb_write)}, NOW(), NOW()
                FROM tmp_sb
                ON CONFLICT (ticker) DO UPDATE SET
                    name           = excluded.name,
                    exchange       = excluded.exchange,
                    market         = excluded.market,
                    industry_level1 = excluded.industry_level1,
                    listed_date    = excluded.listed_date,
                    delisted_date  = excluded.delisted_date,
                    list_status    = excluded.list_status,
                    updated_at     = NOW()
            """)
            conn.execute("DROP VIEW IF EXISTS tmp_sb")

            # ── 同步写入 company_status（状态快照）──────────────────
            today_str = self._get_now().strftime("%Y-%m-%d")
            try:
                cs_df = df_write[["ticker"]].copy()
                cs_df["status_date"] = pd.to_datetime(today_str)
                cs_df["listed_flag"] = True
                cs_df["delisted_flag"] = df_write.get("delisted_date", pd.Series([None]*len(df_write))).notna()
                cs_df["suspended_flag"] = False
                cs_df["st_status"] = False
                cs_df["risk_warning_flag"] = False
                cs_df["tradable_flag"] = ~cs_df["delisted_flag"]
                conn.register("tmp_cs", cs_df)
                conn.execute("""
                    INSERT INTO company_status
                        (ticker, status_date, listed_flag, delisted_flag,
                         suspended_flag, st_status, risk_warning_flag, tradable_flag, created_at)
                    SELECT ticker, status_date, listed_flag, delisted_flag,
                           suspended_flag, st_status, risk_warning_flag, tradable_flag,
                           NOW()
                    FROM tmp_cs
                    ON CONFLICT (ticker, status_date) DO UPDATE SET
                        listed_flag    = excluded.listed_flag,
                        delisted_flag  = excluded.delisted_flag,
                        tradable_flag  = excluded.tradable_flag,
                        updated_at     = NOW()
                """)
                conn.execute("DROP VIEW IF EXISTS tmp_cs")
            except Exception as e:
                logger.debug(f"company_status sync skipped: {e}")

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
        保存行情数据到 DuckDB（v5: 单表 market_daily）

        数据分层原则（v5）：
        - market_daily: 存原始价 OHLCV + adj_factor
          qfq = raw_close × adj_factor（由查询时实时计算，禁止在存储时预计算）
          不再拆分 raw/adjusted 双表

        写入策略：使用 DuckDB register() + INSERT ... SELECT（批量，700x 快于逐行）
        同步更新 sync_progress 表（记录每只股票的最新同步日期，支持断点续跑）
        """
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            df = df.copy()

            # ── 列名兼容：ts_code → ticker ────────────────────────────
            if "ts_code" in df.columns and "ticker" not in df.columns:
                df.rename(columns={"ts_code": "ticker"}, inplace=True)

            if "symbol" not in df.columns:
                df["symbol"] = df["ticker"].str.replace(r"\.[A-Z]+$", "", regex=True)

            # ── 原始价字段 ───────────────────────────────────────────
            # 优先用 raw_* 前缀（fetch_single 新格式），否则降级用 close
            has_raw = "raw_close" in df.columns
            for raw_col, std_col in [("raw_open", "open"), ("raw_high", "high"),
                                      ("raw_low", "low"), ("raw_close", "close")]:
                if raw_col not in df.columns:
                    df[raw_col] = df.get(std_col, 0.0)

            raw_close = df["raw_close"].fillna(0)
            adj_factor = df.get("adj_factor", pd.Series([1.0] * len(df))).fillna(1.0)

            # ── v5 必填字段补默认值 ──────────────────────────────────
            for col in ["open", "high", "low", "close", "pre_close"]:
                if col not in df.columns:
                    df[col] = 0.0
            for col in ["volume", "amount"]:
                if col not in df.columns:
                    df[col] = 0
            for col in ["pct_chg", "turnover_rate"]:
                if col not in df.columns:
                    df[col] = 0.0
            if "adj_factor" not in df.columns:
                df["adj_factor"] = 1.0
            for col in ["suspended_flag", "limit_up_flag", "limit_down_flag", "is_new_stock"]:
                if col not in df.columns:
                    df[col] = False
            for col in ["total_market_cap", "float_market_cap"]:
                if col not in df.columns:
                    df[col] = 0.0
            if "updated_at" not in df.columns:
                df["updated_at"] = None

            # ── 列名映射：旧 → v5 ───────────────────────────────────
            rename = {
                "is_suspend": "suspended_flag",
                "is_suspend": "suspended_flag",
                "limit_up": "limit_up_flag",
                "limit_down": "limit_down_flag",
            }
            df.rename(columns=rename, inplace=True)

            # ── mode=overwrite: 删除旧数据 ──────────────────────────
            if mode == "overwrite":
                codes = df["ticker"].unique().tolist()
                dates_min = df["trade_date"].min()
                dates_max = df["trade_date"].max()
                if codes and not (pd.isna(dates_min) or pd.isna(dates_max)):
                    codes_df = pd.DataFrame({"ticker": codes})
                    conn.register("tmp_codes", codes_df)
                    conn.execute("""
                        DELETE FROM market_daily
                        WHERE ticker IN (SELECT ticker FROM tmp_codes)
                        AND trade_date BETWEEN ? AND ?
                    """, (dates_min, dates_max))
                    conn.execute("DROP VIEW IF EXISTS tmp_codes")

            # ── 写入 market_daily ────────────────────────────────────
            conn.register("tmp_quotes", df)
            try:
                conn.execute("""
                    INSERT INTO market_daily
                        (ticker, trade_date, open, high, low, close, pre_close,
                         volume, amount, turnover_rate, total_market_cap, float_market_cap,
                         adj_factor, suspended_flag, limit_up_flag, limit_down_flag,
                         is_new_stock, created_at, updated_at)
                    SELECT ticker, trade_date,
                           COALESCE(raw_open, open, 0),
                           COALESCE(raw_high, high, 0),
                           COALESCE(raw_low,  low,  0),
                           COALESCE(raw_close, close, 0),
                           COALESCE(pre_close, 0),
                           COALESCE(volume, 0),
                           COALESCE(amount, 0),
                           COALESCE(turnover_rate, 0),
                           COALESCE(total_market_cap, 0),
                           COALESCE(float_market_cap, 0),
                           COALESCE(adj_factor, 1.0),
                           COALESCE(suspended_flag, FALSE),
                           COALESCE(limit_up_flag, FALSE),
                           COALESCE(limit_down_flag, FALSE),
                           COALESCE(is_new_stock, FALSE),
                           NOW(),
                           NOW()
                    FROM tmp_quotes
                    ON CONFLICT(ticker, trade_date) DO UPDATE SET
                        open             = excluded.open,
                        high             = excluded.high,
                        low              = excluded.low,
                        close            = excluded.close,
                        pre_close        = excluded.pre_close,
                        volume           = excluded.volume,
                        amount           = excluded.amount,
                        turnover_rate    = excluded.turnover_rate,
                        total_market_cap = excluded.total_market_cap,
                        float_market_cap = excluded.float_market_cap,
                        adj_factor       = excluded.adj_factor,
                        suspended_flag   = excluded.suspended_flag,
                        limit_up_flag   = excluded.limit_up_flag,
                        limit_down_flag = excluded.limit_down_flag,
                        is_new_stock    = excluded.is_new_stock,
                        updated_at       = NOW()
                """)
            except Exception as e:
                logger.warning(f"market_daily upsert failed: {e}")

            conn.execute("DROP VIEW IF EXISTS tmp_quotes")

            # ── 更新 sync_progress（v5: ticker 列名）─────────────────
            try:
                agg = df.groupby("ticker").agg(
                    last_sync_date=("trade_date", "max"),
                    total_records=("trade_date", "count")
                ).reset_index()
                agg["table_name"] = "market_daily"
                agg["status"]     = "ok"
                agg["error_msg"]  = None
                conn.register("tmp_progress", agg)
                conn.execute("""
                    INSERT INTO sync_progress
                        (ticker, table_name, last_sync_date, total_records, status)
                    SELECT ticker, table_name, last_sync_date, total_records, status
                    FROM tmp_progress
                    ON CONFLICT (ticker, table_name) DO UPDATE SET
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
                WHERE ticker = ? AND table_name = 'market_daily'
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
        """
        Baostock 抓取核心，支持 raw+qfq 双获取计算 adj_factor。

        方案：同时获取 adjustflag='1'(未复权) 和 adjustflag='2'(前复权)，
        用公式 adj_factor = qfq_close / raw_close 计算真正的复权因子。
        这样确保 qfq_close = raw_close * adj_factor 恒成立。
        """
        if not HAS_BAOSTOCK:
            return pd.DataFrame()
        try:
            sym6 = str(symbol).zfill(6)
            from exchange_mapping import build_bs_code as _build_bs_code
            bs_code = _build_bs_code(sym6)

            with self._bs_lock:
                bs.login()
                try:
                    # 同时获取未复权和前复权两份数据
                    fields = "date,open,high,low,close,volume,amount"

                    rs_raw = bs.query_history_k_data_plus(
                        bs_code, fields,
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                        frequency="d", adjustflag="1"
                    )
                    rs_qfq = bs.query_history_k_data_plus(
                        bs_code, fields,
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                        frequency="d", adjustflag="2"
                    )
                finally:
                    bs.logout()

            def _parse(rs):
                if rs is None or rs.error_code != "0":
                    return pd.DataFrame()
                data = []
                while rs.next():
                    data.append(rs.get_row_data())
                df = pd.DataFrame(data, columns=["trade_date", "open", "high", "low", "close", "volume", "amount"])
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                return df

            df_raw = _parse(rs_raw)
            df_qfq = _parse(rs_qfq)

            if df_raw.empty:
                return pd.DataFrame()

            # 合并两份数据，用 trade_date 作为 key
            if not df_qfq.empty:
                df_qfq = df_qfq.rename(columns={
                    "open": "qfq_open", "high": "qfq_high",
                    "low": "qfq_low", "close": "qfq_close"
                })
                df_raw = df_raw.merge(df_qfq[["trade_date", "qfq_open", "qfq_high", "qfq_low", "qfq_close"]],
                                      on="trade_date", how="left")
                # adj_factor = 前复权收盘价 / 未复权收盘价
                df_raw["adj_factor"] = (
                    df_raw["qfq_close"] / df_raw["close"].replace(0, np.nan)
                ).fillna(1.0).replace([np.inf, -np.inf], 1.0)
            else:
                # 备援：没有前复权数据，adj_factor=1.0
                df_raw["qfq_close"] = df_raw["close"]
                df_raw["qfq_open"] = df_raw["open"]
                df_raw["qfq_high"] = df_raw["high"]
                df_raw["qfq_low"] = df_raw["low"]
                df_raw["adj_factor"] = 1.0

            # 基础字段
            df_raw["ts_code"] = build_ts_code(sym6)
            df_raw["pre_close"] = df_raw["qfq_close"].shift(1)
            df_raw["pct_chg"] = (
                df_raw["qfq_close"] / df_raw["pre_close"].replace(0, np.nan) - 1
            ).fillna(0) * 100
            df_raw["is_suspend"] = df_raw["volume"] == 0
            df_raw["data_source"] = "baostock"
            df_raw["turnover"] = 0.0

            # 涨跌停判定
            df_raw = self._apply_limit_flags(df_raw, symbol)

            # 按 save_quotes 的双层字段格式组织：
            # raw_* -> 存入 daily_bar_raw（原始未复权价格）
            # qfq_* -> 存入 daily_bar_adjusted（前复权价格）
            result = pd.DataFrame({
                "ts_code":     df_raw["ts_code"],
                "trade_date":  df_raw["trade_date"],
                # raw 层（原始未复权）
                "raw_open":    df_raw["open"],
                "raw_high":    df_raw["high"],
                "raw_low":     df_raw["low"],
                "raw_close":   df_raw["close"],
                # adjusted 层（前复权）
                "open":        df_raw["qfq_open"],
                "high":        df_raw["qfq_high"],
                "low":         df_raw["qfq_low"],
                "close":       df_raw["qfq_close"],
                "pre_close":   df_raw["pre_close"],
                "volume":      df_raw["volume"],
                "amount":      df_raw["amount"].astype(float),
                "pct_chg":     df_raw["pct_chg"],
                "turnover":    df_raw["turnover"],
                "adj_factor":  df_raw["adj_factor"],
                "is_suspend":  df_raw["is_suspend"],
                "limit_up":    df_raw["limit_up"],
                "limit_down":  df_raw["limit_down"],
                "data_source": df_raw["data_source"],
            })
            return result

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
            # 使用 get_active_stocks() PIT 查询获取当前可交易股票池
            # 不再依赖 _get_local_stocks()（仅保留为显式维护路径）
            today = self._get_now().strftime("%Y-%m-%d")
            symbols = self.get_active_stocks(today)
            if not symbols:
                raise RuntimeError(
                    "get_active_stocks() 返回空列表，stock_basic_history 表可能为空。"
                    "请先调用 sync_stock_list() 同步数据。"
                )
            stats["total"] = len(symbols)

        latest_local = self.get_latest_date()
        if latest_local:
            start_date = (pd.Timestamp(latest_local) + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = self.start_date  # 使用可配置的起始日期

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
                    from exchange_mapping import build_bs_code as _build_bs_code
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
        1.  duplicate_rows               — (ts_code, trade_date) 重复行
        2.  ohlc_violation              — high < low 或 close 超出 [low, high] 范围
        3.  pct_chg_extreme             — 单日涨跌幅绝对值 > 60%（超出任何板块限制）
        4.  adj_factor_jump             — 相邻两日 adj_factor 变化 > 5%（可能分红/拆股但未记录）
        5.  zero_volume_non_suspend     — volume=0 但未标记 is_suspend
        6.  delisted_with_future_data   — 已退市股票（delist_date 非空）仍有晚于退市日的数据
        7.  adj_raw_ratio_invalid       — daily_bar_adjusted.close / daily_bar_raw.close 偏离 adj_factor > 1%
        8.  volume_amount_inconsistent  — 成交额与成交量/收盘价关系偏差 > 20%
        9.  index_date_invalid          — 指数成分 in_date > out_date
        10. pit_universe_size_suspicious — PIT 查询股票池大小异常（< 1000 或 > 6000）
        11. raw_adj_row_count_mismatch  — raw 与 adjusted 行数不一致
        12. raw_adj_pk_missing          — 同一 (ts_code, trade_date) 在 raw 或 adjusted 缺失
        13. qfq_close_mismatch          — qfq_close ≠ raw_close × adj_factor（容差 ±0.1%）
        14. adj_factor_invalid          — adj_factor 缺失 / ≤ 0 / > 100

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

        # ── 8. 成交量/成交额一致性 ──────────────────────────────
        # amount ≈ volume × close（允许 ±20% 误差，兼容停牌日等特殊情况）
        vol_consistency_sql = f"""
            SELECT ts_code, trade_date, volume, amount, close,
                   ABS(amount - volume * close / 10000) / NULLIF(volume * close / 10000, 0) AS rel_diff
            FROM daily_bar_raw
            WHERE trade_date BETWEEN ? AND ? {code_filter_raw}
              AND volume > 0 AND close > 0 AND amount > 0
              AND ABS(amount - volume * close / 10000) / NULLIF(volume * close / 10000, 0) > 0.20
        """
        try:
            vc_df = self.query(vol_consistency_sql, tuple(code_params_raw))
            for _, row in vc_df.iterrows():
                alerts.append(("volume_amount_inconsistent", row["ts_code"], row["trade_date"],
                                f"volume={row['volume']}, amount={row['amount']}, "
                                f"close={row['close']}, rel_diff={row['rel_diff']:.2%}"))
            stats["volume_amount_inconsistent"] = len(vc_df)
        except Exception as e:
            logger.warning(f"volume_amount_inconsistent check failed: {e}")
            stats["volume_amount_inconsistent"] = -1

        # ── 9. 指数成分日期合法性 ───────────────────────────────
        # in_date <= out_date；out_date IS NULL 或 >= in_date
        idx_date_sql = """
            SELECT index_code, ts_code, in_date, out_date,
                   CASE WHEN out_date IS NOT NULL AND out_date < in_date
                        THEN 'out_before_in' END AS issue
            FROM index_constituents_history
            WHERE out_date IS NOT NULL AND out_date < in_date
        """
        try:
            idx_df = self.query(idx_date_sql)
            for _, row in idx_df.iterrows():
                alerts.append(("index_date_invalid", row["index_code"], None,
                                f"ts_code={row['ts_code']}: in_date={row['in_date']} "
                                f"but out_date={row['out_date']} (out_date < in_date)"))
            stats["index_date_invalid"] = len(idx_df)
        except Exception as e:
            logger.warning(f"index_date_invalid check failed: {e}")
            stats["index_date_invalid"] = -1

        # ── 10. 历史股票池回放一致性 ────────────────────────────
        # 验证 PIT 查询：对每个 eff_date，最大 eff_date <= trade_date 的记录数应一致
        # 如果某只股票的 eff_date 缺失，会导致该日期的股票池快照缺少这只股票
        pit_check_sql = f"""
            WITH date_stocks AS (
                SELECT
                    '2020-01-01'::DATE AS check_date,
                    COUNT(DISTINCT ts_code) AS stock_count
                FROM stock_basic_history
                WHERE eff_date <= CAST('2020-01-01' AS DATE)
                  AND list_date <= CAST('2020-01-01' AS DATE)
                  AND (delist_date IS NULL OR delist_date > CAST('2020-01-01' AS DATE))
            ),
            date_stocks_2024 AS (
                SELECT
                    '2024-01-01'::DATE AS check_date,
                    COUNT(DISTINCT ts_code) AS stock_count
                FROM stock_basic_history
                WHERE eff_date <= CAST('2024-01-01' AS DATE)
                  AND list_date <= CAST('2024-01-01' AS DATE)
                  AND (delist_date IS NULL OR delist_date > CAST('2024-01-01' AS DATE))
            )
            SELECT check_date, stock_count FROM date_stocks
            UNION ALL
            SELECT check_date, stock_count FROM date_stocks_2024
        """
        try:
            pit_df = self.query(pit_check_sql)
            # 基本检查：股票池大小应在合理范围（1000~6000）
            for _, row in pit_df.iterrows():
                cnt = int(row["stock_count"])
                if cnt < 1000 or cnt > 6000:
                    alerts.append(("pit_universe_size_suspicious", None, str(row["check_date"])[:10],
                                    f"get_active_stocks({row['check_date']}) returned {cnt} stocks "
                                    f"(expected 1000~6000)"))
                    stats["pit_universe_size_suspicious"] = stats.get("pit_universe_size_suspicious", 0) + 1
                else:
                    stats["pit_universe_size_suspicious"] = stats.get("pit_universe_size_suspicious", 0)
        except Exception as e:
            logger.warning(f"pit_universe_size_suspicious check failed: {e}")

        # ── 11. raw / adjusted 行数一致性 ─────────────────────
        # 验证：(ts_code, trade_date) 在 raw 和 adjusted 两表的行数是否一致
        row_cnt_sql = f"""
            WITH raw_cnt AS (
                SELECT ts_code, trade_date, COUNT(*) AS raw_cnt
                FROM daily_bar_raw
                WHERE trade_date BETWEEN ? AND ? {code_filter_raw}
                GROUP BY ts_code, trade_date
            ),
            adj_cnt AS (
                SELECT ts_code, trade_date, COUNT(*) AS adj_cnt
                FROM daily_bar_adjusted
                WHERE trade_date BETWEEN ? AND ? {code_filter_adj}
                GROUP BY ts_code, trade_date
            )
            SELECT r.ts_code, r.trade_date, r.raw_cnt, a.adj_cnt,
                   ABS(r.raw_cnt - a.adj_cnt) AS diff
            FROM raw_cnt r
            LEFT JOIN adj_cnt a ON r.ts_code = a.ts_code AND r.trade_date = a.trade_date
            WHERE a.ts_code IS NULL OR r.raw_cnt != a.adj_cnt
        """
        try:
            row_params = [start_date, end_date, start_date, end_date]
            if ts_codes:
                row_params += ts_codes + ts_codes
            rc_df = self.query(row_cnt_sql, tuple(row_params))
            for _, row in rc_df.iterrows():
                adj_cnt_val = int(row["adj_cnt"]) if not pd.isna(row["adj_cnt"]) else 0
                alerts.append(("raw_adj_row_count_mismatch", row["ts_code"], row["trade_date"],
                              f"raw={int(row['raw_cnt'])} adj={adj_cnt_val}"))
            stats["raw_adj_row_count_mismatch"] = len(rc_df)
        except Exception as e:
            logger.warning(f"raw_adj_row_count_mismatch check failed: {e}")
            stats["raw_adj_row_count_mismatch"] = -1

        # ── 12. raw 层缺 adjusted 记录（或反之）─────────────────
        # 每张表有另一张表没有的 (ts_code, trade_date)
        pk_missing_sql = f"""
            WITH raw_keys AS (
                SELECT ts_code, trade_date FROM daily_bar_raw
                WHERE trade_date BETWEEN ? AND ? {code_filter_raw}
            ),
            adj_keys AS (
                SELECT ts_code, trade_date FROM daily_bar_adjusted
                WHERE trade_date BETWEEN ? AND ? {code_filter_adj}
            )
            SELECT 'raw_only' AS side, r.ts_code, r.trade_date
            FROM raw_keys r LEFT JOIN adj_keys a ON r.ts_code = a.ts_code AND r.trade_date = a.trade_date
            WHERE a.ts_code IS NULL
            UNION ALL
            SELECT 'adj_only' AS side, a.ts_code, a.trade_date
            FROM adj_keys a LEFT JOIN raw_keys r ON a.ts_code = r.ts_code AND a.trade_date = r.trade_date
            WHERE r.ts_code IS NULL
            LIMIT 200
        """
        try:
            pk_params = [start_date, end_date, start_date, end_date]
            if ts_codes:
                pk_params += ts_codes + ts_codes
            pm_df = self.query(pk_missing_sql, tuple(pk_params))
            for _, row in pm_df.iterrows():
                alerts.append(("raw_adj_pk_missing", row["ts_code"], row["trade_date"],
                              f"{row['side']}: pk exists in {row['side'].replace('_only','')} but not in the other layer"))
            stats["raw_adj_pk_missing"] = len(pm_df)
        except Exception as e:
            logger.warning(f"raw_adj_pk_missing check failed: {e}")
            stats["raw_adj_pk_missing"] = -1

        # ── 13. qfq_close 与 raw_close × adj_factor 关系校验 ────
        # 验证：daily_bar_adjusted.qfq_close = daily_bar_raw.close × daily_bar_adjusted.adj_factor
        # 容差 ±0.1%（浮点误差允许范围）
        qfq_sql = f"""
            SELECT adj.ts_code, adj.trade_date,
                   adj.qfq_close,
                   raw.close AS raw_close,
                   adj.adj_factor,
                   adj.qfq_close - raw.close * adj.adj_factor AS abs_diff
            FROM daily_bar_adjusted adj
            JOIN daily_bar_raw raw
              ON adj.ts_code = raw.ts_code AND adj.trade_date = raw.trade_date
            WHERE adj.trade_date BETWEEN ? AND ?
              AND raw.close > 0 AND adj.adj_factor > 0
              AND ABS(adj.qfq_close - raw.close * adj.adj_factor) > raw.close * adj.adj_factor * 0.001
              {code_filter_adj.replace('AND ts_code', 'AND adj.ts_code')}
            LIMIT 100
        """
        try:
            qfq_params = [start_date, end_date] + (ts_codes or [])
            qfq_df = self.query(qfq_sql, tuple(qfq_params))
            for _, row in qfq_df.iterrows():
                alerts.append(("qfq_close_mismatch", row["ts_code"], row["trade_date"],
                              f"qfq_close={row['qfq_close']:.4f} vs "
                              f"raw_close({row['raw_close']:.4f}) × adj({row['adj_factor']:.4f}) "
                              f"={row['raw_close']*row['adj_factor']:.4f}"))
            stats["qfq_close_mismatch"] = len(qfq_df)
        except Exception as e:
            logger.warning(f"qfq_close_mismatch check failed: {e}")
            stats["qfq_close_mismatch"] = -1

        # ── 14. adj_factor 缺失 / 为 0 / 为负 ───────────────────
        adj_invalid_sql = f"""
            SELECT ts_code, trade_date, adj_factor
            FROM daily_bar_raw
            WHERE trade_date BETWEEN ? AND ?
              AND (adj_factor IS NULL OR adj_factor <= 0 OR adj_factor > 100)
              {code_filter_raw}
            LIMIT 100
        """
        try:
            ai_df = self.query(adj_invalid_sql, tuple(code_params_raw))
            for _, row in ai_df.iterrows():
                val = row["adj_factor"]
                val_str = str(val) if val is not None else "NULL"
                alerts.append(("adj_factor_invalid", row["ts_code"], row["trade_date"],
                              f"adj_factor={val_str} (must be > 0 and <= 100)"))
            stats["adj_factor_invalid"] = len(ai_df)
        except Exception as e:
            logger.warning(f"adj_factor_invalid check failed: {e}")
            stats["adj_factor_invalid"] = -1

        # ── 写入 data_quality_alert 表（批量插入）────────────────
        if alerts:
            with self.get_connection() as conn:
                for alert_type, ts_code, trade_date, detail in alerts:
                    try:
                        conn.execute("""
                            INSERT INTO data_quality_alert
                                (alert_type, ts_code, trade_date, detail)
                            VALUES (?, ?, ?, ?)
                        """, (alert_type, str(ts_code),
                              str(trade_date)[:10] if trade_date else None, detail))
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
        """检查股票是否已退市（PIT 查询，取当前最新状态）

        Parameters
        ----------
        ts_code : str
            股票代码如 '000001.SZ'

        Returns
        -------
        bool: True 表示已退市
        """
        # 使用 stock_basic_history 取最新 eff_date 的记录
        result = self.query("""
            SELECT is_delisted FROM (
                SELECT ts_code, MAX(eff_date) as latest_eff
                FROM stock_basic_history GROUP BY ts_code
            ) latest
            JOIN stock_basic_history h
              ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
            WHERE h.ts_code = ?
        """, (ts_code,))
        if result.empty:
            return False
        return bool(result.iloc[0, 0])

    def get_delist_date(self, ts_code: str) -> Optional[str]:
        """获取退市日期（PIT 查询，取当前最新状态）

        Returns
        -------
        str or None: 退市日期 YYYY-MM-DD，未退市返回 None
        """
        # 使用 stock_basic_history 取最新 eff_date 的记录
        result = self.query("""
            SELECT delist_date FROM (
                SELECT ts_code, MAX(eff_date) as latest_eff
                FROM stock_basic_history GROUP BY ts_code
            ) latest
            JOIN stock_basic_history h
              ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
            WHERE h.ts_code = ? AND h.is_delisted = TRUE
        """, (ts_code,))
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
            # 使用 stock_basic_history 进行 PIT 查询
            df = conn.execute("""
                SELECT t.ts_code, latest.symbol
                FROM tmp_delisted_filter t
                LEFT JOIN (
                    SELECT h2.ts_code, h2.symbol, h2.is_delisted, h2.delist_date
                    FROM (
                        SELECT ts_code, MAX(eff_date) as latest_eff
                        FROM stock_basic_history GROUP BY ts_code
                    ) latest
                    JOIN stock_basic_history h2
                      ON h2.ts_code = latest.ts_code AND h2.eff_date = latest.latest_eff
                ) latest ON t.ts_code = latest.ts_code
                WHERE latest.is_delisted IS NULL
                   OR latest.is_delisted = FALSE
                   OR latest.delist_date IS NULL
                   OR latest.delist_date > ?
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

        start_date = start_date or self.start_date  # 使用可配置的起始日期
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
            SELECT cal_date FROM trading_calendar
            WHERE is_trading_day = TRUE
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
        existing = self.query("SELECT COUNT(*) FROM trading_calendar WHERE is_trading_day=TRUE")
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
            cal.columns = ["cal_date", "is_trading_day"]
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
        sql = "SELECT cal_date FROM trading_calendar WHERE is_trading_day = TRUE"
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
    # 注意：旧表 stock_basic/daily_quotes/index_constituents 已废弃
    _EXPORT_TABLE_WHITELIST = frozenset({
        'daily_bar_adjusted', 'daily_bar_raw', 'stock_basic_history',
        'financial_data', 'index_constituents_history',
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

        delisted = self.query("""
            SELECT COUNT(*) FROM (
                SELECT ts_code, MAX(eff_date) as latest_eff
                FROM stock_basic_history GROUP BY ts_code
            ) latest
            JOIN stock_basic_history h
              ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
            WHERE h.is_delisted = TRUE
        """).iloc[0, 0]

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
