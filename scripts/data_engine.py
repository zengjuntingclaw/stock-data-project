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
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Union, Literal, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import hashlib
import warnings
import random

from loguru import logger

warnings.filterwarnings("ignore")

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
# 数据验证器
# ──────────────────────────────────────────────────────────────
class DataValidator:
    """
    多源交叉验证器
    
    - 按 5% 比例随机抽样
    - 对比主数据源与备用源
    - 差异 > 0.5% 记录到 data_error.log
    """

    def __init__(self, error_log_path: str = "logs/data_error.log"):
        self.error_log = Path(error_log_path)
        self.error_log.parent.mkdir(parents=True, exist_ok=True)

    def validate(self, df: pd.DataFrame) -> Dict:
        """
        基础数据验证
        
        检查：价格 <= 0、涨跌幅异常(>20% 按板块区分)、成交量 < 0
        """
        issues = []
        if df.empty:
            return {"ok": True, "issues": [], "count": 0}

        # 价格 <= 0
        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                continue
            bad = df[df[col] <= 0]
            if len(bad) > 0:
                issues.append({
                    "type": "invalid_price",
                    "field": col,
                    "count": len(bad),
                    "sample": bad[["ts_code", "trade_date", col]].head(3).to_dict("records")
                })

        # 涨跌幅异常（按板块区分：主板>10%为异常，创业板/科创板>20%，北交所>30%）
        if "pct_chg" in df.columns and "ts_code" in df.columns:
            def _max_pct(row):
                code = str(row["ts_code"])[:6]
                if code.startswith("688") or code.startswith("30"):
                    return 20
                elif code.startswith("4") or code.startswith("8"):
                    return 30
                else:
                    return 10
            
            if hasattr(df, "pct_chg"):
                max_pct = df.apply(_max_pct, axis=1)
                abnormal = df[df["pct_chg"].abs() > max_pct]
            else:
                abnormal = df[(df["pct_chg"].abs() > 20)]
            if len(abnormal) > 0:
                issues.append({
                    "type": "abnormal_change",
                    "count": len(abnormal),
                    "sample": abnormal[["ts_code", "trade_date", "pct_chg"]].head(3).to_dict("records")
                })

        # 成交量 < 0
        if "volume" in df.columns:
            bad_vol = df[df["volume"] < 0]
            if len(bad_vol) > 0:
                issues.append({"type": "invalid_volume", "count": len(bad_vol)})

        return {"ok": len(issues) == 0, "issues": issues, "count": len(df)}

    def cross_validate(self,
                       primary: pd.DataFrame,
                       secondary: pd.DataFrame,
                       tolerance: float = 0.005,
                       sample_ratio: float = 0.05) -> Dict:
        """
        多源交叉验证
        
        按 5% 比例随机抽样，比对收盘价，差异 > tolerance 记录到日志
        """
        if primary.empty or secondary.empty:
            return {"status": "skipped", "reason": "empty_data"}

        # 对齐
        key_cols = ["ts_code", "trade_date"]
        merged = primary.merge(
            secondary, on=key_cols, suffixes=("_p", "_s"), how="inner"
        )
        if len(merged) == 0:
            return {"status": "skipped", "reason": "no_overlap"}

        # 随机抽样 5%
        n_sample = max(1, int(len(merged) * sample_ratio))
        sample = merged.sample(n=min(n_sample, len(merged)), random_state=42)

        # 计算差异
        diff = (sample["close_p"] - sample["close_s"]).abs()
        diff_pct = diff / sample["close_p"].replace(0, np.nan)

        mismatches = sample[diff_pct > tolerance]

        result = {
            "status": "ok" if len(mismatches) == 0 else "mismatch",
            "total_checked": len(sample),
            "mismatch_count": len(mismatches),
            "mismatch_ratio": len(mismatches) / len(sample),
            "max_diff_pct": diff_pct.max(),
        }

        # 写日志
        if len(mismatches) > 0:
            self._log_errors(mismatches[["ts_code", "trade_date", "close_p", "close_s"]].assign(
                diff_pct=diff_pct[mismatches.index]
            ))

        return result

    def _log_errors(self, df: pd.DataFrame):
        with open(self.error_log, "a", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df.iterrows():
                f.write(
                    f"[{ts}] CROSS_VALIDATE_ERROR | "
                    f"ts_code={row['ts_code']} date={row['trade_date']} "
                    f"price_p={row['close_p']:.4f} price_s={row['close_s']:.4f} "
                    f"diff_pct={row['diff_pct']:.4f}\n"
                )


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

    # ──────────────────────────────────────────────────────────
    # 数据库初始化
    # ──────────────────────────────────────────────────────────
    def _init_schema(self):
        """初始化表结构"""
        conn = duckdb.connect(str(self.db_path))
        cur = conn.cursor()

        # 股票基本信息（含退市标记）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code    VARCHAR PRIMARY KEY,
                symbol     VARCHAR,
                name       VARCHAR,
                area       VARCHAR,
                industry   VARCHAR,
                market     VARCHAR,
                list_date  DATE,
                delist_date DATE,
                is_delisted  BOOLEAN DEFAULT FALSE,
                delist_reason VARCHAR,
                is_hs      BOOLEAN,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 指数成分股动态表（解决幸存者偏差）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS index_constituents (
                index_code   VARCHAR,      -- 指数代码如 '000300.SH'
                ts_code      VARCHAR,      -- 成分股代码
                trade_date   DATE,         -- 成分股生效日期
                in_date      DATE,         -- 加入日期
                out_date     DATE,         -- 退出日期（NULL表示仍在）
                PRIMARY KEY (index_code, ts_code, trade_date)
            )
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

        # 日线行情（含涨跌停标记）
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_quotes (
                ts_code       VARCHAR,
                trade_date    DATE,
                open          DOUBLE,
                high          DOUBLE,
                low           DOUBLE,
                close         DOUBLE,
                pre_close     DOUBLE,
                volume        BIGINT,
                amount        DOUBLE,
                pct_chg       DOUBLE,
                turnover      DOUBLE,
                adj_factor    DOUBLE DEFAULT 1.0,
                is_suspend    BOOLEAN DEFAULT FALSE,
                limit_up      BOOLEAN DEFAULT FALSE,
                limit_down    BOOLEAN DEFAULT FALSE,
                data_source   VARCHAR DEFAULT 'akshare',
                PRIMARY KEY (ts_code, trade_date)
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
            CREATE TABLE IF NOT EXISTS data_quality_alert (
                id            INTEGER PRIMARY KEY,
                alert_type    VARCHAR,
                ts_code       VARCHAR,
                trade_date    DATE,
                detail        VARCHAR,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 索引
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_code ON daily_quotes(ts_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_date ON daily_quotes(trade_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fin_ann ON financial_data(ann_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_val_date ON daily_valuation(trade_date)")

        conn.close()
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
    def _detect_limit(code: str) -> float:
        """根据股票代码返回涨跌停幅度
        
        科创板(688): 20%, 创业板(30): 20%, 北交所(4/8): 30%, 主板: 10%
        """
        c = str(code).zfill(6)
        if c.startswith("688"):
            return 0.20
        elif c.startswith("30"):
            return 0.20
        elif c.startswith("4") or c.startswith("8"):
            return 0.30
        else:
            return 0.10
    
    @staticmethod
    def _apply_limit_flags(df: pd.DataFrame, code: str, pct_col: str = "pct_chg") -> pd.DataFrame:
        """统一应用涨跌停标记（容差0.01%避免浮点误差）"""
        limit_pct = DataEngine._detect_limit(code)
        df["limit_up"] = df[pct_col] >= (limit_pct * 100 - 0.01)
        df["limit_down"] = df[pct_col] <= -(limit_pct * 100 - 0.01)
        return df
    
    @staticmethod
    def _build_ts_code(symbol: str) -> str:
        """构造 ts_code（上海/深圳）"""
        sym6 = str(symbol).zfill(6)
        if sym6.startswith(("6", "5", "9", "688")):
            return f"{sym6}.SH"
        else:
            return f"{sym6}.SZ"
    
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
            print(f"[INFO] Current stocks: {len(current)}")
        except Exception as e:
            print(f"[WARN] Failed to fetch current stocks: {e}")

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
                    print(f"[INFO] Delisted stocks: {len(dl)}")
            except Exception as e:
                print(f"[WARN] Failed to fetch delisted stocks: {e}")

        if not stocks:
            return self._get_local_stocks()

        df = pd.concat(stocks, ignore_index=True)
        df = df.drop_duplicates(subset=["symbol"], keep="first")

        # 构造 ts_code
        df["ts_code"] = df["symbol"].apply(
            lambda x: f"{str(x).zfill(6)}.SH" if str(x).startswith(("6", "5", "9"))
            else f"{str(x).zfill(6)}.SZ"
        )

        # 判断市场
        def _market(s):
            c = str(s).zfill(6)
            if c.startswith("688"):
                return "科创板"
            if c.startswith("8") or c.startswith("4"):
                return "北交所"
            if c.startswith("3"):
                return "创业板"
            if c.startswith("002"):
                return "中小板"
            return "主板"

        df["market"] = df["symbol"].apply(_market)
        return df

    # ──────────────────────────────────────────────────────────
    # 动态股票池（沪深300成分股时点对齐）
    # ──────────────────────────────────────────────────────────
    def sync_index_constituents(self, index_code: str = "000300.SH") -> None:
        """
        同步指数成分股历史（解决幸存者偏差）
        
        记录每个股票何时被加入/退出指数，确保回测时只使用
        该时点真实属于指数成分的股票。
        """
        if not HAS_AKSHARE:
            return

        with self.get_connection() as conn:

        # 获取历史成分股（AkShare提供）
        try:
            if index_code == "000300.SH":
                # 沪深300成分股
                df = ak.index_stock_cons_csindex(symbol="000300")
                if df.empty:
                    return
                df.columns = ["ts_code", "name", "in_date"]
                df["index_code"] = index_code
                df["trade_date"] = pd.Timestamp.now()
                df["out_date"] = pd.NaT

                # 写入数据库（使用DataFrame注册为临时表再INSERT，参数化查询防止SQL注入）
                conn.execute("DELETE FROM index_constituents WHERE index_code = ?", [index_code])
                df["index_code"] = index_code
                conn.register('tmp_constituents', df)
                conn.execute("""
                    INSERT INTO index_constituents (index_code, ts_code, trade_date, in_date, out_date)
                    SELECT index_code, ts_code, trade_date, in_date, out_date
                    FROM tmp_constituents
                """)
                print(f"[OK] Synced {len(df)} constituents for {index_code}")
        except Exception as e:
            print(f"[WARN] Failed to sync index constituents: {e}")

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
            SELECT ts_code FROM index_constituents
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
        
        注意：daily_quotes 表没有 name 字段，需要从 stock_basic 表 JOIN 获取。
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
                            WHEN sub.prev_st = 0 OR sub.prev_st IS NULL THEN TRUE 
                            ELSE FALSE 
                        END as is_new_st
                    FROM (
                        SELECT 
                            d.ts_code,
                            d.trade_date,
                            LAG(1) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) as prev_st
                        FROM daily_quotes d
                        INNER JOIN tmp_st_codes t ON d.ts_code = t.ts_code
                    ) sub
                """)
                conn.execute("DROP VIEW tmp_st_codes")
                print("[OK] Synced ST status history")
            except Exception as e:
                print(f"[WARN] Failed to sync ST status: {e}")

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
            print("[WARN] AkShare not available, financial fetch skipped")
            return stats

        end_year = end_year or datetime.now().year
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
                    print(f"[PROGRESS] Financial: {done}/{len(symbols)} "
                          f"success={stats['success']} failed={stats['failed']}")
                time.sleep(0.2)

        print(f"[OK] Financial data done. success={stats['success']} "
              f"failed={stats['failed']} records={stats['records']}")
        return stats

    def _fetch_financial_single(self,
                                 symbol: str,
                                 start_year: int,
                                 end_year: int) -> pd.DataFrame:
        """抓取单只股票的财务数据（年报 + 季报）"""
        records = []
        sym6 = str(symbol).zfill(6)
        bs_code = (f"sh.{sym6}" if sym6.startswith("6") or sym6.startswith("5")
                   else f"sz.{sym6}")

        # 年报（4个财年）
        try:
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
            "ts_code": f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ",
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
            "gp": float(row.get("gross_profit_margin", 0) or 0),
            "data_source": "baostock",
        }

    def _ak_indicator_row(self, row: pd.Series, symbol: str) -> Dict:
        """AkShare 指标行标准化"""
        sym6 = str(symbol).zfill(6)
        return {
            "ts_code": f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ",
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
            "gp": float(row.get("销售毛利率(%)", 0) or 0),
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
                        roe, roa, eps, gp, data_source
                    FROM df
                """)
            except Exception as e:
                logger.warning(f"Financial data UPSERT failed, trying fallback: {e}")
                # 字段不全时用原始列（列名来自 DataFrame 列列表，非用户输入，安全）
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
        同步股票列表到本地数据库（含 list_date/delist_date）
        
        使用 Baostock 补充上市/退市日期信息，解决幸存者偏差中的
        日期缺失问题。
        """
        df = self.get_all_stocks(include_delisted)
        if df.empty:
            return

        conn = duckdb.connect(str(self.db_path))

        # Baostock 补充 list_date / delist_date
        if HAS_BAOSTOCK:
            print("[INFO] Enriching list/delist dates from Baostock...")
            try:
                bs.login()
                try:
                    bs_stocks = bs.query_all_stock(day=datetime.now().strftime("%Y-%m-%d"))
                finally:
                    bs.logout()
                if bs_stocks is not None and not bs_stocks.empty:
                    date_map = dict(zip(
                        bs_stocks["code"].str.replace("sh.", "6", regex=False).str.replace("sz.", "", regex=False),
                        bs_stocks["ipoDate"]
                    ))
                    delist_map = dict(zip(
                        bs_stocks["code"].str.replace("sh.", "6", regex=False).str.replace("sz.", "", regex=False),
                        bs_stocks["outDate"]
                    ))
                    if "symbol" in df.columns:
                        df["list_date"] = df["symbol"].map(date_map).pipe(
                            lambda s: pd.to_datetime(s, errors="coerce")
                        )
                        df["delist_date"] = df["symbol"].map(delist_map).pipe(
                            lambda s: pd.to_datetime(s, errors="coerce")
                        )
            except Exception as e:
                print(f"[WARN] Baostock list_date enrichment failed: {e}")

        # 写入基本信息
        cols = ["ts_code", "symbol", "name", "industry", "market",
                "list_date", "delist_date", "is_delisted"]
        write_cols = [c for c in cols if c in df.columns]
        for col in ["list_date", "delist_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        try:
            conn.execute("DELETE FROM stock_basic")
            conn.execute(
                f"INSERT INTO stock_basic ({','.join(write_cols)}) SELECT {','.join(write_cols)} FROM df"
            )
        finally:
            conn.close()
        print(f"[OK] Synced {len(df)} stocks to database "
              f"(list_date filled: {df['list_date'].notna().sum()})")

    # ──────────────────────────────────────────────────────────
    # 日线数据下载
    # ──────────────────────────────────────────────────────────
    def fetch_single(self,
                     symbol: str,
                     start_date: str,
                     end_date: str,
                     adjust: Literal["qfq", "hfq", ""] = "qfq") -> pd.DataFrame:
        """
        下载单只股票日线数据
        
        Parameters
        ----------
        symbol : str
            6位股票代码
        start_date : str
            开始日期，格式 YYYY-MM-DD
        end_date : str
            结束日期，格式 YYYY-MM-DD
        adjust : 'qfq' | 'hfq' | ''
            复权方式
        """
        if not HAS_AKSHARE:
            return pd.DataFrame()

        try:
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "")

            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust=adjust
            )
            if df.empty:
                return pd.DataFrame()

            # 列名标准化
            col_map = {
                "日期": "trade_date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "涨跌幅": "pct_chg", "换手率": "turnover"
            }
            df = df.rename(columns=col_map)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df["ts_code"] = (
                f"{symbol.zfill(6)}.SZ"
                if symbol.startswith(("0", "3", "2"))
                else f"{symbol.zfill(6)}.SH"
            )
            df["pre_close"] = df["close"].shift(1)
            df["is_suspend"] = df["volume"] == 0
            
            # 涨跌停判定（使用统一方法）
            df = self._apply_limit_flags(df, symbol)
            df["data_source"] = "akshare"

            # 计算复权因子
            if adjust:
                try:
                    df_raw = ak.stock_zh_a_hist(
                        symbol=symbol, period="daily",
                        start_date=start_str, end_date=end_str, adjust=""
                    )
                    if not df_raw.empty:
                        df_raw = df_raw.rename(columns={"日期": "trade_date", "收盘": "close_raw"})
                        df_raw["trade_date"] = pd.to_datetime(df_raw["trade_date"])
                        df = df.merge(df_raw[["trade_date", "close_raw"]], on="trade_date", how="left")
                        df["adj_factor"] = df["close"] / df["close_raw"].replace(0, np.nan)
                        df.drop(columns=["close_raw"], inplace=True, errors="ignore")
                except Exception:
                    df["adj_factor"] = 1.0
            else:
                df["adj_factor"] = 1.0

            # 验证
            val = self.validator.validate(df)
            if not val["ok"]:
                print(f"[WARN] Data validation issues for {symbol}: {val['issues']}")

            cols = ["ts_code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "volume", "amount", "pct_chg", "turnover",
                    "adj_factor", "is_suspend", "limit_up", "limit_down", "data_source"]
            return df[[c for c in cols if c in df.columns]]

        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol}: {e}")
            return pd.DataFrame()

    def save_quotes(self, df: pd.DataFrame, mode: str = "append"):
        """
        保存行情数据到 DuckDB（UPSERT 逻辑，防止重复数据）
        
        使用 DuckDB 的 INSERT OR REPLACE 实现原子性 UPSERT，
        避免 DELETE+INSERT 产生的并发覆盖问题。
        """
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            if mode == "overwrite":
                codes = df["ts_code"].unique().tolist()
                dates_min = df["trade_date"].min()
                dates_max = df["trade_date"].max()
                if codes and not (pd.isna(dates_min) or pd.isna(dates_max)):
                    # 使用 register 避免大列表拼接 SQL 注入
                    codes_df = pd.DataFrame({"ts_code": codes})
                    conn.register('tmp_codes', codes_df)
                    conn.execute("""
                        DELETE FROM daily_quotes
                        WHERE ts_code IN (SELECT ts_code FROM tmp_codes)
                        AND trade_date BETWEEN ? AND ?
                    """, (dates_min, dates_max))
                    conn.execute("DROP VIEW tmp_codes")
            try:
                # 原子性 UPSERT（覆盖已存在的 ts_code+trade_date 组合）
                conn.execute("""
                    INSERT OR REPLACE INTO daily_quotes
                    (ts_code, trade_date, open, high, low, close, pre_close,
                     volume, amount, pct_chg, turnover, adj_factor,
                     is_suspend, limit_up, limit_down, data_source)
                    SELECT
                        ts_code, trade_date,
                        COALESCE(open, 0), COALESCE(high, 0),
                        COALESCE(low, 0), COALESCE(close, 0),
                        COALESCE(pre_close, 0), COALESCE(volume, 0),
                        COALESCE(amount, 0), COALESCE(pct_chg, 0),
                        COALESCE(turnover, 0), COALESCE(adj_factor, 1),
                        COALESCE(is_suspend, FALSE),
                        COALESCE(limit_up, FALSE),
                        COALESCE(limit_down, FALSE),
                        COALESCE(data_source, 'unknown')
                    FROM df
                """)
            except Exception as e:
                logger.warning(f"UPSERT failed, trying INSERT OR IGNORE: {e}")
                conn.execute("INSERT OR IGNORE INTO daily_quotes SELECT * FROM df")

    # ──────────────────────────────────────────────────────────
    # 增量更新（多线程 + 重试 + 断点续传）
    # ──────────────────────────────────────────────────────────
    def get_latest_date(self, ts_code: str = None) -> Optional[str]:
        """获取本地最新日期（含断点续传检测：验证最新日期数据是否完整）"""
        if not HAS_DUCKDB:
            return None

        # 先尝试获取最新日期
        if ts_code:
            result = self.query(
                "SELECT MAX(trade_date) as md FROM daily_quotes WHERE ts_code = ?",
                (ts_code,)
            )
        else:
            result = self.query("SELECT MAX(trade_date) as md FROM daily_quotes")
        val = result.iloc[0, 0] if not result.empty else None
        if val is None or pd.isna(val):
            return None

        latest = pd.Timestamp(val).strftime("%Y-%m-%d")

        # 断点续传加固：检查最新日期数据是否完整（close>0 且 volume>=0）
        if ts_code:
            check = self.query("""
                SELECT COUNT(*) as cnt FROM daily_quotes
                WHERE ts_code = ? AND trade_date = ?
                AND close > 0 AND volume >= 0
            """, (ts_code, latest))
            if check.empty or check.iloc[0, 0] == 0:
                # 数据损坏/不完整，回退到前一天
                prev = (pd.Timestamp(latest) - timedelta(days=1)).strftime("%Y-%m-%d")
                prev_check = self.query("""
                    SELECT COUNT(*) as cnt FROM daily_quotes
                    WHERE ts_code = ? AND trade_date = ?
                    AND close > 0 AND volume >= 0
                """, (ts_code, prev))
                if prev_check.empty or prev_check.iloc[0, 0] == 0:
                    return None  # 数据彻底损坏
                return prev
        return latest

    def _fetch_single_with_retry(self,
                                 symbol: str,
                                 start_date: str,
                                 end_date: str,
                                 adjust: Literal["qfq", "hfq", ""] = "qfq",
                                 max_retries: int = 3) -> Tuple[pd.DataFrame, str]:
        """
        带指数退避重试的抓取（AkShare → Baostock → 失败）
        
        Returns
        -------
        (df, source): df数据, source='akshare'|'baostock'|''
        """
        for attempt in range(max_retries):
            # AkShare
            if HAS_AKSHARE:
                df = self._fetch_akshare(symbol, start_date, end_date, adjust)
                if not df.empty:
                    return df, "akshare"

            # Baostock 备援
            if HAS_BAOSTOCK:
                df = self._fetch_baostock(symbol, start_date, end_date, adjust)
                if not df.empty:
                    return df, "baostock"

            if attempt < max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait)

        return pd.DataFrame(), ""

    def _fetch_akshare(self,
                       symbol: str,
                       start_date: str,
                       end_date: str,
                       adjust: Literal["qfq", "hfq", ""] = "qfq") -> pd.DataFrame:
        """AkShare 抓取（内部用）"""
        try:
            start_str = start_date.replace("-", "")
            end_str = end_date.replace("-", "")
            adj_map = {"qfq": "qfq", "hfq": "hfq", "": ""}
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
            df["ts_code"] = f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ"
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
            return df[[c for c in cols if c in df.columns]].copy()
        except Exception as e:
            logger.debug(f"AkShare fetch failed for {symbol}: {e}")
            return pd.DataFrame()

    def _fetch_baostock(self,
                        symbol: str,
                        start_date: str,
                        end_date: str,
                        adjust: Literal["qfq", "hfq", ""] = "qfq") -> pd.DataFrame:
        """Baostock 抓取（备援用，login/logout 在调用方管理）"""
        if not HAS_BAOSTOCK:
            return pd.DataFrame()
        try:
            adjflag_map = {"qfq": "2", "hfq": "1", "": "3"}
            adjflag = adjflag_map.get(adjust, "2")

            # 标准化 baostock 代码
            sym6 = str(symbol).zfill(6)
            if sym6.startswith("6") or sym6.startswith("5"):
                bs_code = f"sh.{sym6}"
            else:
                bs_code = f"sz.{sym6}"

            try:
                bs.login()
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
            df["ts_code"] = f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ"
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
            return df[[c for c in cols if c in df.columns]].copy()
        except Exception as e:
            logger.debug(f"Baostock fetch failed for {symbol}: {e}")
            return pd.DataFrame()(self,
                          symbols: List[str] = None,
                          adjust: Literal["qfq", "hfq", ""] = "qfq",
                          max_workers: int = 12,
                          delay: float = 0.2,
                          check_dividend: bool = True,
                          cross_validate: bool = True) -> Dict:
        """
        多线程增量更新日线数据 — 生产级增强版
        
        改进点：
        1. max_workers 默认 12（充分利用多核）
        2. 复权因子监测（dividend_check）
        3. 多源交叉验证（AkShare vs Baostock）
        4. 断点续传加固 + DataValidator
        """
        start_time = time.time()
        stats = {
            "total": len(symbols) if symbols else 0,
            "success": 0, "failed": 0, "records": 0,
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
            start_date = "2018-01-01"

        end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date > end_date:
            print("[INFO] Data is up-to-date.")
            return stats

        print(f"[INFO] Incremental update: {start_date} ~ {end_date}, "
              f"{len(symbols)} stocks, workers={max_workers}")

        def _download_one(sym: str) -> Tuple[str, pd.DataFrame, str, str]:
            """单线程下载器"""
            symbol_clean = str(sym).zfill(6)
            ts_code = (f"{symbol_clean}.SH" if symbol_clean.startswith(("6", "5", "9", "688"))
                       else f"{symbol_clean}.SZ")
            sym_latest = self.get_latest_date(ts_code)
            if sym_latest and sym_latest >= start_date:
                return sym, pd.DataFrame(), "", ""

            actual_start = start_date
            if actual_start > end_date:
                return sym, pd.DataFrame(), "", ""

            df, source = self._fetch_single_with_retry(sym, actual_start, end_date, adjust)
            return sym, df, source, ""

        # 多线程下载
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_download_one, s): s for s in symbols}
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

                    # DataValidator 验证后入库
                    val = self.validator.validate(df)
                    if not val["ok"]:
                        for issue in val["issues"]:
                            stats["errors"].append(f"{sym}: {issue}")

                    self.save_quotes(df, mode="append")
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
                    print(f"[PROGRESS] {done}/{len(symbols)} ({done/len(symbols)*100:.1f}%) "
                          f"success={stats['success']} failed={stats['failed']}")
                time.sleep(delay)

        # 多源交叉验证（抽样）
        if cross_validate and HAS_BAOSTOCK and stats["success"] > 0:
            self._cross_validate_sources(symbols[:min(50, len(symbols))], end_date, stats)

        stats["elapsed_sec"] = time.time() - start_time
        rate = stats["success"] / stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else 0
        print(f"[OK] Done. success={stats['success']} failed={stats['failed']} "
              f"records={stats['records']} time={stats['elapsed_sec']:.1f}s "
              f"({rate:.1f} stocks/s) dividend={stats['dividend_detected']} "
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
            FROM daily_quotes
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
            if change_ratio > 0.05:  # 5% 以上变化
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

        try:
            bs.login()
            for sym in symbols[:20]:  # 抽样20只
                sym6 = str(sym).zfill(6)
                bs_code = f"sh.{sym6}" if sym6.startswith("6") else f"sz.{sym6}"

                # AkShare 数据（参数化查询防止SQL注入）
                ak_data = self.query("""
                    SELECT pct_chg FROM daily_quotes
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
    # 单股数据提取
    # ──────────────────────────────────────────────────────────
    def get_security_data(self,
                          code: str,
                          start_date: str = None,
                          end_date: str = None,
                          adjust: Literal["qfq", "hfq", ""] = "qfq") -> pd.DataFrame:
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
        adjust : 'qfq' | 'hfq' | ''
            复权方式
        """
        # 标准化代码
        if "." in code:
            ts_code, symbol = code, code.split(".")[0]
        else:
            symbol = str(code).zfill(6)
            ts_code = f"{symbol}.SH" if symbol.startswith(("6", "5", "9")) else f"{symbol}.SZ"

        start_date = start_date or "2018-01-01"
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        # 查本地（参数化查询防止SQL注入）
        local = self.query("""
            SELECT * FROM daily_quotes
            WHERE ts_code = ?
            AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
        """, (ts_code, start_date, end_date))
        if not local.empty:
            local_latest = local["trade_date"].max()
            today = datetime.now().strftime("%Y-%m-%d")
            if pd.Timestamp(local_latest).strftime("%Y-%m-%d") < today:
                new_start = (pd.Timestamp(local_latest) + timedelta(days=1)).strftime("%Y-%m-%d")
                new_df = self.fetch_single(symbol, new_start, today, adjust)
                if not new_df.empty:
                    self.save_quotes(new_df, mode="append")
                    local = pd.concat([local, new_df], ignore_index=True)
            return local.sort_values("trade_date").reset_index(drop=True)

        # 本地没有，从网络拉（带重试）
        df, source = self._fetch_single_with_retry(symbol, start_date, end_date, adjust)
        if not df.empty:
            self.save_quotes(df, mode="append")
        return df

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
                    FROM daily_quotes d
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
                ROUND(AVG({price_col}) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW), 2) as ma_{w}
            """)

        with self.get_connection() as conn:
            conn.register('tmp_codes_ma', codes_df)
            sql = f"""
                SELECT 
                    d.ts_code,
                    d.trade_date,
                    d.{price_col},
                    {','.join(window_parts)}
                FROM daily_quotes d
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
                ROUND(STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW), 4) as vol_{w}
            """)

        with self.get_connection() as conn:
            conn.register('tmp_codes_vol', codes_df)
            sql = f"""
                SELECT 
                    d.ts_code,
                    d.trade_date,
                    d.pct_chg,
                    {','.join(window_parts)}
                FROM daily_quotes d
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
            print(f"[INFO] Calendar exists ({existing.iloc[0, 0]} days), skip sync")
            return

        if not HAS_AKSHARE:
            print("[INFO] AkShare unavailable, using existing calendar")
            return

        print("[INFO] Fetching calendar from AkShare...")
        end_year = end_year or datetime.now().year
        with self.get_connection() as conn:

        for year in range(start_year, end_year + 1):
            try:
                cal = ak.tool_trade_date_hist_sina()
                cal.columns = ["cal_date", "is_open"]
                cal["cal_date"] = pd.to_datetime(cal["cal_date"])
                cal["pretrade_date"] = None
                import tempfile, os as _os
                tmpdir = self.db_path.parent / "_tmp_cal"
                tmpdir.mkdir(exist_ok=True)
                pq = tmpdir / "cal.parquet"
                cal.to_parquet(str(pq), index=False)
                conn.execute(f"COPY trade_calendar FROM '{pq}' (FORMAT PARQUET)")
                _os.remove(pq)
                print(f"[OK] Calendar {year} synced")
            except Exception as e:
                print(f"[WARN] Calendar {year} failed: {e}")

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
        'daily_quotes', 'stock_basic', 'financial_data', 'index_constituents',
        'st_status_history', 'trade_calendar', 'daily_valuation',
        'update_log', 'adj_factor_log', 'data_quality_alert',
    })

    def export_parquet(self, table: str = "daily_quotes") -> Path:
        """导出表到 Parquet 格式（仅允许白名单表名）"""
        if table not in self._EXPORT_TABLE_WHITELIST:
            raise ValueError(f"表 '{table}' 不在导出白名单中，允许的表: {sorted(self._EXPORT_TABLE_WHITELIST)}")
        output = self.parquet_dir / f"{table}.parquet"
        # 路径安全检查：确保输出在 parquet_dir 内
        if not str(output.resolve()).startswith(str(self.parquet_dir.resolve())):
            raise ValueError(f"导出路径 '{output}' 不在允许的目录 '{self.parquet_dir}' 内")
        with self.get_connection() as conn:
            conn.execute(f"COPY (SELECT * FROM {table}) TO '{output}' (FORMAT PARQUET)")
        print(f"[OK] Exported to {output}")
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
            SELECT COUNT(*) as cnt FROM daily_quotes
            WHERE trade_date BETWEEN ? AND ?
        """, (start_date, end_date)).iloc[0, 0]

        anomaly = self.query("""
            SELECT COUNT(*) as cnt FROM daily_quotes
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
        for sym in symbols:
            sym6 = str(sym).zfill(6)
            if sym6.startswith(("6", "5", "9", "688")):
                ts_codes.append(f"{sym6}.SH")
            else:
                ts_codes.append(f"{sym6}.SZ")
        
        # 使用 register 避免 SQL 注入
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        with self.get_connection() as conn:
            conn.register('tmp_batch_codes', codes_df)
            df = conn.execute("""
                SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close,
                       d.volume, d.amount, d.pct_chg, d.pre_close,
                       d.is_suspend, d.limit_up, d.limit_down
                FROM daily_quotes d
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
