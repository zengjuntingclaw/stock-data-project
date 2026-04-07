"""
DataEngine - 鐢熶骇绾ф暟鎹不鐞嗕笌瀛樺偍寮曟搸
=====================================
鏍稿績鍔熻兘锛?
  1. 娑堥櫎骞稿瓨鑰呭亸宸紙鍏ㄩ噺鍘嗗彶鎴愬垎鑲″惈閫€甯傦級
  2. DuckDB + Parquet 鍙屽瓨鍌?
  3. 澧為噺鏇存柊锛堟柇鐐圭画浼狅級
  4. 澶氭簮浜ゅ弶楠岃瘉
  5. Point-in-Time 鏁版嵁绾︽潫
  6. 鍗曡偂鏁版嵁鎻愬彇
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List, Dict, Union, Literal, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import warnings
import random

from loguru import logger

warnings.filterwarnings("ignore")

# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍏ㄥ眬甯搁噺
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
DEFAULT_START_DATE = "2018-01-01"     # 榛樿鏁版嵁璧峰鏃ユ湡
DEFAULT_ADJ_TOLERANCE = 0.005         # 浜ゅ弶楠岃瘉瀹瑰樊 0.5%
DEFAULT_SAMPLE_RATIO = 0.05           # 浜ゅ弶楠岃瘉鎶芥牱姣斾緥 5%
DEFAULT_ADJ_CHANGE_THRESHOLD = 0.05   # 澶嶆潈鍥犲瓙鍙樺寲鍛婅闃堝€?5%
DEFAULT_LIMIT_TOLERANCE = 0.01        # 娑ㄨ穼鍋滃垽瀹氬宸?0.01%
DEFAULT_MAX_WORKERS = 12              # 榛樿澶氱嚎绋嬫暟
DEFAULT_FETCH_DELAY = 0.2             # 榛樿璇锋眰寤惰繜(绉?
DEFAULT_MAX_RETRIES = 3               # 榛樿鏈€澶ч噸璇曟鏁?

# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 渚濊禆瀵煎叆
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
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


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍏叡宸ュ叿鍑芥暟
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def detect_board(symbol: str) -> str:
    """鏍规嵁鑲＄エ浠ｇ爜璇嗗埆鏉垮潡锛堢粺涓€瀹炵幇锛屼緵鎵€鏈夋ā鍧楀鐢級
    
    绉戝垱鏉?688xxx): 绉戝垱鏉? 鍒涗笟鏉?30xxxx): 鍒涗笟鏉?
    鍖椾氦鎵€(8xxxxx/4xxxxx): 鍖椾氦鎵€, 鍏朵綑: 涓绘澘
    """
    import re
    s = str(symbol).zfill(6)
    if re.match(r'^688[0-9]{3}$', s):
        return '绉戝垱鏉?
    elif re.match(r'^30[0-9]{4}$', s):
        return '鍒涗笟鏉?
    elif re.match(r'^8[0-9]{5}$', s) or re.match(r'^4[0-9]{5}$', s):
        return '鍖椾氦鎵€'
    else:
        return '涓绘澘'


def detect_limit(code: str) -> float:
    """鏍规嵁鑲＄エ浠ｇ爜杩斿洖娑ㄨ穼鍋滃箙搴︼紙缁熶竴瀹炵幇锛?
    
    绉戝垱鏉?688): 20%, 鍒涗笟鏉?30): 20%, 鍖椾氦鎵€(4/8): 30%, 涓绘澘: 10%
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


def build_ts_code(symbol: str) -> str:
    """鏋勯€?ts_code锛堜笂娴?娣卞湷锛夛紙缁熶竴瀹炵幇锛?""
    sym6 = str(symbol).zfill(6)
    if sym6.startswith(("6", "5", "9", "688")):
        return f"{sym6}.SH"
    else:
        return f"{sym6}.SZ"


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鏁版嵁楠岃瘉鍣紙宸叉媶鍒嗗埌 data_validator.py锛屼繚鐣欏悜鍚庡吋瀹瑰鍏ワ級
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
from scripts.data_validator import DataValidator


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鏍稿績 DataEngine
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
class DataEngine:
    """
    鏁版嵁寮曟搸 - 缁熶竴鏁版嵁绠＄悊鍏ュ彛
    
    Attributes
    ----------
    db_path : Path
        DuckDB 鏁版嵁搴撹矾寰?
    parquet_dir : Path
        Parquet 瀛樺偍鐩綍
    validator : DataValidator
        鏁版嵁楠岃瘉鍣?
    """

    def __init__(self,
                 db_path: str = None,
                 parquet_dir: str = None):
        # 鏀寔鐜鍙橀噺閰嶇疆锛屾湭璁剧疆鏃朵娇鐢ㄧ浉瀵硅矾寰勯粯璁ゅ€?
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

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鏁版嵁搴撳垵濮嬪寲
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def _init_schema(self):
        """鍒濆鍖栬〃缁撴瀯"""
        conn = duckdb.connect(str(self.db_path))
        cur = conn.cursor()

        # 鑲＄エ鍩烘湰淇℃伅锛堝惈閫€甯傛爣璁帮級
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

        # 鎸囨暟鎴愬垎鑲″姩鎬佽〃锛堣В鍐冲垢瀛樿€呭亸宸級
        cur.execute("""
            CREATE TABLE IF NOT EXISTS index_constituents (
                index_code   VARCHAR,      -- 鎸囨暟浠ｇ爜濡?'000300.SH'
                ts_code      VARCHAR,      -- 鎴愬垎鑲′唬鐮?
                trade_date   DATE,         -- 鎴愬垎鑲＄敓鏁堟棩鏈?
                in_date      DATE,         -- 鍔犲叆鏃ユ湡
                out_date     DATE,         -- 閫€鍑烘棩鏈燂紙NULL琛ㄧず浠嶅湪锛?
                PRIMARY KEY (index_code, ts_code, trade_date)
            )
        """)

        # ST鐘舵€佸巻鍙茶〃锛堟椂闂村簭鍒楃壒寰侊級
        cur.execute("""
            CREATE TABLE IF NOT EXISTS st_status_history (
                ts_code      VARCHAR,
                trade_date   DATE,
                is_st        BOOLEAN,     -- 褰撴棩鏄惁ST
                is_new_st    BOOLEAN,     -- 褰撴棩鏄惁鏂板姞鍏T
                PRIMARY KEY (ts_code, trade_date)
            )
        """)

        # 鏃ョ嚎琛屾儏锛堝惈娑ㄨ穼鍋滄爣璁帮級
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

        # 浜ゆ槗鏃ュ巻
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_calendar (
                cal_date       DATE PRIMARY KEY,
                is_open        BOOLEAN,
                pretrade_date  DATE
            )
        """)

        # 璐㈠姟鏁版嵁锛圥oint-in-Time锛氬惈 ann_date锛?
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financial_data (
                ts_code        VARCHAR,
                ann_date       DATE,          -- 鍏憡鏃ワ紙PIT 绾︽潫鐢級
                end_date       DATE,          -- 鎶ュ憡鏈?
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
                eps            DOUBLE,        -- 鍩烘湰姣忚偂鏀剁泭锛坃bs_profit_row / _ak_indicator_row 鎻愪緵锛?
                data_source    VARCHAR,       -- 鏁版嵁鏉ユ簮鏍囪瘑锛坅kshare / baostock锛?
                PRIMARY KEY (ts_code, end_date, report_type)
            )
        """)

        # 甯傚€兼暟鎹紙姣忔棩蹇収锛?
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

        # 鏁版嵁鏇存柊鏃ュ織
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

        # 澶嶆潈鍥犲瓙鍙樺寲鏃ュ織锛堢敤浜庢娴嬪垎绾?鎷嗚偂锛?
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

        # 鏁版嵁璐ㄩ噺鎶ヨ琛?
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

        # 绱㈠紩
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_code ON daily_quotes(ts_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quotes_date ON daily_quotes(trade_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fin_ann ON financial_data(ann_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_val_date ON daily_valuation(trade_date)")

        conn.close()
        logger.info(f"DataEngine schema initialized: {self.db_path}")

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鏌ヨ鎺ュ彛
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """鎵ц SQL 鏌ヨ锛堟敮鎸佸弬鏁板寲鏌ヨ闃叉SQL娉ㄥ叆锛?""
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
        """鎵ц鍐欐搷浣滐紙鏀寔鍙傛暟鍖栨煡璇㈤槻姝QL娉ㄥ叆锛?""
        conn = duckdb.connect(str(self.db_path))
        try:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        finally:
            conn.close()

    def get_connection(self, read_only: bool = False):
        """鑾峰彇鏁版嵁搴撹繛鎺ワ紙涓婁笅鏂囩鐞嗗櫒锛岀‘淇濊繛鎺ユ纭叧闂級"""
        import contextlib
        @contextlib.contextmanager
        def _conn():
            conn = duckdb.connect(str(self.db_path), read_only=read_only)
            try:
                yield conn
            finally:
                conn.close()
        return _conn()

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 宸ュ叿鏂规硶
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    @staticmethod
    def _detect_limit(code: str) -> float:
        """鏍规嵁鑲＄エ浠ｇ爜杩斿洖娑ㄨ穼鍋滃箙搴︼紙濮旀墭鍒板叕鍏卞嚱鏁帮級"""
        return detect_limit(code)
    
    @staticmethod
    def _apply_limit_flags(df: pd.DataFrame, code: str, pct_col: str = "pct_chg") -> pd.DataFrame:
        """缁熶竴搴旂敤娑ㄨ穼鍋滄爣璁帮紙瀹瑰樊0.01%閬垮厤娴偣璇樊锛?""
        limit_pct = detect_limit(code)
        df["limit_up"] = df[pct_col] >= (limit_pct * 100 - 0.01)
        df["limit_down"] = df[pct_col] <= -(limit_pct * 100 - 0.01)
        return df
    
    @staticmethod
    def _build_ts_code(symbol: str) -> str:
        """鏋勯€?ts_code锛堝鎵樺埌鍏叡鍑芥暟锛?""
        return build_ts_code(symbol)
    
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 骞稿瓨鑰呭亸宸鐞?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_all_stocks(self, include_delisted: bool = True) -> pd.DataFrame:
        """
        鑾峰彇鍏ㄩ噺鑲＄エ鍒楄〃锛堝惈閫€甯傝偂锛?
        
        瑙ｅ喅骞稿瓨鑰呭亸宸殑鍏抽敭鍏ュ彛銆?
        """
        if not HAS_AKSHARE:
            return self._get_local_stocks()

        stocks = []

        # 1. 褰撳墠涓婂競鑲＄エ
        try:
            spot = ak.stock_zh_a_spot_em()
            current = spot[["浠ｇ爜", "鍚嶇О", "鏉垮潡", "鎬诲競鍊?, "娴侀€氬競鍊?]].copy()
            current.columns = ["symbol", "name", "industry", "total_mv", "circ_mv"]
            current["is_delisted"] = False
            current["list_date"] = pd.NaT
            current["delist_date"] = pd.NaT
            stocks.append(current)
            logger.info(f"Current stocks: {len(current)}")
        except (ValueError, KeyError, RuntimeError) as e:
            logger.warning(f"Failed to fetch current stocks: {e}")

        # 2. 閫€甯傝偂绁紙鍏抽敭锛氳В鍐冲垢瀛樿€呭亸宸級
        if include_delisted:
            try:
                delisted = ak.stock_zh_a_delist(symbol="閫€甯?)
                if not delisted.empty and "璇佸埜浠ｇ爜" in delisted.columns:
                    dl = delisted[["璇佸埜浠ｇ爜", "璇佸埜鍚嶇О"]].copy()
                    dl.columns = ["symbol", "name"]
                    dl["is_delisted"] = True
                    dl["industry"] = "閫€甯?
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

        # 鏋勯€?ts_code
        df["ts_code"] = df["symbol"].apply(build_ts_code)

        df["market"] = df["symbol"].apply(detect_board)
        return df

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鍔ㄦ€佽偂绁ㄦ睜锛堟勃娣?00鎴愬垎鑲℃椂鐐瑰榻愶級
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def sync_index_constituents(self, index_code: str = "000300.SH") -> None:
        """
        鍚屾鎸囨暟鎴愬垎鑲″巻鍙诧紙瑙ｅ喅骞稿瓨鑰呭亸宸級
        
        璁板綍姣忎釜鑲＄エ浣曟椂琚姞鍏?閫€鍑烘寚鏁帮紝纭繚鍥炴祴鏃跺彧浣跨敤
        璇ユ椂鐐圭湡瀹炲睘浜庢寚鏁版垚鍒嗙殑鑲＄エ銆?
        """
        if not HAS_AKSHARE:
            return

        with self.get_connection() as conn:
            # 鑾峰彇鍘嗗彶鎴愬垎鑲★紙AkShare鎻愪緵锛?
            try:
                if index_code == "000300.SH":
                    # 娌繁300鎴愬垎鑲?
                    df = ak.index_stock_cons_csindex(symbol="000300")
                    if df.empty:
                        return
                    df.columns = ["ts_code", "name", "in_date"]
                    df["index_code"] = index_code
                    df["trade_date"] = pd.Timestamp.now()
                    df["out_date"] = pd.NaT

                    # 鍐欏叆鏁版嵁搴擄紙浣跨敤DataFrame娉ㄥ唽涓轰复鏃惰〃鍐岻NSERT锛屽弬鏁板寲鏌ヨ闃叉SQL娉ㄥ叆锛?
                    conn.execute("DELETE FROM index_constituents WHERE index_code = ?", [index_code])
                    df["index_code"] = index_code
                    conn.register('tmp_constituents', df)
                    conn.execute("""
                        INSERT INTO index_constituents (index_code, ts_code, trade_date, in_date, out_date)
                        SELECT index_code, ts_code, trade_date, in_date, out_date
                        FROM tmp_constituents
                    """)
                    logger.info(f"Synced {len(df)} constituents for {index_code}")
            except Exception as e:
                logger.warning(f"Failed to sync index constituents: {e}")

    def get_universe_at_date(self, index_code: str, trade_date: str) -> List[str]:
        """
        鑾峰彇鎸囧畾鏃ユ湡鐨勬寚鏁版垚鍒嗚偂鍒楄〃锛堝姩鎬佽偂绁ㄦ睜锛?
        
        Returns
        -------
        List[str]: 鍦ㄨ鏃ユ湡灞炰簬鎸囨暟鎴愬垎鐨勮偂绁ㄤ唬鐮佸垪琛?
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

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # ST鐘舵€佸巻鍙诧紙鏃堕棿搴忓垪鐗瑰緛锛?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def sync_st_status_history(self) -> None:
        """
        鍚屾ST鐘舵€佸巻鍙?
        
        璁板綍姣忓彧鑲＄エ浣曟椂琚玈T/瑙ｉ櫎ST锛岀敤浜庢椂闂村簭鍒楃壒寰侊紝
        涓ョ鍦═鏃ヨ繃婊?鏈潵鐨凷T"銆?
        
        娉ㄦ剰锛歞aily_quotes 琛ㄦ病鏈?name 瀛楁锛岄渶瑕佷粠 stock_basic 琛?JOIN 鑾峰彇銆?
        """
        if not HAS_DUCKDB:
            return

        with self.get_connection() as conn:
            # 浠?stock_basic 鑾峰彇鍚?ST 鐨勮偂绁ㄥ垪琛?
            try:
                # 鍏堟壘鍒版墍鏈夊巻鍙蹭笂鏇炬槸 ST 鐨勮偂绁?
                st_stocks = conn.execute("""
                    SELECT ts_code, name FROM stock_basic 
                    WHERE name LIKE '%ST%' OR name LIKE '%*ST%'
                """).fetchdf()
                
                if st_stocks.empty:
                    return
                
                st_codes = st_stocks['ts_code'].tolist()
                
                # 浠庢棩绾挎暟鎹帹鏂?ST 鐘舵€佸彉鍖栵紙浣跨敤 register 閬垮厤澶у垪琛ㄦ嫾鎺?SQL 娉ㄥ叆椋庨櫓锛?
                # 娉ㄦ剰锛歞aily_quotes 鏃?is_st 瀛楁锛屽彧鑳界敤"ST鑲℃槸鍚﹀嚭鐜扮涓€鏉¤褰?鏉ユ爣璁版柊ST鏃ャ€?
                # is_new_st = TRUE 褰撲笖浠呭綋璇ヨ偂鍦?daily_quotes 涓殑褰撳墠璁板綍涔嬪墠娌℃湁绱ч偦鐨勪氦鏄撹褰曪紝
                # 鍗?prev_trade_date IS NULL 鎴栦笌褰撳墠鏃ユ湡闂撮殧 > 1涓氦鏄撴棩锛堣閫€甯?鍋滅墝鍐嶄笂甯傦級銆?
                # 濡傞渶绮剧‘ST鎽樺附鍘嗗彶锛屽簲鎺ュ叆 AkShare stock_zh_a_st_em 鎺ュ彛銆?
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
                        FROM daily_quotes d
                        INNER JOIN tmp_st_codes t ON d.ts_code = t.ts_code
                    ) sub
                """)
                conn.execute("DROP VIEW tmp_st_codes")
                logger.info("Synced ST status history")
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Failed to sync ST status: {e}")

    def is_st_at_date(self, ts_code: str, trade_date: str) -> bool:
        """妫€鏌ユ煇鑲＄エ鍦ㄦ煇鏃ユ湡鏄惁涓篠T
        
        娉ㄦ剰锛歴t_status_history 鍙褰?ST 鏈熼棿鐨勬暟鎹銆?
        濡傛灉璇ヨ偂鍦ㄦ煡璇㈡棩娌℃湁璁板綍锛岃鏄庝笉鏄?ST锛堟甯?宸叉憳甯斤級銆?
        """
        df = self.query("""
            SELECT is_st FROM st_status_history
            WHERE ts_code = ?
            AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
        """, (ts_code, trade_date))
        # 濡傛灉娌℃湁璁板綍锛岃鏄庤鑲′粠鏈 ST 鎴栧凡鎽樺附锛岃繑鍥?False
        return not df.empty and bool(df.iloc[0, 0])

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 璐㈠姟鏁版嵁鎶撳彇锛圥IT 绾︽潫锛?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def fetch_financial_data(self,
                             ts_code: str = None,
                             start_year: int = None,
                             end_year: int = None,
                             max_workers: int = 4) -> Dict:
        """
        鎶撳彇璐㈠姟鏁版嵁锛堣储鎶?+ 浼板€硷級锛屾敮鎸佸绾跨▼銆?

        鏁版嵁瀵归綈瑙勫垯锛?
        - 浣跨敤 ann_date锛堝叕鍛婃棩锛変綔涓?PIT 绾︽潫鐐?
        - 姣忔湡璐㈡姤瀵瑰簲 end_date锛堟姤鍛婃湡锛夛紝浣嗗叕鍛婂悗鎵嶈兘浣跨敤

        AkShare 瀛楁锛氳惀涓氭€绘敹鍏ャ€佸噣鍒╂鼎銆佽祫浜ф€昏銆佽礋鍊烘€昏銆?
        鍩烘湰姣忚偂鏀剁泭銆佸噣璧勪骇鏀剁泭鐜囥€佹瘡鑲＄幇閲戞祦绛?
        """
        stats = {"success": 0, "failed": 0, "records": 0, "errors": []}
        if not HAS_AKSHARE:
            logger.warning("AkShare not available, financial fetch skipped")
            return stats

        end_year = end_year or datetime.now().year
        start_year = start_year or (end_year - 5)

        # 鍗曞彧鑲＄エ
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

        # 鍏ㄩ噺
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
        """鎶撳彇鍗曞彧鑲＄エ鐨勮储鍔℃暟鎹紙骞存姤 + 瀛ｆ姤锛?""
        records = []
        sym6 = str(symbol).zfill(6)
        bs_code = (f"sh.{sym6}" if sym6.startswith("6") or sym6.startswith("5")
                   else f"sz.{sym6}")

        # 骞存姤锛?涓储骞达級
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

        # 浼板€煎揩鐓э紙鐢?AkShare锛?
        try:
            df_val = ak.stock_a_indicator_lg(secu=symbol)
            if not df_val.empty and "浠ｇ爜" in df_val.columns:
                df_val = df_val.rename(columns={"浠ｇ爜": "symbol"})
                for _, row in df_val.iterrows():
                    records.append(self._ak_indicator_row(row, symbol))
        except Exception as e:
            logger.debug(f"AkShare indicator fetch failed for {symbol}: {e}")

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def _bs_profit_row(self, row: Dict, symbol: str) -> Dict:
        """Baostock 鍒╂鼎琛ㄨ鏍囧噯鍖?""
        sym6 = str(symbol).zfill(6)
        ann = row.get("profit_statements_pub_date", "")
        end = row.get("profit_statements_report_date", "")
        return {
            "ts_code": f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ",
            "ann_date": pd.to_datetime(ann, errors="coerce") if ann else pd.NaT,
            "end_date": pd.to_datetime(end, errors="coerce") if end else pd.NaT,
            "report_type": "骞存姤",
            "revenue": float(row.get("total_operating_revenue", 0) or 0),
            "net_profit": float(row.get("parent_net_profit", 0) or 0),
            "total_assets": float(row.get("total_assets", 0) or 0),
            "total_equity": float(row.get("total_shareholder_equity", 0) or 0),
            "roe": float(row.get("avg_roe", 0) or 0),
            "roa": float(row.get("roe", 0) or 0),
            "eps": float(row.get("basic_eps", 0) or 0),
            "gross_margin": float(row.get("gross_profit_margin", 0) or 0),  # 涓嶥DL瀛楁鍚嶅榻?
            "data_source": "baostock",
        }

    def _ak_indicator_row(self, row: pd.Series, symbol: str) -> Dict:
        """AkShare 鎸囨爣琛屾爣鍑嗗寲"""
        sym6 = str(symbol).zfill(6)
        return {
            "ts_code": f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ",
            "ann_date": pd.Timestamp.today(),
            "end_date": pd.NaT,
            "report_type": "鎸囨爣",
            "revenue": float(row.get("钀ヤ笟鎬绘敹鍏?, 0) or 0),
            "net_profit": float(row.get("鍑€鍒╂鼎", 0) or 0),
            "total_assets": 0,
            "total_equity": 0,
            "roe": float(row.get("鍑€璧勪骇鏀剁泭鐜?%)", 0) or 0),
            "roa": float(row.get("璧勪骇鎶ラ叕鐜?%)", 0) or 0),
            "eps": float(row.get("鍩烘湰姣忚偂鏀剁泭", 0) or 0),
            "gross_margin": float(row.get("閿€鍞瘺鍒╃巼(%)", 0) or 0),  # 涓嶥DL瀛楁鍚嶅榻?
            "pe_ttm": float(row.get("甯傜泩鐜?TTM)", 0) or 0),
            "pb": float(row.get("甯傚噣鐜?, 0) or 0),
            "data_source": "akshare",
        }

    def _save_financial_data(self, df: pd.DataFrame):
        """淇濆瓨璐㈠姟鏁版嵁鍒版暟鎹簱"""
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            # 璐㈠姟鏁版嵁鐢?REPLACE锛堝彲閲嶅鏇存柊锛?
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
                # 瀛楁涓嶅叏鏃剁敤鍘熷鍒楋紙鍒楀悕鏉ヨ嚜 DataFrame 鍒楀垪琛紝闈炵敤鎴疯緭鍏ワ紝瀹夊叏锛?
                cols = [c for c in df.columns if c in
                        ["ts_code", "ann_date", "end_date", "report_type",
                         "revenue", "net_profit", "roe", "roa", "eps",
                         "total_assets", "total_equity", "gross_margin", "data_source"]]
                if cols:
                    col_str = ','.join(cols)
                    conn.execute(f"INSERT OR IGNORE INTO financial_data ({col_str}) SELECT {col_str} FROM df")

    def _get_local_stocks(self) -> pd.DataFrame:
        """浠庢湰鍦版暟鎹簱鑾峰彇鑲＄エ鍒楄〃"""
        return self.query("SELECT * FROM stock_basic")

    def sync_stock_list(self, include_delisted: bool = True):
        """
        鍚屾鑲＄エ鍒楄〃鍒版湰鍦版暟鎹簱锛堝惈 list_date/delist_date锛?
        
        浣跨敤 Baostock 琛ュ厖涓婂競/閫€甯傛棩鏈熶俊鎭紝瑙ｅ喅骞稿瓨鑰呭亸宸腑鐨?
        鏃ユ湡缂哄け闂銆?
        """
        df = self.get_all_stocks(include_delisted)
        if df.empty:
            return

        conn = duckdb.connect(str(self.db_path))

        # Baostock 琛ュ厖 list_date / delist_date
        if HAS_BAOSTOCK:
            logger.info("Enriching list/delist dates from Baostock...")
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
            except (ValueError, KeyError, RuntimeError) as e:
                logger.warning(f"Baostock list_date enrichment failed: {e}")

        # 鍐欏叆鍩烘湰淇℃伅
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
        logger.info(f"Synced {len(df)} stocks to database "
              f"(list_date filled: {df['list_date'].notna().sum()})")

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鏃ョ嚎鏁版嵁涓嬭浇
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def fetch_single(self,
                     symbol: str,
                     start_date: str,
                     end_date: str,
                     adjust: Literal["qfq", ""] = "qfq") -> pd.DataFrame:
        """
        
        Parameters
        ----------
        symbol : str
            6浣嶈偂绁ㄤ唬鐮?
        start_date : str
            寮€濮嬫棩鏈燂紝鏍煎紡 YYYY-MM-DD
        end_date : str
            缁撴潫鏃ユ湡锛屾牸寮?YYYY-MM-DD
        adjust : 'qfq' | ''
            澶嶆潈鏂瑰紡
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

            # 鍒楀悕鏍囧噯鍖?
            col_map = {
                "鏃ユ湡": "trade_date", "寮€鐩?: "open", "鏀剁洏": "close",
                "鏈€楂?: "high", "鏈€浣?: "low", "鎴愪氦閲?: "volume",
                "鎴愪氦棰?: "amount", "娑ㄨ穼骞?: "pct_chg", "鎹㈡墜鐜?: "turnover"
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
            
            # 娑ㄨ穼鍋滃垽瀹氾紙浣跨敤缁熶竴鏂规硶锛?
            df = self._apply_limit_flags(df, symbol)
            df["data_source"] = "akshare"

            # 璁＄畻澶嶆潈鍥犲瓙
            if adjust:
                try:
                    df_raw = ak.stock_zh_a_hist(
                        symbol=symbol, period="daily",
                        start_date=start_str, end_date=end_str, adjust=""
                    )
                    if not df_raw.empty:
                        df_raw = df_raw.rename(columns={"鏃ユ湡": "trade_date", "鏀剁洏": "close_raw"})
                        df_raw["trade_date"] = pd.to_datetime(df_raw["trade_date"])
                        df = df.merge(df_raw[["trade_date", "close_raw"]], on="trade_date", how="left")
                        df["adj_factor"] = df["close"] / df["close_raw"].replace(0, np.nan)
                        df.drop(columns=["close_raw"], inplace=True, errors="ignore")
                except Exception:
                    df["adj_factor"] = 1.0
            else:
                df["adj_factor"] = 1.0

            # 楠岃瘉
            val = self.validator.validate(df)
            if not val["ok"]:
                logger.warning(f"Data validation issues for {symbol}: {val['issues']}")

            cols = ["ts_code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "volume", "amount", "pct_chg", "turnover",
                    "adj_factor", "is_suspend", "limit_up", "limit_down", "data_source"]
            return df[[c for c in cols if c in df.columns]]

        except (ValueError, KeyError, RuntimeError, ConnectionError) as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            return pd.DataFrame()

    def save_quotes(self, df: pd.DataFrame, mode: str = "append"):
        """
        淇濆瓨琛屾儏鏁版嵁鍒?DuckDB锛圲PSERT 閫昏緫锛岄槻姝㈤噸澶嶆暟鎹級
        
        浣跨敤 DuckDB 鐨?INSERT OR REPLACE 瀹炵幇鍘熷瓙鎬?UPSERT锛?
        閬垮厤 DELETE+INSERT 浜х敓鐨勫苟鍙戣鐩栭棶棰樸€?
        """
        if df.empty or not HAS_DUCKDB:
            return
        with self.get_connection() as conn:
            if mode == "overwrite":
                codes = df["ts_code"].unique().tolist()
                dates_min = df["trade_date"].min()
                dates_max = df["trade_date"].max()
                if codes and not (pd.isna(dates_min) or pd.isna(dates_max)):
                    # 浣跨敤 register 閬垮厤澶у垪琛ㄦ嫾鎺?SQL 娉ㄥ叆
                    codes_df = pd.DataFrame({"ts_code": codes})
                    conn.register('tmp_codes', codes_df)
                    conn.execute("""
                        DELETE FROM daily_quotes
                        WHERE ts_code IN (SELECT ts_code FROM tmp_codes)
                        AND trade_date BETWEEN ? AND ?
                    """, (dates_min, dates_max))
                    conn.execute("DROP VIEW tmp_codes")
            try:
                # 鍘熷瓙鎬?UPSERT锛堣鐩栧凡瀛樺湪鐨?ts_code+trade_date 缁勫悎锛?
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

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 澧為噺鏇存柊锛堝绾跨▼ + 閲嶈瘯 + 鏂偣缁紶锛?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_latest_date(self, ts_code: str = None) -> Optional[str]:
        """鑾峰彇鏈湴鏈€鏂版棩鏈燂紙鍚柇鐐圭画浼犳娴嬶細楠岃瘉鏈€鏂版棩鏈熸暟鎹槸鍚﹀畬鏁达級"""
        if not HAS_DUCKDB:
            return None

        # 鍏堝皾璇曡幏鍙栨渶鏂版棩鏈?
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

        # 鏂偣缁紶鍔犲浐锛氭鏌ユ渶鏂版棩鏈熸暟鎹槸鍚﹀畬鏁达紙close>0 涓?volume>=0锛?
        if ts_code:
            check = self.query("""
                SELECT COUNT(*) as cnt FROM daily_quotes
                WHERE ts_code = ? AND trade_date = ?
                AND close > 0 AND volume >= 0
            """, (ts_code, latest))
            if check.empty or check.iloc[0, 0] == 0:
                # 鏁版嵁鎹熷潖/涓嶅畬鏁达紝鍥為€€鍒板墠涓€澶?
                prev = (pd.Timestamp(latest) - timedelta(days=1)).strftime("%Y-%m-%d")
                prev_check = self.query("""
                    SELECT COUNT(*) as cnt FROM daily_quotes
                    WHERE ts_code = ? AND trade_date = ?
                    AND close > 0 AND volume >= 0
                """, (ts_code, prev))
                if prev_check.empty or prev_check.iloc[0, 0] == 0:
                    return None  # 鏁版嵁褰诲簳鎹熷潖
                return prev
        return latest

    def _fetch_single_with_retry(self,
                                 symbol: str,
                                 start_date: str,
                                 end_date: str,
                                 adjust: Literal["qfq", ""] = "qfq",
                                 max_retries: int = DEFAULT_MAX_RETRIES) -> Tuple[pd.DataFrame, str]:
        """
        甯︽寚鏁伴€€閬块噸璇曠殑鎶撳彇锛圓kShare 鈫?Baostock 鈫?澶辫触锛?
        
        Returns
        -------
        (df, source): df鏁版嵁, source='akshare'|'baostock'|''
        """
        for attempt in range(max_retries):
            # AkShare
            if HAS_AKSHARE:
                df = self._fetch_akshare(symbol, start_date, end_date, adjust)
                if not df.empty:
                    return df, "akshare"

            # Baostock 澶囨彺
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
                       adjust: Literal["qfq", ""] = "qfq") -> pd.DataFrame:
        """AkShare 鎶撳彇锛堝唴閮ㄧ敤锛?""
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
                "鏃ユ湡": "trade_date", "寮€鐩?: "open", "鏀剁洏": "close",
                "鏈€楂?: "high", "鏈€浣?: "low", "鎴愪氦閲?: "volume",
                "鎴愪氦棰?: "amount", "娑ㄨ穼骞?: "pct_chg", "鎹㈡墜鐜?: "turnover"
            })
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            sym6 = str(symbol).zfill(6)
            df["ts_code"] = f"{sym6}.SH" if sym6.startswith(("6", "5", "9", "688")) else f"{sym6}.SZ"
            df["pre_close"] = df["close"].shift(1)
            df["is_suspend"] = df["volume"] == 0
            
            # 娑ㄨ穼鍋滃垽瀹?
            df = self._apply_limit_flags(df, symbol)
            df["data_source"] = "akshare"

            # 澶嶆潈鍥犲瓙
            if adjust:
                try:
                    df_raw = ak.stock_zh_a_hist(
                        symbol=symbol, period="daily",
                        start_date=start_str, end_date=end_str, adjust=""
                    )
                    if not df_raw.empty:
                        df_raw = df_raw.rename(columns={"鏃ユ湡": "td2", "鏀剁洏": "close_raw"})
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
        """Baostock 鎶撳彇锛堝鎻寸敤锛宭ogin/logout 鍦ㄦ柟娉曞唴閮ㄧ鐞嗭級"""
        if not HAS_BAOSTOCK:
            return pd.DataFrame()
        try:
            adjflag_map = {"qfq": "2", "": "3"}
            adjflag = adjflag_map.get(adjust, "2")

            # 鏍囧噯鍖?baostock 浠ｇ爜
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
            
            # 娑ㄨ穼鍋滃垽瀹?
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
        """鎵归噺鑾峰彇澶氬彧鑲＄エ鐨勬渶鏂版棩鏈燂紙鍗曟潯SQL鏇夸唬N娆℃煡璇紝鍑忓皯DB I/O锛?
        
        Returns
        -------
        Dict[str, str]: {ts_code: latest_date_str} 瀛楀吀
        """
        if not ts_codes or not HAS_DUCKDB:
            return {}
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        with self.get_connection() as conn:
            conn.register('tmp_batch_latest', codes_df)
            df = conn.execute("""
                SELECT d.ts_code, MAX(d.trade_date) as latest_date
                FROM daily_quotes d
                INNER JOIN tmp_batch_latest t ON d.ts_code = t.ts_code
                GROUP BY d.ts_code
            """).fetchdf()
            conn.execute("DROP VIEW tmp_batch_latest")
        if df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            val = row["latest_date"]
            if val and not pd.isna(val):
                result[row["ts_code"]] = pd.Timestamp(val).strftime("%Y-%m-%d")
        return result

    def update_daily_data(self,
                          symbols: List[str] = None,
                          adjust: Literal["qfq", ""] = "qfq",
                          max_workers: int = 12,
                          delay: float = 0.2,
                          check_dividend: bool = True,
                          cross_validate: bool = True) -> Dict:
        """
        澶氱嚎绋嬪閲忔洿鏂版棩绾挎暟鎹?鈥?鐢熶骇绾у寮虹増 v2
        
        鏀硅繘鐐癸紙Issue #1锛夛細
        1. max_workers 榛樿 12锛堝厖鍒嗗埄鐢ㄥ鏍革級
        2. 鎵归噺 latest_date 缂撳瓨锛堝崟鏉QL鏇夸唬N娆℃煡璇級
        3. 闄愰€熺Щ鍏ョ嚎绋嬪唴閮紙涓荤嚎绋嬩笉鍐?sleep锛?
        4. 鎵归噺鍏ュ簱绱Н鍐欏叆锛堝噺灏慏B杩炴帴寮€閿€锛?
        5. 澶嶆潈鍥犲瓙鐩戞祴锛坉ividend_check锛?
        6. 澶氭簮浜ゅ弶楠岃瘉锛圓kShare vs Baostock锛?
        7. 鏂偣缁紶鍔犲浐 + DataValidator
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

        end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date > end_date:
            logger.info("Data is up-to-date.")
            return stats

        # 鈹€鈹€ Issue #2: 鎵归噺缂撳瓨 latest_date锛堝崟鏉QL鏇夸唬5000+娆℃煡璇級鈹€鈹€
        ts_codes = []
        for sym in symbols:
            sym6 = str(sym).zfill(6)
            if sym6.startswith(("6", "5", "9", "688")):
                ts_codes.append(f"{sym6}.SH")
            else:
                ts_codes.append(f"{sym6}.SZ")

        logger.info(f"Pre-caching latest dates for {len(ts_codes)} stocks...")
        latest_cache = self._batch_get_latest_dates(ts_codes)
        logger.info(f"Cache hit: {len(latest_cache)}/{len(ts_codes)} stocks have local data")

        # 棰勮繃婊わ細鎺掗櫎宸叉槸鏈€鏂颁笖鏃犲鏉冨洜瀛愮殑鑲＄エ
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

        # 鈹€鈹€ 绾跨▼瀹夊叏鐨勬壒閲忕疮绉紦鍐插尯 鈹€鈹€
        import threading
        batch_buffer = []
        batch_lock = threading.Lock()
        BATCH_FLUSH_SIZE = 50  # 姣?0鍙偂绁ㄦ壒閲忓叆搴撲竴娆?

        def _download_one(sym: str) -> Tuple[str, pd.DataFrame, str, str]:
            """鍗曠嚎绋嬩笅杞藉櫒锛堝唴閮ㄨ嚜甯﹂檺閫燂級"""
            try:
                symbol_clean = str(sym).zfill(6)
                ts_code = (f"{symbol_clean}.SH" if symbol_clean.startswith(("6", "5", "9", "688"))
                           else f"{symbol_clean}.SZ")
                # 浣跨敤缂撳瓨鑰岄潪瀹炴椂鏌ヨ
                sym_latest = latest_cache.get(ts_code, "")
                if sym_latest and sym_latest >= start_date:
                    return sym, pd.DataFrame(), "", ""

                actual_start = start_date
                if actual_start > end_date:
                    return sym, pd.DataFrame(), "", ""

                # 绾跨▼鍐呴檺閫燂細閬垮厤骞跺彂璇锋眰杩囩寷
                time.sleep(delay * random.uniform(0.5, 1.5))

                df, source = self._fetch_single_with_retry(sym, actual_start, end_date, adjust)
                return sym, df, source, ""
            except Exception as e:
                return sym, pd.DataFrame(), "", str(e)

        # 澶氱嚎绋嬩笅杞?+ 鎵归噺鍏ュ簱
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_download_one, s): s for s in active_symbols}
            done = 0
            for future in as_completed(futures):
                sym, df, source, err = future.result()
                done += 1
                if not df.empty:
                    # 澶嶆潈鍥犲瓙鐩戞祴
                    if check_dividend:
                        div_alerts = self._check_adj_factor_change(sym, df)
                        if div_alerts:
                            stats["dividend_detected"] += len(div_alerts)

                    # DataValidator 楠岃瘉
                    val = self.validator.validate(df)
                    if not val["ok"]:
                        for issue in val["issues"]:
                            stats["errors"].append(f"{sym}: {issue}")

                    # 绱Н鍒版壒閲忕紦鍐插尯
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

        # 鍒峰嚭缂撳啿鍖哄墿浣欐暟鎹?
        if batch_buffer:
            self.save_quotes(pd.concat(batch_buffer, ignore_index=True), mode="append")
            batch_buffer.clear()

        # 澶氭簮浜ゅ弶楠岃瘉锛堟娊鏍凤級
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
        澶嶆潈鍥犲瓙鍙樺寲妫€娴?
        
        濡傛灉 adj_factor 鍙戠敓闈炶繛缁烦鍙橈紙>5%锛夛紝璇存槑鍙戠敓浜嗗垎绾?鎷嗚偂锛?
        闇€瑕佽褰曟棩蹇楋紝涓ラ噸鏃跺彲瑙﹀彂鍏ㄩ噺閲嶆媺銆?
        """
        if "adj_factor" not in new_data.columns or new_data.empty:
            return []

        alerts = []
        ts_code = new_data["ts_code"].iloc[0]

        # 鑾峰彇鏈湴鏈€杩戠殑澶嶆潈鍥犲瓙
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
            if change_ratio > DEFAULT_ADJ_CHANGE_THRESHOLD:  # 5% 浠ヤ笂鍙樺寲
                alert = {
                    "ts_code": ts_code,
                    "trade_date": new_data["trade_date"].iloc[0],
                    "old_adj": old_adj,
                    "new_adj": new_adj,
                    "change_ratio": change_ratio,
                }
                alerts.append(alert)

                # 璁板綍鍒版棩蹇楄〃锛堝弬鏁板寲鏌ヨ闃叉SQL娉ㄥ叆锛?
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
        AkShare vs Baostock 浜ゅ弶楠岃瘉
        
        鎶芥牱妫€鏌ュ綋鏃ユ定璺屽箙锛屽亸宸?> 0.5% 鎶ヨ
        """
        if not HAS_BAOSTOCK:
            return

        try:
            bs.login()
            for sym in symbols[:20]:  # 鎶芥牱20鍙?
                sym6 = str(sym).zfill(6)
                bs_code = f"sh.{sym6}" if sym6.startswith("6") else f"sz.{sym6}"

                # AkShare 鏁版嵁锛堝弬鏁板寲鏌ヨ闃叉SQL娉ㄥ叆锛?
                ak_data = self.query("""
                    SELECT pct_chg FROM daily_quotes
                    WHERE ts_code LIKE ?
                    AND trade_date = ?
                    LIMIT 1
                """, (f"{sym6}%", trade_date))
                if ak_data.empty:
                    continue

                # Baostock 鏁版嵁
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

                    if diff > 0.5:  # 鍋忓樊瓒呰繃 0.5%
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

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # Issue #3: 鍋滅墝鏃ユ湡濉厖 & 閫€甯傝繃婊?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def _fill_suspend_dates(self,
                            df: pd.DataFrame,
                            start_date: str,
                            end_date: str) -> pd.DataFrame:
        """濉厖鍋滅墝鏃ョ己澶辨棩鏈燂紙Forward Fill锛夛紝瑙ｅ喅鍧囩嚎鏂闂
        
        瀵逛簬鏁版嵁搴撲腑缂哄け鐨勫仠鐗屾棩鏈燂紙volume=0 鎴栨棤鏁版嵁琛岋級锛?
        浣跨敤鍓嶄竴鏃ユ敹鐩樹环濉厖 OHLC锛岀‘淇濇椂闂村簭鍒楄繛缁€с€?
        
        Parameters
        ----------
        df : pd.DataFrame
            宸叉湁鐨勬棩绾挎暟鎹?
        start_date : str
            鏈熸湜鐨勮捣濮嬫棩鏈?YYYY-MM-DD
        end_date : str
            鏈熸湜鐨勭粨鏉熸棩鏈?YYYY-MM-DD
            
        Returns
        -------
        pd.DataFrame: 濉厖鍚庣殑瀹屾暣鏃堕棿搴忓垪
        """
        if df.empty:
            return df

        ts_code = df["ts_code"].iloc[0]
        
        # 鑾峰彇浜ゆ槗鏃ュ巻涓殑瀹屾暣浜ゆ槗鏃ュ簭鍒?
        trade_dates = self.get_trade_dates(start_date, end_date)
        if not trade_dates:
            return df

        existing_dates = set(
            pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d").tolist()
        )
        missing_dates = [d for d in trade_dates if d not in existing_dates]
        
        if not missing_dates:
            return df
        
        # 鏋勯€犵己澶辨棩鏈熺殑濉厖琛岋紙浣跨敤鍓嶅悜濉厖锛?
        fill_rows = []
        last_valid = df.iloc[-1]  # 榛樿浣跨敤鏈€鍚庝竴琛?
        for mdate in missing_dates:
            # 鎵惧埌璇ョ己澶辨棩鏈熶箣鍓嶇殑鏈€杩戞湁鏁堟暟鎹?
            mdt = pd.Timestamp(mdate)
            valid_before = df[pd.to_datetime(df["trade_date"]) < mdt]
            if not valid_before.empty:
                last_valid = valid_before.iloc[-1]
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
        """妫€鏌ヨ偂绁ㄦ槸鍚﹀凡閫€甯?
        
        Parameters
        ----------
        ts_code : str
            鑲＄エ浠ｇ爜濡?'000001.SZ'
            
        Returns
        -------
        bool: True 琛ㄧず宸查€€甯?
        """
        result = self.query(
            "SELECT is_delisted FROM stock_basic WHERE ts_code = ?",
            (ts_code,)
        )
        if result.empty:
            return False
        return bool(result.iloc[0, 0])

    def get_delist_date(self, ts_code: str) -> Optional[str]:
        """鑾峰彇閫€甯傛棩鏈?
        
        Returns
        -------
        str or None: 閫€甯傛棩鏈?YYYY-MM-DD锛屾湭閫€甯傝繑鍥?None
        """
        result = self.query(
            "SELECT delist_date FROM stock_basic WHERE ts_code = ? AND is_delisted = TRUE",
            (ts_code,)
        )
        if result.empty or pd.isna(result.iloc[0, 0]):
            return None
        return pd.Timestamp(result.iloc[0, 0]).strftime("%Y-%m-%d")

    def filter_delisted_stocks(self, symbols: List[str], as_of_date: str) -> List[str]:
        """杩囨护鍦ㄦ寚瀹氭棩鏈熷凡閫€甯傜殑鑲＄エ
        
        Parameters
        ----------
        symbols : List[str]
            鑲＄エ浠ｇ爜鍒楄〃锛?浣嶆暟瀛楋級
        as_of_date : str
            鎴鏃ユ湡 YYYY-MM-DD
            
        Returns
        -------
        List[str]: 浠嶅彲浜ゆ槗鐨勮偂绁ㄤ唬鐮佸垪琛?
        """
        if not symbols or not HAS_DUCKDB:
            return symbols
        
        ts_codes = []
        for sym in symbols:
            sym6 = str(sym).zfill(6)
            if sym6.startswith(("6", "5", "9", "688")):
                ts_codes.append(f"{sym6}.SH")
            else:
                ts_codes.append(f"{sym6}.SZ")
        
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
        
        # 浣跨敤 ts_code 鍒楁槧灏勫洖鍘熷 symbol
        result = []
        for _, row in df.iterrows():
            tc = row["ts_code"]
            if "symbol" in df.columns and not pd.isna(row.get("symbol")):
                result.append(str(row["symbol"]))
            elif tc in sym_map:
                result.append(sym_map[tc])
        
        return result

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鍗曡偂鏁版嵁鎻愬彇
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_security_data(self,
                          code: str,
                          start_date: str = None,
                          end_date: str = None,
                          adjust: Literal["qfq", ""] = "qfq",
                          fill_suspend: bool = True) -> pd.DataFrame:
        """
        鑾峰彇鍗曞彧鑲＄エ鏁版嵁
        
        鍏堟煡鏈湴锛屾病鏈夊垯浠庣綉缁滄媺鍙栥€?
        
        Parameters
        ----------
        code : str
            鑲＄エ浠ｇ爜锛?浣嶆暟瀛楁垨甯﹀悗缂€锛?
        start_date : str, optional
            寮€濮嬫棩鏈?
        end_date : str, optional
            缁撴潫鏃ユ湡
        adjust : 'qfq' | ''
            澶嶆潈鏂瑰紡
        fill_suspend : bool
            鏄惁濉厖鍋滅墝鏃ワ紙榛樿True锛?
        """
        # 鏍囧噯鍖栦唬鐮?
        if "." in code:
            ts_code, symbol = code, code.split(".")[0]
        else:
            symbol = str(code).zfill(6)
            ts_code = f"{symbol}.SH" if symbol.startswith(("6", "5", "9")) else f"{symbol}.SZ"

        start_date = start_date or DEFAULT_START_DATE
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")

        # Issue #3: 妫€鏌ユ槸鍚﹀凡閫€甯傦紙閫€甯傚悗鍋滄鎶撳彇锛?
        delist_date = self.get_delist_date(ts_code)
        if delist_date and delist_date < start_date:
            logger.debug(f"{ts_code} delisted on {delist_date}, skip fetch")
            # 浠嶇劧杩斿洖鏈湴宸叉湁鏁版嵁
            local = self.query("""
                SELECT * FROM daily_quotes
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
            """, (ts_code, start_date, end_date))
            return local

        # 鏌ユ湰鍦帮紙鍙傛暟鍖栨煡璇㈤槻姝QL娉ㄥ叆锛?
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
                # 閫€甯傚悗涓嶆姄鍙?
                if delist_date and new_start > delist_date:
                    new_start = delist_date
                if new_start <= end_date:
                    new_df = self.fetch_single(symbol, new_start, today, adjust)
                    if not new_df.empty:
                        self.save_quotes(new_df, mode="append")
                        local = pd.concat([local, new_df], ignore_index=True)
            result = local.sort_values("trade_date").reset_index(drop=True)
        else:
            # 鏈湴娌℃湁锛屼粠缃戠粶鎷夛紙甯﹂噸璇曪級
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

        # Issue #3: 鍋滅墝鏃ュ～鍏?
        if fill_suspend and not result.empty:
            result = self._fill_suspend_dates(result, start_date, end_date)

        return result

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 璐㈠姟鏁版嵁锛圥IT 绾︽潫锛?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_financial_data_pit(self,
                               ts_code: str,
                               trade_date: str,
                               lookback_days: int = 90) -> pd.DataFrame:
        """
        Point-in-Time 璐㈠姟鏁版嵁鑾峰彇
        
        鍙繑鍥?ann_date <= trade_date 鐨勬暟鎹€?
        
        杩欐槸闃叉鏈潵鍑芥暟鐨勫叧閿柟娉曘€?
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
        鑾峰彇 T 鏃ユ渶鏂扮殑宸插叕鍛婅储鍔℃暟鎹?
        
        鐢ㄤ簬鍥犲瓙璁＄畻鏃剁殑 PIT 绾︽潫銆?
        """
        df = self.get_financial_data_pit(ts_code, trade_date)
        if df.empty:
            return None
        latest = df.iloc[0]
        return latest.to_dict()

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 甯傚€兼暟鎹紙PIT绾︽潫锛氬彧鐢═-1鏃ユ敹鐩樺競鍊硷級
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_market_cap(self, trade_date: str, use_pit: bool = True) -> pd.Series:
        """
        鑾峰彇鏌愭棩鍏ㄥ競鍦哄競鍊?
        
        Parameters
        ----------
        trade_date : str
            鐩爣鏃ユ湡
        use_pit : bool
            True=浣跨敤T-1鏃ュ競鍊硷紙闃叉鏈潵鍑芥暟锛夛紝False=浣跨敤褰撴棩甯傚€?
        """
        if use_pit:
            # PIT绾︽潫锛氬彧鐢═-1鏃ユ敹鐩樺競鍊?
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
        """鑾峰彇鎸囧畾鏃ユ湡鐨勫墠涓€涓氦鏄撴棩"""
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

    # 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
    # DuckDB Window Function 璁＄畻锛堜笅鎺ㄨ嚦SQL灞傦級
    # 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺?
    def compute_rolling_returns(self,
                                  ts_codes: List[str],
                                  start_date: str,
                                  end_date: str,
                                  windows: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """
        DuckDB Window Function 璁＄畻婊氬姩鏀剁泭鐜?
        
        鐩存帴鍦⊿QL灞傝绠楋紝閬垮厤灏嗘捣閲忔暟鎹浇鍏andas銆?
        杩斿洖锛歵s_code, trade_date, window_5, window_10, window_20, window_60
        """
        if not ts_codes:
            return pd.DataFrame()

        # 浣跨敤 register 閬垮厤澶у垪琛ㄦ嫾鎺?SQL 娉ㄥ叆椋庨櫓
        codes_df = pd.DataFrame({"ts_code": ts_codes})
        max_window = max(windows)

        # 鏋勫缓绐楀彛鍑芥暟SQL锛堜娇鐢ㄥ鍒╀箻绉绠楁粴鍔ㄦ敹鐩婄巼锛?
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
        DuckDB Window Function 璁＄畻绉诲姩骞冲潎绾?
        
        鐩存帴鍦⊿QL灞傝绠桵A锛岄伩鍏嶅唴瀛樼垎鐐搞€?
        """
        if not ts_codes:
            return pd.DataFrame()

        # 浣跨敤 register 閬垮厤澶у垪琛ㄦ嫾鎺?SQL 娉ㄥ叆椋庨櫓
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
        DuckDB Window Function 璁＄畻婊氬姩娉㈠姩鐜?
        """
        if not ts_codes:
            return pd.DataFrame()

        # 浣跨敤 register 閬垮厤澶у垪琛ㄦ嫾鎺?SQL 娉ㄥ叆椋庨櫓
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
                FROM daily_quotes d
                INNER JOIN tmp_codes_vol t ON d.ts_code = t.ts_code
                AND d.trade_date BETWEEN ? AND ?
                AND d.volume > 0
                ORDER BY d.ts_code, d.trade_date
            """
            df = conn.execute(sql, (start_date, end_date)).fetchdf()
            conn.execute("DROP VIEW tmp_codes_vol")
            return df

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 浜ゆ槗鏃ュ巻
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def sync_calendar(self, start_year: int = 2018, end_year: int = None):
        """鍚屾浜ゆ槗鏃ュ巻锛堢綉缁滀笉鍙敤鏃惰烦杩囷紝浣跨敤鏈湴宸叉湁鏁版嵁锛?""
        # 妫€鏌ユ湰鍦版槸鍚﹀凡鏈夋棩鍘嗘暟鎹紝鏈夊垯璺宠繃
        existing = self.query("SELECT COUNT(*) FROM trade_calendar WHERE is_open=TRUE")
        if existing.iloc[0, 0] > 100:
            logger.info(f"Calendar exists ({existing.iloc[0, 0]} days), skip sync")
            return

        if not HAS_AKSHARE:
            logger.info("AkShare unavailable, using existing calendar")
            return

        logger.info("Fetching calendar from AkShare...")
        end_year = end_year or datetime.now().year
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
        """鑾峰彇浜ゆ槗鏃ュ垪琛?""
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

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # Parquet 瀵煎嚭
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鍏佽瀵煎嚭鐨勮〃鐧藉悕鍗曪紙闃叉璺緞閬嶅巻鍜孲QL娉ㄥ叆锛?
    _EXPORT_TABLE_WHITELIST = frozenset({
        'daily_quotes', 'stock_basic', 'financial_data', 'index_constituents',
        'st_status_history', 'trade_calendar', 'daily_valuation',
        'update_log', 'adj_factor_log', 'data_quality_alert',
    })

    def export_parquet(self, table: str = "daily_quotes") -> Path:
        """瀵煎嚭琛ㄥ埌 Parquet 鏍煎紡锛堜粎鍏佽鐧藉悕鍗曡〃鍚嶏級"""
        if table not in self._EXPORT_TABLE_WHITELIST:
            raise ValueError(f"琛?'{table}' 涓嶅湪瀵煎嚭鐧藉悕鍗曚腑锛屽厑璁哥殑琛? {sorted(self._EXPORT_TABLE_WHITELIST)}")
        output = self.parquet_dir / f"{table}.parquet"
        # 璺緞瀹夊叏妫€鏌ワ細纭繚杈撳嚭鍦?parquet_dir 鍐?
        if not str(output.resolve()).startswith(str(self.parquet_dir.resolve())):
            raise ValueError(f"瀵煎嚭璺緞 '{output}' 涓嶅湪鍏佽鐨勭洰褰?'{self.parquet_dir}' 鍐?)
        with self.get_connection() as conn:
            conn.execute(f"COPY (SELECT * FROM {table}) TO '{output}' (FORMAT PARQUET)")
        logger.info(f"Exported to {output}")
        return output

    def load_parquet(self, file_path: Path) -> pd.DataFrame:
        """浠?Parquet 鍔犺浇"""
        return pd.read_parquet(file_path)

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鏁版嵁璐ㄩ噺鎶ュ憡
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def quality_report(self, start_date: str, end_date: str) -> Dict:
        """鐢熸垚鏁版嵁璐ㄩ噺鎶ュ憡"""
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


    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 鎵归噺鏁版嵁鑾峰彇锛堜紭鍖?N+1 鏌ヨ闂锛?
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    def get_batch_stock_data(self,
                              symbols: List[str],
                              trade_date: str) -> pd.DataFrame:
        """
        鎵归噺鑾峰彇澶氬彧鑲＄エ鍦ㄦ寚瀹氭棩鏈熺殑鏃ョ嚎鏁版嵁
        
        鐢ㄥ崟鏉?SQL 鏇夸唬 N 娆″惊鐜煡璇紝澶у箙鍑忓皯鏁版嵁搴?I/O銆?
        
        Parameters
        ----------
        symbols : List[str]
            6浣嶈偂绁ㄤ唬鐮佸垪琛?
        trade_date : str
            浜ゆ槗鏃ユ湡 YYYY-MM-DD
            
        Returns
        -------
        pd.DataFrame: 鍚堝苟鍚庣殑鏃ョ嚎鏁版嵁
        """
        if not symbols or not HAS_DUCKDB:
            return pd.DataFrame()
        
        # 鏋勯€?ts_code 鍒楄〃
        ts_codes = []
        for sym in symbols:
            sym6 = str(sym).zfill(6)
            if sym6.startswith(("6", "5", "9", "688")):
                ts_codes.append(f"{sym6}.SH")
            else:
                ts_codes.append(f"{sym6}.SZ")
        
        # 浣跨敤 register 閬垮厤 SQL 娉ㄥ叆
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
        
        # 鎻愬彇6浣嶄唬鐮佷綔涓?symbol 鍒?
        df["symbol"] = df["ts_code"].str.split(".").str[0]
        return df

    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
    # 渚挎嵎鍑芥暟
    # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def load_stock(code: str,
               start: str = None,
               end: str = None,
               adjust: str = "qfq") -> pd.DataFrame:
    """涓€琛屼唬鐮佸姞杞藉崟鍙偂绁ㄦ暟鎹?""
    engine = DataEngine()
    return engine.get_security_data(code, start, end, adjust)


