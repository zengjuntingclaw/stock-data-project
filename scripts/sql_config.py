"""
SQL配置中心 - 统一管理所有SQL语句
================================
避免硬编码SQL分散在各模块，便于维护和审计
"""
from typing import Dict

# ─────────────────────────────────────────────────────────────
# Schema定义（建表语句）
# ─────────────────────────────────────────────────────────────

SCHEMA_SQL: Dict[str, str] = {
    # ── 新口径表（PIT支持）────────────────────────────
    "stock_basic_history": """
        CREATE TABLE IF NOT EXISTS stock_basic_history (
            ts_code TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            exchange TEXT,
            board TEXT,
            industry TEXT,
            market TEXT,
            list_date DATE,
            delist_date DATE,
            is_delisted BOOLEAN DEFAULT FALSE,
            eff_date DATE NOT NULL,
            end_date DATE,
            PRIMARY KEY (ts_code, eff_date)
        )
    """,

    "daily_bar_raw": """
        CREATE TABLE IF NOT EXISTS daily_bar_raw (
            ts_code TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            pct_chg DOUBLE,
            turnover DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "daily_bar_adjusted": """
        CREATE TABLE IF NOT EXISTS daily_bar_adjusted (
            ts_code TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            pct_chg DOUBLE,
            turnover DOUBLE,
            adj_factor DOUBLE DEFAULT 1.0,
            qfq_open DOUBLE,
            qfq_high DOUBLE,
            qfq_low DOUBLE,
            qfq_close DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "index_constituents_history": """
        CREATE TABLE IF NOT EXISTS index_constituents_history (
            index_code TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            in_date DATE NOT NULL,
            out_date DATE,
            PRIMARY KEY (index_code, ts_code, in_date)
        )
    """,

    # ── 旧表（已废弃，保留用于兼容）────────────────────
    "stock_basic": """
        CREATE TABLE IF NOT EXISTS stock_basic (
            symbol TEXT PRIMARY KEY,
            ts_code TEXT NOT NULL,
            name TEXT,
            industry TEXT,
            list_date DATE,
            delist_date DATE,
            is_delisted BOOLEAN DEFAULT FALSE,
            board_type TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "daily_quotes": """
        CREATE TABLE IF NOT EXISTS daily_quotes (
            symbol TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            amount DOUBLE,
            pct_chg DOUBLE,
            adj_factor DOUBLE DEFAULT 1.0,
            turnover DOUBLE,
            PRIMARY KEY (symbol, trade_date)
        )
    """,
    
    "financial_data": """
        CREATE TABLE IF NOT EXISTS financial_data (
            ts_code TEXT NOT NULL,
            ann_date DATE NOT NULL,
            end_date DATE NOT NULL,
            revenue DOUBLE,
            net_profit DOUBLE,
            roe DOUBLE,
            gross_margin DOUBLE,
            debt_ratio DOUBLE,
            PRIMARY KEY (ts_code, end_date, ann_date)
        )
    """,
    
    "trade_calendar": """
        CREATE TABLE IF NOT EXISTS trade_calendar (
            cal_date DATE PRIMARY KEY,
            is_open BOOLEAN DEFAULT TRUE,
            pretrade_date DATE
        )
    """,
    
    "versioned_data": """
        CREATE TABLE IF NOT EXISTS versioned_data (
            ts_code TEXT NOT NULL,
            data_type TEXT NOT NULL,
            trade_date DATE NOT NULL,
            version INTEGER NOT NULL,
            data_json TEXT NOT NULL,
            effective_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, data_type, trade_date, version)
        )
    """,
    
    "data_changelog": """
        CREATE TABLE IF NOT EXISTS data_changelog (
            id INTEGER PRIMARY KEY,
            ts_code TEXT NOT NULL,
            data_type TEXT NOT NULL,
            trade_date DATE NOT NULL,
            old_version INTEGER,
            new_version INTEGER NOT NULL,
            changed_by TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reason TEXT
        )
    """,
    
    "data_audit": """
        CREATE TABLE IF NOT EXISTS data_audit (
            id INTEGER PRIMARY KEY,
            ts_code TEXT NOT NULL,
            data_type TEXT NOT NULL,
            trade_date DATE,
            check_type TEXT NOT NULL,
            check_result TEXT NOT NULL,
            details TEXT,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    # ── 2026-04-10 新增表 ────────────────────────────────────
    "daily_bar_raw": """
        CREATE TABLE IF NOT EXISTS daily_bar_raw (
            ts_code       VARCHAR  NOT NULL,
            trade_date    DATE     NOT NULL,
            symbol        VARCHAR  NOT NULL,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE   NOT NULL,
            volume        BIGINT,
            amount        DOUBLE,
            pct_chg       DOUBLE,
            turnover      DOUBLE,
            data_source   VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "corporate_actions": """
        CREATE TABLE IF NOT EXISTS corporate_actions (
            ts_code       VARCHAR  NOT NULL,
            trade_date    DATE     NOT NULL,
            ann_date      DATE,
            adj_factor    DOUBLE   NOT NULL,
            adj_type      VARCHAR,
            prev_factor   DOUBLE,
            change_ratio  DOUBLE,
            reason        VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "stock_basic_history": """
        CREATE TABLE IF NOT EXISTS stock_basic_history (
            ts_code       VARCHAR  NOT NULL,
            symbol        VARCHAR  NOT NULL,
            name          VARCHAR,
            exchange      VARCHAR,
            area          VARCHAR,
            industry      VARCHAR,
            market        VARCHAR,
            list_date     DATE,
            delist_date   DATE,
            is_delisted   BOOLEAN  DEFAULT FALSE,
            delist_reason VARCHAR,
            board         VARCHAR,
            eff_date      DATE     NOT NULL,
            end_date      DATE,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, eff_date)
        )
    """,

    "daily_valuation": """
        CREATE TABLE IF NOT EXISTS daily_valuation (
            ts_code       VARCHAR  NOT NULL,
            trade_date    DATE     NOT NULL,
            total_mv      DOUBLE,
            circ_mv       DOUBLE,
            pe_ttm        DOUBLE,
            pb            DOUBLE,
            ps_ttm        DOUBLE,
            pcf_cf_ttm    DOUBLE,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "st_status_history": """
        CREATE TABLE IF NOT EXISTS st_status_history (
            ts_code       VARCHAR  NOT NULL,
            trade_date    DATE     NOT NULL,
            is_st         BOOLEAN  NOT NULL,
            is_new_st     BOOLEAN  DEFAULT FALSE,
            is_removed_st BOOLEAN  DEFAULT FALSE,
            st_type       VARCHAR,
            reason        VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "index_constituents_history": """
        CREATE TABLE IF NOT EXISTS index_constituents_history (
            index_code    VARCHAR  NOT NULL,
            ts_code       VARCHAR  NOT NULL,
            in_date       DATE     NOT NULL,
            out_date      DATE,
            source        VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, ts_code, in_date)
        )
    """,

    # ── Schema v4.0 (2026-04-12) ────────────────────────────────────────
    # 收口：清理旧表 + 层间一致性校验 + sync_progress分层跟踪 + 版本统一
    # daily_bar_raw/adj: 1,408,552行完全对齐
    # ── 2026-04-11 补充缺失的 daily_bar_adjusted 和 sync_progress ──
    "daily_bar_adjusted": """
        CREATE TABLE IF NOT EXISTS daily_bar_adjusted (
            ts_code       VARCHAR  NOT NULL,
            trade_date    DATE     NOT NULL,
            open          DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            close         DOUBLE   NOT NULL,   -- 复权收盘价（= raw_close × adj_factor）
            pre_close     DOUBLE,              -- 复权前收（用于复权 pct_chg 计算）
            volume        BIGINT,
            amount        DOUBLE,
            pct_chg       DOUBLE,
            turnover      DOUBLE,
            adj_factor    DOUBLE DEFAULT 1.0,  -- 前复权因子（qfq）
            is_suspend    BOOLEAN DEFAULT FALSE,
            limit_up      BOOLEAN DEFAULT FALSE,
            limit_down    BOOLEAN DEFAULT FALSE,
            data_source   VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "sync_progress": """
        CREATE TABLE IF NOT EXISTS sync_progress (
            ts_code        VARCHAR NOT NULL,
            table_name     VARCHAR NOT NULL,
            last_sync_date DATE,
            last_sync_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_records  INTEGER DEFAULT 0,
            status         VARCHAR DEFAULT 'ok',
            error_msg      VARCHAR,
            PRIMARY KEY (ts_code, table_name)
        )
    """,
}

# ─────────────────────────────────────────────────────────────
# 查询语句（SELECT）
# ─────────────────────────────────────────────────────────────

QUERY_SQL: Dict[str, str] = {
    # 股票基础信息（新口径 - 使用 stock_basic_history）
    "get_stock_basic": """
        SELECT h.* FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE ts_code = ? GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
    """,

    "get_stock_by_ts_code": """
        SELECT h.* FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE ts_code = ? GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
    """,

    "get_all_stocks": """
        SELECT h.* FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.is_delisted = FALSE
        ORDER BY h.symbol
    """,

    "get_delisted_stocks": """
        SELECT h.* FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.is_delisted = TRUE
    """,

    # 日线数据（新口径 - 使用 daily_bar_adjusted）
    "get_daily_quotes": """
        SELECT * FROM daily_bar_adjusted
        WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """,

    "get_latest_quote": """
        SELECT * FROM daily_bar_adjusted
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 1
    """,

    "get_max_trade_date": """
        SELECT MAX(trade_date) as max_date
        FROM daily_bar_adjusted
        WHERE ts_code = ?
    """,

    # 交易日历
    "get_trade_dates": """
        SELECT cal_date FROM trade_calendar
        WHERE is_open = TRUE AND cal_date BETWEEN ? AND ?
        ORDER BY cal_date
    """,

    "is_trade_date": """
        SELECT is_open FROM trade_calendar
        WHERE cal_date = ?
    """,

    # 财务数据（PIT查询）
    "get_financial_data_pit": """
        SELECT * FROM financial_data
        WHERE ts_code = ?
        AND ann_date < ?
        ORDER BY ann_date DESC
        LIMIT 1
    """,

    # 版本化数据
    "get_versioned_data_pit": """
        SELECT * FROM versioned_data
        WHERE ts_code = ?
        AND data_type = ?
        AND trade_date = ?
        AND effective_at < ?
        ORDER BY version DESC
        LIMIT 1
    """,

    "get_version_history": """
        SELECT * FROM versioned_data
        WHERE ts_code = ?
        AND data_type = ?
        AND trade_date = ?
        ORDER BY version DESC
    """,

    # 数据统计（新口径）
    "count_daily_quotes": """
        SELECT COUNT(*) as cnt FROM daily_bar_adjusted
    """,

    "count_stocks": """
        SELECT COUNT(*) as cnt FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.is_delisted = FALSE
    """,

    "count_delisted": """
        SELECT COUNT(*) as cnt FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.is_delisted = TRUE
    """,

    # ── 2026-04-11 新增 PIT / 分层查询 ──────────────────────────

    # 按历史日期查询股票池（PIT，解决幸存者偏差）
    "get_pit_stock_pool": """
        SELECT ts_code, symbol, name, exchange, board,
               list_date, delist_date, is_delisted,
               CASE
                   WHEN delist_date IS NULL THEN TRUE
                   WHEN delist_date > CAST(? AS DATE) THEN TRUE
                   ELSE FALSE
               END AS is_tradable
        FROM stock_basic_history
        WHERE list_date <= CAST(? AS DATE)
          AND (delist_date IS NULL OR delist_date > CAST(? AS DATE))
        ORDER BY exchange, ts_code
    """,

    # 原始日线数据查询（不复权）
    "get_daily_bar_raw": """
        SELECT * FROM daily_bar_raw
        WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """,

    # 复权日线数据查询
    "get_daily_bar_adjusted": """
        SELECT * FROM daily_bar_adjusted
        WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """,

    # 两表联合查询（同时返回原始价和复权价，供回测比对）
    "get_daily_bar_both": """
        SELECT
            r.ts_code, r.trade_date,
            r.open  AS raw_open,  r.high  AS raw_high,
            r.low   AS raw_low,   r.close AS raw_close,
            a.open  AS adj_open,  a.high  AS adj_high,
            a.low   AS adj_low,   a.close AS adj_close,
            r.volume, r.amount, r.pct_chg, r.turnover,
            a.adj_factor, a.is_suspend, a.limit_up, a.limit_down
        FROM daily_bar_raw r
        LEFT JOIN daily_bar_adjusted a
          ON r.ts_code = a.ts_code AND r.trade_date = a.trade_date
        WHERE r.ts_code = ? AND r.trade_date BETWEEN ? AND ?
        ORDER BY r.trade_date
    """,

    # 增量同步进度查询
    "get_sync_progress": """
        SELECT ts_code, last_sync_date, total_records, status, error_msg, last_sync_at
        FROM sync_progress
        WHERE table_name = 'daily_bar_raw'
        ORDER BY last_sync_date DESC
    """,

    # 最新数据质量报警（最近 N 条）
    "get_recent_quality_alerts": """
        SELECT * FROM data_quality_alert
        ORDER BY created_at DESC
        LIMIT ?
    """,
}

# ─────────────────────────────────────────────────────────────
# 数据操作（INSERT/UPDATE/DELETE）
# ─────────────────────────────────────────────────────────────

DML_SQL: Dict[str, str] = {
    # 股票列表（新口径 - stock_basic_history）
    "upsert_stock": """
        INSERT INTO stock_basic_history
        (ts_code, symbol, name, exchange, board, industry, market,
         list_date, delist_date, is_delisted, eff_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, eff_date) DO UPDATE SET
            name         = excluded.name,
            exchange     = excluded.exchange,
            board        = excluded.board,
            industry     = excluded.industry,
            market       = excluded.market,
            list_date    = excluded.list_date,
            delist_date  = excluded.delist_date,
            is_delisted  = excluded.is_delisted,
            end_date     = excluded.end_date
    """,

    "delete_stock": """
        UPDATE stock_basic_history
        SET end_date = CURRENT_DATE
        WHERE ts_code = ? AND end_date IS NULL
    """,

    # 日线数据（新口径 - daily_bar_raw / daily_bar_adjusted）
    "upsert_daily_raw": """
        INSERT INTO daily_bar_raw
        (ts_code, trade_date, open, high, low, close, pre_close,
         volume, amount, pct_chg, turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            open      = excluded.open,
            high      = excluded.high,
            low       = excluded.low,
            close     = excluded.close,
            pre_close = excluded.pre_close,
            volume    = excluded.volume,
            amount    = excluded.amount,
            pct_chg   = excluded.pct_chg,
            turnover  = excluded.turnover
    """,

    "upsert_daily_adjusted": """
        INSERT INTO daily_bar_adjusted
        (ts_code, trade_date, open, high, low, close, pre_close,
         volume, amount, pct_chg, turnover, adj_factor,
         qfq_open, qfq_high, qfq_low, qfq_close)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
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
            qfq_open   = excluded.qfq_open,
            qfq_high   = excluded.qfq_high,
            qfq_low    = excluded.qfq_low,
            qfq_close  = excluded.qfq_close
    """,

    # 旧表 DML（已废弃，仅保留兼容）
    "upsert_daily_quotes": """
        INSERT OR REPLACE INTO daily_quotes
        (symbol, ts_code, trade_date, open, high, low, close, volume, amount, pct_chg, adj_factor, turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,

    "delete_daily_by_symbol": """
        DELETE FROM daily_bar_raw WHERE ts_code = ?
    """,

    "delete_daily_by_date_range": """
        DELETE FROM daily_bar_raw
        WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
    """,
    
    # 财务数据
    "upsert_financial": """
        INSERT OR REPLACE INTO financial_data 
        (ts_code, ann_date, end_date, revenue, net_profit, roe, gross_margin, debt_ratio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    
    # 交易日历
    "upsert_calendar": """
        INSERT OR REPLACE INTO trade_calendar 
        (cal_date, is_open, pretrade_date)
        VALUES (?, ?, ?)
    """,
    
    # 版本化数据
    "insert_versioned": """
        INSERT INTO versioned_data 
        (ts_code, data_type, trade_date, version, data_json, effective_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
    
    "update_version_effective_at": """
        UPDATE versioned_data 
        SET effective_at = ?
        WHERE ts_code = ? AND data_type = ? AND trade_date = ? AND version = ?
    """,
    
    "soft_delete_version": """
        UPDATE versioned_data 
        SET effective_at = '2999-12-31'
        WHERE ts_code = ? AND data_type = ? AND trade_date = ? AND version = ?
    """,
    
    # 审计日志
    "insert_changelog": """
        INSERT INTO data_changelog 
        (ts_code, data_type, trade_date, old_version, new_version, changed_by, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
    
    "insert_audit": """
        INSERT INTO data_audit 
        (ts_code, data_type, trade_date, check_type, check_result, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """,

    # ── 2026-04-11 补充缺失的 DML ────────────────────────────────

    # 原始日线数据（daily_bar_raw）写入
    "upsert_daily_bar_raw": """
        INSERT INTO daily_bar_raw
            (ts_code, trade_date, symbol, open, high, low, close,
             volume, amount, pct_chg, turnover, data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            symbol      = excluded.symbol,
            open        = excluded.open,
            high        = excluded.high,
            low         = excluded.low,
            close       = excluded.close,
            volume      = excluded.volume,
            amount      = excluded.amount,
            pct_chg     = excluded.pct_chg,
            turnover    = excluded.turnover,
            data_source = excluded.data_source
    """,

    # 复权日线数据（daily_bar_adjusted）写入
    "upsert_daily_bar_adjusted": """
        INSERT INTO daily_bar_adjusted
            (ts_code, trade_date, open, high, low, close, pre_close,
             volume, amount, pct_chg, turnover, adj_factor,
             is_suspend, limit_up, limit_down, data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            open        = excluded.open,
            high        = excluded.high,
            low         = excluded.low,
            close       = excluded.close,
            pre_close   = excluded.pre_close,
            volume      = excluded.volume,
            amount      = excluded.amount,
            pct_chg     = excluded.pct_chg,
            turnover    = excluded.turnover,
            adj_factor  = excluded.adj_factor,
            is_suspend  = excluded.is_suspend,
            limit_up    = excluded.limit_up,
            limit_down  = excluded.limit_down,
            data_source = excluded.data_source
    """,

    # 同步进度（sync_progress）UPSERT
    "upsert_sync_progress": """
        INSERT INTO sync_progress
            (ts_code, table_name, last_sync_date, total_records, status, error_msg)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts_code, table_name) DO UPDATE SET
            last_sync_date = GREATEST(sync_progress.last_sync_date, excluded.last_sync_date),
            total_records  = sync_progress.total_records + excluded.total_records,
            last_sync_at   = CURRENT_TIMESTAMP,
            status         = excluded.status,
            error_msg      = excluded.error_msg
    """,

    # 数据质量报警写入
    "insert_data_quality_alert": """
        INSERT INTO data_quality_alert
            (alert_type, ts_code, trade_date, detail)
        VALUES (?, ?, ?, ?)
    """,
}

# ─────────────────────────────────────────────────────────────
# 便捷函数
# ─────────────────────────────────────────────────────────────

def get_sql(category: str, name: str) -> str:
    """获取SQL语句
    
    Args:
        category: 类别 - 'schema', 'query', 'dml'
        name: SQL名称
    
    Returns:
        SQL语句字符串
    """
    categories = {
        'schema': SCHEMA_SQL,
        'query': QUERY_SQL,
        'dml': DML_SQL,
    }
    
    if category not in categories:
        raise ValueError(f"Unknown category: {category}")
    
    sql_dict = categories[category]
    if name not in sql_dict:
        raise ValueError(f"Unknown SQL: {category}.{name}")
    
    return sql_dict[name]


def list_sql_names(category: str = None) -> Dict[str, list]:
    """列出所有可用的SQL名称
    
    Args:
        category: 可选，指定类别
    
    Returns:
        类别到名称列表的映射
    """
    if category:
        categories = {
            'schema': SCHEMA_SQL,
            'query': QUERY_SQL,
            'dml': DML_SQL,
        }
        if category in categories:
            return {category: list(categories[category].keys())}
        return {}
    
    return {
        'schema': list(SCHEMA_SQL.keys()),
        'query': list(QUERY_SQL.keys()),
        'dml': list(DML_SQL.keys()),
    }
