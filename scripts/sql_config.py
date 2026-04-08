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
}

# ─────────────────────────────────────────────────────────────
# 查询语句（SELECT）
# ─────────────────────────────────────────────────────────────

QUERY_SQL: Dict[str, str] = {
    # 股票基础信息
    "get_stock_basic": """
        SELECT * FROM stock_basic 
        WHERE symbol = ?
    """,
    
    "get_stock_by_ts_code": """
        SELECT * FROM stock_basic 
        WHERE ts_code = ?
    """,
    
    "get_all_stocks": """
        SELECT * FROM stock_basic 
        WHERE is_delisted = FALSE
        ORDER BY symbol
    """,
    
    "get_delisted_stocks": """
        SELECT * FROM stock_basic 
        WHERE is_delisted = TRUE
    """,
    
    # 日线数据
    "get_daily_quotes": """
        SELECT * FROM daily_quotes 
        WHERE symbol = ? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """,
    
    "get_latest_quote": """
        SELECT * FROM daily_quotes 
        WHERE symbol = ?
        ORDER BY trade_date DESC 
        LIMIT 1
    """,
    
    "get_max_trade_date": """
        SELECT MAX(trade_date) as max_date 
        FROM daily_quotes 
        WHERE symbol = ?
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
    
    # 数据统计
    "count_daily_quotes": """
        SELECT COUNT(*) as cnt FROM daily_quotes
    """,
    
    "count_stocks": """
        SELECT COUNT(*) as cnt FROM stock_basic
    """,
    
    "count_delisted": """
        SELECT COUNT(*) as cnt FROM stock_basic 
        WHERE is_delisted = TRUE
    """,
}

# ─────────────────────────────────────────────────────────────
# 数据操作（INSERT/UPDATE/DELETE）
# ─────────────────────────────────────────────────────────────

DML_SQL: Dict[str, str] = {
    # 股票列表
    "upsert_stock": """
        INSERT OR REPLACE INTO stock_basic 
        (symbol, ts_code, name, industry, list_date, delist_date, is_delisted, board_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    
    "delete_stock": """
        DELETE FROM stock_basic WHERE symbol = ?
    """,
    
    # 日线数据
    "upsert_daily_quotes": """
        INSERT OR REPLACE INTO daily_quotes 
        (symbol, ts_code, trade_date, open, high, low, close, volume, amount, pct_chg, adj_factor, turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    
    "delete_daily_by_symbol": """
        DELETE FROM daily_quotes WHERE symbol = ?
    """,
    
    "delete_daily_by_date_range": """
        DELETE FROM daily_quotes 
        WHERE symbol = ? AND trade_date BETWEEN ? AND ?
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
