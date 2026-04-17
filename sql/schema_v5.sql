-- A股量化数据库 - 生产级Schema (v5.0)
-- 目标: 完整量化回测 + 因子选股 + 财报驱动

-- ============================================================
-- 1. stock_basic 股票基础表
-- ============================================================
DROP TABLE IF EXISTS stock_basic;
CREATE TABLE stock_basic (
    ticker                  VARCHAR(10) PRIMARY KEY,
    name                    VARCHAR(50),
    full_name               VARCHAR(100),
    exchange                VARCHAR(10),
    market                  VARCHAR(20),
    industry_level1         VARCHAR(50),
    industry_level2         VARCHAR(50),
    industry_level3         VARCHAR(50),
    listed_date             DATE,
    delisted_date           DATE,
    list_status             VARCHAR(10),  -- listed / delisted / paused
    is_financial            BOOLEAN,
    currency                VARCHAR(10) DEFAULT 'CNY',
    country                 VARCHAR(20) DEFAULT 'China',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_market ON stock_basic(market);
CREATE INDEX idx_industry ON stock_basic(industry_level1, industry_level2);

-- ============================================================
-- 2. trading_calendar 交易日历
-- ============================================================
DROP TABLE IF EXISTS trading_calendar;
CREATE TABLE trading_calendar (
    trade_date              DATE PRIMARY KEY,
    is_trading_day         BOOLEAN,
    is_weekend             BOOLEAN,
    is_holiday             BOOLEAN,
    prev_trade_date        DATE,
    next_trade_date        DATE,
    month_end_flag         BOOLEAN,
    quarter_end_flag       BOOLEAN,
    year_end_flag          BOOLEAN
);
CREATE INDEX idx_trading_day ON trading_calendar(trade_date, is_trading_day);

-- ============================================================
-- 3. market_daily 日行情表
-- ============================================================
DROP TABLE IF EXISTS market_daily;
CREATE TABLE market_daily (
    ticker                  VARCHAR(10),
    trade_date              DATE,

    open                    DECIMAL(10,4),
    high                    DECIMAL(10,4),
    low                     DECIMAL(10,4),
    close                   DECIMAL(10,4),
    pre_close               DECIMAL(10,4),

    volume                  BIGINT,
    amount                  DECIMAL(20,4),

    turnover_rate           DECIMAL(10,6),

    total_market_cap        DECIMAL(20,4),
    float_market_cap        DECIMAL(20,4),

    adj_factor              DECIMAL(18,10),

    suspended_flag          BOOLEAN DEFAULT FALSE,
    limit_up_flag           BOOLEAN DEFAULT FALSE,
    limit_down_flag         BOOLEAN DEFAULT FALSE,
    is_new_stock            BOOLEAN DEFAULT FALSE,

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX idx_market_date ON market_daily(trade_date);
CREATE INDEX idx_market_ticker ON market_daily(ticker, trade_date);

-- ============================================================
-- 4. corporate_action 公司行为表
-- ============================================================
DROP TABLE IF EXISTS corporate_action;
CREATE TABLE corporate_action (
    ticker                      VARCHAR(10),
    action_type                 VARCHAR(20),  -- dividend / bonus / split / rights

    announcement_date           DATE,
    record_date                DATE,
    ex_date                    DATE,
    pay_date                   DATE,

    cash_dividend_per_share    DECIMAL(10,6),
    bonus_share_per_share      DECIMAL(10,6),
    transfer_share_per_share   DECIMAL(10,6),

    rights_issue_ratio         DECIMAL(10,6),
    rights_issue_price         DECIMAL(10,4),

    split_ratio                DECIMAL(10,6),
    merge_ratio                DECIMAL(10,6),

    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, ex_date, action_type)
);
CREATE INDEX idx_action_date ON corporate_action(ex_date, announcement_date);

-- ============================================================
-- 5. financial_statement 财务报表(原始)
-- ============================================================
DROP TABLE IF EXISTS financial_statement;
CREATE TABLE financial_statement (
    ticker                  VARCHAR(10),
    report_period_end       DATE,
    announcement_date       DATE,
    available_date         DATE,
    report_type             VARCHAR(10),  -- Q1 / H1 / Q3 / FY

    -- 利润表
    revenue                 DECIMAL(20,4),
    cogs                    DECIMAL(20,4),
    gross_profit            DECIMAL(20,4),
    operating_profit        DECIMAL(20,4),
    net_profit              DECIMAL(20,4),
    net_profit_parent       DECIMAL(20,4),

    -- 资产负债表
    total_assets            DECIMAL(20,4),
    total_liabilities       DECIMAL(20,4),
    equity                  DECIMAL(20,4),
    asset_to_equity         DECIMAL(10,4),

    -- 现金流
    operating_cash_flow     DECIMAL(20,4),
    investing_cash_flow     DECIMAL(20,4),
    financing_cash_flow     DECIMAL(20,4),
    capex                   DECIMAL(20,4),

    -- 指标
    roe_avg                 DECIMAL(10,6),
    roa                     DECIMAL(10,6),
    roic                    DECIMAL(10,6),
    gross_margin            DECIMAL(10,6),
    net_margin              DECIMAL(10,6),

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, report_period_end, report_type)
);
CREATE INDEX idx_announce ON financial_statement(announcement_date);
CREATE INDEX idx_available ON financial_statement(available_date);
CREATE INDEX idx_ticker_period ON financial_statement(ticker, report_period_end);

-- ============================================================
-- 6. financial_factor 财务因子表
-- ============================================================
DROP TABLE IF EXISTS financial_factor;
CREATE TABLE financial_factor (
    ticker                      VARCHAR(10),
    report_period_end           DATE,
    announcement_date           DATE,
    available_date              DATE,

    roe                         DECIMAL(10,6),
    roic                        DECIMAL(10,6),
    gross_margin                DECIMAL(10,6),
    asset_liability_ratio       DECIMAL(10,6),

    net_profit                  DECIMAL(20,4),
    operating_cash_flow         DECIMAL(20,4),

    revenue_yoy                 DECIMAL(10,6),
    net_profit_yoy              DECIMAL(10,6),

    quality_score               DECIMAL(10,6),
    growth_score                DECIMAL(10,6),

    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, report_period_end)
);

-- ============================================================
-- 7. share_structure 股本结构
-- ============================================================
DROP TABLE IF EXISTS share_structure;
CREATE TABLE share_structure (
    ticker                  VARCHAR(10),
    report_period_end       DATE,
    announcement_date       DATE,
    available_date          DATE,

    total_shares            BIGINT,
    float_shares            BIGINT,
    restricted_shares       BIGINT,

    top1_holder_ratio       DECIMAL(10,6),
    top10_holder_ratio      DECIMAL(10,6),

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, report_period_end)
);

-- ============================================================
-- 8. company_status 公司状态表
-- ============================================================
DROP TABLE IF EXISTS company_status;
CREATE TABLE company_status (
    ticker                  VARCHAR(10),
    status_date             DATE,

    listed_flag             BOOLEAN DEFAULT TRUE,
    delisted_flag           BOOLEAN DEFAULT FALSE,
    suspended_flag          BOOLEAN DEFAULT FALSE,

    st_status               BOOLEAN DEFAULT FALSE,
    risk_warning_flag       BOOLEAN DEFAULT FALSE,

    tradable_flag           BOOLEAN DEFAULT TRUE,

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, status_date)
);
CREATE INDEX idx_status_date ON company_status(status_date);

-- ============================================================
-- 9. index_membership 指数成分(可选)
-- ============================================================
DROP TABLE IF EXISTS index_membership;
CREATE TABLE index_membership (
    index_code          VARCHAR(20),
    ticker              VARCHAR(10),
    effective_date      DATE,
    expiry_date         DATE,
    weight              DECIMAL(10,6),

    PRIMARY KEY (index_code, ticker, effective_date)
);
CREATE INDEX idx_index_date ON index_membership(index_code, effective_date);

-- ============================================================
-- 10. valuation_daily 估值表(可选)
-- ============================================================
DROP TABLE IF EXISTS valuation_daily;
CREATE TABLE valuation_daily (
    ticker              VARCHAR(10),
    trade_date          DATE,

    pe_ttm              DECIMAL(10,4),
    pb                  DECIMAL(10,4),
    ps_ttm              DECIMAL(10,4),
    pcf_ttm             DECIMAL(10,4),

    dividend_yield      DECIMAL(10,6),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, trade_date)
);
CREATE INDEX idx_val_date ON valuation_daily(trade_date);

-- ============================================================
-- 完成
-- ============================================================
SELECT 'Schema v5.0 created: 10 tables' AS status;