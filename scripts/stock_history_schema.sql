-- ============================================================
-- 数据仓库标准化 Schema v2.0
-- ============================================================
-- 更新日期: 2026-04-11
-- 说明: 统一 PIT / 分层 / 幸存者偏差消除口径
-- ============================================================

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 1. stock_basic_history - 股票主数据历史表（PIT 专用）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS stock_basic_history (
    ts_code      TEXT    NOT NULL,        -- 证券代码（含交易所后缀）
    symbol       TEXT    NOT NULL,        -- 纯数字代码
    name         TEXT,                    -- 证券名称
    exchange     TEXT,                    -- 交易所: SH/SZ/BJ
    board        TEXT,                    -- 板块: 主板/科创板/创业板/北交所
    industry     TEXT,                    -- 行业分类
    market       TEXT,                    -- 市场类型: main/科技创新/growth
    list_date    DATE,                    -- 上市日期
    delist_date  DATE,                    -- 退市日期
    is_delisted  BOOLEAN DEFAULT FALSE,   -- 是否已退市
    delist_reason TEXT,                   -- 退市原因
    is_suspended BOOLEAN DEFAULT FALSE,   -- 是否停牌
    eff_date     DATE    NOT NULL,        -- 生效日期（PIT 查询依据）
    end_date     DATE,                    -- 失效日期（NULL 表示当前有效）
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ts_code, eff_date)
);

-- 索引：加速 PIT 查询
CREATE INDEX IF NOT EXISTS idx_basic_history_eff
    ON stock_basic_history(eff_date);

CREATE INDEX IF NOT EXISTS idx_basic_history_list
    ON stock_basic_history(list_date, delist_date);

CREATE INDEX IF NOT EXISTS idx_basic_history_exchange
    ON stock_basic_history(exchange);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 2. daily_bar_raw - 日线行情原始层（不复权）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS daily_bar_raw (
    ts_code     TEXT    NOT NULL,        -- 证券代码
    trade_date  DATE    NOT NULL,        -- 交易日期
    open        DOUBLE,                   -- 开盘价
    high        DOUBLE,                   -- 最高价
    low         DOUBLE,                   -- 最低价
    close       DOUBLE,                   -- 收盘价
    pre_close   DOUBLE,                   -- 前收盘价
    volume      BIGINT,                   -- 成交量（股）
    amount      DOUBLE,                   -- 成交额（元）
    pct_chg     DOUBLE,                   -- 涨跌幅（%）
    turnover    DOUBLE,                   -- 换手率（%）

    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_raw_trade_date
    ON daily_bar_raw(trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_code_date
    ON daily_bar_raw(ts_code, trade_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 3. daily_bar_adjusted - 日线行情复权层（复权价 + 因子）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS daily_bar_adjusted (
    ts_code     TEXT    NOT NULL,        -- 证券代码
    trade_date  DATE    NOT NULL,        -- 交易日期
    open        DOUBLE,                   -- 开盘价（不复权）
    high        DOUBLE,                   -- 最高价（不复权）
    low         DOUBLE,                   -- 最低价（不复权）
    close       DOUBLE,                   -- 收盘价（不复权）
    pre_close   DOUBLE,                   -- 前收盘价
    volume      BIGINT,                   -- 成交量（股）
    amount      DOUBLE,                   -- 成交额（元）
    pct_chg     DOUBLE,                   -- 涨跌幅（%）
    turnover    DOUBLE,                   -- 换手率（%）
    adj_factor  DOUBLE DEFAULT 1.0,       -- 复权因子
    qfq_open    DOUBLE,                   -- 前复权开盘价
    qfq_high    DOUBLE,                   -- 前复权最高价
    qfq_low     DOUBLE,                   -- 前复权最低价
    qfq_close   DOUBLE,                   -- 前复权收盘价
    hfq_open    DOUBLE,                   -- 后复权开盘价
    hfq_high    DOUBLE,                   -- 后复权最高价
    hfq_low     DOUBLE,                   -- 后复权最低价
    hfq_close   DOUBLE,                   -- 后复权收盘价

    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_adj_trade_date
    ON daily_bar_adjusted(trade_date);

CREATE INDEX IF NOT EXISTS idx_adj_code_date
    ON daily_bar_adjusted(ts_code, trade_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 4. index_constituents_history - 指数成分股历史表（区间型）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS index_constituents_history (
    index_code  TEXT    NOT NULL,        -- 指数代码（格式: 000300.SH）
    ts_code     TEXT    NOT NULL,        -- 成分股代码
    in_date     DATE    NOT NULL,        -- 加入日期
    out_date    DATE,                    -- 剔除日期（NULL 表示仍在指数中）
    weight      DOUBLE,                   -- 权重（可选）

    PRIMARY KEY (index_code, ts_code, in_date)
);

CREATE INDEX IF NOT EXISTS idx_idx_code_date
    ON index_constituents_history(index_code, in_date, out_date);

CREATE INDEX IF NOT EXISTS idx_idx_member
    ON index_constituents_history(ts_code, index_code);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 5. trade_calendar - 交易日历
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS trade_calendar (
    cal_date  DATE       NOT NULL PRIMARY KEY,
    is_open   BOOLEAN    NOT NULL DEFAULT TRUE,
    is_month_end BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_cal_open
    ON trade_calendar(is_open, cal_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 6. corporate_actions - 上市公司行为（分红、送股、拆合）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS corporate_actions (
    id          BIGINT   NOT NULL DEFAULT nextval('action_seq'),
    ts_code     TEXT     NOT NULL,
    trade_date  DATE     NOT NULL,        -- 股权登记日
    ex_date     DATE     NOT NULL,        -- 除权除息日
    ann_date    DATE     NOT NULL,        -- 公告日期
    action      TEXT,                     -- 分红/送股/配股类型
    dividend    DOUBLE,                   -- 分红金额
    shares      DOUBLE,                   -- 送股比例
    unit        TEXT,                     -- 单位
    eff_date    DATE,                     -- 生效日期（PIT）

    PRIMARY KEY (id, eff_date)
);

CREATE INDEX IF NOT EXISTS idx_action_code
    ON corporate_actions(ts_code, trade_date, eff_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 7. st_status_history - ST 状态历史表
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS st_status_history (
    ts_code      TEXT    NOT NULL,
    status       TEXT    NOT NULL,        -- ST/*ST/ST*/退市预警
    in_date      DATE    NOT NULL,
    out_date     DATE,
    reason       TEXT,                    -- 被ST原因
    eff_date     DATE    NOT NULL,         -- 生效日期

    PRIMARY KEY (ts_code, eff_date)
);

CREATE INDEX IF NOT EXISTS idx_st_code_date
    ON st_status_history(ts_code, in_date, out_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 8. adj_factor_log - 复权因子变更日志
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS adj_factor_log (
    ts_code     TEXT    NOT NULL,
    trade_date  DATE    NOT NULL,
    adj_factor  DOUBLE  NOT NULL,
    eff_date    DATE    NOT NULL,

    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_adj_factor_code
    ON adj_factor_log(ts_code, trade_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 9. sync_progress - 增量同步进度表
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS sync_progress (
    id            BIGINT   NOT NULL DEFAULT nextval('sync_seq'),
    task_name     TEXT     NOT NULL,        -- 任务名: sync_stock_list / sync_daily
    data_source   TEXT,                     -- 数据源: tushare / baostock
    ts_code       TEXT,                     -- 股票代码（可选）
    index_code    TEXT,                     -- 指数代码（可选）
    start_date    DATE,                     -- 起始日期
    end_date      DATE,                     -- 结束日期
    last_success  DATE,                     -- 最后成功日期
    total_count   INTEGER DEFAULT 0,        -- 总数量
    success_count INTEGER DEFAULT 0,        -- 成功数量
    status        TEXT     NOT NULL,        -- running/success/failed/paused
    error_msg     TEXT,                     -- 错误信息
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_sync_task
    ON sync_progress(task_name, ts_code, status);

CREATE INDEX IF NOT EXISTS idx_sync_last
    ON sync_progress(last_success, status);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 10. data_quality_alert - 数据质量告警表
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS data_quality_alert (
    id            BIGINT   NOT NULL DEFAULT nextval('alert_seq'),
    check_type    TEXT     NOT NULL,        -- 检查类型
    ts_code       TEXT,                     -- 股票代码（可选）
    trade_date    DATE,                     -- 交易日期（可选）
    severity      TEXT     NOT NULL,        -- critical/high/medium/low
    alert_msg     TEXT     NOT NULL,        -- 告警信息
    details       JSON,                     -- 详细数据（JSON）
    resolved      BOOLEAN  DEFAULT FALSE,
    resolved_at   TIMESTAMP,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_alert_code_date
    ON data_quality_alert(ts_code, trade_date, severity);

CREATE INDEX IF NOT EXISTS idx_alert_unresolved
    ON data_quality_alert(resolved, created_at);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 11. financial_data - 财务数据表（支持 PIT 查询）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS financial_data (
    ts_code     TEXT    NOT NULL,
    ann_date    DATE    NOT NULL,        -- 公告日期
    report_date DATE    NOT NULL,        -- 报告期
    eff_date    DATE    NOT NULL,        -- 生效日期（PIT）
    -- 基本指标
    total_revenue   DOUBLE,              -- 营业总收入
    revenue         DOUBLE,              -- 营业收入
    total_profit    DOUBLE,              -- 利润总额
    net_profit      DOUBLE,              -- 净利润
    total_assets    DOUBLE,              -- 资产总计
    total_liab      DOUBLE,              -- 负债合计
    equity          DOUBLE,              -- 所有者权益
    -- 比率指标
    roe             DOUBLE,              -- 净资产收益率
    gross_margin    DOUBLE,              -- 毛利率
    net_margin      DOUBLE,              -- 净利率
    debt_ratio      DOUBLE,              -- 资产负债率
    current_ratio    DOUBLE,             -- 流动比率
    quick_ratio     DOUBLE,              -- 速动比率
    -- 每股指标
    eps             DOUBLE,              -- 每股收益
    bps             DOUBLE,              -- 每股净资产
    per_share_revenue DOUBLE,            -- 每股营收

    PRIMARY KEY (ts_code, ann_date, report_date)
);

CREATE INDEX IF NOT EXISTS idx_fin_code_report
    ON financial_data(ts_code, report_date, eff_date);

CREATE INDEX IF NOT EXISTS idx_fin_ann_date
    ON financial_data(ann_date, eff_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 12. update_log - 数据更新日志
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS update_log (
    id            BIGINT   NOT NULL DEFAULT nextval('update_seq'),
    source        TEXT     NOT NULL,        -- 数据源
    table_name    TEXT     NOT NULL,
    record_count  INTEGER,
    status        TEXT,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    error_msg     TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_update_source_table
    ON update_log(source, table_name, created_at);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 兼容层（旧表，仅保留用于迁移参考，不做主数据源）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- stock_basic（旧）- 保留但标注废弃
-- CREATE TABLE IF NOT EXISTS stock_basic (
--     symbol       TEXT PRIMARY KEY,
--     ts_code      TEXT NOT NULL,
--     name         TEXT,
--     industry     TEXT,
--     list_date    DATE,
--     delist_date  DATE,
--     is_delisted  BOOLEAN DEFAULT FALSE,
--     board_type   TEXT,
--     updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );
-- ⚠️ 已废弃，请使用 stock_basic_history

-- daily_quotes（旧）- 保留但标注废弃
-- CREATE TABLE IF NOT EXISTS daily_quotes (
--     symbol      TEXT NOT NULL,
--     ts_code     TEXT NOT NULL,
--     trade_date  DATE NOT NULL,
--     open        DOUBLE,
--     high        DOUBLE,
--     low         DOUBLE,
--     close       DOUBLE,
--     volume      BIGINT,
--     amount      DOUBLE,
--     pct_chg     DOUBLE,
--     adj_factor  DOUBLE DEFAULT 1.0,
--     turnover    DOUBLE,
--     PRIMARY KEY (symbol, trade_date)
-- );
-- ⚠️ 已废弃，请使用 daily_bar_raw + daily_bar_adjusted

-- index_constituents（旧）- 保留但标注废弃
-- CREATE TABLE IF NOT EXISTS index_constituents (
--     index_code TEXT NOT NULL,
--     ts_code    TEXT NOT NULL,
--     trade_date DATE NOT NULL,
--     weight     DOUBLE,
--     PRIMARY KEY (index_code, ts_code, trade_date)
-- );
-- ⚠️ 已废弃，请使用 index_constituents_history


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 视图：当前有效股票池（最新 eff_date 快照）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE OR REPLACE VIEW v_current_stocks AS
SELECT h.*
FROM (
    SELECT ts_code, MAX(eff_date) as latest_eff
    FROM stock_basic_history
    GROUP BY ts_code
) latest
JOIN stock_basic_history h
  ON h.ts_code = latest.ts_code
 AND h.eff_date = latest.latest_eff;


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 视图：当前在市股票（排除已退市）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE OR REPLACE VIEW v_active_stocks AS
SELECT *
FROM v_current_stocks
WHERE is_delisted = FALSE
  AND (delist_date IS NULL OR delist_date > CURRENT_DATE);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 视图：沪深300当前成分股
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE OR REPLACE VIEW v_hs300_members AS
SELECT h.ts_code, h.name, h.industry
FROM index_constituents_history i
JOIN v_current_stocks h ON h.ts_code = i.ts_code
WHERE i.index_code = '000300.SH'
  AND i.out_date IS NULL;
