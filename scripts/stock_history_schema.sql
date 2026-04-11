-- ============================================================
-- 数据仓库标准化 Schema v2.3
-- ============================================================
-- 更新日期: 2026-04-12
-- 说明: 对齐 data_engine.py v3.3；sync_progress 分层跟踪 raw/adjusted；
--       14 项数据质量校验（新增层间一致性 4 项）
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
    symbol      TEXT,                    -- 纯数字代码
    open        DOUBLE,                   -- 开盘价
    high        DOUBLE,                   -- 最高价
    low         DOUBLE,                   -- 最低价
    close       DOUBLE,                   -- 收盘价
    pre_close   DOUBLE,                   -- 前收盘价
    volume      BIGINT,                   -- 成交量（股）
    amount      DOUBLE,                   -- 成交额（元）
    pct_chg     DOUBLE,                   -- 涨跌幅（%）
    turnover    DOUBLE,                   -- 换手率（%）
    adj_factor  DOUBLE DEFAULT 1.0,       -- 复权因子（>1.0=有复权，1.0=原始价）
    is_suspend  BOOLEAN DEFAULT FALSE,   -- 是否停牌
    limit_up    BOOLEAN DEFAULT FALSE,   -- 是否涨停
    limit_down  BOOLEAN DEFAULT FALSE,   -- 是否跌停
    data_source TEXT    DEFAULT 'akshare',  -- 数据来源
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

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
    open        DOUBLE,                   -- 复权开盘价（=qfq_open）
    high        DOUBLE,                   -- 复权最高价（=qfq_high）
    low         DOUBLE,                   -- 复权最低价（=qfq_low）
    close       DOUBLE,                   -- 复权收盘价（=qfq_close）
    pre_close   DOUBLE,                   -- 前复权收盘价
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
    is_suspend  BOOLEAN DEFAULT FALSE,   -- 是否停牌
    limit_up    BOOLEAN DEFAULT FALSE,   -- 是否涨停
    limit_down  BOOLEAN DEFAULT FALSE,   -- 是否跌停
    data_source TEXT    DEFAULT 'akshare',  -- 数据来源
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

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
-- 6. corporate_actions - 除权除息事件表（对齐 data_engine.py）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 来源：adj_factor 跳变检测 或 Baostock 分红数据
-- INSERT 由 adj_factor 跳变检测自动执行
CREATE TABLE IF NOT EXISTS corporate_actions (
    ts_code       VARCHAR NOT NULL,   -- 证券代码
    action_date   DATE    NOT NULL,   -- 除权除息生效日期
    ann_date      DATE,               -- 公告日期（用于PIT约束）
    prev_adj      DOUBLE,             -- 变化前复权因子
    curr_adj      DOUBLE,             -- 变化后复权因子
    action_type   VARCHAR,            -- dividend/split/reverse_split/rights_issue
    change_ratio  DOUBLE,             -- 变化幅度（curr/prev - 1）
    reason        VARCHAR,             -- 事件描述（如"分红0.5元/股"）
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ts_code, action_date)
);

CREATE INDEX IF NOT EXISTS idx_action_code_date
    ON corporate_actions(ts_code, action_date, ann_date);


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
-- 8. adj_factor_log - 复权因子变更日志（对齐 data_engine.py）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 记录相邻交易日之间的复权因子跳变（>5%），用于检测分红/拆股事件
-- INSERT 由 _check_adj_factor_change() 自动执行
CREATE TABLE IF NOT EXISTS adj_factor_log (
    ts_code        TEXT      NOT NULL,   -- 股票代码
    trade_date     DATE      NOT NULL,   -- 发生跳变的交易日
    adj_factor_old DOUBLE,              -- 跳变前的因子值
    adj_factor_new DOUBLE,              -- 跳变后的因子值
    change_ratio   DOUBLE,              -- 变化幅度（绝对值 > 5% 才记录）
    detected_at    TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,  -- 检测时间

    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_adj_factor_code
    ON adj_factor_log(ts_code, trade_date);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 9. sync_progress - 增量同步进度表
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS sync_progress (
    -- 实际使用的结构（与 data_engine.py __init_schema__ 对齐）：
    -- 每只股票 × 每个表独立跟踪，支持断点续跑
    ts_code        TEXT       NOT NULL,   -- 股票代码
    table_name     TEXT       NOT NULL,   -- 目标表: daily_bar_raw / daily_bar_adjusted / stock_basic_history
    last_sync_date DATE,                  -- 最后成功同步的交易日（断点续跑核心）
    last_sync_at   TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,  -- 最后同步时间戳
    total_records  INTEGER    DEFAULT 0,  -- 累计同步记录数
    status         TEXT       DEFAULT 'ok',  -- ok / failed / in_progress
    error_msg      TEXT,                  -- 最后一次错误信息

    PRIMARY KEY (ts_code, table_name)
);

CREATE INDEX IF NOT EXISTS idx_sync_progress_date
    ON sync_progress(last_sync_date);

CREATE INDEX IF NOT EXISTS idx_sync_status
    ON sync_progress(status, ts_code);


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 10. data_quality_alert - 数据质量告警表
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 10. data_quality_alert - 数据质量告警表（对齐 data_engine.py）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- INSERT 由 run_data_quality_check() 和 data_qa_pipeline.py 批量执行
-- 校验项（14项）：duplicate_rows / ohlc_violation / pct_chg_extreme /
--   adj_factor_jump / zero_volume_non_suspend / delisted_with_future_data /
--   adj_raw_ratio_invalid / volume_amount_inconsistent / index_date_invalid /
--   pit_universe_size_suspicious /
--   raw_adj_row_count_mismatch / raw_adj_pk_missing / qfq_close_mismatch / adj_factor_invalid
CREATE SEQUENCE IF NOT EXISTS seq_dqa_id START 1;

CREATE TABLE IF NOT EXISTS data_quality_alert (
    id          INTEGER   PRIMARY KEY DEFAULT nextval('seq_dqa_id'),
    alert_type  TEXT,                  -- 告警类型（如 'duplicate_rows'）
    ts_code     TEXT,                  -- 股票代码（可选）
    trade_date  DATE,                  -- 交易日期（可选）
    detail      TEXT,                  -- 详细描述
    created_at  TIMESTAMP DEFAULT NOW() -- 记录时间
);

CREATE INDEX IF NOT EXISTS idx_alert_code_date
    ON data_quality_alert(ts_code, trade_date);

CREATE INDEX IF NOT EXISTS idx_alert_type
    ON data_quality_alert(alert_type);


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
-- 12. update_log - 数据更新日志（对齐 data_engine.py）
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE IF NOT EXISTS update_log (
    id            INTEGER   PRIMARY KEY,
    table_name    VARCHAR,               -- 目标表名
    ts_code       VARCHAR,               -- 股票代码（可选）
    start_date    DATE,                  -- 起始日期
    end_date      DATE,                  -- 结束日期
    record_count  INTEGER,               -- 本次同步记录数
    update_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 更新时间
    status        VARCHAR,               -- ok / failed / in_progress
    error_message VARCHAR                -- 错误信息
);

CREATE INDEX IF NOT EXISTS idx_update_table_time
    ON update_log(table_name, update_time);


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
