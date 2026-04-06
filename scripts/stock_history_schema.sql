-- 股票历史名称表（消除幸存者偏差）
-- 用于获取"交易当日"的股票名称（判断ST状态）
CREATE TABLE IF NOT EXISTS stock_name_history (
    ts_code VARCHAR,
    name VARCHAR,
    start_date DATE,
    end_date DATE,
    is_st BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ts_code, start_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_name_history_code ON stock_name_history(ts_code);
CREATE INDEX IF NOT EXISTS idx_name_history_date ON stock_name_history(start_date, end_date);

-- 股票历史行业分类表
-- 用于获取"交易当日"的行业分类
CREATE TABLE IF NOT EXISTS industry_history (
    ts_code VARCHAR,
    industry VARCHAR,
    industry_code VARCHAR,
    source VARCHAR DEFAULT 'sw',  -- 申万/中信
    start_date DATE,
    end_date DATE,
    PRIMARY KEY (ts_code, start_date, source)
);

CREATE INDEX IF NOT EXISTS idx_industry_code ON industry_history(ts_code);
CREATE INDEX IF NOT EXISTS idx_industry_date ON industry_history(start_date, end_date);

-- 查询函数：获取指定日期的股票名称
CREATE OR REPLACE FUNCTION get_stock_name_at_date(
    code VARCHAR, 
    query_date DATE
) RETURNS TABLE (
    ts_code VARCHAR,
    name VARCHAR,
    is_st BOOLEAN
) AS $$
    SELECT ts_code, name, is_st
    FROM stock_name_history
    WHERE ts_code = code
      AND start_date <= query_date
      AND (end_date IS NULL OR end_date >= query_date)
    ORDER BY start_date DESC
    LIMIT 1;
$$;

-- 查询函数：获取指定日期的行业分类
CREATE OR REPLACE FUNCTION get_industry_at_date(
    code VARCHAR,
    query_date DATE,
    src VARCHAR DEFAULT 'sw'
) RETURNS TABLE (
    ts_code VARCHAR,
    industry VARCHAR,
    industry_code VARCHAR
) AS $$
    SELECT ts_code, industry, industry_code
    FROM industry_history
    WHERE ts_code = code
      AND source = src
      AND start_date <= query_date
      AND (end_date IS NULL OR end_date >= query_date)
    ORDER BY start_date DESC
    LIMIT 1;
$$;
