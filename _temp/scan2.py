import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb
db = duckdb.connect('C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb', read_only=True)

# stock_basic_history 真实股票样本
print('=== stock_basic_history 真实股票 ===')
r = db.execute("""
    SELECT ts_code, name, exchange, list_date, delist_date, is_delisted, delist_reason
    FROM stock_basic_history
    WHERE name NOT LIKE '%指数%' AND name NOT LIKE '%指基%'
    LIMIT 10
""").df()
print(r.to_string())

# stock_basic 样本
print('\n=== stock_basic 样本 ===')
r2 = db.execute('SELECT ts_code, name, exchange, list_date, delist_date, is_delisted FROM stock_basic LIMIT 10').df()
print(r2.to_string())

# index_constituents 各指数数量
print('\n=== 指数成分各指数数量 ===')
r3 = db.execute('SELECT index_code, COUNT(*) as cnt FROM index_constituents_history GROUP BY index_code').df()
print(r3.to_string())

# daily_bar_adjusted 样本
print('\n=== daily_bar_adjusted 样本 (000001.SZ 2024) ===')
r4 = db.execute("""
    SELECT ts_code, trade_date, open, high, low, close, adj_factor, volume, pct_chg
    FROM daily_bar_adjusted
    WHERE ts_code='000001.SZ' AND trade_date>='2024-01-01'
    ORDER BY trade_date LIMIT 5
""").df()
print(r4.to_string())

# daily_bar_raw 样本
print('\n=== daily_bar_raw 样本 (000001.SZ 2024) ===')
r5 = db.execute("""
    SELECT ts_code, trade_date, open, high, low, close, adj_factor, volume
    FROM daily_bar_raw
    WHERE ts_code='000001.SZ' AND trade_date>='2024-01-01'
    ORDER BY trade_date LIMIT 5
""").df()
print(r5.to_string())

# adj_factor 一致性验证
print('\n=== adj_factor 一致性验证 ===')
r6 = db.execute("""
    SELECT a.ts_code, a.trade_date, a.adj_factor as adj_adjusted, b.adj_factor as adj_raw
    FROM daily_bar_adjusted a
    JOIN daily_bar_raw b ON a.ts_code=b.ts_code AND a.trade_date=b.trade_date
    WHERE a.ts_code='000001.SZ' AND a.trade_date>='2024-01-01'
    ORDER BY a.trade_date LIMIT 10
""").df()
print(r6.to_string())

# 缺失adj_factor行数
print('\n=== daily_bar_raw adj_factor 缺失 ===')
r7 = db.execute("SELECT COUNT(*) FROM daily_bar_raw WHERE adj_factor IS NULL").fetchone()
print(f'NULL count: {r7[0]:,}')
r8 = db.execute("SELECT COUNT(*) FROM daily_bar_raw WHERE adj_factor = 1.0").fetchone()
print(f'=1.0 count: {r8[0]:,}')

# data_quality_alert 内容
print('\n=== data_quality_alert 内容 ===')
r9 = db.execute("SELECT * FROM data_quality_alert ORDER BY created_at DESC LIMIT 13").df()
print(r9[['check_type','severity','description','ts_code']].to_string())

db.close()
