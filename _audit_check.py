"""深度审查数据质量"""
import duckdb

conn = duckdb.connect('data/stock_data.duckdb', read_only=True)

# 1. market字段分布
print('=== market字段分布 ===')
df = conn.execute('SELECT market, COUNT(*) as cnt FROM stock_basic GROUP BY market ORDER BY cnt DESC').fetchdf()
for _, row in df.iterrows():
    print(f'  {repr(row.market)}: {row.cnt}')

# 2. 北交所股票
print()
print('=== 北交所股票(bj后缀) ===')
bj = conn.execute("""
    SELECT ts_code, symbol, list_date, delist_date, market 
    FROM stock_basic 
    WHERE ts_code LIKE '% BJ' OR ts_code LIKE '%BJ'
    LIMIT 20
""").fetchdf()
print(f'北交所股票数: {len(bj)}')
print(bj.head(10).to_string())

# 3. 北交所行情数据
print()
print('=== daily_quotes 北交所数据 ===')
bj_q = conn.execute("""
    SELECT COUNT(DISTINCT ts_code) as cnt, COUNT(*) as rows 
    FROM daily_quotes 
    WHERE ts_code LIKE '% BJ'
""").fetchdf()
print(bj_q.to_string())

# 4. is_suspend字段
print()
print('=== daily_quotes is_suspend分布 ===')
sus = conn.execute('SELECT is_suspend, COUNT(*) as cnt FROM daily_quotes GROUP BY is_suspend').fetchdf()
print(sus.to_string())

# 5. 上市日期真实性
print()
print('=== 上市日期分布 ===')
real_dates = conn.execute("""
    SELECT list_date, COUNT(*) as cnt 
    FROM stock_basic 
    WHERE list_date > '2000-01-01'
    GROUP BY list_date 
    ORDER BY cnt DESC 
    LIMIT 15
""").fetchdf()
print(real_dates.to_string())

# 6. 4开头代码分布
print()
print('=== 4/8开头股票 ===')
codes_48 = conn.execute("""
    SELECT ts_code, symbol, market, list_date
    FROM stock_basic 
    WHERE symbol LIKE '4%' OR symbol LIKE '8%'
    LIMIT 20
""").fetchdf()
print(f'4/8开头股票数: {len(codes_48)}')
print(codes_48.head(10).to_string())

# 7. stock_basic全量概览
print()
print('=== stock_basic全量概览 ===')
total = conn.execute('SELECT COUNT(*) as cnt FROM stock_basic').fetchdf()
print(f'总记录: {total.iloc[0,0]}')
delisted = conn.execute('SELECT COUNT(*) as cnt FROM stock_basic WHERE is_delisted = TRUE').fetchdf()
print(f'退市: {delisted.iloc[0,0]}')
null_list = conn.execute('SELECT COUNT(*) as cnt FROM stock_basic WHERE list_date IS NULL').fetchdf()
print(f'无上市日期: {null_list.iloc[0,0]}')

# 8. 样本ts_code
print()
print('=== 样本ts_code ===')
samples = conn.execute('SELECT DISTINCT ts_code FROM stock_basic LIMIT 30').fetchdf()
for s in samples['ts_code'].values:
    print(f'  {s}')

conn.close()
