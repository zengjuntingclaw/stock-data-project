import duckdb
DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=== TABLE ROW COUNTS ===")
for t in ['stock_basic_history','stock_basic','daily_bar_raw','daily_bar_adjusted',
          'corporate_actions','st_status_history','index_constituents_history',
          'trade_calendar','data_quality_alert']:
    cnt = conn.execute('SELECT COUNT(*) FROM %s' % t).fetchone()[0]
    print('  %s: %s' % (t, cnt))

print("\n=== STOCK_BASIC_HISTORY ===")
total = conn.execute('SELECT COUNT(*) FROM stock_basic_history').fetchone()[0]
delisted = conn.execute('SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE').fetchone()[0]
null_date = conn.execute('SELECT COUNT(*) FROM stock_basic_history WHERE list_date IS NULL').fetchone()[0]
sentinel = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date='2099-12-31'").fetchone()[0]
print('  Total: %s, Delisted: %s, No IPO date: %s, Sentinel: %s' % (total, delisted, null_date, sentinel))

index_pollution = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%指数%' OR name LIKE '%指基%'").fetchone()[0]
print('  Index pollution: %s' % index_pollution)

print("\nExchange distribution:")
ex_dist = conn.execute('SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC').fetchall()
for e, c in ex_dist: print('  %s: %s' % (e, c))

print("\nSamples:")
sample = conn.execute('SELECT ts_code,name,exchange,list_date,is_delisted FROM stock_basic_history LIMIT 5').fetchall()
for s in sample: print('  %s' % (s,))

print("\n=== DATA QUALITY ===")
dup_adj = conn.execute('SELECT COUNT(*) - COUNT(DISTINCT ts_code||trade_date) FROM daily_bar_adjusted').fetchone()[0]
dup_raw = conn.execute('SELECT COUNT(*) - COUNT(DISTINCT ts_code||trade_date) FROM daily_bar_raw').fetchone()[0]
ca_cnt = conn.execute('SELECT COUNT(*) FROM corporate_actions').fetchone()[0]
print('  daily_bar_adjusted duplicate rows: %s' % dup_adj)
print('  daily_bar_raw duplicate rows: %s' % dup_raw)
print('  corporate_actions rows: %s' % ca_cnt)

conn.close()
