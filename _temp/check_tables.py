import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
old_tables = ['stock_basic', 'daily_quotes', 'index_constituents']
for t in old_tables:
    try:
        count = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f'[OLD] {t}: {count} rows')
    except Exception as e:
        print(f'[OLD] {t}: NOT FOUND ({e})')
new_tables = ['stock_basic_history', 'daily_bar_raw', 'daily_bar_adjusted', 'index_constituents_history', 'adj_factor', 'sync_progress']
for t in new_tables:
    try:
        count = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f'[NEW] {t}: {count} rows')
    except Exception as e:
        print(f'[NEW] {t}: ERROR ({e})')
db.close()
