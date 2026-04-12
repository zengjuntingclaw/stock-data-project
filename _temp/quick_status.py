import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
c = duckdb.connect(DB, read_only=True)

tables = [r[0] for r in c.execute('SHOW TABLES').fetchall()]
print("Tables:", tables)

# Key metrics
corp = c.execute('SELECT COUNT(*) FROM corporate_actions').fetchone()[0]
st = c.execute('SELECT COUNT(*) FROM st_status_history').fetchone()[0]
sp_latest = c.execute('SELECT MAX(last_sync_date) FROM sync_progress').fetchone()[0]
daily_latest = c.execute('SELECT MAX(trade_date) FROM daily_bar_adjusted').fetchone()[0]
daily_count = c.execute('SELECT COUNT(*) FROM daily_bar_adjusted').fetchone()[0]
idx = c.execute('SELECT COUNT(DISTINCT index_code) FROM index_constituents_history').fetchone()[0]
delist_sentinel = c.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date='2099-12-31'").fetchone()[0]

print(f"corporate_actions:         {corp:,}")
print(f"st_status_history:         {st:,}")
print(f"sync_progress latest:      {sp_latest}")
print(f"daily_bar latest:          {daily_latest}")
print(f"daily_bar count:          {daily_count:,}")
print(f"index_constituents_codes: {idx}")
print(f"delist sentinel:          {delist_sentinel} (should be 2)")

# Table sizes
for t in tables:
    n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n:,}")

c.close()
