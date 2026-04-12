"""
Phase 5 Step 3: Incremental sync (断点续跑)
Read sync_progress, find the latest date per stock, fetch only new data.
"""
import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb, time, os

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=" * 60)
print("Phase 5 Step 3: Incremental Sync Check")
print("=" * 60)

# Latest dates in the DB
print("\n[1] Current data coverage")
daily_max = conn.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
daily_min = conn.execute("SELECT MIN(trade_date) FROM daily_bar_adjusted").fetchone()[0]
print(f"  daily_bar_adjusted range: {daily_min} to {daily_max}")
print(f"  Total rows: {conn.execute('SELECT COUNT(*) FROM daily_bar_adjusted').fetchone()[0]:,}")

# sync_progress breakdown
print("\n[2] sync_progress by table")
sp = conn.execute("""
    SELECT table_name,
           COUNT(*) as stocks,
           MAX(last_sync_date) as max_date,
           MIN(last_sync_date) as min_date
    FROM sync_progress
    GROUP BY table_name
""").fetchdf()
print(sp.to_string(index=False))

# Find stocks with stale sync (older than 2026-04-03)
stale = conn.execute("""
    SELECT COUNT(DISTINCT ts_code) FROM sync_progress
    WHERE last_sync_date < '2026-04-01'
""").fetchone()[0]
print(f"\n  Stocks with stale sync (< 2026-04-01): {stale}")

# New stocks not in sync_progress
missing = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history sbh
    WHERE is_delisted = FALSE
      AND ts_code NOT IN (
          SELECT ts_code FROM sync_progress WHERE table_name = 'daily_bar_adjusted'
      )
""").fetchone()[0]
print(f"  Active stocks NOT in sync_progress: {missing}")

# Recommended sync plan
print("\n[3] Recommended sync plan")
if daily_max >= '2026-04-10':
    print("  Data is up to date (latest >= 2026-04-10)")
    print("  No incremental sync needed.")
else:
    print(f"  Data ends at {daily_max}")
    print(f"  Target: fetch from {daily_max} to latest trading day")
    print(f"  Stale stocks: {stale}, Missing: {missing}")

conn.close()

# Check trade calendar for next trading day
print("\n[4] Trade calendar coverage")
conn2 = duckdb.connect(DB, read_only=True)
cal = conn2.execute("""
    SELECT MAX(trade_date) FROM trade_calendar
    WHERE is_trading_day = TRUE
""").fetchone()[0]
print(f"  Latest trading day in calendar: {cal}")
conn2.close()
print("\nDONE")
