import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=" * 60)
print("Phase 5 Step 3: Incremental Sync Analysis")
print("=" * 60)

daily_max = conn.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
daily_min = conn.execute("SELECT MIN(trade_date) FROM daily_bar_adjusted").fetchone()[0]
total_active = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = FALSE
""").fetchone()[0]
total_all = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]

print(f"\n[1] Data range: {daily_min} to {daily_max}")
print(f"    stock_basic_history total: {total_all}, active: {total_active}")

# sync_progress breakdown
print("\n[2] sync_progress")
sp_total = conn.execute("SELECT COUNT(DISTINCT ts_code) FROM sync_progress").fetchone()[0]
sp_active_tracked = conn.execute("""
    SELECT COUNT(*) FROM sync_progress sp
    JOIN stock_basic_history sbh ON sbh.ts_code = sp.ts_code
    WHERE sbh.is_delisted = FALSE
""").fetchone()[0]
print(f"    Total tracked stocks: {sp_total}")
print(f"    Active stocks tracked: {sp_active_tracked}")
print(f"    Active stocks NOT tracked: {total_active - sp_active_tracked}")

# Stale sync
stale = conn.execute("""
    SELECT COUNT(DISTINCT ts_code) FROM sync_progress
    WHERE last_sync_date < '2026-04-01'::DATE
""").fetchone()[0]
print(f"    Stale sync (< 2026-04-01): {stale}")

# New records needed
missing = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history sbh
    WHERE is_delisted = FALSE
      AND ts_code NOT IN (SELECT ts_code FROM sync_progress)
""").fetchone()[0]
print(f"    New active stocks to add: {missing}")

# sync_progress by table
print("\n[3] sync_progress by table")
sp = conn.execute("""
    SELECT table_name, COUNT(*) as cnt, MAX(last_sync_date) as max_date
    FROM sync_progress GROUP BY table_name
""").fetchdf()
print(sp.to_string(index=False))

print(f"\n[4] Conclusion:")
print(f"    sync_progress tracked: {sp_total} stocks (from initial v3 download)")
print(f"    Missing from sync: {total_all - sp_total} stocks (not in initial download)")
print(f"    Need: insert {missing} missing active stocks")
print(f"    Need: refresh {stale} stale records")

conn.close()
