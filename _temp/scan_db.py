import sys, os
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

# 1. All tables and row counts
print("=" * 60)
print("TABLE INVENTORY")
print("=" * 60)
tables = conn.execute("SHOW TABLES").fetchdf()
for _, row in tables.iterrows():
    tname = row['name']
    cnt = conn.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"  {tname}: {cnt:,} rows")

print()
print("=" * 60)
print("SCHEMA VERSION CHECK (data_engine.py _init_schema)")
print("=" * 60)

# Check key fields in stock_basic_history
try:
    cols = [r[1] for r in conn.execute("DESCRIBE stock_basic_history").fetchall()]
    print(f"stock_basic_history columns: {cols}")
except Exception as e:
    print(f"  stock_basic_history error: {e}")

# Check key fields in daily_bar_adjusted
try:
    cols = [r[1] for r in conn.execute("DESCRIBE daily_bar_adjusted").fetchall()]
    print(f"\ndaily_bar_adjusted columns: {cols}")
except Exception as e:
    print(f"  daily_bar_adjusted error: {e}")

# Check key fields in daily_bar_raw
try:
    cols = [r[1] for r in conn.execute("DESCRIBE daily_bar_raw").fetchall()]
    print(f"\ndaily_bar_raw columns: {cols}")
except Exception as e:
    print(f"  daily_bar_raw error: {e}")

# Check sync_progress
try:
    cols = [r[1] for r in conn.execute("DESCRIBE sync_progress").fetchall()]
    rows = conn.execute("SELECT * FROM sync_progress LIMIT 5").fetchdf()
    print(f"\nsync_progress columns: {cols}")
    print(f"sync_progress rows: {conn.execute('SELECT COUNT(*) FROM sync_progress').fetchone()[0]}")
    print(rows[['ts_code','table_name','last_sync_date','status']].to_string())
except Exception as e:
    print(f"  sync_progress error: {e}")

# Check data_quality_alert
try:
    dqa = conn.execute("SELECT COUNT(*) FROM data_quality_alert").fetchone()[0]
    print(f"\ndata_quality_alert: {dqa} rows")
except Exception as e:
    print(f"  data_quality_alert error: {e}")

print()
print("=" * 60)
print("DATA QUALITY SPOT CHECKS")
print("=" * 60)

# Check adj_factor consistency: qfq_close vs raw_close * adj_factor
try:
    check = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN ABS(qfq_close - raw.close * adj.adj_factor) < 0.01 THEN 1 ELSE 0 END) as consistent,
            SUM(CASE WHEN adj.adj_factor IS NULL OR adj.adj_factor <= 0 THEN 1 ELSE 0 END) as bad_factor
        FROM daily_bar_adjusted adj
        JOIN daily_bar_raw raw ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
        WHERE adj.qfq_close IS NOT NULL AND raw.close IS NOT NULL
    """).fetchone()
    print(f"raw/adj consistency: {check[0]:,} rows total, {check[1]:,} consistent, {check[2]} bad adj_factor")
except Exception as e:
    print(f"  adj consistency check error: {e}")

# Check delisted stocks delist_date status
try:
    s = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN delist_date IS NULL THEN 1 WHEN delist_date='2099-12-31' THEN 1 ELSE 0 END) as bad,
            SUM(CASE WHEN delist_date IS NOT NULL AND delist_date != '2099-12-31' THEN 1 ELSE 0 END) as good
        FROM stock_basic_history WHERE is_delisted=TRUE
    """).fetchone()
    print(f"Delisted stocks: {s[0]} total, {s[1]} bad sentinel/NULL date, {s[2]} good")
except Exception as e:
    print(f"  delisted check error: {e}")

# Check index constituents
try:
    idx = conn.execute("""
        SELECT index_code, COUNT(*) as cnt, COUNT(DISTINCT ts_code) as uniq
        FROM index_constituents_history GROUP BY index_code
    """).fetchdf()
    print(f"\nIndex constituents:\n{idx.to_string()}")
except Exception as e:
    print(f"  index constituents error: {e}")

# Check st_status_history
try:
    st = conn.execute("SELECT COUNT(*) FROM st_status_history").fetchone()[0]
    print(f"\nst_status_history: {st} rows")
except Exception as e:
    print(f"  st_status_history error: {e}")

# Check corporate_actions
try:
    ca = conn.execute("SELECT COUNT(*) FROM corporate_actions").fetchone()[0]
    print(f"corporate_actions: {ca} rows")
except Exception as e:
    print(f"  corporate_actions error: {e}")

# Check trade_calendar
try:
    tc = conn.execute("SELECT COUNT(*) FROM trade_calendar").fetchone()[0]
    print(f"trade_calendar: {tc} rows")
except Exception as e:
    print(f"  trade_calendar error: {e}")

conn.close()
