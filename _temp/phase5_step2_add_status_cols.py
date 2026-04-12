"""
Phase 5 Step 2: Add is_st / is_delisted / is_suspended to stock_basic_history.
Data source: st_status_history (391,157 rows, already filled)
Strategy:
  - is_delisted: from stock_basic_history.delist_date (already has real dates)
  - is_st: derive from st_status_history - latest entry per stock
  - is_suspended: detect from daily_bar - volume=0 consecutive days > threshold
"""
import sys, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("=" * 60)
print("Phase 5 Step 2: Add is_st / is_delisted / is_suspended")
print("=" * 60)

# --- Check st_status_history structure ---
print("\n[1] st_status_history structure")
st_sample = conn.execute("""
    SELECT * FROM st_status_history LIMIT 5
""").fetchdf()
print(st_sample.to_string())
st_count = conn.execute("SELECT COUNT(DISTINCT ts_code) FROM st_status_history").fetchone()[0]
print(f"Distinct stocks: {st_count}")
st_cols = [r[0] for r in conn.execute("DESCRIBE st_status_history").fetchall()]
print(f"Columns: {st_cols}")

# --- Latest ST status per stock ---
print("\n[2] Latest ST status per stock")
latest_st = conn.execute("""
    WITH ranked AS (
        SELECT ts_code, trade_date, is_st,
               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
        FROM st_status_history
    )
    SELECT ts_code, trade_date, is_st
    FROM ranked WHERE rn = 1
""").fetchdf()
print(f"Total stocks in st_history: {len(latest_st)}")
st_stocks = latest_st[latest_st['is_st'] == True]
print(f"Currently ST: {len(st_stocks)}")
non_st = latest_st[latest_st['is_st'] == False]
print(f"Currently non-ST: {len(non_st)}")
print("ST stocks sample:")
print(st_stocks.head(10).to_string(index=False))

# --- Check if stock_basic_history has the columns ---
print("\n[3] stock_basic_history current columns")
sbh_cols = [r[0] for r in conn.execute("DESCRIBE stock_basic_history").fetchall()]
print(f"Columns: {sbh_cols}")
has_st = 'is_st' in sbh_cols
has_suspended = 'is_suspended' in sbh_cols
print(f"Has is_st: {has_st}, Has is_suspended: {has_suspended}")

# --- Add columns if missing ---
if not has_st:
    print("\n[4] Adding is_st column")
    conn.execute("ALTER TABLE stock_basic_history ADD COLUMN is_st BOOLEAN DEFAULT FALSE")
    print("  Added is_st")
if not has_suspended:
    print("\n[5] Adding is_suspended column")
    conn.execute("ALTER TABLE stock_basic_history ADD COLUMN is_suspended BOOLEAN DEFAULT FALSE")
    print("  Added is_suspended")

# --- Update is_st from st_status_history ---
print("\n[6] Updating is_st from st_status_history")
# Join with latest ST status
conn.execute("""
    WITH latest_st AS (
        SELECT ts_code, is_st,
               ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
        FROM st_status_history
    )
    UPDATE stock_basic_history AS sbh
    SET is_st = lst.is_st
    FROM latest_st lst
    WHERE lst.ts_code = sbh.ts_code AND lst.rn = 1
""")
conn.commit()

st_updated = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE
""").fetchone()[0]
print(f"  Stocks marked as ST: {st_updated}")

# --- Derive is_suspended from daily_bar (consecutive volume=0) ---
print("\n[7] Detecting suspended stocks from daily_bar (consecutive volume=0 > 30 days)")
suspended = conn.execute("""
    WITH daily_vol AS (
        SELECT ts_code, trade_date, volume,
               CASE WHEN volume = 0 THEN 1 ELSE 0 END as is_zero
        FROM daily_bar_raw
    ),
    streaks AS (
        SELECT ts_code, trade_date, is_zero,
               SUM(CASE WHEN is_zero = 0 THEN 1 ELSE 0 END)
                   OVER (PARTITION BY ts_code ORDER BY trade_date) as grp
        FROM daily_vol
    ),
    max_streak AS (
        SELECT ts_code, MAX(CASE WHEN is_zero = 1 THEN COUNT(*) END) as max_susp_days
        FROM streaks
        GROUP BY ts_code
    )
    SELECT ts_code, max_susp_days
    FROM max_streak
    WHERE max_susp_days > 30
    ORDER BY max_susp_days DESC
""").fetchdf()

print(f"  Stocks suspended >30 consecutive days: {len(suspended)}")
if len(suspended) > 0:
    print(suspended.head(20).to_string(index=False))
    
    # Update is_suspended in stock_basic_history
    susp_list = [r for r in suspended['ts_code'].tolist()]
    placeholders = ','.join(['?' for _ in susp_list])
    conn.execute(f"""
        UPDATE stock_basic_history SET is_suspended = TRUE
        WHERE ts_code IN ({placeholders})
    """, susp_list)
    conn.commit()
    print(f"  Updated {len(susp_list)} stocks as suspended")

susp_updated = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history WHERE is_suspended = TRUE
""").fetchone()[0]
print(f"  Total suspended in stock_basic_history: {susp_updated}")

# --- Final verification ---
print("\n[8] Final stock_basic_history status columns")
final = conn.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN is_delisted = TRUE THEN 1 ELSE 0 END) as delisted,
        SUM(CASE WHEN is_st = TRUE THEN 1 ELSE 0 END) as st,
        SUM(CASE WHEN is_suspended = TRUE THEN 1 ELSE 0 END) as suspended
    FROM stock_basic_history
""").fetchdf()
print(final.to_string(index=False))

# --- Sample rows with new columns ---
print("\n[9] Sample rows with new status columns")
sample = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, is_delisted, is_st, is_suspended
    FROM stock_basic_history
    WHERE is_st = TRUE OR is_suspended = TRUE OR is_delisted = TRUE
    LIMIT 10
""").fetchdf()
print(sample.to_string(index=False))

conn.close()
print("\nDONE - Phase 5 Step 2 complete")
