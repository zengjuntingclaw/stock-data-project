import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=" * 60)
print("Phase 5 Step 2 Final Verification")
print("=" * 60)

# All status columns
print("\n[1] stock_basic_history status summary")
r = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN is_delisted = TRUE THEN 1 ELSE 0 END) as delisted,
        SUM(CASE WHEN is_st = TRUE THEN 1 ELSE 0 END) as st,
        SUM(CASE WHEN is_suspended = TRUE THEN 1 ELSE 0 END) as suspended,
        SUM(CASE WHEN is_delisted = FALSE AND is_st = FALSE AND is_suspended = FALSE THEN 1 ELSE 0 END) as normal
    FROM stock_basic_history
""").fetchone()
print(f"  Total: {r[0]}, Delisted: {r[1]}, ST: {r[2]}, Suspended: {r[3]}, Normal: {r[4]}")

# Sample ST stocks
print("\n[2] Sample ST stocks")
sample = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, is_delisted, is_st
    FROM stock_basic_history WHERE is_st = TRUE LIMIT 5
""").fetchdf()
print(sample.to_string(index=False))

# Query time filter for is_st (as backup approach)
print("\n[3] Query-time ST filter (no column needed)")
r2 = conn.execute("""
    SELECT COUNT(*) as st_count
    FROM (
        SELECT ts_code, is_st
        FROM (
            SELECT ts_code, trade_date, is_st,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
            FROM st_status_history
        )
        WHERE rn = 1 AND is_st = TRUE
    ) latest_st
    JOIN stock_basic_history sbh ON sbh.ts_code = latest_st.ts_code
    WHERE sbh.is_delisted = FALSE
""").fetchone()[0]
print(f"  Active ST stocks (query-time): {r2}")

conn.close()
print("\nPhase 5 Step 2 DONE")
