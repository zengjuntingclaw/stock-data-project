"""
Phase 5 Step 2 (fix): Add is_st column to stock_basic_history.
DuckDB Dependency Error workaround: 
  1. Create new table with correct schema (+is_st BOOLEAN)
  2. Copy data with is_st populated from st_status_history
  3. Drop old table, rename new table
"""
import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("=" * 60)
print("Phase 5 Step 2: Add is_st (DuckDB ALTER workaround)")
print("=" * 60)

# Step 1: Create new table with correct schema
print("\n[1] Creating stock_basic_history_v2 with is_st column")
conn.execute("""
    CREATE TABLE stock_basic_history_v2 AS
    SELECT 
        sbh.ts_code, sbh.symbol, sbh.name, sbh.exchange, sbh.area,
        sbh.industry, sbh.market, sbh.list_date, sbh.delist_date,
        sbh.is_delisted, sbh.delist_reason, sbh.board, sbh.eff_date,
        sbh.end_date, sbh.created_at, sbh.is_suspended,
        CASE 
            WHEN latest_st.is_st IS NOT NULL THEN latest_st.is_st
            ELSE FALSE
        END AS is_st
    FROM stock_basic_history sbh
    LEFT JOIN (
        SELECT ts_code, is_st
        FROM (
            SELECT ts_code, trade_date, is_st,
                   ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
            FROM st_status_history
        )
        WHERE rn = 1
    ) latest_st ON latest_st.ts_code = sbh.ts_code
""")
n = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2").fetchone()[0]
print(f"  Created with {n} rows")

# Step 2: Check the new table
print("\n[2] Verifying new table")
cols = [r[0] for r in conn.execute("DESCRIBE stock_basic_history_v2").fetchall()]
print(f"  Columns: {cols}")
st_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2 WHERE is_st = TRUE").fetchone()[0]
susp_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2 WHERE is_suspended = TRUE").fetchone()[0]
delist_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2 WHERE is_delisted = TRUE").fetchone()[0]
print(f"  ST: {st_count}, Suspended: {susp_count}, Delisted: {delist_count}")

# Step 3: Check if stock_basic depends on old table
print("\n[3] Checking dependencies")
refs = conn.execute("""
    SELECT table_name, column_name, foreign_table_name
    FROM duckdb_constraints()
    WHERE foreign_table_name = 'stock_basic_history'
""").fetchdf()
print(f"  Tables referencing stock_basic_history: {len(refs)}")
if len(refs) > 0:
    print(refs.to_string(index=False))

# Step 4: Swap tables
print("\n[4] Swapping tables")
# Rename old to _old
conn.execute("ALTER TABLE stock_basic_history RENAME TO stock_basic_history_old")
print("  Renamed stock_basic_history -> stock_basic_history_old")
# Rename new to stock_basic_history
conn.execute("ALTER TABLE stock_basic_history_v2 RENAME TO stock_basic_history")
print("  Renamed stock_basic_history_v2 -> stock_basic_history")
# Drop old
conn.execute("DROP TABLE stock_basic_history_old")
print("  Dropped stock_basic_history_old")
conn.commit()

# Step 5: Final verification
print("\n[5] Final verification")
final_cols = [r[0] for r in conn.execute("DESCRIBE stock_basic_history").fetchall()]
print(f"  Columns: {final_cols}")
final_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
st_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
susp_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_suspended = TRUE").fetchone()[0]
delist_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = TRUE").fetchone()[0]
print(f"  Total: {final_count}, ST: {st_final}, Suspended: {susp_final}, Delisted: {delist_final}")

# Sample ST stocks
print("\n[6] Sample ST stocks")
sample = conn.execute("""
    SELECT ts_code, name, list_date, is_delisted, is_st, is_suspended
    FROM stock_basic_history WHERE is_st = TRUE LIMIT 5
""").fetchdf()
print(sample.to_string(index=False))

# Sample suspended stocks
print("\n[7] Sample suspended stocks (>30 consecutive zero-volume days)")
susp = conn.execute("""
    SELECT ts_code, name, list_date, is_delisted, is_suspended
    FROM stock_basic_history WHERE is_suspended = TRUE LIMIT 5
""").fetchdf()
print(susp.to_string(index=False))

conn.close()
print("\nDONE")
