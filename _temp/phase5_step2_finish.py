import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("[3] Checking dependencies on stock_basic_history")
# Check what duckdb_constraints shows
refs = conn.execute("SELECT * FROM duckdb_constraints() LIMIT 5").fetchdf()
print("Constraint columns:", refs.columns.tolist())
print(refs.head())

# Check if stock_basic_history_v2 exists (created in previous run)
exists_v2 = conn.execute("""
    SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_basic_history_v2'
""").fetchone()[0]
exists_old = conn.execute("""
    SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_basic_history_old'
""").fetchone()[0]
exists_main = conn.execute("""
    SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_basic_history'
""").fetchone()[0]
print(f"\nstock_basic_history_v2 exists: {exists_v2}")
print(f"stock_basic_history_old exists: {exists_old}")
print(f"stock_basic_history exists: {exists_main}")

# Final verification
print("\n[5] Final verification")
final_cols = [r[0] for r in conn.execute("DESCRIBE stock_basic_history").fetchall()]
print(f"  Columns: {final_cols}")
final_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
st_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
delist_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = TRUE").fetchone()[0]
print(f"  Total: {final_count}, ST: {st_final}, Delisted: {delist_final}")

# Sample ST stocks
print("\n[6] Sample ST stocks")
sample = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, is_delisted, is_st
    FROM stock_basic_history WHERE is_st = TRUE LIMIT 5
""").fetchdf()
print(sample.to_string(index=False))

conn.close()
print("\nDONE")
