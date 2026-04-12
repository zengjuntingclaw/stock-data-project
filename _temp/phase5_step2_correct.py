import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("Checking current state of both tables...")
v2_st = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2 WHERE is_st = TRUE").fetchone()[0]
main_st = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
v2_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2").fetchone()[0]
main_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
print(f"stock_basic_history_v2: {v2_count} rows, {v2_st} ST")
print(f"stock_basic_history:     {main_count} rows, {main_st} ST")

# The correct table is stock_basic_history_v2 (has is_st data)
# stock_basic_history (old) has 0 ST
# Fix: rename _v2 -> stock_basic_history (overwrite old)
print("\nFixing: stock_basic_history_v2 has the correct data, stock_basic_history is stale.")
print("Swapping to put _v2 data into stock_basic_history...")

# Step 1: Rename current stock_basic_history to _stale
conn.execute("ALTER TABLE stock_basic_history RENAME TO stock_basic_history_stale")
print("  Renamed stock_basic_history -> stock_basic_history_stale")

# Step 2: Rename stock_basic_history_v2 to stock_basic_history
conn.execute("ALTER TABLE stock_basic_history_v2 RENAME TO stock_basic_history")
print("  Renamed stock_basic_history_v2 -> stock_basic_history")

# Step 3: Drop stale table
conn.execute("DROP TABLE stock_basic_history_stale")
print("  Dropped stock_basic_history_stale")

conn.commit()

# Final verification
print("\nFinal verification:")
final_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
st_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
delist_final = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = TRUE").fetchone()[0]
print(f"  Total: {final_count}, ST: {st_final}, Delisted: {delist_final}")

# Sample
print("\nSample ST stocks:")
sample = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, is_delisted, is_st
    FROM stock_basic_history WHERE is_st = TRUE LIMIT 5
""").fetchdf()
print(sample.to_string(index=False))

# Show all tables
print("\nAll tables:")
tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
print(f"  {tables}")

conn.close()
print("\nDONE")
