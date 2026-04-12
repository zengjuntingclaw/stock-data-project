import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

# Check if _old_backup still exists
old_backup = conn.execute("""
    SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_basic_history_old_backup'
""").fetchone()[0]
print(f"stock_basic_history_old_backup exists: {old_backup}")

# Check v2 count
v2 = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2").fetchone()[0]
print(f"stock_basic_history_v2 rows: {v2}")

# Drop duplicates
print("\nDropping duplicates...")
conn.execute("DROP TABLE IF EXISTS stock_basic_history_v2")
conn.execute("DROP TABLE IF EXISTS stock_basic_history_old_backup")
conn.commit()

# Verify
tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
print(f"Tables after cleanup: {tables}")

# Final status
print("\nFinal stock_basic_history:")
r = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN is_delisted = TRUE THEN 1 ELSE 0 END) as delisted,
        SUM(CASE WHEN is_st = TRUE THEN 1 ELSE 0 END) as st,
        SUM(CASE WHEN is_delisted = FALSE AND is_st = FALSE THEN 1 ELSE 0 END) as active_normal,
        SUM(CASE WHEN is_delisted = FALSE AND is_st = TRUE THEN 1 ELSE 0 END) as active_st
    FROM stock_basic_history
""").fetchone()
print(f"  Total: {r[0]}")
print(f"  Active (normal): {r[3]}")
print(f"  Active (ST):     {r[4]}")
print(f"  Delisted:        {r[1]}")
print(f"  ST (all time):   {r[2]}")

conn.close()
print("\nDONE")
