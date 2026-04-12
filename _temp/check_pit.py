import sys, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

# 1. PIT query for stocks active on a specific date
print("=== PIT Query: Stocks active on 2024-06-01 ===")
active = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, is_delisted
    FROM stock_basic_history
    WHERE list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL)
    LIMIT 10
""").fetchdf()
print(f"Active stocks: {len(active)} (total). Sample:")
print(active.to_string())

print(f"\nTotal active on 2024-06-01:")
cnt = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL)
""").fetchone()[0]
print(f"  {cnt}")

# 2. Check if any 327 delisted stocks are incorrectly included in 2024-06-01
wrong = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted = TRUE
      AND list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchone()[0]
print(f"\nDelisted stocks incorrectly included in 2024-06-01 active pool: {wrong}")

# 3. Check delist_date status
status = conn.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN delist_date IS NULL THEN 1 ELSE 0 END) as null_date,
        SUM(CASE WHEN delist_date = '2099-12-31' THEN 1 ELSE 0 END) as sentinel,
        SUM(CASE WHEN delist_date IS NOT NULL AND delist_date != '2099-12-31' THEN 1 ELSE 0 END) as real_date
    FROM stock_basic_history WHERE is_delisted = TRUE
""").fetchone()
print(f"\nDelisted status: total={status[0]}, null={status[1]}, sentinel={status[2]}, real={status[3]}")

# 4. checkpoint_manager state
print("\n=== checkpoint_manager ===")
try:
    cp_tables = conn.execute("SHOW TABLES").fetchdf()
    cp_tables = cp_tables[cp_tables['name'].str.contains('checkpoint', na=False)]
    print(cp_tables.to_string())
except:
    print("No checkpoint tables found")

conn.close()
