import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)
r = conn.execute("""
    SELECT ts_code, name, list_date, delist_date
    FROM stock_basic_history
    WHERE is_delisted=TRUE AND delist_date='2099-12-31'
""").fetchall()
print("Last 2 sentinel delisted:")
for s in r:
    print(f"  {s}")
conn.close()
