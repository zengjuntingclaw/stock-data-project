import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb
DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)
real = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date!='2099-12-31'").fetchone()[0]
sentinel = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND (delist_date IS NULL OR delist_date='2099-12-31')").fetchone()[0]
print(f"Delisted status: Real dates={real}, Sentinel/Null={sentinel}")
sample = conn.execute("SELECT ts_code,name,list_date,delist_date FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date!='2099-12-31' LIMIT 5").fetchall()
for s in sample:
    print(f"  {s}")
conn.close()
