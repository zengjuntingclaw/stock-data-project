import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
c = duckdb.connect(DB, read_only=True)
cols = [r[0] for r in c.execute("DESCRIBE daily_bar_raw").fetchall()]
print("daily_bar_raw columns:", cols)
# Sample existing row
row = c.execute("SELECT * FROM daily_bar_raw LIMIT 1").fetchone()
print("Sample row:", row)
c.close()
