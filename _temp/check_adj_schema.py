import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
c = duckdb.connect(DB, read_only=True)
raw_cols = [r[0] for r in c.execute("DESCRIBE daily_bar_raw").fetchall()]
adj_cols = [r[0] for r in c.execute("DESCRIBE daily_bar_adjusted").fetchall()]
print("daily_bar_raw:", raw_cols)
print("daily_bar_adjusted:", adj_cols)
print()
# Sample from daily_bar_adjusted
row = c.execute("SELECT * FROM daily_bar_adjusted LIMIT 1").fetchone()
print("Sample adj row:", row)
c.close()
