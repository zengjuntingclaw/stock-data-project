import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb
db = duckdb.connect('C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb')
print("index_constituents_history columns:")
print([c[1] for c in db.execute("PRAGMA table_info('index_constituents_history')").fetchall()])
print("\nindex_constituents_history sample:")
r = db.execute("SELECT * FROM index_constituents_history LIMIT 3").fetchall()
for x in r: print(" ", x)
db.close()
