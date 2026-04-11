import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

db = duckdb.connect('C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb', read_only=True)

def schema(t):
    try:
        cols = db.execute(f"PRAGMA table_info('{t}')").df()
        print(f'\n[{t}]')
        for _, r in cols.iterrows():
            print(f'  {r["name"]:30s} {r["ctype"]:10s}  pk={r["pk"]}')
    except Exception as e:
        print(f'[{t}] ERROR: {e}')

for t in ['stock_basic_history','corporate_actions','index_constituents_history',
          'daily_bar_adjusted','daily_bar_raw','st_status_history','data_quality_alert']:
    schema(t)

print("\n\n=== 样本数据 ===")
print("\n[stock_basic_history] sample:")
r = db.execute("SELECT * FROM stock_basic_history LIMIT 3").df()
for c in r.columns: print(f'  {c}: {list(r[c])}')

print("\n[index_constituents_history] sample:")
r = db.execute("SELECT * FROM index_constituents_history LIMIT 5").df()
for c in r.columns: print(f'  {c}: {list(r[c])}')

print("\n[corporate_actions] sample:")
r = db.execute("SELECT * FROM corporate_actions LIMIT 5").df()
for c in r.columns: print(f'  {c}: {list(r[c])}')

print("\n[st_status_history] sample companies:")
r = db.execute("SELECT ts_code,trade_date,is_st,reason FROM st_status_history LIMIT 10").df()
print(r.to_string())

db.close()
