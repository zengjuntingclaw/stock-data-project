import duckdb
db = duckdb.connect('C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb', read_only=True)

for tbl in ['daily_bar_adjusted','daily_bar_raw','stock_basic_history','stock_basic','corporate_actions',
            'index_constituents_history','st_status_history','data_quality_alert']:
    cols = db.execute(f"PRAGMA table_info('{tbl}')").fetchall()
    print(f'\n[{tbl}]  PK=', end='')
    pk = [c[5] for c in cols if c[5]]
    print(pk)
    for c in cols:
        print(f'  {str(c[1]):30s}  {str(c[2]):10s}  null={c[3]}  default={c[4]}')
db.close()
