import sys, os, datetime
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')

import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)

lines = []

tables = db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()
for t in tables:
    cnt = db.execute('SELECT COUNT(*) FROM "' + t[0] + '"').fetchone()[0]
    lines.append('T:' + t[0] + ':' + str(cnt))

pit = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date = (SELECT MAX(eff_date) FROM stock_basic_history)").fetchone()[0]
lines.append('PIT_ACTIVE:' + str(pit))

idx300 = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE index_code = '000300.XSHG' AND '2024-06-01' >= in_date AND (out_date IS NULL OR '2024-06-01' <= out_date)").fetchone()[0]
lines.append('IDX_300_2024-06-01:' + str(idx300))

raw_range = db.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_bar_raw").fetchone()
lines.append('RAW_RANGE:' + str(raw_range[0]) + ':' + str(raw_range[1]))

adj_range = db.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_bar_adjusted").fetchone()
lines.append('ADJ_RANGE:' + str(adj_range[0]) + ':' + str(adj_range[1]))

idx_range = db.execute("SELECT MIN(in_date), MAX(out_date) FROM index_constituents_history").fetchone()
lines.append('IDX_RANGE:' + str(idx_range[0]) + ':' + str(idx_range[1]))

db.close()

with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\db_result.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
