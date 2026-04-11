import sys, os
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
lines = []
# PIT eff_date 分布
for d, c in db.execute("SELECT eff_date, COUNT(*) FROM stock_basic_history GROUP BY eff_date ORDER BY eff_date DESC LIMIT 5").fetchall():
    lines.append("PIT:" + str(d) + ":" + str(c))
s1 = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date <= '2024-06-01' AND (delist_date IS NULL OR delist_date > '2024-06-01')").fetchone()[0]
lines.append("ACTIVE_2024-06-01:" + str(s1))
# IDX out_date
lines.append("IDX_out_null:" + str(db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE out_date IS NULL").fetchone()[0]))
lines.append("IDX_out_set:" + str(db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE out_date IS NOT NULL").fetchone()[0]))
# IDX 2024-06-01
for idx in ['000300.XSHG', '000905.XSHG', '000852.XSHG']:
    cnt = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE index_code = ? AND '2024-06-01' >= in_date AND (out_date IS NULL OR out_date >= '2024-06-01')", [idx]).fetchone()[0]
    lines.append("IDX_" + idx + "_2024-06-01:" + str(cnt))
db.close()
with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\pit_result.txt', 'w') as f:
    f.write('\n'.join(lines))
