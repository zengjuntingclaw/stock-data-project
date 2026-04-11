import sys, os
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
lines = []
# eff_date 分布
for d, c in db.execute("SELECT eff_date, COUNT(*) FROM stock_basic_history GROUP BY eff_date ORDER BY eff_date DESC LIMIT 10").fetchall():
    lines.append("TOP_EFF:" + str(d) + ":" + str(c))
# list_date 范围
r = db.execute("SELECT MIN(list_date), MAX(list_date) FROM stock_basic_history").fetchone()
lines.append("LIST_RANGE:" + str(r[0]) + ":" + str(r[1]))
# delist_date 分布
dn = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE delist_date IS NOT NULL").fetchone()[0]
lines.append("DELISTED:" + str(dn))
lines.append("ACTIVE:" + str(5608 - dn))
# 直接用 list_date/delist_date 查询 2024-06-01
cnt = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE list_date <= '2024-06-01' AND (delist_date IS NULL OR delist_date > '2024-06-01')").fetchone()[0]
lines.append("QUERY_2024-06-01:" + str(cnt))
# 查 eff_date 的意义
for d, c in db.execute("SELECT eff_date, COUNT(*) FROM stock_basic_history GROUP BY eff_date ORDER BY eff_date ASC LIMIT 5").fetchall():
    lines.append("EARLY_EFF:" + str(d) + ":" + str(c))
# 样本记录
for r in db.execute("SELECT ts_code, eff_date, list_date, delist_date FROM stock_basic_history LIMIT 5").fetchall():
    lines.append("SAMPLE:" + str(r))
db.close()
with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\pit_analyze_result.txt', 'w') as f:
    f.write('\n'.join(lines))
