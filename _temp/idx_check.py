import sys, os
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
lines = []
# 查 index_code 唯一值
codes = db.execute("SELECT DISTINCT index_code FROM index_constituents_history").fetchall()
for c in codes:
    lines.append("IDX_CODE:" + str(c[0]))
# 样本记录
for r in db.execute("SELECT * FROM index_constituents_history LIMIT 3").fetchall():
    lines.append("SAMPLE:" + str(r))
# 试不同的 index_code 格式
for idx in ['000300', '000300.XSHG', '399300.XSHE', '000300.SH', '399300.SZ']:
    cnt = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE index_code = ?", [idx]).fetchone()[0]
    lines.append("TRY_" + idx + ":" + str(cnt))
# in_date 的值
dates = db.execute("SELECT DISTINCT in_date FROM index_constituents_history ORDER BY in_date").fetchall()
for d in dates:
    lines.append("IN_DATE:" + str(d[0]))
# out_date 的值
odates = db.execute("SELECT DISTINCT out_date FROM index_constituents_history ORDER BY out_date").fetchall()
for d in odates:
    lines.append("OUT_DATE:" + str(d[0]))
db.close()
with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\idx_check_result.txt', 'w') as f:
    f.write('\n'.join(lines))
