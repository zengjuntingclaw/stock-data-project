import sys, os, datetime
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')

import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)

lines = []

# PIT 表的 eff_date 分布
eff_dates = db.execute("SELECT eff_date, COUNT(*) FROM stock_basic_history GROUP BY eff_date ORDER BY eff_date DESC LIMIT 10").fetchall()
lines.append("--- PIT eff_date 分布 ---")
for d, c in eff_dates:
    lines.append("PIT_DATE:" + str(d) + ":" + str(c))

# 2024-06-01 当天的有效股票
s1 = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date <= '2024-06-01'").fetchone()[0]
lines.append("PIT_eff<=2024-06-01:" + str(s1))

# 2024-06-01 时仍在交易的股票
s2 = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date <= '2024-06-01' AND (delist_date IS NULL OR delist_date > '2024-06-01')").fetchone()[0]
lines.append("PIT_active_on_2024-06-01:" + str(s2))

# index_constituents_history 的 out_date
out_null = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE out_date IS NULL").fetchone()[0]
lines.append("IDX_out_date_null:" + str(out_null))
out_not_null = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE out_date IS NOT NULL").fetchone()[0]
lines.append("IDX_out_date_not_null:" + str(out_not_null))

# 直接查询 2024-06-01 的沪深300成分
idx_test = db.execute("SELECT COUNT(*) FROM index_constituents_history WHERE index_code = '000300.XSHG' AND '2024-06-01' >= in_date AND (out_date IS NULL OR out_date > '2024-06-01')").fetchone()[0]
lines.append("IDX_direct_2024-06-01:" + str(idx_test))

# 用 DataEngine 测试
from scripts.data_engine import DataEngine
de = DataEngine(data_dir='data')
stocks = de.get_active_stocks('2024-06-01')
lines.append("DE_get_active_stocks_2024-06-01:" + str(len(stocks)))

df_idx = de.get_index_constituents('000300.XSHG', '2024-06-01')
lines.append("DE_get_index_000300_2024-06-01:" + str(len(df_idx)))

db.close()

with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\pit_debug.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
