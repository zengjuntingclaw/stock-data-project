import sys, os
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
lines = []

# Test 1: 正确的 PIT 查询 - 对每个 ts_code 取 eff_date <= trade_date 的最新记录
query1 = """
SELECT COUNT(*) FROM (
    SELECT ts_code, MAX(eff_date) as latest_eff
    FROM stock_basic_history
    WHERE eff_date <= '2024-06-01'
    GROUP BY ts_code
    HAVING MAX(eff_date) IS NOT NULL
) latest
JOIN stock_basic_history s ON s.ts_code = latest.ts_code AND s.eff_date = latest.latest_eff
WHERE s.list_date <= '2024-06-01' AND (s.delist_date IS NULL OR s.delist_date > '2024-06-01')
"""
cnt1 = db.execute(query1).fetchone()[0]
lines.append("FIXED_PIT_ACTIVE_2024-06-01:" + str(cnt1))

# Test 2: index_constituents - 正确逻辑 in_date <= trade_date AND (out_date > trade_date OR out_date IS NULL)
query2 = """
SELECT COUNT(*) FROM index_constituents_history
WHERE index_code = '000300.XSHG'
  AND in_date <= '2024-06-01'
  AND (out_date > '2024-06-01' OR out_date IS NULL)
"""
cnt2 = db.execute(query2).fetchone()[0]
lines.append("FIXED_IDX_300_2024-06-01:" + str(cnt2))

# Test 3: 用 in_date <= trade_date AND out_date IS NULL (当前数据只有 2020-01-01 in_date, out_date 都是 NULL)
query3 = """
SELECT COUNT(*) FROM index_constituents_history
WHERE index_code = '000300.XSHG'
  AND in_date <= '2024-06-01'
  AND out_date IS NULL
"""
cnt3 = db.execute(query3).fetchone()[0]
lines.append("IDX_300_2024-06-01_null_out:" + str(cnt3))

# 查 eff_date 分布（每年多少条）
for year in [2019, 2020, 2021, 2022, 2023, 2024, 2025]:
    cnt = db.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date >= '" + str(year) + "-01-01' AND eff_date < '" + str(year+1) + "-01-01'").fetchone()[0]
    lines.append("YEAR_" + str(year) + ":" + str(cnt))

db.close()
with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\sql_test_result.txt', 'w') as f:
    f.write('\n'.join(lines))
