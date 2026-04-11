import duckdb

db = duckdb.connect('data/stock_data.duckdb', read_only=True)

print("=== stock_basic_history 表结构分析 ===")

# 检查每个 ts_code 的记录数
r = db.execute("""
    SELECT ts_code, COUNT(*) as cnt, MIN(eff_date) as first_eff, MAX(eff_date) as last_eff
    FROM stock_basic_history
    GROUP BY ts_code
    ORDER BY cnt DESC
    LIMIT 10
""").fetchall()

print("ts_code | 记录数 | 首次eff_date | 最近eff_date")
for row in r:
    print(f"  {row[0]}: {row[1]}条 | {row[2]} | {row[3]}")

# 检查有多个 eff_date 的记录
r = db.execute("""
    SELECT COUNT(*) as multi_cnt FROM (
        SELECT ts_code FROM stock_basic_history
        GROUP BY ts_code HAVING COUNT(*) > 1
    ) sub
""").fetchone()
print(f"\n有多个 eff_date 的 ts_code 数: {r[0]}")

db.close()
print("\nOK")
