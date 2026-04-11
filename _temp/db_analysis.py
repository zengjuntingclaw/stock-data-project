import duckdb

db = duckdb.connect('data/stock_data.duckdb', read_only=True)

print("=== stock_basic_history 表分析 ===")

# 总记录数
r = db.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()
print(f"总记录数: {r[0]}")

# 不同的 ts_code 数
r = db.execute("SELECT COUNT(DISTINCT ts_code) FROM stock_basic_history").fetchone()
print(f"不同的 ts_code 数: {r[0]}")

# 当前有效股票池（PIT查询）
r = db.execute("""
    SELECT COUNT(*) FROM (
        SELECT h.ts_code FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE eff_date <= CAST('2026-04-11' AS DATE)
            GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.list_date <= CAST('2026-04-11' AS DATE)
          AND (h.delist_date IS NULL OR h.delist_date > CAST('2026-04-11' AS DATE))
    ) sub
""").fetchone()
print(f"2026-04-11 有效股票池: {r[0]}")

# 2024-01-01 的股票池
r = db.execute("""
    SELECT COUNT(*) FROM (
        SELECT h.ts_code FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE eff_date <= CAST('2024-01-01' AS DATE)
            GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.list_date <= CAST('2024-01-01' AS DATE)
          AND (h.delist_date IS NULL OR h.delist_date > CAST('2024-01-01' AS DATE))
    ) sub
""").fetchone()
print(f"2024-01-01 有效股票池: {r[0]}")

db.close()
print("OK")
