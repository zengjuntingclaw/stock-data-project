"""数据库深度调试"""
import sys
sys.path.insert(0, '.')

print("=" * 60)
print("数据库深度调试")
print("=" * 60)

import duckdb
from scripts.data_engine import DataEngine

engine = DataEngine()
db_path = engine.db_path
conn = duckdb.connect(str(db_path), read_only=True)
cur = conn.cursor()

# Check what stock codes are in daily_bar_raw
print("\n=== daily_bar_raw ts_code 样例 ===")
cur.execute("SELECT DISTINCT ts_code FROM daily_bar_raw LIMIT 10")
codes = [r[0] for r in cur.fetchall()]
print(f"Sample codes: {codes}")

# Check the date range in daily_bar_raw
print("\n=== daily_bar_raw 日期范围 ===")
cur.execute("SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM daily_bar_raw")
min_d, max_d, count = cur.fetchone()
print(f"Range: {min_d} to {max_d}, Count: {count}")

# Try a query with wildcards
print("\n=== 测试 ts_code 模糊匹配 ===")
cur.execute("SELECT ts_code, trade_date, close FROM daily_bar_raw WHERE ts_code LIKE '%600000%' LIMIT 5")
rows = cur.fetchall()
print(f"Results: {rows}")

cur.execute("SELECT ts_code, trade_date, close FROM daily_bar_raw WHERE ts_code LIKE '600000%' LIMIT 5")
rows = cur.fetchall()
print(f"Results (600000%): {rows}")

# Check the exact format of ts_code
print("\n=== ts_code 格式检查 ===")
cur.execute("SELECT DISTINCT ts_code FROM daily_bar_raw WHERE ts_code LIKE '600%' LIMIT 5")
rows = cur.fetchall()
print(f"600xxx codes: {rows}")

cur.execute("SELECT DISTINCT ts_code FROM daily_bar_raw WHERE ts_code LIKE '000%' LIMIT 5")
rows = cur.fetchall()
print(f"000xxx codes: {rows}")

# Check stock_basic_history
print("\n=== stock_basic_history 样例 ===")
cur.execute("SELECT ts_code, list_date, delist_date FROM stock_basic_history LIMIT 5")
rows = cur.fetchall()
print(f"Records: {rows}")

# Check index_constituents_history
print("\n=== index_constituents_history 结构 ===")
cur.execute("SELECT COUNT(*), MIN(in_date), MAX(in_date), COUNT(out_date) FROM index_constituents_history")
count, min_in, max_in, out_count = cur.fetchone()
print(f"Count: {count}, in_date range: {min_in} to {max_in}, records with out_date: {out_count}")

cur.execute("SELECT index_code, ts_code, in_date, out_date FROM index_constituents_history LIMIT 10")
rows = cur.fetchall()
print(f"Sample records: {rows}")

# Check for different index_code
print("\n=== index_code 种类 ===")
cur.execute("SELECT DISTINCT index_code FROM index_constituents_history")
codes = [r[0] for r in cur.fetchall()]
print(f"Index codes: {codes}")

conn.close()

print("\n" + "=" * 60)
