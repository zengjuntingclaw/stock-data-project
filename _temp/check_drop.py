import sys, os
sys.path.insert(0, '.')
import duckdb
from scripts.data_engine import DataEngine

conn = duckdb.connect('data/stock_data.duckdb')

# 检查 stock_basic
try:
    cnt = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
    print(f"stock_basic 记录数: {cnt}")
except Exception as e:
    print(f"stock_basic 表不存在（已 DROP）: {e}")

conn.close()

# 功能验证
de = DataEngine(db_path='data/stock_data.duckdb')
stocks = de.get_active_stocks('2024-01-02')
print(f"get_active_stocks('2024-01-02'): {len(stocks)} 只, 示例: {stocks[:3]}")

adj = de.get_daily_adjusted('600000.SH', '2024-01-02', '2024-01-05')
print(f"get_daily_adjusted('600000.SH'): {len(adj)} 条")
print(f"  qfq_close 有值: {adj['qfq_close'].notna().sum()}/{len(adj)}")
print(f"  hfq_close 有值: {adj['hfq_close'].notna().sum()}/{len(adj)}")
if adj['qfq_close'].notna().any():
    print(f"  qfq_close 示例: {adj['qfq_close'].dropna().tolist()}")
    print(f"  hfq_close 示例: {adj['hfq_close'].dropna().tolist()}")
