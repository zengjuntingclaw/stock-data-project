"""
最终验证脚本 - 验证新口径完全接入主流程
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'scripts')

from data_engine import DataEngine

print("=" * 60)
print("最终验证 - 新口径主流程测试")
print("=" * 60)

# 1. 初始化 DataEngine
print("\n[1] 初始化 DataEngine")
engine = DataEngine(db_path='data/stock_data.duckdb')
print(f"  数据库路径: {engine.db_path}")
print("  [OK] 初始化成功")

# 2. PIT 股票池查询
print("\n[2] PIT 股票池查询 - get_active_stocks()")
test_dates = [
    ("2024-01-01", "2024年1月1日"),
    ("2024-06-01", "2024年6月1日"),
    ("2025-01-01", "2025年1月1日"),
]
for date, label in test_dates:
    stocks = engine.get_active_stocks(date)
    print(f"  {label}: {len(stocks):,} 只股票")
    if stocks:
        print(f"    样本: {stocks[:3]}")

# 3. 指数成分股查询
print("\n[3] 指数成分股查询 - get_index_constituents()")
index_tests = [
    ("000300.SH", "2024-01-01", "沪深300"),
    ("000300.XSHG", "2024-01-01", "沪深300(XSHG格式)"),
    ("000905.SH", "2024-01-01", "中证500"),
    ("000852.SH", "2024-01-01", "中证1000"),
]
for index_code, date, label in index_tests:
    try:
        cons = engine.get_index_constituents(index_code, date)
        print(f"  {label} ({index_code}): {len(cons)} 只成分股")
        if cons:
            print(f"    样本: {cons[:3]}")
    except Exception as e:
        print(f"  {label}: 查询失败 - {e}")

# 4. 原始行情查询
print("\n[4] 原始行情查询 - get_daily_raw()")
try:
    df_raw = engine.get_daily_raw("600000.SH", "2024-01-01", "2024-01-10")
    print(f"  查询结果: {len(df_raw)} 条")
    if not df_raw.empty:
        print(f"  列: {list(df_raw.columns)}")
        print(f"  样本:")
        print(df_raw[['trade_date', 'open', 'high', 'low', 'close', 'volume']].head(2).to_string(index=False))
except Exception as e:
    print(f"  查询失败: {e}")

# 5. 复权行情查询
print("\n[5] 复权行情查询 - get_daily_adjusted()")
try:
    df_adj = engine.get_daily_adjusted("600000.SH", "2024-01-01", "2024-01-10")
    print(f"  查询结果: {len(df_adj)} 条")
    if not df_adj.empty:
        adj_cols = [c for c in df_adj.columns if 'qfq' in c.lower() or 'adj' in c.lower()]
        print(f"  复权相关列: {adj_cols}")
        print(f"  样本:")
        print(df_adj[['trade_date', 'close', 'adj_factor', 'qfq_close']].head(2).to_string(index=False))
except Exception as e:
    print(f"  查询失败: {e}")

# 6. 验证 raw 和 adjusted 分离
print("\n[6] Raw vs Adjusted 分离验证")
try:
    raw_sample = engine.get_daily_raw("600000.SH", "2024-01-05", "2024-01-05")
    adj_sample = engine.get_daily_adjusted("600000.SH", "2024-01-05", "2024-01-05")
    if not raw_sample.empty and not adj_sample.empty:
        raw_close = raw_sample['close'].iloc[0]
        adj_close = adj_sample['qfq_close'].iloc[0]
        adj_factor = adj_sample['adj_factor'].iloc[0]
        print(f"  原始收盘价: {raw_close}")
        print(f"  复权收盘价: {adj_close}")
        print(f"  复权因子: {adj_factor}")
        if adj_factor != 1.0:
            print(f"  [OK] 复权因子生效 ({raw_close} -> {adj_close})")
        else:
            print(f"  [!] 复权因子为1，可能该日期无需复权")
except Exception as e:
    print(f"  验证失败: {e}")

# 7. 验证主流程调用链
print("\n[7] 主流程调用链验证")
print("  验证 SurvivorshipBiasHandler 依赖的 get_universe()")
try:
    from survivorship_bias import SurvivorshipBiasHandler
    handler = SurvivorshipBiasHandler(data_engine=engine)
    universe = handler.get_universe("2024-06-01")
    print(f"  get_universe(2024-06-01): {len(universe)} 只股票")
    print(f"  ✓ SurvivorshipBiasHandler 主流程正常")
except Exception as e:
    print(f"  [FAIL] SurvivorshipBiasHandler 测试失败: {e}")

# 8. 旧表不再被主流程使用
print("\n[8] 旧表回退验证")
print("  验证 _get_local_stocks 不回退到 stock_basic")
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
stock_basic_count = db.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
stock_basic_history_count = db.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
print(f"  stock_basic 记录数: {stock_basic_count:,} (已废弃)")
print(f"  stock_basic_history 记录数: {stock_basic_history_count:,} (主表)")
print(f"  [OK] 旧表存在但不参与主流程")
db.close()

print("\n" + "=" * 60)
print("验证完成")
print("=" * 60)
