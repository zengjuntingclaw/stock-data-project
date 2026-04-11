"""
最终验证脚本 - 确认主流程走新表
"""
import sys
sys.path.insert(0, '.')

print("=" * 60)
print("最终验证：确认主流程走新表")
print("=" * 60)

# 1. 验证 build_ts_code
print("\n[1] build_ts_code 验证:")
from scripts.exchange_mapping import build_ts_code
tests = [('600000', '600000.SH'), ('000001', '000001.SZ'),
         ('688001', '688001.SH'), ('430001', '430001.BJ'), ('920001', '920001.BJ')]
bts_ok = all(build_ts_code(s) == e for s, e in tests)
print(f"  build_ts_code: {'PASS' if bts_ok else 'FAIL'}")

# 2. 验证 DataEngine 可以正常初始化
print("\n[2] DataEngine 初始化:")
try:
    from scripts.data_engine import DataEngine
    de = DataEngine(db_path='data/stock_data.duckdb')
    print("  DataEngine(db_path=...) OK")
except Exception as e:
    print(f"  DataEngine 初始化失败: {e}")
    sys.exit(1)

# 3. 验证主流程方法
print("\n[3] 主流程方法验证:")
methods = ['get_active_stocks', 'get_index_constituents',
           'get_daily_raw', 'get_daily_adjusted']
for m in methods:
    if hasattr(de, m):
        print(f"  {m}: OK")
    else:
        print(f"  {m}: MISSING!")

# 4. 验证 PIT 查询
print("\n[4] PIT 查询验证:")
try:
    stocks = de.get_active_stocks('2024-06-01')
    print(f"  get_active_stocks('2024-06-01'): {len(stocks)} 只股票")
    if len(stocks) > 100:
        print("  PIT 查询正常")
    else:
        print("  PIT 查询返回数量异常")
except Exception as e:
    print(f"  PIT 查询失败: {e}")

# 5. 验证指数成分
print("\n[5] 指数成分查询:")
try:
    df = de.get_index_constituents('000300.SH', '2024-06-01')
    print(f"  沪深300成分股: {len(df)} 只")
    df2 = de.get_index_constituents('000300.XSHG', '2024-06-01')
    print(f"  沪深300(.XSHG格式): {len(df2)} 只")
except Exception as e:
    print(f"  指数成分查询失败: {e}")

# 6. 验证行情查询
print("\n[6] 行情查询验证:")
try:
    df_raw = de.get_daily_raw('000001.SZ', '2024-01-10', '2024-01-15')
    print(f"  daily_bar_raw: {len(df_raw)} 条")
    df_adj = de.get_daily_adjusted('000001.SZ', '2024-01-10', '2024-01-15')
    print(f"  daily_bar_adjusted: {len(df_adj)} 条")
except Exception as e:
    print(f"  行情查询失败: {e}")

# 7. 验证 _get_local_stocks 不回退
print("\n[7] _get_local_stocks 验证:")
try:
    # 直接测试 _get_local_stocks
    result = de._get_local_stocks()
    print(f"  _get_local_stocks(): {len(result)} 条记录")
    if not result.empty:
        # 检查返回的是 stock_basic_history 数据
        if 'eff_date' in result.columns:
            print("  使用 stock_basic_history (有 eff_date 字段)")
        else:
            print("  可能使用旧表 (无 eff_date 字段)")
except Exception as e:
    print(f"  _get_local_stocks 失败: {e}")

# 8. 验证 SurvivorshipBiasHandler
print("\n[8] SurvivorshipBiasHandler 验证:")
try:
    from scripts.survivorship_bias import SurvivorshipBiasHandler
    sbh = SurvivorshipBiasHandler(data_engine=de)
    stocks = sbh.get_universe('2024-06-01')
    print(f"  SurvivorshipBiasHandler.get_universe(): {len(stocks)} 只股票")
except Exception as e:
    print(f"  SurvivorshipBiasHandler 失败: {e}")

print("\n" + "=" * 60)
print("验证完成")
print("=" * 60)
