"""功能验证脚本 - 测试所有新方法"""
import sys
sys.path.insert(0, '.')

print("=" * 60)
print("功能验证脚本")
print("=" * 60)

# Test DataEngine methods
print("\n=== 1. DataEngine 方法检查 ===")
from scripts.data_engine import DataEngine

engine = DataEngine()

methods_to_check = [
    'get_active_stocks',
    'get_index_constituents',
    'get_daily_raw',
    'get_daily_adjusted',
    'get_universe_at_date',
]

for method in methods_to_check:
    exists = hasattr(engine, method)
    print(f'  {method}: {"EXISTS" if exists else "MISSING"}')

# Test functional calls
print("\n=== 2. 功能调用验证 ===")

# Test get_active_stocks
try:
    stocks = engine.get_active_stocks("2020-01-02")
    print(f'  get_active_stocks("2020-01-02"): {len(stocks)} stocks')
    if stocks:
        print(f'    Sample: {stocks[:5]}')
except Exception as e:
    print(f'  get_active_stocks FAILED: {e}')

# Test get_index_constituents
try:
    df = engine.get_index_constituents("000300.SH", "2020-01-02")
    print(f'  get_index_constituents("000300.SH", "2020-01-02"): {len(df)} rows')
    if not df.empty:
        print(f'    Columns: {list(df.columns)}')
        print(f'    Sample: {df.head(2).to_dict("records")}')
except Exception as e:
    print(f'  get_index_constituents FAILED: {e}')

# Test get_daily_raw
try:
    df_raw = engine.get_daily_raw("600000.SH", "2020-01-01", "2020-01-10")
    print(f'  get_daily_raw("600000.SH", "2020-01-01", "2020-01-10"): {len(df_raw)} rows')
    if not df_raw.empty:
        print(f'    Columns: {list(df_raw.columns)}')
        print(f'    Sample: {df_raw.head(1).to_dict("records")}')
except Exception as e:
    print(f'  get_daily_raw FAILED: {e}')

# Test get_daily_adjusted
try:
    df_adj = engine.get_daily_adjusted("600000.SH", "2020-01-01", "2020-01-10")
    print(f'  get_daily_adjusted("600000.SH", "2020-01-01", "2020-01-10"): {len(df_adj)} rows')
    if not df_adj.empty:
        print(f'    Columns: {list(df_adj.columns)}')
        print(f'    Sample: {df_adj.head(1).to_dict("records")}')
except Exception as e:
    print(f'  get_daily_adjusted FAILED: {e}')

# Test raw vs adjusted difference
print("\n=== 3. 原始价 vs 复权价对比 ===")
if not df_raw.empty and not df_adj.empty:
    # Compare close prices
    raw_close = df_raw['close'].iloc[0]
    adj_close = df_adj['close'].iloc[0]
    raw_date = df_raw['trade_date'].iloc[0]
    adj_date = df_adj['trade_date'].iloc[0]
    print(f'  600000.SH at {raw_date}:')
    print(f'    Raw close: {raw_close}')
    print(f'    Adj close: {adj_close}')
    print(f'    Difference: {abs(raw_close - adj_close):.4f}')
    
    # Check if they're different (for stocks with dividends)
    if abs(raw_close - adj_close) > 0.01:
        print('    NOTE: Raw and adjusted prices are different (expected for dividend stocks)')
    else:
        print('    NOTE: Raw and adjusted prices are similar (may indicate no adjustment needed)')

# Test index constituents PIT query
print("\n=== 4. 指数成分历史PIT验证 ===")
try:
    # 2020年的沪深300成分
    df_2020 = engine.get_index_constituents("000300.SH", "2020-06-01")
    print(f'  2020-06-01 沪深300成分股: {len(df_2020)} 只')
    
    # 2024年的沪深300成分
    df_2024 = engine.get_index_constituents("000300.SH", "2024-06-01")
    print(f'  2024-06-01 沪深300成分股: {len(df_2024)} 只')
    
    # 成分股变化
    codes_2020 = set(df_2020['ts_code'].tolist()) if not df_2020.empty else set()
    codes_2024 = set(df_2024['ts_code'].tolist()) if not df_2024.empty else set()
    
    added = codes_2024 - codes_2020
    removed = codes_2020 - codes_2024
    print(f'  2020->2024 新加入: {len(added)} 只')
    print(f'  2020->2024 已剔除: {len(removed)} 只')
    
    if added:
        print(f'    Sample added: {list(added)[:5]}')
    if removed:
        print(f'    Sample removed: {list(removed)[:5]}')
except Exception as e:
    print(f'  Index constituents PIT test FAILED: {e}')

print("\n" + "=" * 60)
print("验证完成")
print("=" * 60)
