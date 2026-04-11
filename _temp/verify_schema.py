"""验证脚本 - 检查数据口径落地情况"""
import sys
sys.path.insert(0, '.')

print("=" * 60)
print("数据口径验证脚本")
print("=" * 60)

# Test 1: Verify build_ts_code unified
print("\n=== 1. build_ts_code 统一验证 ===")
from scripts.exchange_mapping import build_ts_code, classify_exchange

test_cases = [
    ('600000', 'SH', '600000.SH'),   # 沪市主板
    ('688001', 'SH', '688001.SH'),   # 科创板
    ('000001', 'SZ', '000001.SZ'),   # 深市主板
    ('300001', 'SZ', '300001.SZ'),   # 创业板
    ('430001', 'BJ', '430001.BJ'),   # 北交所老股
    ('830001', 'BJ', '830001.BJ'),   # 北交所新股
    ('920001', 'BJ', '920001.BJ'),   # 北交所2024新代码
    ('000008', 'BJ', '000008.BJ'),   # 北交所8开头
]

all_pass = True
for symbol, expected_exchange, expected_ts_code in test_cases:
    ts_code = build_ts_code(symbol)
    exchange, _ = classify_exchange(symbol)
    status = 'OK' if (exchange == expected_exchange and ts_code == expected_ts_code) else 'FAIL'
    if status == 'FAIL':
        all_pass = False
    print(f'{status} {symbol} -> {ts_code} (expected {expected_exchange}/{expected_ts_code})')

# Test 2: Check DataEngine methods
print("\n=== 2. DataEngine 方法检查 ===")
from scripts.data_engine import DataEngine

engine = DataEngine()

methods_to_check = [
    'get_active_stocks',
    'get_index_constituents',  # 可能不存在
    'get_daily_raw',  # 可能不存在
    'get_daily_adjusted',  # 可能不存在
    'get_universe_at_date',  # 指数成分历史
    'save_stock_basic_snapshot',  # PIT快照
    'get_stocks_as_of',  # PIT查询
    'sync_index_constituents',  # 指数同步
]

for method in methods_to_check:
    exists = hasattr(engine, method)
    print(f'  {method}: {"EXISTS" if exists else "MISSING"}')

# Test 3: Check database tables
print("\n=== 3. 数据库表结构验证 ===")
try:
    import duckdb
    db_path = engine.db_path
    conn = duckdb.connect(str(db_path))
    cur = conn.cursor()
    
    # Check if new tables exist
    expected_tables = [
        'stock_basic',  # 旧表（保留用于兼容）
        'stock_basic_history',  # 新PIT表
        'daily_bar_raw',  # 新原始价表
        'daily_bar_adjusted',  # 新复权价表
        'index_constituents_history',  # 新指数历史表
        'sync_progress',  # 断点续传表
        'st_status_history',  # ST状态历史
        'trade_calendar',  # 交易日历
        'corporate_actions',  # 除权除息
    ]
    
    cur.execute("SHOW TABLES")
    existing_tables = set(row[0] for row in cur.fetchall())
    
    for table in expected_tables:
        exists = table in existing_tables
        print(f'  {table}: {"EXISTS" if exists else "MISSING"}')
    
    # Check if daily_quotes still exists (旧表)
    if 'daily_quotes' in existing_tables:
        print('  WARNING: daily_quotes (旧表) still exists!')
    
    # Check index_constituents (旧表)
    if 'index_constituents' in existing_tables:
        print('  WARNING: index_constituents (旧表) still exists!')
    
    conn.close()
except Exception as e:
    print(f'  数据库检查失败: {e}')

# Test 4: Run functional test if tables exist
print("\n=== 4. 功能验证 ===")
try:
    import duckdb
    db_path = engine.db_path
    conn = duckdb.connect(str(db_path))
    cur = conn.cursor()
    
    # Check stock_basic_history record count
    try:
        cur.execute("SELECT COUNT(*) FROM stock_basic_history")
        count = cur.fetchone()[0]
        print(f'  stock_basic_history: {count} records')
    except Exception as e:
        print(f'  stock_basic_history: 检查失败 - {e}')
    
    # Check daily_bar_raw record count
    try:
        cur.execute("SELECT COUNT(*) FROM daily_bar_raw")
        count = cur.fetchone()[0]
        print(f'  daily_bar_raw: {count} records')
    except Exception as e:
        print(f'  daily_bar_raw: 检查失败 - {e}')
    
    # Check daily_bar_adjusted record count
    try:
        cur.execute("SELECT COUNT(*) FROM daily_bar_adjusted")
        count = cur.fetchone()[0]
        print(f'  daily_bar_adjusted: {count} records')
    except Exception as e:
        print(f'  daily_bar_adjusted: 检查失败 - {e}')
    
    # Check index_constituents_history record count
    try:
        cur.execute("SELECT COUNT(*) FROM index_constituents_history")
        count = cur.fetchone()[0]
        print(f'  index_constituents_history: {count} records')
    except Exception as e:
        print(f'  index_constituents_history: 检查失败 - {e}')
    
    conn.close()
except Exception as e:
    print(f'  功能验证失败: {e}')

print("\n" + "=" * 60)
print("验证完成")
print("=" * 60)
