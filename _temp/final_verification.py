"""最终交付验证脚本"""
import sys
sys.path.insert(0, '.')

print("=" * 70)
print("数据口径标准化重构 - 最终交付验证报告")
print("=" * 70)

import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)

# 1. 表结构检查
print("\n【1. 数据库表清单】")
tables = db.execute("""
    SELECT table_name, 
           (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as col_count
    FROM information_schema.tables t 
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()
for t, cols in tables:
    count = db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    print(f'  {t}: {count} rows, {cols} cols')

# 2. 新表数据验证
print("\n【2. 新表数据验证】")
print(f"  stock_basic_history eff_date 最新: {db.execute(\"SELECT MAX(eff_date) FROM stock_basic_history\").fetchone()[0]}")
print(f"  index_constituents_history 覆盖: {db.execute(\"SELECT MIN(in_date), MAX(out_date) FROM index_constituents_history\").fetchone()}")
print(f"  daily_bar_raw 覆盖: {db.execute(\"SELECT MIN(trade_date), MAX(trade_date) FROM daily_bar_raw\").fetchone()}")
db.close()

# 3. Python 功能验证
print("\n【3. Python 功能验证】")
from scripts.data_engine import DataEngine
from scripts.survivorship_bias import SurvivorshipBiasHandler
from scripts.exchange_mapping import build_ts_code

de = DataEngine(data_dir='data')

# Test 1: build_ts_code
print("\n  [build_ts_code 测试]")
tests = [('600000', '600000.SH'), ('000001', '000001.SZ'), ('688001', '688001.SH'), 
         ('430001', '430001.BJ'), ('920001', '920001.BJ')]
for inp, expected in tests:
    result = build_ts_code(inp)
    status = '[OK]' if result == expected else f'[FAIL: got {result}]'
    print(f'    {inp} -> {result} {status}')

# Test 2: PIT query
print("\n  [PIT 股票池查询]")
stocks = de.get_active_stocks('2024-06-01')
print(f'    2024-06-01 有效股票数: {len(stocks)}')
print(f'    样本: {stocks[:5]}')

# Test 3: Index constituents
print("\n  [指数成分股历史查询]")
df_idx = de.get_index_constituents('000300.XSHG', '2024-06-01')
print(f'    沪深300 在 2024-06-01 成分股数: {len(df_idx)}')

# Test 4: Daily raw
print("\n  [原始行情查询]")
df_raw = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
print(f'    平安银行 raw 数据: {len(df_raw)} 条')
if not df_raw.empty:
    row = df_raw.iloc[0]
    print(f'    样本: {row["trade_date"]} open={row["open"]} close={row["close"]}')

# Test 5: Daily adjusted
print("\n  [复权行情查询]")
df_adj = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
print(f'    平安银行 adjusted 数据: {len(df_adj)} 条')
if not df_adj.empty:
    row = df_adj.iloc[0]
    print(f'    样本: {row["trade_date"]} qfq_close={row["qfq_close"]} adj_factor={row["adj_factor"]}')

# Test 6: Survivorship bias handler with PIT
print("\n  [幸存者偏差消除（PIT模式）]")
sbh = SurvivorshipBiasHandler(data_engine=de)
date = __import__('datetime').datetime(2024, 6, 1)
pit_stocks = sbh.get_universe(date, include_delisted=False)
print(f'    2024-06-01 股票池(PIT): {len(pit_stocks)} 只')

print("\n" + "=" * 70)
print("验证完成")
print("=" * 70)
