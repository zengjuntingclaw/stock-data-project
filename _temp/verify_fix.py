import sys, os, datetime
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
from scripts.data_engine import DataEngine
from scripts.exchange_mapping import build_ts_code
from scripts.survivorship_bias import SurvivorshipBiasHandler

lines = []
lines.append("=== 修复验证 ===")

# Test 1: build_ts_code
for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
    r = build_ts_code(inp)
    lines.append(('BTS_OK' if r==exp else 'BTS_FAIL') + ':' + inp + ':' + r)

de = DataEngine(data_dir='data')

# Test 2: PIT 活跃股票查询（修复后）
stocks_2024 = de.get_active_stocks('2024-06-01')
lines.append("PIT_2024-06-01:" + str(len(stocks_2024)))
stocks_2023 = de.get_active_stocks('2023-01-01')
lines.append("PIT_2023-01-01:" + str(len(stocks_2023)))
stocks_now = de.get_active_stocks('2026-04-01')
lines.append("PIT_2026-04-01:" + str(len(stocks_now)))

# Test 3: 指数成分股（修复后 - 使用 .SH 格式）
df_idx1 = de.get_index_constituents('000300.SH', '2024-06-01')
lines.append("IDX_000300.SH_2024-06-01:" + str(len(df_idx1)))
df_idx2 = de.get_index_constituents('000300.XSHG', '2024-06-01')
lines.append("IDX_000300.XSHG_2024-06-01:" + str(len(df_idx2)))
df_idx3 = de.get_index_constituents('000905.SH', '2024-06-01')
lines.append("IDX_000905.SH_2024-06-01:" + str(len(df_idx3)))
df_idx4 = de.get_index_constituents('000852.SH', '2024-06-01')
lines.append("IDX_000852.SH_2024-06-01:" + str(len(df_idx4)))

# Test 4: get_universe_at_date
uni1 = de.get_universe_at_date('000300.SH', '2024-06-01')
lines.append("UNI_000300.SH_2024-06-01:" + str(len(uni1)))
uni2 = de.get_universe_at_date('000300.XSHG', '2024-06-01')
lines.append("UNI_000300.XSHG_2024-06-01:" + str(len(uni2)))

# Test 5: 行情查询
df_raw = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
lines.append("RAW_000001.SZ:" + str(len(df_raw)))
df_adj = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
lines.append("ADJ_000001.SZ:" + str(len(df_adj)))

# Test 6: SurvivorshipBiasHandler PIT
sbh = SurvivorshipBiasHandler(data_engine=de)
pit_stocks = sbh.get_universe(datetime.datetime(2024,6,1), include_delisted=False)
lines.append("SBH_2024-06-01:" + str(len(pit_stocks)))

lines.append("=== 验证完成 ===")
with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\fix_verify.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
