import sys, os
sys.path.insert(0, '.')
from scripts.data_engine import DataEngine
from scripts.exchange_mapping import build_ts_code
from scripts.survivorship_bias import SurvivorshipBiasHandler
import datetime

os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')

lines = []
lines.append("FINAL_VERIFICATION_START")

# build_ts_code tests
for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
    r = build_ts_code(inp)
    lines.append('BTS:' + inp + ':' + r + ':OK' if r==exp else 'BTS:' + inp + ':' + r + ':FAIL')

de = DataEngine(data_dir='data')
stocks = de.get_active_stocks('2024-06-01')
lines.append('PIT:2024-06-01:' + str(len(stocks)))

df_idx = de.get_index_constituents('000300.XSHG', '2024-06-01')
lines.append('IDX:000300:2024-06-01:' + str(len(df_idx)))

df_raw = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
lines.append('RAW:000001.SZ:' + str(len(df_raw)))

df_adj = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
lines.append('ADJ:000001.SZ:' + str(len(df_adj)))

sbh = SurvivorshipBiasHandler(data_engine=de)
pit = sbh.get_universe(datetime.datetime(2024,6,1), include_delisted=False)
lines.append('SBH:2024-06-01:' + str(len(pit)))

lines.append("FINAL_VERIFICATION_END")

with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\verify_result.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
