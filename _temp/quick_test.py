import sys, os, traceback
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
lines = []
try:
    from scripts.exchange_mapping import build_ts_code
    for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
        r = build_ts_code(inp)
        lines.append(('OK' if r==exp else 'FAIL') + ':BTS:' + inp + ':' + r)
except Exception as e:
    lines.append('ERROR_BTS:' + str(e))

try:
    from scripts.data_engine import DataEngine
    de = DataEngine(data_dir='data')
    lines.append('DE_INIT:OK')
except Exception as e:
    lines.append('ERROR_DE_INIT:' + str(e))
    traceback.print_exc()

try:
    stocks = de.get_active_stocks('2024-06-01')
    lines.append('PIT_2024-06-01:' + str(len(stocks)))
except Exception as e:
    lines.append('ERROR_PIT:' + str(e))
    traceback.print_exc()

try:
    df = de.get_index_constituents('000300.SH', '2024-06-01')
    lines.append('IDX_000300.SH:' + str(len(df)))
except Exception as e:
    lines.append('ERROR_IDX:' + str(e))
    traceback.print_exc()

try:
    df2 = de.get_index_constituents('000300.XSHG', '2024-06-01')
    lines.append('IDX_000300.XSHG:' + str(len(df2)))
except Exception as e:
    lines.append('ERROR_IDX_XSHG:' + str(e))
    traceback.print_exc()

try:
    df3 = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
    lines.append('RAW:' + str(len(df3)))
except Exception as e:
    lines.append('ERROR_RAW:' + str(e))

try:
    df4 = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
    lines.append('ADJ:' + str(len(df4)))
except Exception as e:
    lines.append('ERROR_ADJ:' + str(e))

with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\quick_test.txt', 'w') as f:
    f.write('\n'.join(lines))
