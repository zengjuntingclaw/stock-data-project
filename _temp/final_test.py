import sys, os
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
os.chdir(r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
lines = []
try:
    from scripts.exchange_mapping import build_ts_code
    for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
        r = build_ts_code(inp)
        lines.append(('OK' if r==exp else 'FAIL') + ':BTS:' + inp + ':' + r)
except Exception as e:
    lines.append('ERROR:' + str(e))

try:
    from scripts.data_engine import DataEngine
    de = DataEngine(db_path='data/stock_data.duckdb')
    lines.append('DE_INIT:OK')
except Exception as e:
    lines.append('ERROR_DE:' + str(e))

try:
    stocks = de.get_active_stocks('2024-06-01')
    lines.append('PIT_2024-06-01:' + str(len(stocks)))
except Exception as e:
    lines.append('ERROR_PIT:' + str(e))

try:
    df = de.get_index_constituents('000300.SH', '2024-06-01')
    lines.append('IDX_000300.SH:' + str(len(df)))
except Exception as e:
    lines.append('ERROR_IDX:' + str(e))

try:
    df2 = de.get_index_constituents('000300.XSHG', '2024-06-01')
    lines.append('IDX_000300.XSHG:' + str(len(df2)))
except Exception as e:
    lines.append('ERROR_IDX_XSHG:' + str(e))

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

try:
    import datetime
    from scripts.survivorship_bias import SurvivorshipBiasHandler
    sbh = SurvivorshipBiasHandler(data_engine=de)
    pit = sbh.get_universe(datetime.datetime(2024,6,1), include_delisted=False)
    lines.append('SBH_2024-06-01:' + str(len(pit)))
except Exception as e:
    lines.append('ERROR_SBH:' + str(e))

with open(r'c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\final_test.txt', 'w') as f:
    f.write('\n'.join(lines))
