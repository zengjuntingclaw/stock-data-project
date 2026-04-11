import sys
sys.path.insert(0, r'c:\Users\zengj\.qclaw\workspace\stock_data_project')
try:
    from scripts.exchange_mapping import build_ts_code
    for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
        r = build_ts_code(inp)
        print('BTS:' + inp + ':' + r + ':OK' if r==exp else 'BTS:' + inp + ':' + r + ':FAIL')
except Exception as e:
    print('ERROR:' + str(e))
    import traceback
    traceback.print_exc()
