import sys, time, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\bs_test.log'
with open(LOG, 'w') as f: f.write('START\n'); f.flush()

try:
    import baostock as bs
    with open(LOG, 'a') as f: f.write('import bs OK\n'); f.flush()
    
    with open(LOG, 'a') as f: f.write('logging in...\n'); f.flush()
    lg = bs.login()
    with open(LOG, 'a') as f: f.write(f'login: {lg.error_msg}\n'); f.flush()
    
    if lg.error_msg != 'success':
        with open(LOG, 'a') as f: f.write('LOGIN FAILED\n'); f.flush()
        sys.exit(1)
    
    # Quick test
    with open(LOG, 'a') as f: f.write('querying 600000...\n'); f.flush()
    t0 = time.time()
    rs = bs.query_history_k_data_plus('sh.600000',
        'date,open,high,low,close,volume,amount,adjustflag',
        start_date='2026-04-01', end_date='2026-04-11',
        frequency='d', adjustflag='2')
    elapsed = time.time() - t0
    with open(LOG, 'a') as f: f.write(f'query done in {elapsed:.2f}s\n'); f.flush()
    
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    with open(LOG, 'a') as f: f.write(f'rows: {len(rows)}\n'); f.flush()
    
    bs.logout()
    with open(LOG, 'a') as f: f.write('DONE\n'); f.flush()
    
except Exception as ex:
    with open(LOG, 'a') as f: f.write(f'ERROR: {ex}\n'); f.flush()

with open(LOG) as f: print(f.read())
