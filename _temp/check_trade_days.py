"""Test Baostock for actual trading days in early April 2026"""
import sys, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\trade_day_test.log'
def log(msg):
    print(msg)
    with open(LOG, 'a') as f: f.write(msg + '\n'); f.flush()

log('=== Trading Day Test ===')
import baostock as bs
lg = bs.login()
log(f'Baostock: {lg.error_msg}')

# Test 600000.SH for different dates
test_dates = ['2026-03-31', '2026-04-01', '2026-04-02', '2026-04-03',
              '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10', '2026-04-11']
for start in test_dates:
    rs = bs.query_history_k_data_plus('sh.600000',
        'date,open,high,low,close',
        start_date=start, end_date=start,
        frequency='d', adjustflag='2')
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    log(f'  600000.SH {start}: {len(rows)} rows {rows}')

bs.logout()
with open(LOG) as f: print(f.read())
