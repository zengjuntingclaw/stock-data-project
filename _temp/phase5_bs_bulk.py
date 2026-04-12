"""
Phase 5 - Use Baostock's stock_daily query (all stocks, single date)
to fetch 04-07, 04-08, 04-09, 04-10 in 4 API calls.
Much faster than per-stock queries.
"""
import sys, time, datetime
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_bulk.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 Bulk Sync START ===')

try:
    import duckdb, baostock as bs

    conn = duckdb.connect(DB)
    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    log(f'Tracked: {len(tracked)}')
    conn.close()

    lg = bs.login()
    log(f'Baostock: {lg.error_msg}')
    if lg.error_msg != 'success':
        log('FATAL'); sys.exit(1)

    def pf(v):
        try: return float(v) if v and v != '' else 0.0
        except: return 0.0

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    days = ['2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10']
    
    # Test with sz.399300 (CSI 300 - has 300 stocks)
    log('Testing stock_daily for sz.399300 on 2026-04-07...')
    rs = bs.query_stock_daily('sz.399300', '2026-04-07')
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    log(f'  stock_daily result: {len(rows)} rows')
    if rows:
        log(f'  Sample: {rows[0]}')

    # Also try sh.600000
    log('Testing stock_daily for sh.600000 on 2026-04-07...')
    rs2 = bs.query_stock_daily('sh.600000', '2026-04-07')
    rows2 = []
    while rs2.next():
        rows2.append(rs2.get_row_data())
    log(f'  stock_daily: {len(rows2)} rows')
    if rows2:
        log(f'  Sample: {rows2[0]}')

    bs.logout()

except Exception as e:
    import traceback
    log(f'FATAL: {e}')
    log(traceback.format_exc())

with open(LOG) as f: print(f.read())
