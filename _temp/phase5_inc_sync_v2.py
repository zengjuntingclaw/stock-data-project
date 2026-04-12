"""Phase 5 incremental sync v2 - using correct DuckDB schema"""
import sys, time, datetime
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_sync_v2.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG, 'a') as f: f.write(line + '\n'); f.flush()

log('=== Phase 5 Incremental Sync v2 START ===')

try:
    import duckdb, baostock as bs

    conn = duckdb.connect(DB)
    log('DB connected (exclusive mode)')

    # Get stale stocks
    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    
    latest = conn.execute("""
        SELECT ts_code, MAX(trade_date) as max_date
        FROM daily_bar_adjusted
        WHERE ts_code IN (SELECT DISTINCT ts_code FROM sync_progress WHERE table_name = 'daily_bar_adjusted')
        GROUP BY ts_code
    """).fetchdf()
    
    stale = latest[latest['max_date'] < '2026-04-04']['ts_code'].tolist()
    log(f'Total tracked: {len(tracked)}, Stale (< 2026-04-04): {len(stale)}')
    conn.close()

    if not stale:
        log('Nothing to sync'); sys.exit(0)

    # Baostock login
    lg = bs.login()
    log(f'Baostock: {lg.error_msg}')
    if lg.error_msg != 'success':
        log('FATAL: Baostock unavailable'); sys.exit(1)

    def to_bs(code):
        sym, ex = code.split('.')
        return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

    def parse_float(v):
        try: return float(v) if v and v != '' else 0.0
        except: return 0.0

    start_date = '2026-04-04'
    end_date = '2026-04-11'
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn2 = duckdb.connect(DB)
    fetched_stocks = 0
    fetched_rows = 0
    errors = 0
    t0 = time.time()

    for i, code in enumerate(stale):
        try:
            bs_code = to_bs(code)
            sym = code.split('.')[0]
            
            rs = bs.query_history_k_data_plus(bs_code,
                'date,open,high,low,close,volume,amount,adjustflag,pct_chg,turnover',
                start_date=start_date, end_date=end_date,
                frequency='d', adjustflag='2')
            
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if rows:
                for r in rows:
                    trade_date = r[0]
                    o = parse_float(r[1]); h = parse_float(r[2])
                    lo = parse_float(r[3]); c = parse_float(r[4])
                    vol = parse_float(r[5]); amt = parse_float(r[6])
                    pct = parse_float(r[9]); tovr = parse_float(r[10])
                    adj = 1.0  # raw data from Baostock qfq = raw close * 1.0
                    
                    # INSERT raw
                    conn2.execute("""
                        INSERT INTO daily_bar_raw
                        (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
                         pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
                         is_suspend,limit_up,limit_down)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, [code, trade_date, sym, o, h, lo, c, vol, amt,
                          pct, tovr, 'baostock', now_str, adj, None, False, False, False])
                    
                    # INSERT adjusted (qfq = raw since adj_factor=1.0)
                    conn2.execute("""
                        INSERT INTO daily_bar_adjusted
                        (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
                         pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
                         data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
                         hfq_open,hfq_high,hfq_low,hfq_close)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, [code, trade_date, o, h, lo, c, None, vol, amt,
                          pct, tovr, adj, False, False, False,
                          'baostock', now_str, o, h, lo, c, o, h, lo, c])
                
                conn2.commit()
                fetched_stocks += 1
                fetched_rows += len(rows)
        except Exception as e:
            errors += 1
            if errors <= 3:
                log(f'  ERROR {code}: {e}')

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed if elapsed > 0 else 1
            eta = (len(stale) - i - 1) / rate
            log(f'  {i+1}/{len(stale)} ({elapsed:.0f}s, ETA {eta:.0f}s) fetched={fetched_stocks}')

    bs.logout()
    total = time.time() - t0
    log(f'DONE: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s')

    # Update sync_progress
    conn3 = duckdb.connect(DB)
    for code in stale:
        conn3.execute("""
            UPDATE sync_progress SET last_sync_date = '2026-04-11'
            WHERE ts_code = ? AND table_name IN ('daily_bar_adjusted', 'daily_bar_raw')
        """, [code])
    conn3.commit()
    conn3.close()
    log('sync_progress updated')

except Exception as e:
    import traceback
    log(f'FATAL: {e}')
    log(traceback.format_exc())

with open(LOG) as f: print(f.read())
