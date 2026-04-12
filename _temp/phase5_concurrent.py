"""
Phase 5 incremental sync - CONCURRENT version (4 threads, range query, batch insert)
Speed: 812 stocks / 4 threads * 2.18s = ~7.5 minutes
"""
import sys, time, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_concurrent.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG, 'a') as f: f.write(line + '\n'); f.flush()

log('=== Phase 5 Concurrent Sync START ===')

try:
    import duckdb, baostock as bs

    conn = duckdb.connect(DB)
    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    log(f'Tracked: {len(tracked)}')
    conn.close()

    def to_bs(code):
        sym, ex = code.split('.')
        return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

    def pf(v):
        try: return float(v) if v and v != '' else 0.0
        except: return 0.0

    # The KEY finding: 5-field range query WORKS
    # pct_chg,turnover cause Baostock bug -> calculate locally
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def fetch_stock(code):
        """Fetch one stock's data for 04-07 to 04-10"""
        results = []
        try:
            rs = bs.query_history_k_data_plus(to_bs(code),
                'date,open,high,low,close,volume,amount',
                start_date='2026-04-07', end_date='2026-04-10',
                frequency='d', adjustflag='2')
            while rs.next():
                results.append((code,) + tuple(rs.get_row_data()))
        except:
            pass
        return code, results

    # Baostock login (global session - Baostock handles per-process)
    lg = bs.login()
    log(f'Baostock: {lg.error_msg}')
    if lg.error_msg != 'success':
        log('FATAL'); sys.exit(1)

    t0 = time.time()
    fetched_stocks = 0
    fetched_rows = 0
    all_results = []

    # Concurrent download: 4 workers
    log(f'Starting concurrent download with 4 threads...')
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_stock, code): code for code in tracked}
        done = 0
        for future in as_completed(futures):
            code, rows = future.result()
            done += 1
            if rows:
                all_results.extend(rows)
                fetched_stocks += 1
                fetched_rows += len(rows)
            if done % 200 == 0 or done == len(tracked):
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 1
                eta = (len(tracked) - done) / rate
                log(f'  {done}/{len(tracked)} ({elapsed:.0f}s, ETA {eta:.0f}s) fetched={fetched_stocks} rows={fetched_rows}')

    bs.logout()
    dl_time = time.time() - t0
    log(f'Download: {fetched_stocks} stocks, {fetched_rows} rows in {dl_time:.0f}s')

    if fetched_rows == 0:
        log('FATAL: No data fetched!'); sys.exit(1)

    # Batch insert into DuckDB
    log(f'Inserting {fetched_rows} rows into DuckDB...')
    conn2 = duckdb.connect(DB)
    
    batch_raw = []
    batch_adj = []
    
    # Sort by code+date for pct_chg calculation
    all_results.sort(key=lambda x: (x[0], x[1]))
    prev_closes = {}  # ts_code -> prev_close
    
    for (code, td, o, h, lo, c_str, vol_str, amt_str) in all_results:
        o = pf(o); h = pf(h); lo = pf(lo)
        c = pf(c_str); vol = pf(vol_str); amt = pf(amt_str)
        sym = code.split('.')[0]
        adj = 1.0
        
        # Calc pct_chg locally
        prev = prev_closes.get(code)
        if prev and prev != 0:
            pct = (c - prev) / prev * 100
        else:
            pct = 0.0
        
        batch_raw.append((
            code, td, sym, o, h, lo, c, vol, amt,
            pct, 0.0, 'baostock', now_str, adj, None, False, False, False
        ))
        batch_adj.append((
            code, td, o, h, lo, c, None, vol, amt,
            pct, 0.0, adj, False, False, False,
            'baostock', now_str, o, h, lo, c, o, h, lo, c
        ))
        prev_closes[code] = c

    # Batch insert
    conn2.executemany("""
        INSERT INTO daily_bar_raw
        (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
         pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
         is_suspend,limit_up,limit_down)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch_raw)
    conn2.executemany("""
        INSERT INTO daily_bar_adjusted
        (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
         pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
         data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
         hfq_open,hfq_high,hfq_low,hfq_close)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch_adj)
    conn2.commit()
    ins_time = time.time() - t0 - dl_time
    log(f'Insert: {fetched_rows} rows in {ins_time:.0f}s')

    # Update sync_progress
    conn3 = duckdb.connect(DB)
    for code in tracked:
        conn3.execute("""
            UPDATE sync_progress SET last_sync_date = '2026-04-10'
            WHERE ts_code = ? AND table_name IN ('daily_bar_adjusted', 'daily_bar_raw')
        """, [code])
    conn3.commit()
    mx = conn3.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    cnt = conn3.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    new = conn3.execute("SELECT COUNT(*) FROM daily_bar_adjusted WHERE trade_date >= '2026-04-07'").fetchone()[0]
    log(f'After: MAX={mx}, TOTAL={cnt}, NEW_04={new}')
    conn3.close()

    total = time.time() - t0
    log(f'TOTAL: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s')

except Exception as e:
    import traceback
    log(f'FATAL: {e}')
    log(traceback.format_exc())

with open(LOG) as f: print(f.read())
