"""Phase 5 incremental sync - sequential download for 812 stale stocks"""
import sys, time, os, datetime
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_sync.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG, 'a') as f: f.write(line + '\n'); f.flush()

log('=== Phase 5 Incremental Sync START ===')

try:
    import duckdb, baostock as bs

    # --- Connect and analyze ---
    conn = duckdb.connect(DB)
    log('DB connected')

    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    log(f'Tracked stocks: {len(tracked)}')

    # Check which ones need update
    latest = conn.execute("""
        SELECT ts_code, MAX(trade_date) as max_date
        FROM daily_bar_adjusted
        WHERE ts_code IN (SELECT DISTINCT ts_code FROM sync_progress WHERE table_name = 'daily_bar_adjusted')
        GROUP BY ts_code
    """).fetchdf()
    
    stale = latest[latest['max_date'] < '2026-04-04']['ts_code'].tolist()
    up2date = latest[latest['max_date'] >= '2026-04-04']
    log(f'Already up to date (>= 2026-04-04): {len(up2date)}')
    log(f'Need update: {len(stale)}')
    
    conn.close()

    if not stale:
        log('Nothing to sync. Exiting.')
        sys.exit(0)

    # --- Baostock login ---
    lg = bs.login()
    log(f'Baostock: {lg.error_msg}')
    if lg.error_msg != 'success':
        log('FATAL: Baostock unavailable'); sys.exit(1)

    def to_bs(code):
        sym, ex = code.split('.')
        return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

    # Determine date range
    start_date = '2026-04-04'
    end_date = '2026-04-11'

    # --- Sequential download (safe, no threading, logs every 50) ---
    conn2 = duckdb.connect(DB)
    fetched_rows = 0
    fetched_stocks = 0
    t0 = time.time()

    for i, code in enumerate(stale):
        try:
            bs_code = to_bs(code)
            rs = bs.query_history_k_data_plus(bs_code,
                'date,open,high,low,close,volume,amount,adjustflag',
                start_date=start_date, end_date=end_date,
                frequency='d', adjustflag='2')
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            
            if rows:
                # Insert into daily_bar_raw
                for r in rows:
                    conn2.execute("""
                        INSERT INTO daily_bar_raw (ts_code,trade_date,open,high,low,close,volume,amount,adjustflag)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, [code, r[0], float(r[1]) if r[1] else 0,
                          float(r[2]) if r[2] else 0, float(r[3]) if r[3] else 0,
                          float(r[4]) if r[4] else 0, float(r[5]) if r[5] else 0,
                          float(r[6]) if r[6] else 0, r[7]])
                    # qfq = raw close * adj_factor (adj_factor=1.0 for this period)
                    raw_close = float(r[4]) if r[4] else 0
                    conn2.execute("""
                        INSERT INTO daily_bar_adjusted 
                        (ts_code,trade_date,open,high,low,close,volume,amount,adj_factor,qfq_close,pct_chg,turnover,adjustflag)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, [code, r[0], float(r[1]) if r[1] else 0,
                          float(r[2]) if r[2] else 0, float(r[3]) if r[3] else 0,
                          raw_close, float(r[5]) if r[5] else 0,
                          float(r[6]) if r[6] else 0,
                          1.0, raw_close, 0.0, 0.0, r[7]])
                conn2.commit()
                fetched_stocks += 1
                fetched_rows += len(rows)
        except Exception as e:
            log(f'  ERROR {code}: {e}')

        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed
            eta = (len(stale) - i - 1) / rate
            log(f'  Progress: {i+1}/{len(stale)} ({elapsed:.0f}s elapsed, ETA {eta:.0f}s), fetched={fetched_stocks}')

    bs.logout()
    total = time.time() - t0
    log(f'COMPLETE: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s')

    # Update sync_progress
    conn3 = duckdb.connect(DB)
    for code in stale:
        conn3.execute("""
            UPDATE sync_progress SET last_sync_date = '2026-04-11'
            WHERE ts_code = ? AND table_name = 'daily_bar_adjusted'
        """, [code])
    conn3.commit()
    conn3.close()
    log('sync_progress updated')

except Exception as e:
    import traceback
    log(f'FATAL ERROR: {e}')
    log(traceback.format_exc())

with open(LOG) as f: print(f.read())
