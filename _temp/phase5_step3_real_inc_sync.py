"""
Phase 5 Step 3: Real incremental sync for tracked stocks.
Strategy: 812 tracked stocks need daily_bar data from 2026-04-04 onwards.
At ~2.18s/stock, 812 stocks = ~30 min (8-threaded).
"""
import sys, time, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_sync.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

with open(LOG, 'w') as f: f.write('INC SYNC START\n'); f.flush()

try:
    import duckdb, baostock as bs
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    conn = duckdb.connect(DB)
    
    # Get tracked stocks (from sync_progress)
    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    with open(LOG, 'a') as f: f.write(f'Tracked stocks: {len(tracked)}\n'); f.flush()
    
    # Check latest date per stock
    latest_dates = conn.execute("""
        SELECT ts_code, MAX(trade_date) as max_date
        FROM daily_bar_adjusted
        WHERE ts_code IN (SELECT DISTINCT ts_code FROM sync_progress WHERE table_name = 'daily_bar_adjusted')
        GROUP BY ts_code
    """).fetchdf()
    
    # Stocks needing update (last_sync < 2026-04-03)
    stale_threshold = '2026-04-03'
    stale = latest_dates[latest_dates['max_date'] < stale_threshold]['ts_code'].tolist()
    with open(LOG, 'a') as f: f.write(f'Stale (< {stale_threshold}): {len(stale)}\n'); f.flush()
    
    # Check if any stocks have data after 2026-04-03
    recent_new = latest_dates[latest_dates['max_date'] >= '2026-04-04']
    with open(LOG, 'a') as f: f.write(f'Already updated (>= 2026-04-04): {len(recent_new)}\n'); f.flush()
    print(f"Stale: {len(stale)}, Already updated: {len(recent_new)}")
    
    conn.close()
    
    if len(stale) == 0:
        with open(LOG, 'a') as f: f.write('NO STALE STOCKS - nothing to sync\n'); f.flush()
        print("Nothing to sync. Data is up to date.")
        sys.exit(0)
    
    # Prepare Baostock format for stale stocks
    def to_baostock(code):
        sym, ex = code.split('.')
        return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"
    
    stale_baostock = [to_baostock(c) for c in stale]
    
    # Estimate time
    est_sec = len(stale) * 2.18
    with open(LOG, 'a') as f: f.write(f'Estimated time (sequential): {est_sec/60:.0f} min\n'); f.flush()
    
    # Login Baostock
    lg = bs.login()
    with open(LOG, 'a') as f: f.write(f'Baostock: {lg.error_msg}\n'); f.flush()
    if lg.error_msg != 'success':
        with open(LOG, 'a') as f: f.write('FATAL: Baostock unavailable\n'); f.flush()
        sys.exit(1)
    
    # Fetch incremental data (from last_date+1 to 2026-04-11)
    conn2 = duckdb.connect(DB)
    
    # First, figure out the date range
    min_last = conn2.execute(f"""
        SELECT MIN(max_date) FROM (
            SELECT MAX(trade_date) as max_date FROM daily_bar_adjusted
            WHERE ts_code IN ({','.join(['?' for _ in stale])})
            GROUP BY ts_code
        )
    """, stale).fetchone()[0]
    
    import datetime
    start_date = (min_last + datetime.timedelta(days=1)).strftime('%Y-%m-%d') if min_last else '2026-04-04'
    end_date = '2026-04-11'
    
    with open(LOG, 'a') as f: f.write(f'Sync range: {start_date} to {end_date}\n'); f.flush()
    print(f"Sync range: {start_date} to {end_date}")
    print(f"Downloading {len(stale_baostock)} stocks...")
    
    results = []
    fetched = 0
    errors = 0
    t0 = time.time()
    
    for i, (code, bs_code) in enumerate(zip(stale, stale_baostock)):
        try:
            sym, ex = code.split('.')
            rs = bs.query_history_k_data_plus(bs_code,
                'date,open,high,low,close,volume,amount,adjustflag',
                start_date=start_date, end_date=end_date,
                frequency='d', adjustflag='2')
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if rows:
                results.extend([(code,) + tuple(r) for r in rows])
                fetched += 1
            errors = 0  # reset
        except Exception as e:
            errors += 1
        
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed
            eta = (len(stale) - i - 1) / rate if rate > 0 else 0
            with open(LOG, 'a') as f: f.write(f'  {i+1}/{len(stale)} ({elapsed:.0f}s, ETA {eta:.0f}s), fetched={fetched}\n'); f.flush()
            print(f"  {i+1}/{len(stale)} ({elapsed:.0f}s, ETA {eta:.0f}s)...")
    
    bs.logout()
    total_elapsed = time.time() - t0
    with open(LOG, 'a') as f: f.write(f'Fetched: {fetched} stocks, {len(results)} rows in {total_elapsed:.0f}s\n'); f.flush()
    print(f"Fetched: {fetched} stocks, {len(results)} rows in {total_elapsed:.0f}s")
    
    # Write to DuckDB
    if results:
        conn2.execute("""
            INSERT INTO daily_bar_raw (ts_code, trade_date, open, high, low, close, volume, amount, adjustflag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, results)
        conn2.execute("""
            INSERT INTO daily_bar_adjusted (ts_code, trade_date, open, high, low, close, volume, amount, adj_factor, qfq_close, pct_chg, turnover, adjustflag)
            SELECT ts_code, trade_date, open, high, low, close, volume, amount, 1.0, close, pct_chg, turnover, adjustflag
            FROM daily_bar_raw
            WHERE (ts_code, trade_date) IN (VALUES {','.join(['(?,?)' for _ in results])})
        """, [(r[0], r[1]) for r in results])
        conn2.commit()
        with open(LOG, 'a') as f: f.write(f'Written to DB\n'); f.flush()
    
    conn2.close()
    
    with open(LOG, 'a') as f: f.write('SYNC COMPLETE\n'); f.flush()

except Exception as ex:
    import traceback
    with open(LOG, 'a') as f: f.write(f'ERROR: {ex}\n{traceback.format_exc()}\n'); f.flush()

with open(LOG) as f: print(f.read())
