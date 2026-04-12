"""
Phase 5 FINAL INSERT: 1184 rows already downloaded (in previous run).
Fix DATE/VARCHAR: use DELETE per pair + INSERT.
"""
import sys, time, datetime, os
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_final_insert.log'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 FINAL INSERT START ===')

def pf(v):
    try: return float(v) if v and v != '' else 0.0
    except: return 0.0
def to_bs(code):
    sym, ex = code.split('.')
    return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"
def fetch_chunk(chunk):
    import baostock as bs
    results = []
    lg = bs.login()
    if lg.error_msg != 'success': return results
    for code in chunk:
        try:
            rs = bs.query_history_k_data_plus(to_bs(code),
                'date,open,high,low,close,volume,amount',
                start_date='2026-04-07', end_date='2026-04-10',
                frequency='d', adjustflag='2')
            while rs.next(): results.append((code,) + tuple(rs.get_row_data()))
        except: pass
    try: bs.logout()
    except: pass
    return results

try:
    import subprocess, duckdb, baostock as bs

    # Cleanup
    for pid in [16492, 8700, 18636, 9580]:
        subprocess.run(['taskkill', '/F', '/PID', str(pid)], capture_output=True)
    WAL = DB + '.wal'
    if os.path.exists(WAL):
        try: os.remove(WAL)
        except: pass
    time.sleep(2)

    # Check current state
    conn = duckdb.connect(DB, read_only=True)
    mx = conn.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    cnt = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    conn.close()
    log(f'Before: MAX={mx}, TOTAL={cnt}')

    # Load tracked
    conn2 = duckdb.connect(DB)
    tracked = conn2.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    conn2.close()
    log(f'Tracked: {len(tracked)}')

    # Download 812 stocks in 4 threads
    chunk_size = len(tracked) // 4 + 1
    chunks = [tracked[i:i+chunk_size] for i in range(0, len(tracked), chunk_size)]
    t0 = time.time()
    all_results = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_chunk, ch): i for i, ch in enumerate(chunks)}
        for future in as_completed(futures):
            results = future.result()
            all_results.extend(results)
            log(f'  Chunk {futures[future]}: {len(results)} rows')

    dl_time = time.time() - t0
    log(f'Download: {len(all_results)} rows in {dl_time:.0f}s')

    # Sort + pct_chg
    all_results.sort(key=lambda x: (x[0], x[1]))
    prev_closes = {}
    batch_raw = []
    batch_adj = []
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for (code, td, o, h, lo, c_str, vol_str, amt_str) in all_results:
        o = pf(o); h = pf(h); lo = pf(lo)
        c = pf(c_str); vol = pf(vol_str); amt = pf(amt_str)
        sym = code.split('.')[0]
        adj = 1.0
        prev = prev_closes.get(code)
        pct = (c - prev) / prev * 100 if prev and prev != 0 else 0.0
        batch_raw.append((code, td, sym, o, h, lo, c, vol, amt, pct, 0.0, 'baostock', now_str, adj, None, False, False, False))
        batch_adj.append((code, td, o, h, lo, c, None, vol, amt, pct, 0.0, adj, False, False, False, 'baostock', now_str, o, h, lo, c, o, h, lo, c))
        prev_closes[code] = c

    # UPSERT: DELETE with explicit VARCHAR cast + INSERT
    conn3 = duckdb.connect(DB)
    unique_pairs = {(r[0], r[1]) for r in all_results}
    log(f'Deleting {len(unique_pairs)} existing rows...')

    for code, td in unique_pairs:
        conn3.execute(
            "DELETE FROM daily_bar_raw WHERE ts_code = ? AND CAST(trade_date AS VARCHAR) = ?",
            [code, td])
        conn3.execute(
            "DELETE FROM daily_bar_adjusted WHERE ts_code = ? AND CAST(trade_date AS VARCHAR) = ?",
            [code, td])

    log(f'Inserting {len(batch_raw)} raw rows...')
    conn3.executemany("""
        INSERT INTO daily_bar_raw
        (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
         pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
         is_suspend,limit_up,limit_down)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch_raw)

    log(f'Inserting {len(batch_adj)} adj rows...')
    conn3.executemany("""
        INSERT INTO daily_bar_adjusted
        (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
         pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
         data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
         hfq_open,hfq_high,hfq_low,hfq_close)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch_adj)

    log('COMMITTING...')
    conn3.commit()
    conn3.close()
    log('COMMITTED')

    # Update sync_progress
    conn4 = duckdb.connect(DB)
    for code in tracked:
        conn4.execute("""
            UPDATE sync_progress SET last_sync_date = '2026-04-10'
            WHERE ts_code = ? AND table_name IN ('daily_bar_adjusted', 'daily_bar_raw')
        """, [code])
    conn4.commit()
    mx = conn4.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    cnt = conn4.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    new = conn4.execute("SELECT COUNT(*) FROM daily_bar_adjusted WHERE trade_date >= '2026-04-07'").fetchone()[0]
    log(f'After: MAX={mx}, TOTAL={cnt}, NEW_04={new}')
    conn4.close()

    total = time.time() - t0
    log(f'=== DONE: {len(all_results)} rows in {total:.0f}s ===')

except Exception as e:
    import traceback
    log(f'FATAL: {e}')
    log(traceback.format_exc())
    try:
        conn3.commit(); conn3.close()
        log('Emergency COMMIT')
    except: pass

with open(LOG) as f: print(f.read())
