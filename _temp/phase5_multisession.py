"""
Phase 5: Multi-session concurrent (4 threads, each with own Baostock login).
Each thread: login -> fetch its stocks -> logout (isolated sessions)
812 / 4 threads * 2.18s = ~7.5 minutes
"""
import sys, time, datetime, os
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_multisession.log'
BATCH = 100  # DuckDB insert batch size

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 Multi-Session Sync START ===')

# Cleanup
import subprocess
log('Cleanup...')
for pid in [4696, 4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True)
WAL = DB + '.wal'
if os.path.exists(WAL):
    try: os.remove(WAL); log('Removed WAL')
    except: pass
time.sleep(2)

# Load tracked
log('Loading DuckDB...')
import duckdb
conn = duckdb.connect(DB)
tracked = conn.execute("""
    SELECT DISTINCT ts_code FROM sync_progress
    WHERE table_name = 'daily_bar_adjusted'
""").fetchdf()['ts_code'].tolist()
conn.close()
log(f'Tracked: {len(tracked)}')

# Split into 4 chunks
chunk_size = len(tracked) // 4 + 1
chunks = [tracked[i:i+chunk_size] for i in range(0, len(tracked), chunk_size)]
for i, ch in enumerate(chunks):
    log(f'  Chunk {i}: {len(ch)} stocks')

def pf(v):
    try: return float(v) if v and v != '' else 0.0
    except: return 0.0

def to_bs(code):
    sym, ex = code.split('.')
    return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

def fetch_chunk(chunk_stocks):
    """Thread: login -> fetch chunk -> logout"""
    import baostock as bs
    results = []
    lg = bs.login()
    if lg.error_msg != 'success':
        return results
    for code in chunk_stocks:
        sym = code.split('.')[0]
        try:
            rs = bs.query_history_k_data_plus(to_bs(code),
                'date,open,high,low,close,volume,amount',
                start_date='2026-04-07', end_date='2026-04-10',
                frequency='d', adjustflag='2')
            while rs.next():
                results.append((code,) + tuple(rs.get_row_data()))
        except:
            pass
    bs.logout()
    return results

log('Starting 4-thread concurrent fetch...')
now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
t0 = time.time()

all_results = []
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_chunk, chunk): i
                for i, chunk in enumerate(chunks)}
    done = 0
    for future in as_completed(futures):
        chunk_idx = futures[future]
        results = future.result()
        all_results.extend(results)
        done += 1
        elapsed = time.time() - t0
        log(f'  Thread {chunk_idx}: fetched {len(results)} rows in {elapsed:.0f}s')

dl_time = time.time() - t0
log(f'Download: {len(all_results)} total rows in {dl_time:.0f}s')

# Sort and calc pct_chg
all_results.sort(key=lambda x: (x[0], x[1]))
prev_closes = {}
batch_raw = []
batch_adj = []

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

log(f'Inserting {len(batch_raw)} rows into DuckDB...')
conn2 = duckdb.connect(DB)
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
conn2.close()
log(f'Inserted')

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
log(f'=== DONE: {len(all_results)} rows in {total:.0f}s ({total/60:.1f} min) ===')

with open(LOG) as f: print(f.read())
