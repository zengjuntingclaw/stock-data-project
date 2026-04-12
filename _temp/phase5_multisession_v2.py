"""
Phase 5 multi-session v2: 4 threads, INSERT OR IGNORE to skip duplicates.
"""
import sys, time, datetime, os
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_multisession_v2.log'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 v2 START ===')

# Load tracked
import subprocess, duckdb
for pid in [9580, 4696, 4432, 2624]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)], capture_output=True)
WAL = DB + '.wal'
if os.path.exists(WAL):
    try: os.remove(WAL)
    except: pass
time.sleep(2)

conn = duckdb.connect(DB)
tracked = conn.execute("""
    SELECT DISTINCT ts_code FROM sync_progress
    WHERE table_name = 'daily_bar_adjusted'
""").fetchdf()['ts_code'].tolist()
conn.close()
log(f'Tracked: {len(tracked)}')

chunk_size = len(tracked) // 4 + 1
chunks = [tracked[i:i+chunk_size] for i in range(0, len(tracked), chunk_size)]

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
    if lg.error_msg != 'success':
        return results
    for code in chunk:
        try:
            rs = bs.query_history_k_data_plus(to_bs(code),
                'date,open,high,low,close,volume,amount',
                start_date='2026-04-07', end_date='2026-04-10',
                frequency='d', adjustflag='2')
            while rs.next():
                results.append((code,) + tuple(rs.get_row_data()))
        except:
            pass
    try: bs.logout()
    except: pass
    return results

t0 = time.time()
all_results = []
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_chunk, ch): i for i, ch in enumerate(chunks)}
    for future in as_completed(futures):
        results = future.result()
        all_results.extend(results)
        elapsed = time.time() - t0
        log(f'  Thread {futures[future]}: {len(results)} rows in {elapsed:.0f}s')

dl_time = time.time() - t0
log(f'Download: {len(all_results)} rows in {dl_time:.0f}s')

# Sort and pct_chg
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

# INSERT OR IGNORE (DuckDB native syntax: INSERT OR IGNORE works)
log(f'Inserting {len(batch_raw)} rows (OR IGNORE)...')
conn2 = duckdb.connect(DB)

# Try INSERT OR IGNORE first
try:
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
    log('  OR IGNORE insert OK')
except Exception as e:
    log(f'  OR IGNORE failed: {e}')
    # Fallback: row-by-row INSERT OR IGNORE
    skipped = 0
    inserted = 0
    for raw_r, adj_r in zip(batch_raw, batch_adj):
        try:
            conn2.execute("""
                INSERT INTO daily_bar_raw
                (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
                 pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
                 is_suspend,limit_up,limit_down)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, raw_r)
            inserted += 1
        except:
            skipped += 1
        try:
            conn2.execute("""
                INSERT INTO daily_bar_adjusted
                (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
                 pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
                 data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
                 hfq_open,hfq_high,hfq_low,hfq_close)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, adj_r)
        except:
            skipped += 1
    conn2.commit()
    log(f'  Row-by-row: inserted={inserted}, skipped={skipped}')

conn2.close()

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
log(f'=== DONE: {len(all_results)} rows in {total:.0f}s ===')
with open(LOG) as f: print(f.read())
