"""
Phase 5 monolith: kill zombies -> remove WAL -> DuckDB check
-> Baostock bulk test -> concurrent sync
All in ONE process, no subprocess calls.
"""
import sys, time, datetime, os
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
WAL = DB + '.wal'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_monolith.log'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG, 'a') as f: f.write(line + '\n'); f.flush()

log('=== Phase 5 Monolith START ===')

# ---- Step 1: Kill zombie PIDs ----
log('Step 1: Kill zombie PIDs...')
import subprocess
for pid in [4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580, 10588]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True)
log('  PIDs killed')

# ---- Step 2: Remove WAL ----
log('Step 2: Remove DuckDB WAL...')
if os.path.exists(WAL):
    try:
        os.remove(WAL)
        log(f'  Removed {WAL}')
    except Exception as e:
        log(f'  Cannot remove WAL: {e}')
else:
    log('  No WAL found')

time.sleep(3)

# ---- Step 3: DuckDB check ----
log('Step 3: DuckDB check...')
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    tracked = c.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    c.close()
    log(f'  DuckDB OK: MAX={mx}, Tracked={len(tracked)}')
except Exception as e:
    log(f'  DuckDB ERROR: {e}')
    log('  FATAL: Cannot proceed')
    sys.exit(1)

# ---- Step 4: Baostock bulk test ----
log('Step 4: Baostock test...')
import baostock as bs

lg = bs.login()
log(f'  Baostock: {lg.error_msg}')
if lg.error_msg != 'success':
    log('  FATAL'); sys.exit(1)

def pf(v):
    try: return float(v) if v and v != '' else 0.0
    except: return 0.0

# Test 5-field range query
t0 = time.time()
rs = bs.query_history_k_data_plus('sh.600000',
    'date,open,high,low,close,volume,amount',
    start_date='2026-04-07', end_date='2026-04-10',
    frequency='d', adjustflag='2')
rows = []
while rs.next():
    rows.append(rs.get_row_data())
elapsed = time.time() - t0
log(f'  Range query: {len(rows)} rows in {elapsed:.2f}s')
if rows:
    log(f'  Sample: {rows[0]}')

# ---- Step 5: Concurrent sync ----
log(f'Step 5: Concurrent sync of {len(tracked)} stocks...')
now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def to_bs(code):
    sym, ex = code.split('.')
    return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

def fetch_stock(code):
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

t0 = time.time()
all_results = []
fetched_stocks = 0
fetched_rows = 0

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

dl_time = time.time() - t0
log(f'  Download: {fetched_stocks} stocks, {fetched_rows} rows in {dl_time:.0f}s')

if fetched_rows == 0:
    log('  FATAL: No data!')
    bs.logout()
    sys.exit(1)

# ---- Step 6: Batch insert ----
log(f'Step 6: Batch insert {fetched_rows} rows...')

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
log(f'  Inserted {fetched_rows} rows')

# ---- Step 7: Update sync_progress ----
log('Step 7: Update sync_progress...')
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
log(f'  After: MAX={mx}, TOTAL={cnt}, NEW_04={new}')
conn3.close()

bs.logout()

total = time.time() - t0
log(f'=== DONE: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s ===')

with open(LOG) as f: print(f.read())
