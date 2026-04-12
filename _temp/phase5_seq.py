"""
Phase 5 sequential - no threading, simple and reliable.
Sequential Baostock queries + batch DuckDB insert every 100 stocks.
812 stocks * 2.18s = ~29 minutes (predictable, no lock issues)
"""
import sys, time, datetime, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_seq.log'
BATCH = 100  # insert every 100 stocks

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 Sequential Sync START ===')

# ---- Kill zombies + Remove WAL ----
log('Cleanup...')
import subprocess
for pid in [4696, 4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True)
WAL = DB + '.wal'
if os.path.exists(WAL):
    try: os.remove(WAL); log('Removed WAL')
    except: pass
time.sleep(2)

# ---- Load tracked stocks ----
log('Loading DuckDB...')
import duckdb
conn = duckdb.connect(DB)
tracked = conn.execute("""
    SELECT DISTINCT ts_code FROM sync_progress
    WHERE table_name = 'daily_bar_adjusted'
""").fetchdf()['ts_code'].tolist()
conn.close()
log(f'Tracked: {len(tracked)}')

# ---- Baostock ----
log('Connecting to Baostock...')
import baostock as bs
lg = bs.login()
log(f'Baostock: {lg.error_msg}')
if lg.error_msg != 'success':
    log('FATAL'); sys.exit(1)

def to_bs(code):
    sym, ex = code.split('.')
    return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"
def pf(v):
    try: return float(v) if v and v != '' else 0.0
    except: return 0.0

now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
t0 = time.time()
batch_raw = []
batch_adj = []
prev_closes = {}
fetched_stocks = 0
fetched_rows = 0
batch_num = 0

for i, code in enumerate(tracked):
    sym = code.split('.')[0]

    # Sequential query (thread-safe, no session corruption)
    try:
        rs = bs.query_history_k_data_plus(to_bs(code),
            'date,open,high,low,close,volume,amount',
            start_date='2026-04-07', end_date='2026-04-10',
            frequency='d', adjustflag='2')
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
    except:
        rows = []

    if rows:
        # Sort by date for pct_chg calc
        rows.sort(key=lambda x: x[0])
        for r in rows:
            td = r[0]
            o = pf(r[1]); h = pf(r[2]); lo = pf(r[3])
            c = pf(r[4]); vol = pf(r[5]); amt = pf(r[6])
            adj = 1.0
            prev = prev_closes.get(code)
            pct = (c - prev) / prev * 100 if prev and prev != 0 else 0.0

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
        fetched_stocks += 1
        fetched_rows += len(rows)

    # Batch insert every BATCH stocks
    if (i + 1) % BATCH == 0 or i == len(tracked) - 1:
        batch_num += 1
        if batch_raw:
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
            batch_raw = []
            batch_adj = []

        elapsed = time.time() - t0
        rate = (i+1) / elapsed if elapsed > 0 else 1
        eta = (len(tracked) - i - 1) / rate
        log(f'Batch {batch_num}: {i+1}/{len(tracked)} ({elapsed:.0f}s, ETA {eta:.0f}s) fetched={fetched_stocks} rows={fetched_rows}')

bs.logout()
dl_time = time.time() - t0
log(f'Fetched: {fetched_stocks} stocks, {fetched_rows} rows in {dl_time:.0f}s')

if fetched_rows == 0:
    log('WARNING: No data fetched!')

# ---- Update sync_progress ----
log('Updating sync_progress...')
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
log(f'=== DONE: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s ===')

with open(LOG) as f: print(f.read())
