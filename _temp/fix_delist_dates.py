"""Fix delist_date for 327 delisted stocks using Baostock query_stock_basic"""
import sys, time
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
LOG = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/delist_fix.txt'

def log(msg):
    print(msg)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')

conn = duckdb.connect(DB)

# Get all delisted stocks
delisted = conn.execute("""
    SELECT ts_code, symbol
    FROM stock_basic_history
    WHERE is_delisted = TRUE AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchall()
log(f"Delisted stocks to query: {len(delisted)}")

# Baostock batch query (login once, query many, logout once)
# query_stock_basic is fast (~0.01s per call), so we can batch with one login

results = {}

def query_batch(batch):
    """Query a batch of stocks with one Baostock login"""
    lg = bs.login()
    batch_results = {}
    for ts_code, symbol in batch:
        sym = ts_code.split('.')[0]
        ex = ts_code.split('.')[1].lower()
        rs = bs.query_stock_basic(code='%s.%s' % (ex, sym))
        while rs.next():
            row = rs.get_row_data()
            # Fields: code, code_name, ipoDate, outDate, type, status
            out_date = row[3] if len(row) > 3 else None
            if out_date and out_date.strip():
                batch_results[ts_code] = out_date.strip()
    bs.logout()
    return batch_results

batch_size = 100
log(f"Starting batch query (batch_size={batch_size})...")

start = time.time()
n_batches = (len(delisted) + batch_size - 1) // batch_size
for i in range(0, len(delisted), batch_size):
    chunk = delisted[i:i+batch_size]
    batch_num = i // batch_size + 1
    log(f"  Batch {batch_num}/{n_batches}: {i+1}-{min(i+batch_size, len(delisted))}...")
    batch_results = query_batch(chunk)
    results.update(batch_results)
    elapsed = time.time() - start
    log(f"    Got {len(batch_results)} out_dates, total {len(results)} so far ({elapsed:.1f}s)")

log(f"\nTotal out_dates retrieved: {len(results)}/{len(delisted)}")

# Update DB
n_updated = 0
for ts_code, out_date in results.items():
    conn.execute("""
        UPDATE stock_basic_history
        SET delist_date = ?, eff_date = ?
        WHERE ts_code = ? AND is_delisted = TRUE
    """, [out_date, out_date, ts_code])
    n_updated += 1

log(f"Updated {n_updated} delist_date values in DB")

# Sync to stock_basic
n_sync = 0
for ts_code, out_date in results.items():
    r = conn.execute("""
        UPDATE stock_basic
        SET delist_date = ?, is_delisted = TRUE
        WHERE ts_code = ?
    """, [out_date, ts_code])
    n_sync += 1

log(f"Synced {n_sync} to stock_basic")

# Verify
real_dates = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted = TRUE
    AND delist_date IS NOT NULL
    AND delist_date != '2099-12-31'
""").fetchone()[0]
sentinel = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted = TRUE
    AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchone()[0]

log(f"\nFINAL delist_date status:")
log(f"  Real dates: {real_dates}")
log(f"  Sentinel/null: {sentinel}")
log(f"  Total delisted: {real_dates + sentinel}")

# Show sample
log("\nSample of fixed delisted stocks:")
sample = conn.execute("""
    SELECT ts_code, name, list_date, delist_date, delist_reason
    FROM stock_basic_history
    WHERE is_delisted = TRUE AND delist_date IS NOT NULL AND delist_date != '2099-12-31'
    LIMIT 10
""").fetchall()
for s in sample:
    log(f"  {s}")

conn.close()
log(f"\nALL DONE ({time.time()-start:.1f}s)")
