"""Fix delist_date: concurrent Baostock queries"""
import sys, time, threading
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
LOG = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/delist_fix2.txt'

def log(msg):
    print(msg)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')

conn = duckdb.connect(DB)
delisted = conn.execute("""
    SELECT ts_code FROM stock_basic_history
    WHERE is_delisted = TRUE AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchall()
conn.close()
log(f"Delisted: {len(delisted)} stocks")

results = {}
lock = threading.Lock()
count = [0]

def query_one(ts_code):
    sym = ts_code.split('.')[0]
    ex = ts_code.split('.')[1].lower()
    lg = bs.login()
    rs = bs.query_stock_basic(code='%s.%s' % (ex, sym))
    out_date = None
    while rs.next():
        row = rs.get_row_data()
        if len(row) > 3 and row[3] and row[3].strip():
            out_date = row[3].strip()
    bs.logout()
    with lock:
        count[0] += 1
        if count[0] % 20 == 0:
            log(f"  Progress: {count[0]}/{len(delisted)}")
    return ts_code, out_date

log(f"Starting concurrent query (8 workers)...")
start = time.time()
n_threads = 8
with ThreadPoolExecutor(max_workers=n_threads) as ex:
    futures = {ex.submit(query_one, d[0]): d[0] for d in delisted}
    for f in as_completed(futures):
        ts_code, out_date = f.result()
        if out_date:
            results[ts_code] = out_date

elapsed = time.time() - start
log(f"Done: {len(results)}/{len(delisted)} in {elapsed:.1f}s ({elapsed/len(delisted)*1000:.0f}ms/stock)")

# Update DB
conn = duckdb.connect(DB)
n_updated = 0
for ts_code, out_date in results.items():
    conn.execute("""
        UPDATE stock_basic_history
        SET delist_date = ?, eff_date = ?
        WHERE ts_code = ? AND is_delisted = TRUE
    """, [out_date, out_date, ts_code])
    conn.execute("""
        UPDATE stock_basic
        SET delist_date = ?, is_delisted = TRUE
        WHERE ts_code = ?
    """, [out_date, ts_code])
    n_updated += 1
conn.close()
log(f"Updated {n_updated} stocks in DB")

# Verify
conn2 = duckdb.connect(DB, read_only=True)
real = conn2.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date!='2099-12-31'").fetchone()[0]
sentinel = conn2.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND (delist_date IS NULL OR delist_date='2099-12-31')").fetchone()[0]
log(f"FINAL: Real dates={real}, Sentinel/Null={sentinel}, Total={real+sentinel}")
log("\nSample:")
for r in conn2.execute("SELECT ts_code,name,list_date,delist_date FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date!='2099-12-31' LIMIT 5").fetchall():
    log(f"  {r}")
conn2.close()
log("ALL DONE")
