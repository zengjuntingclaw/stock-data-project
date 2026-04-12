"""
Phase 4 Step 0: 查退市日期（并发 Baostock）+ 写回 DB
"""
import sys, os, time, threading
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
LOG = 'C:/Users/zengj/.qclaw/workspace\stock_data_project\_temp/step0_log.txt'

def log(msg):
    print(msg)
    try:
        with open(LOG, 'a', encoding='utf-8') as f:
            f.write(str(msg) + '\n')
    except Exception:
        pass

conn = duckdb.connect(DB, read_only=True)
delisted = conn.execute("""
    SELECT ts_code FROM stock_basic_history
    WHERE is_delisted = TRUE
    AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchall()
conn.close()
log(f"Delisted stocks to query: {len(delisted)}")

results = {}
done_count = [0]
lock = threading.Lock()

def query_one(ts_code):
    sym = ts_code.split('.')[0]
    ex = ts_code.split('.')[1].lower()
    out_date = None
    try:
        lg = bs.login()
        rs = bs.query_stock_basic(code='%s.%s' % (ex, sym))
        while rs.next():
            row = rs.get_row_data()
            if len(row) > 3 and row[3] and str(row[3]).strip():
                out_date = str(row[3]).strip()
        bs.logout()
    except Exception as e:
        pass
    with lock:
        done_count[0] += 1
        if done_count[0] % 50 == 0:
            log(f"  Progress: {done_count[0]}/{len(delisted)}")
    return ts_code, out_date

start = time.time()
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(query_one, d[0]): d[0] for d in delisted}
    for f in as_completed(futures):
        ts_code, out_date = f.result()
        if out_date:
            results[ts_code] = out_date

elapsed = time.time() - start
got = len(results)
log(f"Baostock results: {got}/{len(delisted)} in {elapsed:.1f}s")

# Write to DB
conn2 = duckdb.connect(DB)
fixed = 0
for ts_code, out_date in results.items():
    conn2.execute("""
        UPDATE stock_basic_history
        SET delist_date = ?, eff_date = ?
        WHERE ts_code = ? AND is_delisted = TRUE
    """, [out_date, out_date, ts_code])
    fixed += 1

conn2.execute("""
    UPDATE stock_basic_history
    SET delist_date = '2099-12-31', eff_date = '2099-12-31'
    WHERE is_delisted = TRUE AND delist_date IS NULL
""")
log(f"Updated {fixed} real dates + rest set to 2099-12-31")

# Show sample
sample = conn2.execute("""
    SELECT ts_code, name, list_date, delist_date
    FROM stock_basic_history
    WHERE is_delisted = TRUE
    ORDER BY delist_date
    LIMIT 8
""").fetchall()
log("Sample delisted stocks:")
for s in sample:
    log(f"  {s}")
conn2.close()
log(f"Step 0 DONE ({time.time()-start:.1f}s total)")
