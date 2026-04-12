"""
Phase 4 Step 0c: 退市日期批量修复
- 分4批，每批 ~1 分钟，总计 ~4 分钟
- 每批内并发 8 线程
- 用临时文件保存中间结果，断点可续
"""
import sys, os, time, json
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
STATE_FILE = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\delist_state.json'
BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 0  # 0=all, 1/2/3/4=batch

conn = duckdb.connect(DB, read_only=True)
delisted = [r[0] for r in conn.execute("""
    SELECT ts_code FROM stock_basic_history
    WHERE is_delisted = TRUE
    AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchall()]
conn.close()
print(f"Total to query: {len(delisted)}")

# Load or init state
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as f:
        state = json.load(f)
    results = state.get('results', {})
    print(f"Resuming: {len(results)} already done")
else:
    results = {}
    with open(STATE_FILE, 'w') as f:
        json.dump({'results': {}}, f)

# Batch assignment
N_BATCH = 4
total = len(delisted)
chunk_size = (total + N_BATCH - 1) // N_BATCH
start_idx = BATCH * chunk_size
end_idx = min(start_idx + chunk_size, total)
batch_stocks = delisted[start_idx:end_idx]
print(f"Batch {BATCH}/{N_BATCH}: indices {start_idx}-{end_idx} ({len(batch_stocks)} stocks)")

if BATCH == 0:
    # Run all
    todo = delisted
else:
    todo = batch_stocks

# Already done?
todo = [s for s in todo if s not in results]
print(f"Still to query: {len(todo)}")

done_lock = __import__('threading').Lock()

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
    except Exception:
        pass
    with done_lock:
        if out_date:
            results[ts_code] = out_date
        done = len(results)
        if done % 20 == 0:
            print(f"  Progress: {done}/{len(delisted)}")
    return ts_code, out_date

start = time.time()
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(query_one, s): s for s in todo}
    for f in as_completed(futures):
        f.result()  # wait for completion

elapsed = time.time() - start
print(f"Batch done: {len(results)}/{len(delisted)} total in {elapsed:.1f}s")

# Save state
with open(STATE_FILE, 'w') as f:
    json.dump({'results': results}, f)

# If all done, write to DB
if len(results) >= len(delisted) * 0.95:  # 95%+ done = commit
    conn2 = duckdb.connect(DB)
    fixed = 0
    for ts_code, out_date in results.items():
        conn2.execute("""
            UPDATE stock_basic_history SET delist_date=?, eff_date=?
            WHERE ts_code=? AND is_delisted=TRUE
        """, [out_date, out_date, ts_code])
        fixed += 1
    conn2.execute("""
        UPDATE stock_basic_history SET delist_date='2099-12-31', eff_date='2099-12-31'
        WHERE is_delisted=TRUE AND delist_date IS NULL
    """)
    conn2.close()
    print(f"DB updated: {fixed} real dates, rest sentinel")
    # Clean state file
    os.remove(STATE_FILE)
else:
    print(f"Not enough results ({len(results)}/{len(delisted)}), state saved for resume")
    print(f"Run again with batch number to continue: py phase4_step0c.py {min(BATCH+1, N_BATCH)}")
