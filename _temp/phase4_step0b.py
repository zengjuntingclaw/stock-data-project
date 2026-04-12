"""
Phase 4 Step 0b: 查退市日期 — 单 Baostock 会话 + 顺序查询（最快）
327只 × ~0.4s = ~130s，超时保护：每50只打印进度
"""
import sys, os, time
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

conn = duckdb.connect(DB, read_only=True)
delisted = [r[0] for r in conn.execute("""
    SELECT ts_code FROM stock_basic_history
    WHERE is_delisted = TRUE
    AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchall()]
conn.close()
print(f"Delisted stocks to query: {len(delisted)}")

lg = bs.login()
print(f"Baostock login: {lg.error_msg}")

results = {}
start = time.time()
for i, ts_code in enumerate(delisted):
    sym = ts_code.split('.')[0]
    ex = ts_code.split('.')[1].lower()
    out_date = None
    try:
        rs = bs.query_stock_basic(code='%s.%s' % (ex, sym))
        while rs.next():
            row = rs.get_row_data()
            if len(row) > 3 and row[3] and str(row[3]).strip():
                out_date = str(row[3]).strip()
    except Exception:
        pass
    if out_date:
        results[ts_code] = out_date
    if (i + 1) % 50 == 0:
        elapsed = time.time() - start
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(delisted) - i - 1) / rate if rate > 0 else 0
        print(f"  {i+1}/{len(delisted)} ({elapsed:.0f}s, ETA {eta:.0f}s) — got {len(results)}")

bs.logout()
elapsed = time.time() - start
print(f"Baostock done: {len(results)}/{len(delisted)} in {elapsed:.1f}s")

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
# NULL -> sentinel (shouldn't exist but just in case)
conn2.execute("""
    UPDATE stock_basic_history
    SET delist_date = '2099-12-31', eff_date = '2099-12-31'
    WHERE is_delisted = TRUE AND delist_date IS NULL
""")
conn2.close()
print(f"Updated {fixed} real delist_dates in DB")

# Verify
conn3 = duckdb.connect(DB, read_only=True)
sample = conn3.execute("""
    SELECT ts_code, name, list_date, delist_date
    FROM stock_basic_history WHERE is_delisted = TRUE
    ORDER BY delist_date LIMIT 10
""").fetchall()
print("Sample delisted:")
for s in sample:
    print(f"  {s}")
conn3.close()
print(f"Step 0b DONE")
