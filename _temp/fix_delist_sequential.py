"""Fix delist dates - single Baostock session, sequential, no threading"""
import sys, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb, baostock as bs

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)
delisted = [r[0] for r in conn.execute("""
    SELECT ts_code FROM stock_basic_history
    WHERE is_delisted = TRUE AND delist_date = '2099-12-31'
""").fetchall()]
conn.close()
print(f"Delisted stocks: {len(delisted)}")

lg = bs.login()
print(f"Baostock: {lg.error_msg}")

results = {}
start = time.time()
for i, ts_code in enumerate(delisted):
    sym, ex = ts_code.split('.')
    ex = ex.lower()
    out_date = None
    rs = bs.query_stock_basic(code=f'{ex}.{sym}')
    while rs.next():
        row = rs.get_row_data()
        if len(row) > 3 and row[3] and str(row[3]).strip():
            out_date = str(row[3]).strip()
    if out_date:
        results[ts_code] = out_date
    if (i + 1) % 25 == 0:
        elapsed = time.time() - start
        rate = (i + 1) / elapsed
        eta = (len(delisted) - i - 1) / rate if rate > 0 else 0
        print(f"  {i+1}/{len(delisted)} ({elapsed:.0f}s, ETA {eta:.0f}s) — got {len(results)}")

bs.logout()
print(f"Got {len(results)}/{len(delisted)} in {time.time()-start:.0f}s")

# Write to DB
conn2 = duckdb.connect(DB)
fixed = 0
for ts_code, out_date in results.items():
    conn2.execute("""
        UPDATE stock_basic_history SET delist_date=?, eff_date=?
        WHERE ts_code=? AND is_delisted=TRUE
    """, [out_date, out_date, ts_code])
    fixed += 1
conn2.close()
print(f"Fixed {fixed} delist_dates")

# Verify
conn3 = duckdb.connect(DB, read_only=True)
sample = conn3.execute("""
    SELECT ts_code, name, list_date, delist_date FROM stock_basic_history
    WHERE is_delisted=TRUE ORDER BY delist_date LIMIT 5
""").fetchall()
print("Sample:")
for s in sample:
    print(f"  {s}")
# Count good vs sentinel
good = conn3.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date != '2099-12-31'
""").fetchone()[0]
sentinel = conn3.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted=TRUE AND delist_date = '2099-12-31'
""").fetchone()[0]
print(f"Real dates: {good}, Sentinel: {sentinel}")
conn3.close()
print("DONE")
