import sys, os
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs, time, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
LOG = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/step1c_out.txt'

def log(msg):
    print(msg)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')

def convert_ts(bs_code):
    m = re.match(r'^([a-z]+)\.(.+)$', bs_code)
    if not m: return bs_code
    return '%s.%s' % (m.group(2), m.group(1).upper())

log("Step 1: Reset all is_delisted = FALSE")
conn = duckdb.connect(DB)
conn.execute("UPDATE stock_basic_history SET is_delisted = FALSE, delist_reason = NULL, delist_date = NULL")
conn.execute("UPDATE stock_basic SET is_delisted = FALSE, delist_reason = NULL")
log("Done")

log("Step 2: Get Baostock active stocks")
lg = bs.login()
rs = bs.query_all_stock(day='2026-04-10')
baostock_codes = set()
while rs.next():
    row = rs.get_row_data()
    ts = convert_ts(row[0])
    baostock_codes.add(ts)
bs.logout()
log("Baostock active: %d" % len(baostock_codes))

local = set(r[0] for r in conn.execute("SELECT ts_code FROM stock_basic_history").fetchall())
missing = local - baostock_codes
log("Step 3: Local but not in Baostock: %d (potential delisted)" % len(missing))

if missing:
    def query_one(ts_code):
        sym = ts_code.split('.')[0]
        ex = ts_code.split('.')[1].lower()
        lg2 = bs.login()
        rs2 = bs.query_stock_basic(code='%s.%s' % (ex, sym))
        out_date = None
        while rs2.next():
            row = rs2.get_row_data()
            out_date = row[3] if len(row) > 3 else None
        bs.logout()
        return ts_code, out_date

    n_marked = 0
    batch = list(missing)
    for i in range(0, len(batch), 50):
        chunk = batch[i:i+50]
        log("  Query %d-%d/%d..." % (i+1, min(i+50,len(batch)), len(batch)))
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(query_one, c): c for c in chunk}
            for f in as_completed(futures):
                ts_code, out_date = f.result()
                if out_date and out_date.strip():
                    conn.execute("""
                        UPDATE stock_basic_history
                        SET is_delisted = TRUE, delist_date = ?, delist_reason = 'baostock_outDate'
                        WHERE ts_code = ? AND is_delisted = FALSE
                    """, [out_date.strip(), ts_code])
                    n_marked += 1
        time.sleep(0.3)
    log("Marked delisted: %d" % n_marked)

log("Step 4: Sync stock_basic")
conn.execute("""
    UPDATE stock_basic AS sb
    SET is_delisted = COALESCE(
        (SELECT s.is_delisted FROM stock_basic_history s WHERE s.ts_code = sb.ts_code), FALSE
    )
""")

total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
log("FINAL: stock_basic_history: Total=%d, Active=%d, Delisted=%d" % (total, total-delisted, delisted))
sb_total = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
sb_delisted = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=TRUE").fetchone()[0]
log("FINAL: stock_basic: Total=%d, Delisted=%d" % (sb_total, sb_delisted))
log("Index pollution: %d" % conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%%指数%%'").fetchone()[0])
log("Exchange dist:")
for ex, cnt in conn.execute("SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC").fetchall():
    log("  %s: %s" % (ex, cnt))
log("Recent samples:")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, is_delisted FROM stock_basic_history ORDER BY list_date DESC NULLS LAST LIMIT 5").fetchall():
    log("  %s" % (r,))
conn.close()
log("ALL DONE")
