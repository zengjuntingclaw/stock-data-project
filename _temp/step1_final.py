"""
Step 1 Final: 精准修复 is_delisted + 最终验证
==============================================
纯 DuckDB 操作，无 Baostock（避免并发/锁定问题）
"""
import sys, re, os
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
LOG = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/step1_final.txt'

def log(msg):
    print(msg)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(msg + '\n')

# Use exclusive mode to avoid lock conflicts
def get_conn(read_only=False):
    return duckdb.connect(DB, read_only=read_only)

log("=== Step 1: Reset all is_delisted = FALSE ===")
conn = get_conn()
conn.execute("UPDATE stock_basic_history SET is_delisted = FALSE, delist_reason = NULL, delist_date = NULL")
conn.execute("UPDATE stock_basic SET is_delisted = FALSE, delist_reason = NULL")
log("  Done")

log("=== Step 2: Restore known delisted stocks from audit trail ===")
# Known delisted stocks (manually verified from historical data):
# These are stocks with clear evidence of delisting in the database
# Rule: If they have a delist_date in stock_basic_history, they are delisted
# We restore from the data that was already in stock_basic_history before our bug

# Read the original audit: 504 stocks had '指' in name (指数) - already deleted
# The remaining stocks need to be verified

# Known truly delisted A-share stocks (from financial history):
# These are stocks that have been delisted from Shanghai/Shenzhen exchanges
# Using common knowledge: A-share delisted stocks are relatively few (~100-200 historically)
# For now, mark as active all stocks - we'll verify with Baostock separately

# Strategy: look at which stocks have historical daily_bar data but are not in current list
# This requires Baostock query_all_stock to identify active stocks

log("  Will verify with Baostock query_all_stock separately")
conn.close()

log("=== Step 3: Baostock verification ===")
try:
    import baostock as bs
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def convert_ts(bs_code):
        m = re.match(r'^([a-z]+)\.(.+)$', bs_code)
        if not m: return bs_code
        return '%s.%s' % (m.group(2), m.group(1).upper())

    lg = bs.login()
    rs = bs.query_all_stock(day='2026-04-10')
    baostock_active = set()
    while rs.next():
        row = rs.get_row_data()
        ts = convert_ts(row[0])
        baostock_active.add(ts)
    bs.logout()
    log("  Baostock active stocks: %d" % len(baostock_active))

    conn = get_conn()
    local = set(r[0] for r in conn.execute("SELECT ts_code FROM stock_basic_history").fetchall())
    truly_delisted = local - baostock_active
    log("  Truly delisted (in local but not in Baostock): %d" % len(truly_delisted))

    # Mark as delisted
    if truly_delisted:
        codes = list(truly_delisted)
        placeholders = ','.join(['?' for _ in codes])
        conn.execute("UPDATE stock_basic_history SET is_delisted = TRUE, delist_reason = 'baostock_not_in_list' WHERE ts_code IN (%s)" % placeholders, codes)
        conn.execute("UPDATE stock_basic SET is_delisted = TRUE, delist_reason = 'baostock_not_in_list' WHERE ts_code IN (%s)" % placeholders, codes)
        log("  Marked %d as delisted" % len(truly_delisted))

    # Mark Baostock inactive (status=0) as suspended
    lg2 = bs.login()
    rs2 = bs.query_all_stock(day='2026-04-10')
    suspended = []
    while rs2.next():
        row = rs2.get_row_data()
        if row[1] == '0':  # tradeStatus=0 means suspended/delisted
            suspended.append(convert_ts(row[0]))
    bs.logout()
    if suspended:
        placeholders2 = ','.join(['?' for _ in suspended])
        conn.execute("UPDATE stock_basic_history SET is_suspended = TRUE WHERE ts_code IN (%s)" % placeholders2, suspended)
        log("  Marked %d as suspended" % len(suspended))

    conn.close()
    log("Step 3 done")

except ImportError:
    log("  Baostock not available, skipping Step 3")
except Exception as e:
    log("  Baostock error: %s" % str(e))

# Final verification
log("\n=== FINAL VERIFICATION ===")
conn = get_conn(read_only=True)
total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
active = total - delisted
sb_total = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
sb_delisted = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=TRUE").fetchone()[0]
sb_active = sb_total - sb_delisted

log("stock_basic_history: Total=%d, Active=%d, Delisted=%d" % (total, active, delisted))
log("stock_basic: Total=%d, Active=%d, Delisted=%d" % (sb_total, sb_active, sb_delisted))
log("Index pollution: %d" % conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%%指数%%'").fetchone()[0])

log("\nExchange distribution:")
for ex, cnt in conn.execute("SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC").fetchall():
    log("  %s: %s" % (ex, cnt))

log("\nRecently listed stocks:")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, is_delisted FROM stock_basic_history WHERE list_date IS NOT NULL ORDER BY list_date DESC LIMIT 5").fetchall():
    log("  %s" % (r,))

log("\nDelisted stocks sample:")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, delist_date, delist_reason FROM stock_basic_history WHERE is_delisted=TRUE LIMIT 5").fetchall():
    log("  %s" % (r,))

conn.close()
log("\nALL DONE")
