import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

conn = duckdb.connect(DB, read_only=True)
total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
sb_total = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
sb_delisted = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=TRUE").fetchone()[0]
sb_active = sb_total - sb_delisted

print("=== FINAL STATE ===")
print("stock_basic_history: Total=%d, Active=%d, Delisted=%d" % (total, total-delisted, delisted))
print("stock_basic: Total=%d, Active=%d, Delisted=%d" % (sb_total, sb_active, sb_delisted))
print("Index pollution: %d" % conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%%指数%%'").fetchone()[0])

print("\nExchange distribution:")
for ex, cnt in conn.execute("SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC").fetchall():
    print("  %s: %s" % (ex, cnt))

print("\nRecently listed stocks:")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, is_delisted FROM stock_basic_history WHERE list_date IS NOT NULL ORDER BY list_date DESC LIMIT 5").fetchall():
    print("  %s" % (r,))

print("\nDelisted stocks sample:")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, delist_date, delist_reason FROM stock_basic_history WHERE is_delisted=TRUE LIMIT 10").fetchall():
    print("  %s" % (r,))

print("\nstock_basic_history columns:")
for col in conn.execute("PRAGMA table_info('stock_basic_history')").fetchall():
    print("  %s: %s" % (col[1], col[2]))

print("\nstock_basic columns:")
for col in conn.execute("PRAGMA table_info('stock_basic')").fetchall():
    print("  %s: %s" % (col[1], col[2]))

print("\n=== DATABASE SUMMARY ===")
tables = ['daily_bar_raw','daily_bar_adjusted','stock_basic','stock_basic_history',
          'corporate_actions','st_status_history','index_constituents_history',
          'trade_calendar','data_quality_alert','financial_data']
for t in tables:
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
        print("  %s: %s" % (t, cnt))
    except:
        print("  %s: NOT FOUND" % t)

conn.close()
