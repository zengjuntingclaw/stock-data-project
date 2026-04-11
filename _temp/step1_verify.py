"""
Step 1 Final Fix: 修复 is_suspended 列 + 最终验证
"""
import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

conn = duckdb.connect(DB)
# Check if is_suspended exists
cols = [c[1] for c in conn.execute("PRAGMA table_info('stock_basic_history')").fetchall()]
if 'is_suspended' not in cols:
    print("Adding is_suspended column...")
    conn.execute("ALTER TABLE stock_basic_history ADD COLUMN is_suspended BOOLEAN DEFAULT FALSE")
    conn.execute("ALTER TABLE stock_basic ADD COLUMN is_suspended BOOLEAN DEFAULT FALSE")
    print("Done")
else:
    print("is_suspended column exists")

conn.close()

# Now read-only verification
conn2 = duckdb.connect(DB, read_only=True)
total = conn2.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn2.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
sb_total = conn2.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
sb_delisted = conn2.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=TRUE").fetchone()[0]
sb_active = sb_total - sb_delisted

print("\n=== FINAL VERIFICATION ===")
print("stock_basic_history: Total=%d, Active=%d, Delisted=%d" % (total, total-delisted, delisted))
print("stock_basic: Total=%d, Active=%d, Delisted=%d" % (sb_total, sb_active, sb_delisted))
print("Index pollution: %d" % conn2.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%%指数%%'").fetchone()[0])

print("\nExchange distribution:")
for ex, cnt in conn2.execute("SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC").fetchall():
    print("  %s: %s" % (ex, cnt))

print("\nRecently listed stocks:")
for r in conn2.execute("SELECT ts_code, name, exchange, list_date, is_delisted FROM stock_basic_history WHERE list_date IS NOT NULL ORDER BY list_date DESC LIMIT 5").fetchall():
    print("  %s" % (r,))

print("\nDelisted stocks sample:")
for r in conn2.execute("SELECT ts_code, name, exchange, list_date, delist_date, delist_reason FROM stock_basic_history WHERE is_delisted=TRUE LIMIT 10").fetchall():
    print("  %s" % (r,))

print("\nTable columns:")
for col in conn2.execute("PRAGMA table_info('stock_basic_history')").fetchall():
    print("  %s: %s" % (col[1], col[2]))

conn2.close()
print("\nALL DONE")
