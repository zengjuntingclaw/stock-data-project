"""
Step 1c: 精准修复 is_delisted 误标
====================================
策略：
  1. 重置全部为 is_delisted=FALSE（假设大多数是活跃的）
  2. Baostock query_all_stock 返回的 status='0' 为停牌股（非退市）
  3. 本地有但 Baostock 当前列表没有 = 可能退市
  4. 用 batch query_stock_basic 确认 outDate
"""
import sys, re
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

def convert_ts(bs_code):
    m = re.match(r'^([a-z]+)\.(.+)$', bs_code)
    if not m: return bs_code
    return '%s.%s' % (m.group(2), m.group(1).upper())

conn = duckdb.connect(DB)

# Step 1: 重置全部为活跃
print("Step 1: 重置全部 is_delisted = FALSE...")
conn.execute("UPDATE stock_basic_history SET is_delisted = FALSE, delist_reason = NULL, delist_date = NULL")
conn.execute("UPDATE stock_basic SET is_delisted = FALSE, delist_reason = NULL WHERE is_delisted IS NULL OR is_delisted = TRUE")
print("  Done")

# Step 2: 获取 Baostock 当前活跃股票列表
print("\nStep 2: 获取 Baostock 当前活跃股票...")
lg = bs.login()
rs = bs.query_all_stock(day='2026-04-10')
baostock_codes = set()
inactive = []
while rs.next():
    row = rs.get_row_data()
    code, status, name = row
    ts = convert_ts(code)
    baostock_codes.add(ts)
    if status == '0':
        inactive.append((ts, name))
bs.logout()
print("  Baostock 活跃: %d, 停牌(非退市): %d" % (len(baostock_codes), len(inactive)))

# Step 3: 找本地有但 Baostock 无的（可能是退市）
local = set(r[0] for r in conn.execute("SELECT ts_code FROM stock_basic_history").fetchall())
missing = local - baostock_codes
print("\nStep 3: 本地有但 Baostock 无: %d 只（可能是退市）" % len(missing))
for m in sorted(list(missing))[:10]:
    print("  ", m)

# Step 4: 批量查询 outDate（只查缺失的）
if missing:
    print("\nStep 4: 批量查询 outDate...")

    def query_one(ts_code):
        sym = ts_code.split('.')[0]
        ex = ts_code.split('.')[1].lower()
        bs_code = '%s.%s' % (ex, sym)
        lg2 = bs.login()
        rs2 = bs.query_stock_basic(code=bs_code)
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
        print("  查询 %d-%d/%d..." % (i+1, min(i+50,len(batch)), len(batch)))
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(query_one, c): c for c in chunk}
            for f in as_completed(futures):
                ts_code, out_date = f.result()
                if out_date and out_date.strip():
                    conn.execute("""
                        UPDATE stock_basic_history
                        SET is_delisted = TRUE, delist_date = ?, delist_reason = 'baostock_outDate'
                        WHERE ts_code = ?
                    """, [out_date.strip(), ts_code])
                    n_marked += 1
        time.sleep(0.3)

    print("  标记退市: %d" % n_marked)

# Step 5: 同步 stock_basic
print("\nStep 5: 同步 stock_basic is_delisted...")
conn.execute("""
    UPDATE stock_basic AS sb
    SET is_delisted = COALESCE(
        (SELECT s.is_delisted FROM stock_basic_history s WHERE s.ts_code = sb.ts_code), FALSE
    )
""")
print("  Done")

# Final verification
print("\n=== FINAL VERIFICATION ===")
total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
active = total - delisted
sb_total = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
sb_delisted = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=TRUE").fetchone()[0]
print("stock_basic_history: Total=%d, Active=%d, Delisted=%d" % (total, active, delisted))
print("stock_basic: Total=%d, Delisted=%d" % (sb_total, sb_delisted))
print("Index pollution: %s" % conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE name LIKE '%指数%'").fetchone()[0])
print("Exchange dist:")
for ex, cnt in conn.execute("SELECT exchange, COUNT(*) FROM stock_basic_history GROUP BY exchange ORDER BY COUNT(*) DESC").fetchall():
    print("  %s: %s" % (ex, cnt))
print("\nSamples (recently listed):")
for r in conn.execute("SELECT ts_code, name, exchange, list_date, is_delisted FROM stock_basic_history ORDER BY list_date DESC NULLS LAST LIMIT 5").fetchall():
    print("  %s" % (r,))
conn.close()
