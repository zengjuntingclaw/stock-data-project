"""
Step 1b FIX: 紧急修复 is_delisted 批量误标问题
================================================
问题：UPDATE 缺少 WHERE 条件，导致所有活跃股被误标为退市
修复：用 baostock.query_history_stock_list 获取真正退市股票列表
"""
import sys, re
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb, baostock as bs

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB)

# Step 1: 批量查询 Baostock 的退市股票列表
# 遍历多个历史日期，找到已退市股票
print("=== 查询 Baostock 历史股票列表（识别真正退市） ===")

delisted_set = set()

def convert_ts(bs_code):
    """sh.000001 -> 000001.SH"""
    m = re.match(r'^([a-z]+)\.(.+)$', bs_code)
    if not m: return bs_code
    return '%s.%s' % (m.group(2), m.group(1).upper())

# 退市股票专用接口
lg = bs.login()
rs = bs.query_delist_data()
print('Delist data API:', rs.error_code, rs.error_msg)
delist_stocks = []
while rs.next():
    row = rs.get_row_data()
    delist_stocks.append(row)
bs.logout()
print('退市股票列表: %d' % len(delist_stocks))
for r in delist_stocks[:10]:
    print(' ', r)

# 同时：从 history_k_data 查询已退市股票的 outDate
# 使用 Baostock 分批次查询
print("\n用历史快照补全 delist_date...")

# 先把所有股票改回 is_delisted=FALSE
print("Step 1: 重置所有 is_delisted = FALSE")
conn.execute("UPDATE stock_basic_history SET is_delisted = FALSE, delist_reason = NULL WHERE is_delisted = TRUE")
conn.execute("UPDATE stock_basic SET is_delisted = FALSE, delist_reason = NULL WHERE is_delisted = TRUE")
conn.execute("UPDATE stock_basic SET is_delisted = FALSE WHERE is_delisted IS NULL")
print("重置完成")

# Step 2: 用 baostock.query_all_stock 识别活跃状态
# 如果 query_all_stock 返回 status='0' → 停牌（非退市）
lg = bs.login()
rs = bs.query_all_stock(day='2026-04-10')
all_stocks = {}
while rs.next():
    row = rs.get_row_data()
    code = row[0]
    status = row[1]
    all_stocks[code] = status
bs.logout()
print('\nBaostock 当前列表: %d 只' % len(all_stocks))

# 判断真正退市：本地有但 Baostock 当前列表没有
local_all = conn.execute("SELECT ts_code FROM stock_basic_history").df()
local_codes = set(local_all['ts_code'])

baostock_codes = set()
for bs_code in all_stocks.keys():
    ts = convert_ts(bs_code)
    baostock_codes.add(ts)

truly_delisted = local_codes - baostock_codes
print('本地有但 Baostock 当前无（可能是退市或非常早期）: %d' % len(truly_delisted))

# 用 Baostock 逐只查询获取 outDate
print('\n补全真正退市股的 delist_date...')
n_updated = 0
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def query_out_date(ts_code):
    sym = ts_code.split('.')[0]
    ex = ts_code.split('.')[1].lower()
    bs_code = '%s.%s' % (ex, sym)
    lg2 = bs.login()
    rs2 = bs.query_stock_basic(code=bs_code)
    out_date = None
    while rs2.next():
        row = rs2.get_row_data()
        out_date = row[3] if len(row) > 3 else None   # outDate
    bs.logout()
    return ts_code, out_date

# 只查询可能是退市的（limit 200，只取样本）
sample_delisted = list(truly_delisted)[:500]
batch_size = 50
all_results = {}

for i in range(0, len(sample_delisted), batch_size):
    batch = sample_delisted[i:i+batch_size]
    print('  查询 %d-%d/%d...' % (i+1, min(i+batch_size,len(sample_delisted)), len(sample_delisted)))
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(query_out_date, c): c for c in batch}
        for f in as_completed(futures):
            ts, out = f.result()
            if out and out.strip():
                all_results[ts] = out.strip()
    time.sleep(0.3)

print('\n获取到 outDate: %d/%d' % (len(all_results), len(sample_delisted)))

# 标记为退市
for ts_code, out_date in all_results.items():
    conn.execute("""
        UPDATE stock_basic_history
        SET is_delisted = TRUE, delist_date = ?, delist_reason = 'baostock_outDate'
        WHERE ts_code = ? AND is_delisted = FALSE
    """, [out_date, ts_code])
    n_updated += 1

print('标记 %d 只为退市' % n_updated)

# 最终验证
total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE").fetchone()[0]
print('\n=== 最终验证 ===')
print('Total: %d, Delisted: %d, Active: %d' % (total, delisted, total-delisted))
print('stock_basic 同步...')
conn.execute("UPDATE stock_basic SET is_delisted = (SELECT s.is_delisted FROM stock_basic_history s WHERE s.ts_code = stock_basic.ts_code)")
print('Done')
conn.close()
