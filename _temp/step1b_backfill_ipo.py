"""
Step 1b Fix: 用 baostock.query_stock_basic 补全 1479 只新股的 list_date
=========================================================================
query_stock_basic(code) 返回: code, code_name, ipoDate, outDate
批量查询股票基础信息（1次1只，但可以并发）
"""
import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb
import baostock as bs
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

def get_missing_list_date_codes():
    """获取 stock_basic_history 中 list_date 为 NULL 的股票"""
    conn = duckdb.connect(DB, read_only=True)
    rows = conn.execute("""
        SELECT ts_code, symbol
        FROM stock_basic_history
        WHERE list_date IS NULL OR eff_date = '2099-12-31'
    """).fetchall()
    conn.close()
    return rows


def fetch_baostock_basic(code):
    """查询单只股票的基础信息"""
    lg = bs.login()
    if lg.error_code != '0':
        return None, None, None
    rs = bs.query_stock_basic(code=code)
    ipo_date = None
    out_date = None
    while rs.error_code == '0' and rs.next():
        row = rs.get_row_data()
        ipo_date = row[2] if len(row) > 2 else None   # ipoDate
        out_date = row[3] if len(row) > 3 else None   # outDate
    bs.logout()
    return ipo_date, out_date, None


def convert_code(ts_code):
    """000001.SH → sh.000001"""
    m = re.match(r'^(\d+)\.(\w+)$', ts_code)
    if not m:
        return ts_code
    return f"{m.group(2).lower()}.{m.group(1)}"


def main():
    print("=== Step 1b: 补全 list_date ===")
    missing = get_missing_list_date_codes()
    print(f"待补全股票: {len(missing)} 只")

    if not missing:
        print("无需补全")
        return

    # 分批查询（避免频繁登录退出）
    batch_size = 50
    results = {}
    total = len(missing)

    for i in range(0, total, batch_size):
        batch = missing[i:i+batch_size]
        print(f"  查询 {i+1}-{min(i+batch_size,total)}/{total}...")

        def fetch_one(code_info):
            ts_code, symbol = code_info
            bs_code = convert_code(ts_code)
            ipo, out, _ = fetch_baostock_basic(bs_code)
            return ts_code, ipo, out

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(fetch_one, c): c for c in batch}
            for f in as_completed(futures):
                ts, ipo, out = f.result()
                if ipo and ipo.strip():
                    results[ts] = (ipo.strip(), out.strip() if out else None)

        if i < total - batch_size:
            time.sleep(0.5)

    print(f"\n获取到 IPO 日期: {len(results)}/{len(missing)}")

    if results:
        conn = duckdb.connect(DB)
        updated = 0
        for ts_code, (list_date, delist_date) in results.items():
            delist_reason = 'baostock_missing_out' if delist_date else None
            conn.execute("""
                UPDATE stock_basic_history
                SET list_date = ?,
                    delist_date = ?,
                    is_delisted = CASE WHEN ? IS NOT NULL AND ? != '' THEN TRUE ELSE FALSE END,
                    delist_reason = ?,
                    eff_date = ?
                WHERE ts_code = ?
            """, [list_date, delist_date, delist_date, delist_date, delist_reason, list_date, ts_code])
            updated += 1

        conn.close()
        print(f"更新 {updated} 条 IPO 日期")

    # 最终验证
    conn = duckdb.connect(DB, read_only=True)
    null_list_date = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE list_date IS NULL").fetchone()[0]
    sentinel_eff = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE eff_date = '2099-12-31'").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
    delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = TRUE").fetchone()[0]
    print(f"\n最终: 总{total} | 有IPO日期{total-null_list_date} | 无日期{null_list_date} | 哨兵{sentinel_eff} | 退市{delisted}")

    # 样本
    print("\n样本（前5）:")
    sample = conn.execute("SELECT ts_code,name,exchange,list_date,is_delisted FROM stock_basic_history LIMIT 5").fetchall()
    for s in sample: print(f"  {s}")
    conn.close()


if __name__ == '__main__':
    main()
