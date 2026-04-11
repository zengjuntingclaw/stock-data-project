"""
Step 1: 清理 stock_basic_history 指数污染 + 统一 schema
=========================================================
问题：
  - AkShare stock_zh_a_spot_em() 同时返回股票+指数（如 000001.SH=证券综合指数）
  - 导致 stock_basic_history 混入指数数据

修复：
  1. 识别并删除指数数据（通过 symbol 特征识别：6位数字+指数标记）
  2. 统一 schema（删除 sql_config.py 旧表定义，统一由 data_engine._init_schema 管理）
"""

import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'

def get_conn():
    return duckdb.connect(DB)


def step1a_clean_index_contamination():
    """Step 1a: 清理 stock_basic_history 中的指数污染"""
    conn = get_conn()

    # 检查：有哪些 exchange ？
    exchanges = conn.execute("""
        SELECT exchange, COUNT(*) as cnt
        FROM stock_basic_history
        GROUP BY exchange
        ORDER BY cnt DESC
    """).fetchall()
    print("=== Step 1a: stock_basic_history exchange 分布 ===")
    for ex, cnt in exchanges:
        print(f"  {ex}: {cnt}")

    # 识别疑似指数：exchange='SH' 但 name 含'指数'/'指基'/'综指'
    suspects = conn.execute("""
        SELECT ts_code, name, exchange, list_date
        FROM stock_basic_history
        WHERE name LIKE '%指数%' OR name LIKE '%指基%' OR name LIKE '%综指%'
           OR name LIKE '%国债%指数%' OR name LIKE '%沪深300%'
    """).fetchall()
    print(f"\n=== 疑似指数记录: {len(suspects)} 条 ===")
    for s in suspects[:20]:
        print(f"  {s}")

    # 识别真正的股票：按代码前缀
    # 000xxx.SH: 可能是指数（上证指数系列）
    # 600xxx.SH / 601xxx.SH / 603xxx.SH / 605xxx.SH: 上证主板
    # 688xxx.SH: 科创板
    # 000xxx.SZ / 001xxx.SZ / 002xxx.SZ / 003xxx.SZ: 深证
    # 300xxx.SZ: 创业板
    # 430xxx.BJ / 830xxx.BJ / 920xxx.BJ: 北交所
    real_stock_patterns = [
        ('SH', '6%'), ('SH', '688%'),   # 上证主板 + 科创板
        ('SZ', '0%'), ('SZ', '1%'), ('SZ', '2%'), ('SZ', '3%'),  # 深证
        ('BJ', '4%'), ('BJ', '8%'), ('BJ', '9%'),  # 北交所
    ]

    # 先统计被污染的行
    suspect_codes = [s[0] for s in suspects]
    print(f"\n删除 {len(suspect_codes)} 条指数污染记录...")

    if suspect_codes:
        placeholders = ','.join(['?' for _ in suspect_codes])
        deleted = conn.execute(f"""
            DELETE FROM stock_basic_history
            WHERE ts_code IN ({placeholders})
        """, suspect_codes)
        print(f"已删除 {deleted.fetchone()[0] if hasattr(deleted, 'fetchone') else 'N/A'} 条")

    # 验证清理结果
    remaining = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
    print(f"\n清理后 stock_basic_history: {remaining} 行")

    conn.close()
    print("Step 1a 完成\n")


def step1b_sync_status_from_baostock():
    """
    Step 1b: 用 Baostock query_all_stock 更新 is_delisted 状态
    =================================================================
    Baostock query_all_stock 返回 (code, tradeStatus, code_name)：
      - tradeStatus='1' = 正常交易
      - tradeStatus='0' = 停牌/退市/暂停上市
    只用这个更新 is_delisted，不重建整表（避免丢失已有字段）
    """
    try:
        import baostock as bs
    except ImportError:
        print("Baostock 未安装，跳过状态同步")
        return

    conn = get_conn()
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        conn.close()
        return
    print(f"Baostock 登录: {lg.error_msg}")

    rs = bs.query_all_stock(day='2026-04-10')
    stocks = []
    while rs.error_code == '0' and rs.next():
        stocks.append(rs.get_row_data())
    bs.logout()
    print(f"Baostock 返回 {len(stocks)} 条（含指数和股票混合）")

    if not stocks:
        conn.close()
        return

    import pandas as pd
    df = pd.DataFrame(stocks, columns=['code', 'tradeStatus', 'code_name'])

    # 过滤指数（code_name 含'指数'/'指基'）
    mask = ~(
        df['code_name'].str.contains('指数', na=False) |
        df['code_name'].str.contains('指基', na=False)
    )
    df = df[mask].copy()
    print(f"过滤指数后: {len(df)} 条")

    # 转换为 ts_code 格式：sh.000001 → 000001.SH
    # 现有 stock_basic_history 的格式是 symbol.EXCHANGE（如 000001.SH）
    # Baostock 返回的是 EXCHANGE.symbol（如 sh.000001）
    df['ts_code'] = df['code'].str.replace(r'^([a-z]+)\.(.+)$', r'\2.\1', regex=True).str.upper()

    # 更新 stock_basic_history 的 is_delisted 状态
    # tradeStatus='0' → is_delisted=TRUE
    inactive = df[df['tradeStatus'] == '0']
    active = df[df['tradeStatus'] == '1']
    print(f"  活跃(1): {len(active)}, 非活跃(0): {len(inactive)}")

    # 批量更新非活跃股票
    if len(inactive) > 0:
        inactive_codes = inactive['ts_code'].tolist()
        placeholders = ','.join(['?' for _ in inactive_codes])
        conn.execute(f"""
            UPDATE stock_basic_history
            SET is_delisted = TRUE, delist_reason = 'baostock_tradeStatus_0'
            WHERE ts_code IN ({placeholders})
        """, inactive_codes)
        print(f"  标记 {len(inactive_codes)} 只为退市")

    # 检查现有 stock_basic_history 是否被 Baostock 覆盖
    existing = conn.execute("SELECT ts_code FROM stock_basic_history").df()
    baostock_codes = set(df['ts_code'])
    existing_codes = set(existing['ts_code'])
    new_in_baostock = baostock_codes - existing_codes
    missing_from_baostock = existing_codes - baostock_codes

    if new_in_baostock:
        print(f"  Baostock 有但本地无: {len(new_in_baostock)}（新增上市股），逐条入库中...")
        # 用逐条INSERT（安全，兼容 NULL 日期）
        new_stocks = df[df['ts_code'].isin(new_in_baostock)]
        n_inserted = 0
        for _, row in new_stocks.iterrows():
            ts = row['ts_code']
            sym = ts.split('.')[0]
            name = row['code_name']
            ex = ts.split('.')[1]
            conn.execute("""
                INSERT OR IGNORE INTO stock_basic_history
                (ts_code, symbol, name, exchange, area, industry, market,
                 list_date, delist_date, is_delisted, delist_reason, board, eff_date, end_date)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, FALSE, NULL, NULL, '2099-12-31', NULL)
            """, [ts, sym, name, ex])
            n_inserted += 1
        print(f"  补充入库 {n_inserted} 只新增股")

    if missing_from_baostock:
        print(f"  本地有但 Baostock 无: {len(missing_from_baostock)}（可能已退市）")
        # 标记为退市
        missing = list(missing_from_baostock)
        placeholders2 = ','.join(['?' for _ in missing])
        conn.execute(f"""
            UPDATE stock_basic_history
            SET is_delisted = TRUE, delist_reason = 'baostock_missing'
            WHERE ts_code IN ({placeholders2}) AND is_delisted = FALSE
        """, missing)

    # 验证
    total = conn.execute("SELECT COUNT(*) FROM stock_basic_history").fetchone()[0]
    delisted = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted = TRUE").fetchone()[0]
    print(f"\nstock_basic_history: {total} 总, {delisted} 退市")

    conn.close()
    print("Step 1b 完成\n")


def step1c_clean_stock_basic():
    """Step 1c: 清理 stock_basic 表中的指数污染（同 Step 1a）"""
    conn = get_conn()

    # stock_basic 无 exchange 列，但同样被污染
    suspects = conn.execute("""
        SELECT ts_code, name FROM stock_basic
        WHERE name LIKE '%指数%' OR name LIKE '%指基%' OR name LIKE '%综指%'
    """).fetchall()
    print(f"\n=== Step 1c: stock_basic 指数污染: {len(suspects)} 条 ===")

    if suspects:
        suspect_codes = [s[0] for s in suspects]
        placeholders = ','.join(['?' for _ in suspect_codes])
        conn.execute(f"DELETE FROM stock_basic WHERE ts_code IN ({placeholders})", suspect_codes)
        print(f"已删除 {len(suspect_codes)} 条")

    print(f"清理后 stock_basic: {conn.execute('SELECT COUNT(*) FROM stock_basic').fetchone()[0]} 行")
    conn.close()
    print("Step 1c 完成\n")


def step1d_add_exchange_to_stock_basic():
    """Step 1d: 给 stock_basic 补充 exchange 列（从 ts_code 派生）"""
    conn = get_conn()

    # 检查是否有 exchange 列
    cols = [c[1] for c in conn.execute("PRAGMA table_info('stock_basic')").fetchall()]
    if 'exchange' not in cols:
        print("=== Step 1d: ALTER TABLE stock_basic ADD exchange ===")
        conn.execute("ALTER TABLE stock_basic ADD COLUMN exchange VARCHAR")
        conn.execute("""
            UPDATE stock_basic
            SET exchange = CASE
                WHEN ts_code LIKE '%.SH' THEN 'SH'
                WHEN ts_code LIKE '%.SZ' THEN 'SZ'
                WHEN ts_code LIKE '%.BJ' THEN 'BJ'
                ELSE 'SZ'
            END
        """)
        print(f"exchange 列已添加，更新完成")

    # 补充 board 列
    if 'board' not in cols:
        print("ALTER TABLE stock_basic ADD board...")
        conn.execute("ALTER TABLE stock_basic ADD COLUMN board VARCHAR")
        conn.execute("""
            UPDATE stock_basic SET board = 'kcb' WHERE ts_code LIKE '688%.SH'
        """)
        conn.execute("UPDATE stock_basic SET board = 'chinext' WHERE ts_code LIKE '300%.SZ'")
        conn.execute("UPDATE stock_basic SET board = 'bse' WHERE ts_code LIKE '4%.BJ' OR ts_code LIKE '8%.BJ' OR ts_code LIKE '9%.BJ'")
        conn.execute("UPDATE stock_basic SET board = 'main' WHERE board IS NULL")
        print("board 列已添加")

    conn.close()
    print("Step 1d 完成\n")


def main():
    print("=" * 60)
    print("Step 1: 清理指数污染 + 重建 stock_basic_history")
    print("=" * 60)

    step1a_clean_index_contamination()
    step1c_clean_stock_basic()
    step1d_add_exchange_to_stock_basic()
    step1b_sync_status_from_baostock()

    # 最终验证
    conn = get_conn()
    print("\n=== 最终验证 ===")
    for t in ['stock_basic_history', 'stock_basic']:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {cnt} 行")

    # 检查是否还有指数残留
    suspects = conn.execute("""
        SELECT COUNT(*) FROM stock_basic_history
        WHERE name LIKE '%指数%' OR name LIKE '%指基%'
    """).fetchone()[0]
    print(f"  stock_basic_history 指数残留: {suspects}")

    conn.close()


if __name__ == '__main__':
    main()
