"""
重建 stock_basic 表（Baostock 真实数据）

从 Baostock 获取全部 A股（沪深）证券信息，替换现有的虚假数据。
包含：真实上市日期、退市日期、股票名称、退市状态

执行方式：
    python scripts/rebuild_stock_basic.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import baostock as bs
import pandas as pd
import duckdb

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'data', 'stock_data.duckdb'
)


def _bs_to_ts_code(bs_code):
    if not bs_code or str(bs_code).strip() == '':
        return ''
    code = str(bs_code).strip().lower()
    if code.startswith('sh.'):
        return code[3:].zfill(6) + '.SH'
    elif code.startswith('sz.'):
        return code[3:].zfill(6) + '.SZ'
    elif code.startswith('bj.'):
        return code[3:].zfill(6) + '.BJ'
    return bs_code


def fetch_all_a_stocks():
    print('[DOWN] Connecting to Baostock...')
    lg = bs.login()
    if lg.error_code != '0':
        raise RuntimeError('Baostock login failed: ' + lg.error_msg)
    print('[OK] Login: ' + lg.error_msg)

    print('[DOWN] Fetching stock list...')
    rs = bs.query_stock_basic(code_name='')
    data = []
    while rs.error_code == '0' and rs.next():
        data.append(rs.get_row_data())
    bs.logout()

    df = pd.DataFrame(data, columns=rs.fields)
    print('[INFO] Total records: ' + str(len(df)))

    # 只保留 A股（type 1=沪市A，type 2=深市A）
    a_stocks = df[df['type'].isin(['1', '2'])].copy()
    print('[INFO] A-shares: ' + str(len(a_stocks)))

    # 转换 ts_code
    a_stocks['ts_code'] = a_stocks['code'].apply(_bs_to_ts_code)

    # 标准化字段
    # 注意：stock_basic 表无 exchange/code 列，ts_code 已含交易所(.SH/.SZ)
    a_stocks['symbol'] = a_stocks['ts_code'].str[:6]
    a_stocks['name'] = a_stocks['code_name']
    a_stocks['list_date'] = a_stocks['ipoDate']
    # delist_date: 空字符串转为 None（DuckDB DATE 列可接受 NULL）
    a_stocks['delist_date'] = a_stocks['outDate'].replace('', None)

    # is_delisted: 有退市日期 = True
    def _map_status(row):
        od = str(row['outDate']).strip() if row['outDate'] else ''
        return True if od else False

    a_stocks['is_delisted'] = a_stocks.apply(_map_status, axis=1)

    # area/industry/market/del_list_reason/is_hs 留空
    a_stocks['area'] = ''
    a_stocks['industry'] = ''
    a_stocks['market'] = ''
    a_stocks['delist_reason'] = ''
    a_stocks['is_hs'] = True  # 沪深股票

    result = a_stocks[['ts_code', 'symbol', 'name',
                        'list_date', 'delist_date', 'is_delisted',
                        'area', 'industry', 'market', 'delist_reason', 'is_hs']].copy()
    result = result[result['ts_code'].notna() & (result['ts_code'] != '')]

    # 空日期转 None（DuckDB DATE 列不接受空字符串）
    for col in ('list_date', 'delist_date'):
        result[col] = result[col].replace('', None)

    active = (~result['is_delisted']).sum()
    delisted = result['is_delisted'].sum()
    print('[INFO] Active: ' + str(active) + ', Delisted: ' + str(delisted))

    return result.reset_index(drop=True)


def rebuild_stock_basic():
    print('=' * 60)
    print('[BUILD] Rebuild stock_basic with Baostock data')
    print('=' * 60)

    df = fetch_all_a_stocks()

    print('\n[DOWN] DB: ' + DB_PATH)
    conn = duckdb.connect(DB_PATH, read_only=False)

    existing = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
    print('[INFO] Existing records: ' + str(existing))

    print('[DEL] Deleting old data...')
    conn.execute("DELETE FROM stock_basic")

    print('[DOWN] Inserting ' + str(len(df)) + ' records...')
    conn.execute("DELETE FROM stock_basic")
    conn.execute("INSERT INTO stock_basic BY NAME SELECT * FROM df")

    new_count = conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=false").fetchone()[0]
    delisted = conn.execute("SELECT COUNT(*) FROM stock_basic WHERE is_delisted=true").fetchone()[0]

    print('\n[OK] Done!')
    print('  Total:    ' + str(new_count))
    print('  Active:   ' + str(active))
    print('  Delisted: ' + str(delisted))

    print('\n[SAMPLE] Active stocks:')
    rows = conn.execute("""
        SELECT ts_code, name, list_date FROM stock_basic
        WHERE is_delisted=false ORDER BY list_date LIMIT 5
    """).fetchall()
    for r in rows:
        print('  ' + str(r))

    print('\n[SAMPLE] Delisted stocks:')
    rows = conn.execute("""
        SELECT ts_code, name, list_date, delist_date FROM stock_basic
        WHERE is_delisted=true LIMIT 5
    """).fetchall()
    for r in rows:
        print('  ' + str(r))

    conn.close()


if __name__ == '__main__':
    try:
        rebuild_stock_basic()
        print('\n[COMPLETE] stock_basic rebuilt!')
    except Exception as e:
        print('\n[ERROR] ' + str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)
