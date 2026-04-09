"""
补全退市股历史行情（Baostock，串行保守版）

串行下载 + 智能重试 + 断点续传
"""
import os, sys, time, warnings
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
warnings.filterwarnings('ignore')

import baostock as bs
import pandas as pd
import duckdb

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'data', 'stock_data.duckdb'
)
BATCH_COMMIT = 20   # 每20只commit一次
RETRY_DELAY  = [1, 3, 8]  # 重试间隔(秒)

FIELDS = 'date,code,open,high,low,close,volume,amount,adjustflag,pctChg,turn'


def _ts_to_bs_code(ts_code):
    code = str(ts_code)
    suffix = code[-2:]
    num = code[:6]
    if suffix == 'SH': return 'sh.' + num
    if suffix == 'SZ': return 'sz.' + num
    if suffix == 'BJ': return 'bj.' + num
    return ts_code


def _bs_login():
    for delay in RETRY_DELAY:
        try:
            lg = bs.login()
            if str(lg.error_code) == '0':
                return True
        except Exception:
            pass
        time.sleep(delay)
    return False


def _fetch_single(ts_code, list_date=None, delist_date=None):
    bs_code = _ts_to_bs_code(ts_code)
    start = str(list_date) if list_date else '1990-01-01'
    end = str(delist_date) if delist_date else '2026-12-31'

    for attempt in range(3):
        try:
            if not _bs_login():
                continue

            rs = bs.query_history_k_data_plus(
                bs_code, FIELDS,
                start_date=start, end_date=end,
                frequency='d', adjustflag='3'
            )
            rows = []
            while str(rs.error_code) == '0' and rs.next():
                rows.append(rs.get_row_data())
            bs.logout()

            if not rows:
                return ts_code, 0, None

            df = pd.DataFrame(rows, columns=rs.fields)
            df['ts_code'] = ts_code
            df = df.rename(columns={
                'date': 'trade_date',
                'pctChg': 'pct_chg',
                'turn': 'turnover',
            })
            df = df.replace('', None)
            for col in ('open','high','low','close','volume','amount','pct_chg','turnover'):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df[df['close'].notna()].copy()
            df['adj_factor']   = 1.0
            df['pre_close']    = None
            df['is_suspend']   = False
            df['limit_up']     = False
            df['limit_down']   = False
            df['data_source']  = 'baostock'
            df = df[['ts_code','trade_date','open','high','low','close',
                     'pre_close','volume','amount','pct_chg','turnover',
                     'adj_factor','is_suspend','limit_up','limit_down','data_source']]
            return ts_code, len(df), df

        except Exception:
            bs.logout()
            if attempt < 2:
                time.sleep(RETRY_DELAY[attempt])
    return ts_code, -1, 'all_retries_failed'


def _insert_df(conn, df):
    """批量插入：注册临时表 + INSERT（忽略主键冲突，0.01s/1000行 vs 3s/1000行）"""
    if df is None or df.empty:
        return 0
    try:
        conn.register('df_batch', df)
        conn.execute("""
            INSERT OR IGNORE INTO daily_quotes
            SELECT * FROM df_batch
        """)
        conn.unregister('df_batch')
        return len(df)
    except Exception:
        conn.unregister('df_batch')
        return 0


def main():
    print('=' * 60)
    print('[DELISTED] Baostock serial fill (conservative)')
    print('=' * 60)

    conn = duckdb.connect(DB_PATH)

    # 获取缺失行情的退市股
    rows = conn.execute("""
        SELECT s.ts_code, s.list_date, s.delist_date
        FROM stock_basic s
        WHERE s.is_delisted = true
          AND NOT EXISTS (
              SELECT 1 FROM daily_quotes q
              WHERE q.ts_code = s.ts_code
          )
        ORDER BY s.delist_date DESC NULLS LAST
    """).fetchall()
    conn.close()

    total = len(rows)
    print(f'[INFO] {total} delisted stocks need quotes\n')

    if total == 0:
        print('[OK] All done.')
        return

    conn = duckdb.connect(DB_PATH)
    done = 0
    err = 0
    total_inserted = 0
    batch_buf = []
    batch_rows = 0
    last_report = 0

    for i, (ts_code, list_date, delist_date) in enumerate(rows):
        ts2, n, df = _fetch_single(ts_code, list_date, delist_date)

        if n > 0 and df is not None:
            batch_buf.append(df)
            batch_rows += n
            done += 1
        elif n == 0:
            done += 1
        else:
            err += 1
            print(f'  [ERR {i+1}/{total}] {ts_code}: {df}')

        # 每BATCH_COMMIT只或最后一只commit
        if (done + err) % BATCH_COMMIT == 0 or (done + err) == total:
            for b in batch_buf:
                total_inserted += _insert_df(conn, b)
            conn.commit()
            batch_buf = []
            batch_rows = 0
            print(f'  [{i+1}/{total}] done={done} err={err} inserted=+{total_inserted}')

        # 两只之间加小延迟（避免触发Baostock限流）
        if (done + err) < total:
            time.sleep(0.5)

    # 剩余
    if batch_buf:
        for b in batch_buf:
            total_inserted += _insert_df(conn, b)
        conn.commit()

    # 最终统计
    conn.close()
    conn2 = duckdb.connect(DB_PATH)
    total_q    = conn2.execute('SELECT COUNT(*) FROM daily_quotes').fetchone()[0]
    total_stocks = conn2.execute('SELECT COUNT(DISTINCT ts_code) FROM daily_quotes').fetchone()[0]
    del_w      = conn2.execute("""
        SELECT COUNT(*) FROM stock_basic s
        WHERE s.is_delisted=true
          AND EXISTS(SELECT 1 FROM daily_quotes q WHERE q.ts_code=s.ts_code)
    """).fetchone()[0]
    conn2.close()

    print()
    print('=' * 60)
    print('[OK] Done!')
    print(f'  Fetched:       {done}/{total}')
    print(f'  Errors:         {err}')
    print(f'  Rows inserted: +{total_inserted}')
    print(f'  Total quotes:  {total_q}')
    print(f'  Total stocks: {total_stocks}')
    print(f'  Delisted:     {del_w}/354  (was 42)')


if __name__ == '__main__':
    main()
