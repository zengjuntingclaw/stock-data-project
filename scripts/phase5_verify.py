"""
Stage 1 verification v4: Correct formula + fixed merge
========================================================
adj_factor invariant (from first principles):
  qfq_price[T] * adj_factor[T] = raw_price[T]
  => adj_factor[T] = raw_price[T] / qfq_price[T]
  => adj_factor[0] = raw_price[0] / qfq_price[0] = raw_price[0] / raw_price[0] = 1.0

Key insight:
  - Baostock "qfq" = 前复权：将历史价格向前调整到最新价格水平
  - raw[T] * adj_factor[T] = raw[latest]  -- WRONG
  - qfq[T] * adj_factor[T] = raw[T]       -- CORRECT (adjust historical qfq back to raw)

Usage:
  python scripts/phase5_verify.py --stock 000001.SZ
"""
import sys, os, time, math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding='utf-8')

import baostock as bs
import pandas as pd
import duckdb
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(REPO, 'data', 'stock_data.duckdb')


def log(msg, tag='INFO'):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f'[{ts}] [{tag}] {msg}')


def bs_login():
    r = bs.login()
    return r.error_code == '0'


def fetch_bs(ts_code, flag):
    bc = ts_code.replace('.SZ', '.sz').replace('.SH', '.sh').replace('.BJ', '.bj')
    rs = bs.query_history_k_data_plus(
        bc, 'date,open,high,low,close,volume,amount',
        start_date='2018-01-01', end_date='2026-12-31',
        frequency='d', adjustflag=flag)
    rows = []
    while (rs.error_code == '0') & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=rs.fields)
    df = df[df['close'] != '']
    for c in ['open', 'high', 'low', 'close', 'volume', 'amount']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def main(stock='000001.SZ'):
    t0 = time.time()
    log(f'Stage 1 verification: {stock}')

    # Step 1: Baostock
    if not bs_login():
        log('Baostock login FAIL', 'ERROR')
        return
    log('Baostock login OK')

    log('Fetching Baostock raw (adjustflag=3)...')
    df_raw = fetch_bs(stock, '3')
    log(f'Raw: {len(df_raw)} rows  {df_raw["date"].min()} ~ {df_raw["date"].max()}')

    log('Fetching Baostock qfq (adjustflag=2)...')
    time.sleep(0.5)
    df_qfq = fetch_bs(stock, '2')
    log(f'Qfq: {len(df_qfq)} rows')

    bs.logout()

    # Step 2: Merge on date (keep both close prices)
    df = df_raw.merge(df_qfq, on='date', suffixes=('_raw', '_qfq'))
    df = df.dropna(subset=['close_raw', 'close_qfq'])
    df = df.sort_values('date').reset_index(drop=True)
    log(f'Merged: {len(df)} rows on date')

    # Step 3: Compute adj_factor
    # Baostock convention: adj_factor[latest] = 1.0
    # Formula: adj_factor[i] = (raw[i] / qfq[i]) / (raw[latest] / qfq[latest])
    # Invariant: qfq[i] * adj_factor[i] = raw[latest]  for all i
    n = len(df)
    raw_latest = df.at[n - 1, 'close_raw']
    qfq_latest = df.at[n - 1, 'close_qfq']
    adj = (df['close_raw'].values / df['close_qfq'].values) / (raw_latest / qfq_latest)
    df['adj_factor'] = adj

    log(f'adj_factor[0]={df.at[0,"adj_factor"]:.8f}  adj_factor[-1]={df.at[len(df)-1,"adj_factor"]:.8f}')
    log(f'raw_close[0]={df.at[0,"close_raw"]:.3f}  raw_close[-1]={df.at[len(df)-1,"close_raw"]:.3f}')
    log(f'qfq_close[0]={df.at[0,"close_qfq"]:.3f}  qfq_close[-1]={df.at[len(df)-1,"close_qfq"]:.3f}')

    # Step 4: Internal consistency - qfq[T] * adj[T] == raw[T]
    log('Internal consistency check: qfq[T] * adj[T] == raw[T]')
    max_err = 0.0
    bad = []
    for i in range(len(df)):
        lhs = df.at[i, 'close_qfq'] * df.at[i, 'adj_factor']
        rhs = df.at[i, 'close_raw']
        err = abs(lhs - rhs) / rhs * 100 if rhs > 0 else 0
        if err > max_err:
            max_err = err
        if err > 0.01 and len(bad) < 5:
            bad.append((df.at[i, 'date'], lhs, rhs, err))
    log(f'  max_err={max_err:.8f}%', 'OK' if max_err < 0.01 else 'FAIL')
    for b in bad:
        log(f'    {b[0]}  lhs={b[1]:.4f}  rhs={b[2]:.4f}  err={b[3]:.4f}%', 'WARN')

    # Step 5: OHLC consistency
    log('OHLC consistency:')
    checks = [
        ('high >= low',  df['high_raw'] >= df['low_raw']),
        ('high >= open', df['high_raw'] >= df['open_raw']),
        ('high >= close',df['high_raw'] >= df['close_raw']),
        ('low <= open',  df['low_raw']  <= df['open_raw']),
        ('low <= close', df['low_raw']  <= df['close_raw']),
    ]
    all_ok = True
    for name, mask in checks:
        bad = int((~mask).sum())
        tag = 'OK' if bad == 0 else 'WARN'
        if bad > 0:
            all_ok = False
        log(f'  {name}: {bad} anomalies', tag)
    log(f'OHLC: {"ALL PASS" if all_ok else "HAS WARNINGS"}', 'OK' if all_ok else 'WARN')

    # Step 6: Write to DuckDB
    log('Writing to DuckDB...')
    conn = duckdb.connect(DB)

    # Delete old data for this stock
    conn.execute("DELETE FROM daily_bar_raw WHERE ts_code=?", [stock])
    conn.execute("DELETE FROM daily_bar_adjusted WHERE ts_code=?", [stock])

    # Build daily_bar_raw
    raw_df = pd.DataFrame({
        'ts_code':    stock,
        'trade_date': pd.to_datetime(df['date']).dt.date,
        'symbol':     stock.split('.')[0],
        'open':       df['open_raw'].values,
        'high':       df['high_raw'].values,
        'low':        df['low_raw'].values,
        'close':      df['close_raw'].values,
        'volume':     df['volume_raw'].values,
        'amount':     df['amount_raw'].values,
        'adj_factor': df['adj_factor'].values,
    })
    conn.execute("INSERT INTO daily_bar_raw BY NAME SELECT * FROM raw_df")

    # Build daily_bar_adjusted (qfq prices = raw / adj_factor)
    adj_df = pd.DataFrame({
        'ts_code':    stock,
        'trade_date': pd.to_datetime(df['date']).dt.date,
        'open':       df['open_qfq'].values,
        'high':       df['high_qfq'].values,
        'low':        df['low_qfq'].values,
        'close':      df['close_qfq'].values,
        'volume':     df['volume_raw'].values,
        'amount':     df['amount_raw'].values,
        'adj_factor': df['adj_factor'].values,
    })
    conn.execute("INSERT INTO daily_bar_adjusted BY NAME SELECT * FROM adj_df")
    conn.close()
    log(f'Wrote: {len(raw_df)} rows to both tables')

    # Step 7: Verify DuckDB write
    log('Verifying DuckDB write...')
    conn = duckdb.connect(DB, read_only=True)
    raw_db = conn.execute(
        "SELECT trade_date, close, adj_factor FROM daily_bar_raw WHERE ts_code=? ORDER BY trade_date",
        [stock]).fetchdf()
    adj_db = conn.execute(
        "SELECT trade_date, close, adj_factor FROM daily_bar_adjusted WHERE ts_code=? ORDER BY trade_date",
        [stock]).fetchdf()
    conn.close()

    log(f'  daily_bar_raw read: {len(raw_db)} rows', 'OK' if len(raw_db) == len(df) else 'FAIL')
    log(f'  daily_bar_adjusted read: {len(adj_db)} rows', 'OK' if len(adj_db) == len(df) else 'FAIL')

    # Check adj_factor in DuckDB matches
    raw_db = raw_db.sort_values('trade_date').reset_index(drop=True)
    adj_db = adj_db.sort_values('trade_date').reset_index(drop=True)
    df_s   = df.sort_values('date').reset_index(drop=True)

    af_raw_ok = (abs(raw_db['adj_factor'].values - df_s['adj_factor'].values) < 1e-9).all()
    af_adj_ok = (abs(adj_db['adj_factor'].values - df_s['adj_factor'].values) < 1e-9).all()
    qfq_ok    = (abs(adj_db['close'].values - df_s['close_qfq'].values) < 1e-6).all()

    log(f'  adj_factor raw match: {af_raw_ok}', 'OK' if af_raw_ok else 'FAIL')
    log(f'  adj_factor adj match: {af_adj_ok}', 'OK' if af_adj_ok else 'FAIL')
    log(f'  adj close == qfq:     {qfq_ok}',    'OK' if qfq_ok    else 'FAIL')

    # Step 8: PIT queries
    log('PIT query verification:')
    conn = duckdb.connect(DB, read_only=True)
    for date, desc in [
        ('2020-01-01', '2020-01-01 (before events)'),
        ('2024-01-15', '2024-01-15 (after events)'),
        ('2026-04-10', '2026-04-10 (latest)'),
    ]:
        cnt = conn.execute("""
            SELECT COUNT(*) FROM stock_basic_history
            WHERE ts_code=? AND list_date<=? AND (delist_date>? OR delist_date IS NULL)
        """, [stock, date, date]).fetchone()[0]
        icon = 'OK' if cnt >= 1 else 'FAIL'
        log(f'  stock_basic_history @{date} ({desc}): [{icon}] {cnt}',
            'OK' if cnt >= 1 else 'FAIL')

        bar = conn.execute(
            "SELECT COUNT(*) FROM daily_bar_raw WHERE ts_code=? AND trade_date<=?",
            [stock, date]).fetchone()[0]
        log(f'  daily_bar_raw @{date}: {bar} rows')
    conn.close()

    elapsed = time.time() - t0
    log(f'Stage 1 complete in {elapsed:.1f}s', 'OK')


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--stock', default='000001.SZ')
    args = p.parse_args()
    main(args.stock)
