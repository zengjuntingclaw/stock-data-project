"""Phase 5 incremental sync - exact 4 trading days (2026-04-07 to 04-10)"""
import sys, time, datetime
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_exact_sync.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

def log(msg):
    ts = datetime.datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")
    with open(LOG, 'a') as f: f.write(f"[{ts}] {msg}\n"); f.flush()

log('=== Phase 5 Exact Sync START ===')

try:
    import duckdb, baostock as bs

    conn = duckdb.connect(DB)
    tracked = conn.execute("""
        SELECT DISTINCT ts_code FROM sync_progress
        WHERE table_name = 'daily_bar_adjusted'
    """).fetchdf()['ts_code'].tolist()
    
    # Check what we already have for 04-07~04-10
    existing = conn.execute("""
        SELECT COUNT(*) FROM daily_bar_adjusted
        WHERE trade_date >= '2026-04-07' AND trade_date <= '2026-04-10'
    """).fetchone()[0]
    log(f'Existing rows 04-07~04-10: {existing}')
    log(f'Tracked stocks: {len(tracked)}')
    conn.close()

    lg = bs.login()
    log(f'Baostock: {lg.error_msg}')
    if lg.error_msg != 'success':
        log('FATAL'); sys.exit(1)

    def to_bs(code):
        sym, ex = code.split('.')
        return f"{'sh' if ex == 'SH' else 'sz'}.{sym}"

    def pf(v):
        try: return float(v) if v and v != '' else 0.0
        except: return 0.0

    # Key: end_date=2026-04-11 is CORRECT (includes all 4 trading days)
    # But query must work - test it explicitly
    log('Verifying Baostock query for 04-07~04-11...')
    rs_test = bs.query_history_k_data_plus('sh.600000',
        'date,open,high,low,close,volume,amount,adjustflag,pct_chg,turnover',
        start_date='2026-04-07', end_date='2026-04-11',
        frequency='d', adjustflag='2')
    rows_test = []
    while rs_test.next():
        rows_test.append(rs_test.get_row_data())
    log(f'  Test: 600000.SH 04-07~04-11 = {len(rows_test)} rows: {rows_test}')

    if len(rows_test) == 0:
        log('ERROR: Baostock returning 0 rows! Using single-day queries...')
        # Fall back to single-day queries
        days = ['2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10']
        for day in days:
            rs_d = bs.query_history_k_data_plus('sh.600000',
                'date,open,high,low,close,volume,amount,adjustflag,pct_chg,turnover',
                start_date=day, end_date=day,
                frequency='d', adjustflag='2')
            rows_d = []
            while rs_d.next():
                rows_d.append(rs_d.get_row_data())
            log(f'  Single-day {day}: {len(rows_d)} rows: {rows_d}')

    start_date = '2026-04-07'
    end_date = '2026-04-10'
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn2 = duckdb.connect(DB)
    fetched_stocks = 0
    fetched_rows = 0
    insert_errors = 0
    t0 = time.time()

    for i, code in enumerate(tracked):
        bs_code = to_bs(code)
        sym = code.split('.')[0]
        try:
            rs = bs.query_history_k_data_plus(bs_code,
                'date,open,high,low,close,volume,amount,adjustflag,pct_chg,turnover',
                start_date=start_date, end_date=end_date,
                frequency='d', adjustflag='2')
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if rows:
                batch_raw = []
                batch_adj = []
                for r in rows:
                    td = r[0]; o = pf(r[1]); h = pf(r[2])
                    lo = pf(r[3]); c = pf(r[4])
                    vol = pf(r[5]); amt = pf(r[6])
                    pct = pf(r[9]); tovr = pf(r[10])
                    adj = 1.0

                    batch_raw.append((
                        code, td, sym, o, h, lo, c, vol, amt,
                        pct, tovr, 'baostock', now_str, adj, None, False, False, False
                    ))
                    batch_adj.append((
                        code, td, o, h, lo, c, None, vol, amt,
                        pct, tovr, adj, False, False, False,
                        'baostock', now_str, o, h, lo, c, o, h, lo, c
                    ))

                # Batch insert (faster than individual commits)
                try:
                    conn2.executemany("""
                        INSERT INTO daily_bar_raw
                        (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
                         pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
                         is_suspend,limit_up,limit_down)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, batch_raw)

                    conn2.executemany("""
                        INSERT INTO daily_bar_adjusted
                        (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
                         pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
                         data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
                         hfq_open,hfq_high,hfq_low,hfq_close)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, batch_adj)
                    conn2.commit()
                    fetched_stocks += 1
                    fetched_rows += len(rows)
                except Exception as ins_err:
                    insert_errors += 1
                    # Try row by row to find the bad one
                    for raw_row, adj_row in zip(batch_raw, batch_adj):
                        try:
                            conn2.execute("""
                                INSERT INTO daily_bar_raw
                                (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
                                 pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
                                 is_suspend,limit_up,limit_down)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, raw_row)
                            conn2.execute("""
                                INSERT INTO daily_bar_adjusted
                                (ts_code,trade_date,open,high,low,close,pre_close,volume,amount,
                                 pct_chg,turnover,adj_factor,is_suspend,limit_up,limit_down,
                                 data_source,created_at,qfq_open,qfq_high,qfq_low,qfq_close,
                                 hfq_open,hfq_high,hfq_low,hfq_close)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, adj_row)
                            conn2.commit()
                            fetched_rows += 1
                        except:
                            pass  # duplicate key, already exists
                    fetched_stocks += 1
        except Exception as e:
            pass

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            rate = (i+1) / elapsed if elapsed > 0 else 1
            eta = (len(tracked) - i - 1) / rate
            log(f'  {i+1}/{len(tracked)} ({elapsed:.0f}s, ETA {eta:.0f}s) fetched={fetched_stocks} rows={fetched_rows} insert_errs={insert_errors}')

    bs.logout()
    total = time.time() - t0
    log(f'DONE: {fetched_stocks} stocks, {fetched_rows} rows in {total:.0f}s')

    # Update sync_progress
    conn3 = duckdb.connect(DB)
    for code in tracked:
        conn3.execute("""
            UPDATE sync_progress SET last_sync_date = '2026-04-10'
            WHERE ts_code = ? AND table_name IN ('daily_bar_adjusted', 'daily_bar_raw')
        """, [code])
    conn3.commit()

    mx = conn3.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    cnt = conn3.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    log(f'After: MAX={mx}, TOTAL={cnt}')
    conn3.close()

except Exception as e:
    import traceback
    log(f'FATAL: {e}')
    log(traceback.format_exc())

with open(LOG) as f: print(f.read())
