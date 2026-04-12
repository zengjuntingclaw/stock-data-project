import sys, time, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

# Remove WAL first
WAL = DB + '.wal'
if os.path.exists(WAL):
    try: os.remove(WAL); print(f'Removed WAL')
    except Exception as e: print(f'WAL remove failed: {e}')
time.sleep(2)

try:
    import duckdb
    conn = duckdb.connect(DB, read_only=True)
    mx = conn.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    cnt = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    new = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted WHERE trade_date >= '2026-04-07'").fetchone()[0]
    # Sample
    sample = conn.execute("""
        SELECT ts_code, trade_date, close, volume
        FROM daily_bar_adjusted
        WHERE trade_date >= '2026-04-07'
        LIMIT 5
    """).fetchall()
    print(f"MAX date: {mx}")
    print(f"Total rows: {cnt}")
    print(f"New rows (>=04-07): {new}")
    print(f"Sample new rows:")
    for s in sample:
        print(f"  {s}")
    conn.close()
except Exception as e:
    print(f"ERROR: {e}")
