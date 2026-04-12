"""Kill zombies, remove WAL, test DuckDB, run bulk test"""
import subprocess, time, os, sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
WAL = DB + '.wal'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_bulk.log'

print('=== Phase 5: Kill + WAL cleanup + Bulk Test ===')

# Kill zombies
for pid in [4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580, 10588]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True, text=True)
print('Killed zombie PIDs')

# Remove WAL (this releases the lock)
for f in [WAL, DB + '.lock']:
    if os.path.exists(f):
        try:
            os.remove(f)
            print(f'Removed {f}')
        except Exception as e:
            print(f'Cannot remove {f}: {e}')
    else:
        print(f'Not found: {f}')

time.sleep(3)

# Verify DuckDB
print('Verifying DuckDB...')
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    c.close()
    print(f'DuckDB OK: MAX={mx}')
except Exception as e:
    print(f'DuckDB ERROR: {e}')
    sys.exit(1)

# Test Baostock bulk API
print('\n=== Baostock Bulk API Test ===')
import baostock as bs
lg = bs.login()
print(f'Baostock: {lg.error_msg}')

def pf(v):
    try: return float(v) if v and v != '' else 0.0
    except: return 0.0

for code, name in [('sh.600000', 'Pufa Bank'), ('sz.399300', 'CSI 300')]:
    t0 = time.time()
    rs = bs.query_stock_daily(code, '2026-04-07')
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    elapsed = time.time() - t0
    print(f'{code} ({name}): {len(rows)} rows in {elapsed:.2f}s')
    if rows:
        print(f'  Sample: {rows[0]}')

bs.logout()
print('\nNow run phase5_concurrent.py for the actual sync')
