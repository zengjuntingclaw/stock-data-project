import subprocess, time, os, sys

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

# Kill all known zombie Python PIDs
for pid in [4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580, 10588]:
    try:
        r = subprocess.run(['taskkill', '/F', '/PID', str(pid)],
            capture_output=True, text=True)
        if 'SUCCESS' in r.stdout:
            print(f'Killed PID {pid}')
    except:
        pass

# Remove DuckDB lock
lock = DB + '.lock'
if os.path.exists(lock):
    try:
        os.remove(lock)
        print(f'Removed lock file')
    except Exception as e:
        print(f'Cannot remove lock: {e}')

time.sleep(3)

# Test DuckDB
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    c.close()
    print(f'DuckDB OK: MAX date = {mx}')
except Exception as e:
    print(f'DuckDB ERROR: {e}')
    time.sleep(5)
    try:
        import duckdb
        c = duckdb.connect(DB, read_only=True)
        mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
        c.close()
        print(f'DuckDB OK after wait: MAX date = {mx}')
    except Exception as e2:
        print(f'Still locked: {e2}')
        sys.exit(1)

print('READY - starting phase5_bulk.py...')
