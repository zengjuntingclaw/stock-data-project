"""Kill old Python processes and run phase5_working.py via Python subprocess"""
import subprocess, sys, time, os

SCRIPT = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.py'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.log'
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

print('=== Cleanup Phase ===')

# Kill known PIDs via Python subprocess (avoids PowerShell SM issue)
for pid in [23844, 2540, 14456, 15892, 20120, 10580]:
    try:
        r = subprocess.run(['taskkill', '/F', '/PID', str(pid)],
            capture_output=True, text=True)
        if 'SUCCESS' in r.stdout:
            print(f'Killed PID {pid}')
    except:
        pass

# Remove DuckDB lock file
lock = DB + '.lock'
if os.path.exists(lock):
    try:
        os.remove(lock)
        print(f'Removed lock: {lock}')
    except Exception as e:
        print(f'Cannot remove lock: {e}')

time.sleep(2)

# Verify DuckDB
print('Testing DuckDB access...')
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    c.close()
    print(f'DuckDB OK: MAX date = {mx}')
except Exception as e:
    print(f'DuckDB ERROR: {e}')
    print('Giving 5 more seconds...')
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

print('Starting phase5_working.py...')
result = subprocess.run([sys.executable, SCRIPT])
sys.exit(result.returncode)
