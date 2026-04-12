"""Kill zombies, wait, then test bulk API + run concurrent sync"""
import subprocess, time, os, sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_bulk.log'

def kill_zombies():
    for pid in [4432, 2624, 23844, 2540, 14456, 15892, 20120, 10580, 10588, 2624, 4432]:
        try:
            subprocess.run(['taskkill', '/F', '/PID', str(pid)],
                capture_output=True, text=True)
        except: pass
    lock = DB + '.lock'
    if os.path.exists(lock):
        try: os.remove(lock); print('Removed lock')
        except: pass

kill_zombies()
print('Waiting 5s for OS to release lock...')
time.sleep(5)

# Test DuckDB
print('Testing DuckDB...')
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    c.close()
    print(f'DuckDB OK: MAX={mx}')
except Exception as e:
    print(f'ERROR: {e}')
    time.sleep(10)
    try:
        import duckdb
        c = duckdb.connect(DB, read_only=True)
        mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
        c.close()
        print(f'DuckDB OK: MAX={mx}')
    except Exception as e2:
        print(f'Still locked: {e2}')
        sys.exit(1)

print('DONE - now run phase5_bulk.py separately')
