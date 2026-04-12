"""Kill old DuckDB lock holders, then run phase5_working.py"""
import subprocess, sys, os, time

PY_SCRIPT = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.py'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.log'

# Kill Python processes holding duckdb file (brute force)
print("Killing old Python processes...")
procs = subprocess.run(['powershell', '-Command',
    'Get-Process python* | Where-Object {$_.MainWindowTitle -eq "" -and $_.Responding -eq $false} | Stop-Process -Force -ErrorAction SilentlyContinue; echo done'],
    capture_output=True, text=True)
print(procs.stdout.strip())
time.sleep(2)

# Also kill by PID if we know them
old_pids = [23844, 2540, 14456, 15892, 20120]
for pid in old_pids:
    r = subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True, text=True)
    if 'SUCCESS' in r.stdout or 'ERROR' not in r.stdout:
        print(f"  Killed PID {pid}")
time.sleep(1)

# Verify DuckDB is free
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
try:
    import duckdb
    conn = duckdb.connect(DB, read_only=True)
    mx = conn.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    print(f"DuckDB OK: MAX date = {mx}")
    conn.close()
except Exception as e:
    print(f"DuckDB still locked: {e}")
    print("Trying harder...")
    # Force delete .lock files
    lock_files = [DB + '.lock', DB.replace('.duckdb', '.wal.lock')]
    for lf in lock_files:
        if os.path.exists(lf):
            try: os.remove(lf); print(f"  Removed lock: {lf}")
            except: pass
    time.sleep(2)
    try:
        conn = duckdb.connect(DB, read_only=True)
        print(f"DuckDB OK after lock removal")
        conn.close()
    except Exception as e2:
        print(f"Still locked: {e2}")

# Now run phase5_working.py
print(f"\nRunning phase5_working.py...")
result = subprocess.run(['py', '-3', PY_SCRIPT], capture_output=False)
sys.exit(result.returncode)
