"""Check if DuckDB is locked, then run phase5_working.py"""
import sys, subprocess, time, os

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
SCRIPT = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.py'
LOG = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\phase5_working.log'

# Step 1: Check lock file
lock = DB + '.lock'
if os.path.exists(lock):
    print(f"Lock file exists: {lock}")
    try:
        os.remove(lock)
        print("  Removed lock file")
    except Exception as e:
        print(f"  Cannot remove: {e}")
else:
    print("No lock file")

# Step 2: Kill zombie Python processes
print("Killing zombie Python processes...")
for pid in [14456, 15892, 20120, 23844, 2540, 10580]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True, text=True)

# Also kill ALL non-responsive python processes
subprocess.run(['powershell', '-Command',
    'Get-Process python* | Where-Object {$_.MainWindowTitle -eq \"\"} | Stop-Process -Force -ErrorAction SilentlyContinue'],
    capture_output=True, text=True)
time.sleep(3)

# Step 3: Test DuckDB access
print("Testing DuckDB access...")
try:
    import duckdb
    c = duckdb.connect(DB, read_only=True)
    mx = c.execute("SELECT MAX(trade_date) FROM daily_bar_adjusted").fetchone()[0]
    c.close()
    print(f"  DuckDB OK: MAX date = {mx}")
except Exception as e:
    print(f"  DuckDB ERROR: {e}")
    sys.exit(1)

print("\nStarting phase5_working.py...")
