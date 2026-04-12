"""Phase 5 Step 3: Incremental sync - Baostock availability check + batch download"""
import sys, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import baostock as bs

print("Testing Baostock connectivity...")
lg = bs.login()
print(f"  Login: {lg.error_msg}")
if lg.error_msg != 'success':
    print("FATAL: Baostock unavailable, cannot proceed")
    sys.exit(1)

# Test query speed
print("\nTesting query speed (5 samples)...")
times = []
for i, code in enumerate(['sh.600000', 'sz.000001', 'sh.600519', 'sz.000002', 'sh.601318']):
    t0 = time.time()
    rs = bs.query_history_k_data_plus(code,
        "date,open,high,low,close,volume,amount,adjustflag",
        start_date='2026-04-01', end_date='2026-04-11',
        frequency="d", adjustflag="2")
    elapsed = time.time() - t0
    rows = 0
    while rs.next():
        rows += 1
    times.append(elapsed)
    print(f"  {code}: {elapsed:.2f}s, {rows} rows")

avg = sum(times) / len(times)
print(f"\n  Average: {avg:.2f}s/query")
print(f"  Estimated time for 4981 stocks: {4981 * avg / 60:.0f} min (sequential)")

bs.logout()

# If avg < 0.3s: sequential download is feasible
# If avg >= 0.3s: need to skip or batch
if avg < 0.5:
    print(f"\nBaostock is AVAILABLE and FAST enough.")
    print(f"Starting sequential download in background...")
    print(f"Strategy: add missing stocks to sync_progress, download in batches")
else:
    print(f"\nBaostock is SLOW ({avg:.2f}s/query).")
    print(f"Consider: only download for recent stocks, skip early-history stocks.")
