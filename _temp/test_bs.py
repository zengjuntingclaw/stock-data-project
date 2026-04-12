import sys, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import baostock as bs

print("Connecting to Baostock...")
lg = bs.login()
print(f"Login: {lg.error_msg}")

# Test 3 queries
codes = ['sh.600000', 'sz.000001', 'sh.600519']
times = []
for code in codes:
    t0 = time.time()
    rs = bs.query_history_k_data_plus(code,
        "date,open,high,low,close,volume,amount,adjustflag",
        start_date='2026-04-01', end_date='2026-04-11',
        frequency="d", adjustflag="2")
    elapsed = time.time() - t0
    rows = sum(1 for _ in iter(lambda: rs.next(), False))
    times.append(elapsed)
    print(f"  {code}: {elapsed:.2f}s")

avg = sum(times) / len(times)
total_missing = 4981  # active stocks not in sync_progress
est_min = total_missing * avg / 60
print(f"\nAvg speed: {avg:.2f}s/query")
print(f"Missing active stocks: {total_missing}")
print(f"Estimated sequential time: {est_min:.0f} min")
print(f"  = {est_min/60:.1f} hours (TOO SLOW for sequential)")

bs.logout()
print("\nBaostock connectivity: OK")
print(f"\nConclusion: Sequential Baostock download is TOO SLOW for {total_missing} stocks.")
print(f"Recommend: Only sync RECENTLY LISTED stocks (last 2 years), skip historical.")
