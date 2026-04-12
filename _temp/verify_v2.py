# -*- coding: utf-8 -*-
"""Schema v2.2 最终验证 v2 - 单一连接"""
import os, sys, duckdb, importlib

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
sys.path.insert(0, workspace)
os.chdir(workspace)

OUT_PATH = os.path.join(workspace, "_temp", "verify_v2_result.txt")
OUT = open(OUT_PATH, "w", encoding="utf-8", buffering=1)

def log(msg=""):
    print(msg, file=OUT, flush=True)

results = []
def check(name, cond, detail=""):
    results.append((cond, name, detail))
    tag = "PASS" if cond else "FAIL"
    log(f"[{tag}] {name}" + (f" | {detail}" if detail else ""))

log("=" * 55)
log("Schema v2.2 Runtime Verification")
log("=" * 55)

# ============ TEST 1: DEFAULT_START_DATE ============
log("\n[Test 1] DEFAULT_START_DATE env var")
os.environ["STOCK_START_DATE"] = "2020-01-01"
# Reload to pick up env
if "scripts.data_engine" in sys.modules:
    del sys.modules["scripts.data_engine"]
import scripts.data_engine as dm1
importlib.reload(dm1)
from scripts.data_engine import DEFAULT_START_DATE as DSD
check("STOCK_START_DATE=2020-01-01", DSD == "2020-01-01", f"got={DSD}")

# ============ TEST 2: DROP stock_basic ============
log("\n[Test 2] DROP old stock_basic")
conn = duckdb.connect(r"data\stock_data.duckdb")
conn.execute("CREATE TABLE IF NOT EXISTS stock_basic (id INT)")
# Use DE's connection indirectly via fresh instance
if "scripts.data_engine" in sys.modules:
    del sys.modules["scripts.data_engine"]
import scripts.data_engine as dm2
importlib.reload(dm2)
from scripts.data_engine import DataEngine as DE2
de = DE2(db_path=r"data\stock_data.duckdb")
tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
check("stock_basic removed", "stock_basic" not in tables, f"tables={tables}")

# ============ TEST 3: qfq/hfq fields ============
log("\n[Test 3] daily_bar_adjusted qfq/hfq 8 fields")
fields = [r[0] for r in conn.execute("DESCRIBE daily_bar_adjusted").fetchall()]
qfq8 = ["qfq_open","qfq_high","qfq_low","qfq_close","hfq_open","hfq_high","hfq_low","hfq_close"]
missing = [f for f in qfq8 if f not in fields]
check("8 qfq/hfq fields exist", len(missing)==0, f"missing={missing}")

# ============ TEST 4: get_active_stocks PIT ============
log("\n[Test 4] get_active_stocks PIT query")
try:
    result = de.get_active_stocks("2024-06-01")
    check("PIT query >100 records", len(result) > 100, f"got={len(result)}")
except Exception as e:
    check("PIT query exception", False, str(e))

# ============ TEST 5: get_index_constituents ============
log("\n[Test 5] get_index_constituents historical")
try:
    result = de.get_index_constituents("000300.SH", "2024-01-01")
    check("Constituents >0 records", len(result) > 0, f"got={len(result)}")
except Exception as e:
    check("Constituents exception", False, str(e))

# ============ TEST 6: raw vs adjusted separation ============
log("\n[Test 6] raw vs adjusted price separation")
try:
    raw = de.get_daily_raw("000001.SZ", "2024-01-01", "2024-01-10")
    adj = de.get_daily_adjusted("000001.SZ", "2024-01-01", "2024-01-10")
    check("raw has data", len(raw) > 0, f"rows={len(raw)}")
    check("adj has data", len(adj) > 0, f"rows={len(adj)}")
    check("adj has qfq_close", "qfq_close" in adj.columns, "")
    check("raw has adj_factor", "adj_factor" in raw.columns, "")
    check("raw/adj column sets differ",
          set(raw.columns) != set(adj.columns),
          f"raw={len(raw.columns)}, adj={len(adj.columns)}")
except Exception as e:
    for name in ["raw data","adj data","qfq_close","adj_factor","column diff"]:
        check(name, False, str(e)[:80])

# ============ TEST 7: SurvivorshipBiasHandler ============
log("\n[Test 7] SurvivorshipBiasHandler pipeline")
try:
    from scripts.survivorship_bias import SurvivorshipBiasHandler
    sbh = SurvivorshipBiasHandler(de)
    check("SBH import+init", True, "OK")
except Exception as e:
    check("SBH exception", False, str(e)[:80])

# ============ TEST 8: sync_progress resumable ============
log("\n[Test 8] sync_progress resumable UPSERT")
sync_fields = [r[0] for r in conn.execute("DESCRIBE sync_progress").fetchall()]
has_key = any("update" in f.lower() or "status" in f.lower() for f in sync_fields)
check("sync_progress has key fields", has_key, f"fields={sync_fields}")

# ============ TEST 9: quality checks ============
log("\n[Test 9] data quality checks")
qc_methods = [m for m in dir(de) if "quality" in m.lower() or "check" in m.lower()]
check(">=2 quality check methods", len(qc_methods) >= 2, f"methods={qc_methods}")

# ============ TEST 10: get_all_stocks no fallback ============
log("\n[Test 10] get_all_stocks no silent fallback")
import inspect
src = inspect.getsource(de._get_all_stocks_impl)
has_rt = "RuntimeError" in src
no_fallback = "_get_local_stocks" not in src
check("RuntimeError raised", has_rt, "")
check("no _get_local_stocks in impl", no_fallback, "")

# ============ SUMMARY ============
log("\n" + "=" * 55)
passed = sum(1 for c,_,_ in results if c)
total = len(results)
log(f"SUMMARY: {passed}/{total} PASSED")
for i,(cond,name,detail) in enumerate(results,1):
    tag = "PASS" if cond else "FAIL"
    log(f"  [{tag}] Test {i}: {name}" + (f" | {detail}" if detail else ""))
log("=" * 55)

conn.close()
OUT.close()
print(f"Done. Results in {OUT_PATH}")
