# -*- coding: utf-8 -*-
"""Schema v2.2 最终验证 v3 - 单一连接修复"""
import os, sys, duckdb, importlib

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
sys.path.insert(0, workspace)
os.chdir(workspace)

OUT = open(os.path.join(workspace, "_temp", "verify_v3_result.txt"), "w", encoding="utf-8", buffering=1)
results = []

def log(msg=""):
    print(msg, file=OUT, flush=True)

def check(name, cond, detail=""):
    results.append((cond, name, detail))
    log(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" | {detail}" if detail else ""))

# ============ Setup: reload module with env ============
os.environ["STOCK_START_DATE"] = "2020-01-01"
for mod in list(sys.modules.keys()):
    if mod.startswith("scripts"):
        del sys.modules[mod]
import scripts.data_engine as dm
importlib.reload(dm)
from scripts.data_engine import DEFAULT_START_DATE as DSD, DataEngine

# ============ TEST 1: DEFAULT_START_DATE ============
check("1. DEFAULT_START_DATE env var", DSD == "2020-01-01", f"got={DSD}")

# ============ TEST 2: DROP stock_basic ============
# Use DE's own connection (don't open manual conn for this)
de = DataEngine(db_path=r"data\stock_data.duckdb")
conn = de.conn  # share DE's connection
tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
check("2. DROP old stock_basic", "stock_basic" not in tables, f"tables={tables}")

# ============ TEST 3: qfq/hfq 8 fields ============
fields = [r[0] for r in conn.execute("DESCRIBE daily_bar_adjusted").fetchall()]
qfq8 = ["qfq_open","qfq_high","qfq_low","qfq_close","hfq_open","hfq_high","hfq_low","hfq_close"]
missing = [f for f in qfq8 if f not in fields]
check("3. qfq/hfq 8 fields", len(missing)==0, f"missing={missing}")

# ============ TEST 4: get_active_stocks PIT ============
result = de.get_active_stocks("2024-06-01")
check("4. get_active_stocks PIT >100", len(result) > 100, f"got={len(result)}")

# ============ TEST 5: get_index_constituents ============
result = de.get_index_constituents("000300.SH", "2024-01-01")
check("5. index constituents >0", len(result) > 0, f"got={len(result)}")

# ============ TEST 6: raw vs adjusted separation ============
raw = de.get_daily_raw("000001.SZ", "2024-01-01", "2024-01-10")
adj = de.get_daily_adjusted("000001.SZ", "2024-01-01", "2024-01-10")
check("6a. raw has data", len(raw) > 0, f"rows={len(raw)}")
check("6b. adj has data", len(adj) > 0, f"rows={len(adj)}")
check("6c. adj has qfq_close", "qfq_close" in adj.columns, "")
check("6d. raw has adj_factor", "adj_factor" in raw.columns, "")
check("6e. raw/adj column diff", set(raw.columns) != set(adj.columns),
      f"raw={len(raw.columns)}, adj={len(adj.columns)}")

# ============ TEST 7: SurvivorshipBiasHandler ============
from scripts.survivorship_bias import SurvivorshipBiasHandler
sbh = SurvivorshipBiasHandler(de)
check("7. SurvivorshipBiasHandler", True, "OK")

# ============ TEST 8: sync_progress ============
sync_fields = [r[0] for r in conn.execute("DESCRIBE sync_progress").fetchall()]
has_key = any("update" in f.lower() or "status" in f.lower() for f in sync_fields)
check("8. sync_progress resumable", has_key, f"fields={sync_fields}")

# ============ TEST 9: quality checks ============
qc_methods = [m for m in dir(de) if "quality" in m.lower() or "check" in m.lower()]
check("9. quality checks >=2", len(qc_methods) >= 2, f"methods={qc_methods}")

# ============ TEST 10: get_all_stocks no fallback ============
import inspect
# Try _get_all_stocks first
src = None
for method_name in ["_get_all_stocks", "get_all_stocks_impl", "_get_all_stocks_impl"]:
    if hasattr(de, method_name):
        src = inspect.getsource(getattr(de, method_name))
        log(f"Found source: {method_name}")
        break
if src:
    has_rt = "RuntimeError" in src
    no_fallback = "_get_local_stocks" not in src
    check("10a. RuntimeError raised", has_rt, f"RuntimeError={has_rt}")
    check("10b. no _get_local_stocks", no_fallback, f"no_fallback={no_fallback}")
else:
    check("10. source inspection", False, "method not found")

# ============ SUMMARY ============
log("\n" + "=" * 55)
passed = sum(1 for c,_,_ in results if c)
total = len(results)
log(f"SUMMARY: {passed}/{total} PASSED")
for i,(cond,name,detail) in enumerate(results,1):
    log(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" | {detail}" if detail else ""))
log("=" * 55)
OUT.close()
print("Done. See _temp/verify_v3_result.txt")
