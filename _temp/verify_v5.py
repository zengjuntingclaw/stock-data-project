# -*- coding: utf-8 -*-
"""Schema v2.2 验证 - 独立连接测试"""
import os, sys, duckdb, importlib

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
sys.path.insert(0, workspace)
os.chdir(workspace)

OUT = open(os.path.join(workspace, "_temp", "verify_v5_result.txt"), "w", encoding="utf-8", buffering=1)
results = []

def log(msg=""):
    print(msg, file=OUT, flush=True)

def check(name, cond, detail=""):
    results.append((cond, name, detail))
    log(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" | {detail}" if detail else ""))

log("=" * 55)
log("Schema v2.2 Verification v5")
log("=" * 55)

# ============ Env var test ============
os.environ["STOCK_START_DATE"] = "2020-01-01"
for mod in list(sys.modules.keys()):
    if mod.startswith("scripts"):
        del sys.modules[mod]
import scripts.data_engine as dm
importlib.reload(dm)
from scripts.data_engine import DEFAULT_START_DATE as DSD, DataEngine

check("1. DEFAULT_START_DATE env", DSD == "2020-01-01", f"got={DSD}")

# ============ DE instance ============
de = DataEngine(db_path=r"data\stock_data.duckdb")

# ============ Test 2-9 using fresh connections ============
DB = r"data\stock_data.duckdb"

def fresh_query(sql):
    """Open fresh connection, query, close."""
    c = duckdb.connect(DB, read_only=True)
    try:
        df = c.sql(sql).df()
        return df
    finally:
        c.close()

def fresh_execute(sql):
    c = duckdb.connect(DB)
    try:
        c.execute(sql)
    finally:
        c.close()

# TEST 2: DROP old stock_basic
tables = fresh_query("SHOW TABLES")["name"].tolist()
check("2. DROP old stock_basic", "stock_basic" not in tables, f"stock_basic={('stock_basic' in tables)}")

# TEST 3: qfq/hfq fields
fields = fresh_query("DESCRIBE daily_bar_adjusted")["column_name"].tolist()
qfq8 = ["qfq_open","qfq_high","qfq_low","qfq_close","hfq_open","hfq_high","hfq_low","hfq_close"]
missing = [f for f in qfq8 if f not in fields]
check("3. qfq/hfq 8 fields", len(missing)==0, f"missing={missing}")

# TEST 4: get_active_stocks
result = de.get_active_stocks("2024-06-01")
check("4. get_active_stocks PIT >100", len(result) > 100, f"got={len(result)}")

# TEST 5: get_index_constituents
result = de.get_index_constituents("000300.SH", "2024-01-01")
check("5. index constituents >0", len(result) > 0, f"got={len(result)}")

# TEST 6: raw vs adjusted
raw = de.get_daily_raw("000001.SZ", "2024-01-01", "2024-01-10")
adj = de.get_daily_adjusted("000001.SZ", "2024-01-01", "2024-01-10")
check("6a. raw has data", len(raw) > 0, f"rows={len(raw)}")
check("6b. adj has data", len(adj) > 0, f"rows={len(adj)}")
check("6c. adj has qfq_close", "qfq_close" in adj.columns, f"qfq_close={('qfq_close' in adj.columns)}")
check("6d. raw has adj_factor", "adj_factor" in raw.columns, f"adj_factor={('adj_factor' in raw.columns)}")
check("6e. raw/adj col diff", set(raw.columns) != set(adj.columns),
      f"raw={len(raw.columns)}, adj={len(adj.columns)}")

# TEST 7: SurvivorshipBiasHandler
from scripts.survivorship_bias import SurvivorshipBiasHandler
sbh = SurvivorshipBiasHandler(data_engine=de)
check("7. SurvivorshipBiasHandler", True, "OK")

# TEST 8: sync_progress
sync_fields = fresh_query("DESCRIBE sync_progress")["column_name"].tolist()
has_key = any("update" in f.lower() or "status" in f.lower() for f in sync_fields)
check("8. sync_progress resumable", has_key, f"fields={sync_fields}")

# TEST 9: quality checks
qc = [m for m in dir(de) if "quality" in m.lower() or "check" in m.lower()]
check("9. quality checks >=2", len(qc) >= 2, f"methods={qc}")

# TEST 10: no fallback in get_all_stocks
import inspect
method = None
for name in ["get_all_stocks", "_get_all_stocks"]:
    if hasattr(de, name):
        method = name
        break
if method:
    src = inspect.getsource(getattr(de, method))
    has_rt = "RuntimeError" in src
    no_fb = "_get_local_stocks" not in src
    check("10a. RuntimeError raised", has_rt, f"has_rt={has_rt}")
    check("10b. no _get_local_stocks", no_fb, f"no_fb={no_fb}")
else:
    check("10. method found", False, f"checked: {[m for m in dir(de) if 'all' in m and 'stock' in m]}")

# SUMMARY
log("\n" + "=" * 55)
passed = sum(1 for c,_,_ in results if c)
total = len(results)
log(f"SUMMARY: {passed}/{total} PASSED")
for cond, name, detail in results:
    log(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" | {detail}" if detail else ""))
log("=" * 55)
OUT.close()
print("Done")
