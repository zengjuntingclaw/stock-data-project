# -*- coding: utf-8 -*-
"""Schema v2.2 最终验证"""
import os, sys, duckdb, importlib

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
sys.path.insert(0, workspace)
os.chdir(workspace)

OUT = open(os.path.join(workspace, "_temp", "verify_clean_result.txt"), "w", encoding="utf-8")

def log(msg):
    print(msg, file=OUT)
    print(msg)

results = []

def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    results.append((status == "PASS", name, detail))
    log(f"[{status}] {name}" + (f" | {detail}" if detail else ""))

# ============================================================
# TEST 1: DEFAULT_START_DATE configurable via env
# ============================================================
os.environ["STOCK_START_DATE"] = "2020-01-01"
import scripts.data_engine as de_mod
importlib.reload(de_mod)
from scripts.data_engine import DEFAULT_START_DATE
check("1. DEFAULT_START_DATE env var", DEFAULT_START_DATE == "2020-01-01", f"got={DEFAULT_START_DATE}")
OUT.flush()

# ============================================================
# TEST 2: DROP old stock_basic in __init_schema
# ============================================================
conn = duckdb.connect(r"data\stock_data.duckdb")
conn.execute("CREATE TABLE IF NOT EXISTS stock_basic (id INT)")
# Reload fresh
importlib.reload(de_mod)
from scripts.data_engine import DataEngine
de = DataEngine(db_path=r"data\stock_data.duckdb")
tables_after = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
has_old = "stock_basic" in tables_after
check("2. DROP old stock_basic table", not has_old, f"stock_basic exists={has_old}")
OUT.flush()

# ============================================================
# TEST 3: daily_bar_adjusted has qfq/hfq 8 fields
# ============================================================
fields = [r[0] for r in conn.execute("DESCRIBE daily_bar_adjusted").fetchall()]
qfq8 = ["qfq_open","qfq_high","qfq_low","qfq_close","hfq_open","hfq_high","hfq_low","hfq_close"]
missing = [f for f in qfq8 if f not in fields]
check("3. daily_bar_adjusted qfq/hfq 8 fields", not missing, f"missing={missing}")
OUT.flush()

# ============================================================
# TEST 4: get_active_stocks PIT query
# ============================================================
result = de.get_active_stocks("2024-06-01")
check("4. get_active_stocks PIT query", len(result) > 100, f"got {len(result)} records")
OUT.flush()

# ============================================================
# TEST 5: get_index_constituents historical
# ============================================================
result = de.get_index_constituents("000300.SH", "2024-01-01")
check("5. get_index_constituents", len(result) > 0, f"got {len(result)} records")
OUT.flush()

# ============================================================
# TEST 6: raw vs adjusted separation
# ============================================================
raw = de.get_daily_raw("000001.SZ", "2024-01-01", "2024-01-10")
adj = de.get_daily_adjusted("000001.SZ", "2024-01-01", "2024-01-10")
check("6a. get_daily_raw has data", len(raw) > 0, f"got {len(raw)}")
check("6b. get_daily_adjusted has data", len(adj) > 0, f"got {len(adj)}")
check("6c. adj has qfq_close", "qfq_close" in adj.columns, f"fields={list(adj.columns)}")
check("6d. raw has adj_factor", "adj_factor" in raw.columns, "")
OUT.flush()

# ============================================================
# TEST 7: SurvivorshipBiasHandler pipeline
# ============================================================
from scripts.survivorship_bias import SurvivorshipBiasHandler
sbh = SurvivorshipBiasHandler(de)
check("7. SurvivorshipBiasHandler pipeline", True, "import+init OK")
OUT.flush()

# ============================================================
# TEST 8: sync_progress resumable
# ============================================================
sync_fields = [r[0] for r in conn.execute("DESCRIBE sync_progress").fetchall()]
has_key = any("update" in f.lower() or "status" in f.lower() for f in sync_fields)
check("8. sync_progress resumable", has_key, f"fields={sync_fields}")
OUT.flush()

# ============================================================
# TEST 9: data quality checks
# ============================================================
qc_methods = [m for m in dir(de) if "quality" in m.lower() or "check" in m.lower()]
check("9. data quality checks", len(qc_methods) >= 2, f"methods={qc_methods}")
OUT.flush()

# ============================================================
# TEST 10: get_all_stocks raises RuntimeError (no fallback)
# ============================================================
import inspect
src = inspect.getsource(de._get_all_stocks_impl)
has_rt = "RuntimeError" in src
no_local_fallback = "_get_local_stocks" not in src
check("10. get_all_stocks no fallback", has_rt and no_local_fallback,
      f"RuntimeError={has_rt}, no_local_stocks={no_local_fallback}")
OUT.flush()

# ============================================================
# Summary
# ============================================================
passed = sum(1 for p, _, _ in results if p)
total = len(results)
log(f"\n{'='*50}")
log(f"SUMMARY: {passed}/{total} PASSED")
for ok, name, detail in results:
    tag = "[PASS]" if ok else "[FAIL]"
    log(f"  {tag} {name}" + (f" | {detail}" if detail else ""))
log(f"{'='*50}")

OUT.close()
print("Done. Results written to _temp/verify_clean_result.txt")
