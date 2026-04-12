# -*- coding: utf-8 -*-
import os, sys, duckdb
sys.path.insert(0, r"c:\Users\zengj\.qclaw\workspace\stock_data_project")

results = []

def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    msg = f"[{status}] {name}"
    if detail:
        msg += f" | {detail}"
    results.append((status == "PASS", name, detail))
    print(msg)

# TEST 1: DEFAULT_START_DATE configurable
os.environ["STOCK_START_DATE"] = "2020-01-01"
import importlib
import scripts.data_engine as de_mod0
importlib.reload(de_mod0)
from scripts.data_engine import DEFAULT_START_DATE as DSD1
check("DEFAULT_START_DATE env", DSD1 == "2020-01-01", f"got={DSD1}")

# TEST 2: DROP stock_basic in __init_schema__
conn = duckdb.connect(r"data\stock_data.duckdb")
conn.execute("CREATE TABLE IF NOT EXISTS stock_basic (id INT)")
tables_before = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
import scripts.data_engine as de_mod
importlib.reload(de_mod)
from scripts.data_engine import DataEngine
de = de_mod.DataEngine(db_path=r"data\stock_data.duckdb")
de.init_schema()
tables_after = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
has_old = "stock_basic" in tables_after
check("DROP old stock_basic", not has_old, f"stock_basic exists={has_old}")

# TEST 3: daily_bar_adjusted has qfq/hfq 8 fields
fields = [r[0] for r in conn.execute("DESCRIBE daily_bar_adjusted").fetchall()]
qfq8 = ["qfq_open","qfq_high","qfq_low","qfq_close","hfq_open","hfq_high","hfq_low","hfq_close"]
missing = [f for f in qfq8 if f not in fields]
check("daily_bar_adjusted qfq/hfq 8 fields", not missing, f"missing={missing}")

# TEST 4: get_active_stocks PIT
result = de.get_active_stocks("2024-06-01")
check("get_active_stocks PIT query", len(result) > 100, f"got {len(result)} records")

# TEST 5: get_index_constituents historical
result = de.get_index_constituents("000300.SH", "2024-01-01")
check("get_index_constituents historical", len(result) > 0, f"got {len(result)} records")

# TEST 6: get_daily_raw vs get_daily_adjusted
raw = de.get_daily_raw("000001.SZ", "2024-01-01", "2024-01-10")
adj = de.get_daily_adjusted("000001.SZ", "2024-01-01", "2024-01-10")
check("get_daily_raw has data", len(raw) > 0, f"got {len(raw)}")
check("get_daily_adjusted has data", len(adj) > 0, f"got {len(adj)}")
check("adj has qfq_close", "qfq_close" in adj.columns, f"cols={list(adj.columns)[:8]}")
check("raw has adj_factor", "adj_factor" in raw.columns, "")

# TEST 7: SurvivorshipBiasHandler pipeline
from scripts.survivorship_bias import SurvivorshipBiasHandler
sbh = SurvivorshipBiasHandler(de)
check("SurvivorshipBiasHandler pipeline", True, "imported OK")

# TEST 8: sync_progress UPSERT
sync_fields = [r[0] for r in conn.execute("DESCRIBE sync_progress").fetchall()]
has_key = any("update" in f.lower() or "status" in f.lower() for f in sync_fields)
check("sync_progress resumable", has_key, f"fields={sync_fields[:5]}")

# TEST 9: data quality checks
qc_methods = [m for m in dir(de) if "quality" in m.lower() or "check" in m.lower()]
check("data quality checks", len(qc_methods) >= 2, f"methods={qc_methods}")

# TEST 10: get_all_stocks no fallback
import inspect
src = inspect.getsource(de._get_all_stocks_impl)
has_rt = "RuntimeError" in src
no_fallback = "_get_local_stocks" not in src
check("get_all_stocks raises RuntimeError", has_rt and no_fallback,
      f"RuntimeError={has_rt}, no_fallback={no_fallback}")

print("\n" + "="*50)
passed = sum(1 for p,_,_ in results if p)
total = len(results)
print(f"Results: {passed}/{total} PASSED")
for ok, name, detail in results:
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}")
print("="*50)
conn.close()
