"""
Schema v5.0 Final Closure Verification
Verify all remaining old-code paths have been removed
"""
import sys
from pathlib import Path
import re

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

import duckdb

print("=" * 60)
print("Schema v5.0 Final Closure Verification")
print("=" * 60)

# -- 0. Code path verification (static analysis) --
print("\n[0] Code Path Verification (static)")
de_src = open(project_root / "scripts/data_engine.py", encoding="utf-8").read()

checks = {}

# 0.1 update_daily_data: symbols=None branch uses get_active_stocks (not _get_local_stocks)
uda_pattern = re.search(
    r'def update_daily_data\(.*?\n(.*?)(?=\n    def [a-zA-Z_]|\Z)',
    de_src, re.DOTALL
)
if uda_pattern:
    uda_body = uda_pattern.group(1)
    checks["update_daily_data: no _get_local_stocks fallback"] = "_get_local_stocks" not in uda_body
    checks["update_daily_data: uses get_active_stocks"] = "get_active_stocks" in uda_body
else:
    checks["update_daily_data: function found"] = False

# 0.2 DEFAULT_START_DATE reads from env
checks["DEFAULT_START_DATE env priority"] = 'os.environ.get("STOCK_START_DATE"' in de_src

# 0.3 get_all_stocks throws RuntimeError (no silent fallback)
gas_pattern = re.search(
    r'def get_all_stocks\(.*?\n(.*?)(?=\n    def [a-zA-Z_]|\Z)',
    de_src, re.DOTALL
)
if gas_pattern:
    gas_body = gas_pattern.group(1)
    checks["get_all_stocks: raises RuntimeError"] = "raise RuntimeError" in gas_body
    checks["get_all_stocks: no _get_local_stocks fallback"] = "_get_local_stocks" not in gas_body

# 0.4 save_snapshot does NOT call get_all_stocks
snp_pattern = re.search(
    r'def save_stock_basic_snapshot\(.*?\n(.*?)(?=\n    def [a-zA-Z_]|\Z)',
    de_src, re.DOTALL
)
if snp_pattern:
    snp_body = snp_pattern.group(1)
    checks["save_snapshot: does NOT call get_all_stocks"] = "self.get_all_stocks" not in snp_body

# 0.5 sync_stock_list does NOT call get_all_stocks
ssl_pattern = re.search(
    r'def sync_stock_list\(.*?\n(.*?)(?=\n    def [a-zA-Z_]|\Z)',
    de_src, re.DOTALL
)
if ssl_pattern:
    ssl_body = ssl_pattern.group(1)
    checks["sync_stock_list: does NOT call get_all_stocks"] = "self.get_all_stocks" not in ssl_body

# 0.6 SurvivorshipBiasHandler does NOT call get_all_stocks
sbh_path = project_root / "scripts/survivorship_bias.py"
if sbh_path.exists():
    sbh_src = open(sbh_path, encoding="utf-8").read()
    checks["SurvivorshipBiasHandler: no get_all_stocks"] = "get_all_stocks" not in sbh_src
else:
    checks["SurvivorshipBiasHandler: file exists"] = False

# 0.7 security_master does NOT call get_all_stocks
sm_path = project_root / "scripts/security_master.py"
if sm_path.exists():
    sm_src = open(sm_path, encoding="utf-8").read()
    checks["security_master: no get_all_stocks"] = "get_all_stocks" not in sm_src
else:
    checks["security_master: file exists"] = False

# 0.8 main_v2_production does NOT use old tables
mv2_path = project_root / "main_v2_production.py"
if mv2_path.exists():
    mv2_src = open(mv2_path, encoding="utf-8").read()
    checks["main_v2_production: no old tables"] = (
        "stock_basic" not in mv2_src and "daily_quotes" not in mv2_src
    )
else:
    checks["main_v2_production: file exists"] = False

all_passed = True
for name, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    if not passed:
        all_passed = False
    print(f"  [{status}] {name}")

# -- 1. Database table verification --
print("\n[1] Database Table Verification")
db_path = project_root / "data/stock_data.duckdb"
db = duckdb.connect(str(db_path), read_only=True)

expected_new_tables = [
    "stock_basic_history",
    "daily_bar_raw",
    "daily_bar_adjusted",
    "index_constituents_history",
    "sync_progress",
    "data_quality_alert",
]

tables = db.execute("SHOW TABLES").fetchdf()
existing = set(tables["name"].tolist())

for tbl in expected_new_tables:
    if tbl in existing:
        cnt = db.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  [PASS] {tbl}: {cnt} records")
    else:
        print(f"  [FAIL] {tbl}: does not exist!")
        all_passed = False

# Check adjusted field fill rate
print("\n[2] Adjusted Field Fill Rate")
adj_total = db.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
if adj_total > 0:
    for field in ["qfq_close", "hfq_close", "adj_factor"]:
        filled = db.execute(f"SELECT COUNT(*) FROM daily_bar_adjusted WHERE {field} IS NOT NULL").fetchone()[0]
        pct = filled / adj_total * 100
        status = "PASS" if pct == 100 else "FAIL"
        if pct != 100:
            all_passed = False
        print(f"  [{status}] {field}: {filled}/{adj_total} ({pct:.1f}%)")

db.close()

# -- 3. Runtime verification --
print("\n[3] Runtime Verification")
try:
    from scripts.data_engine import DataEngine

    engine = DataEngine()

    # 3.1 DEFAULT_START_DATE configuration
    print(f"  engine.start_date = {engine.start_date}")
    import os
    print(f"  env STOCK_START_DATE = {os.environ.get('STOCK_START_DATE', '(not set)')}")

    # 3.2 get_active_stocks PIT queries
    stocks_2020 = engine.get_active_stocks("2020-01-01")
    print(f"  get_active_stocks('2020-01-01'): {len(stocks_2020)} stocks")
    print(f"  samples: {stocks_2020[:3]}")

    stocks_2024 = engine.get_active_stocks("2024-06-01")
    print(f"  get_active_stocks('2024-06-01'): {len(stocks_2024)} stocks")

    # 3.3 get_index_constituents
    cons = engine.get_index_constituents("000300.SH", "2020-01-01")
    print(f"  get_index_constituents('000300.SH', '2020-01-01'): {len(cons)} records")

    # 3.4 Daily raw / adjusted
    df_raw = engine.get_daily_raw("000001.SZ", start_date="2024-01-01", end_date="2024-01-10")
    df_adj = engine.get_daily_adjusted("000001.SZ", start_date="2024-01-01", end_date="2024-01-10")
    print(f"  get_daily_raw: {len(df_raw)} records")
    print(f"  get_daily_adjusted: {len(df_adj)} records")
    if not df_adj.empty and "qfq_close" in df_adj.columns:
        print(f"  qfq_close sample: {df_adj['qfq_close'].iloc[0]}")

    print("  [PASS] All runtime checks passed")

except Exception as e:
    print(f"  [FAIL] Runtime error: {e}")
    import traceback
    traceback.print_exc()
    all_passed = False

# -- Final verdict --
print("\n" + "=" * 60)
if all_passed:
    print("Final verdict: ALL CHECKS PASSED - schema v5.0 closure complete")
else:
    print("Final verdict: SOME CHECKS FAILED - review FAIL items above")
print("=" * 60)
