"""Schema v5.1 完整验证脚本"""
import sys, os, re
sys.path.insert(0, '.')
from pathlib import Path

# ================================================================
# 1. 代码路径验证
# ================================================================
print("=" * 60)
print("1. CODE PATH VERIFICATION")
print("=" * 60)

with open("scripts/data_engine.py", "r", encoding="utf-8") as f:
    de_src = f.read()

checks = {}

# 1a. update_daily_data 不走 _get_local_stocks
pos = de_src.find("def update_daily_data(")
if pos >= 0:
    body = de_src[pos:pos+3000]
    end = body.find("\n    def ", 20)
    uda_body = body[:end] if end > 0 else body
    checks["update_daily_data 不走 _get_local_stocks"] = "_get_local_stocks" not in uda_body
else:
    checks["update_daily_data 不走 _get_local_stocks"] = False

# 1b. fetch_financial_data 不走 _get_local_stocks
pos2 = de_src.find("def fetch_financial_data(")
if pos2 >= 0:
    body2 = de_src[pos2:pos2+3000]
    end2 = body2.find("\n    def ", 20)
    ff_body = body2[:end2] if end2 > 0 else body2
    checks["fetch_financial_data 不走 _get_local_stocks"] = "_get_local_stocks" not in ff_body
else:
    checks["fetch_financial_data 不走 _get_local_stocks"] = False

# 1c. DEFAULT_START_DATE 环境变量优先
checks["DEFAULT_START_DATE 环境变量优先"] = (
    'os.environ.get("STOCK_START_DATE"' in de_src
    and 'self.start_date = start_date if start_date else DEFAULT_START_DATE' in de_src
)

# 1d. 主流程接入 SurvivorshipBiasHandler + data_engine
with open("scripts/backtest_engine_v3.py", "r", encoding="utf-8") as f:
    be_src = f.read()
checks["主流程接 SurvivorshipBiasHandler(data_engine)"] = (
    "SurvivorshipBiasHandler(data_engine=data_engine)" in be_src
)

# 1e. SurvivorshipBiasHandler 使用 get_active_stocks
with open("scripts/survivorship_bias.py", "r", encoding="utf-8") as f:
    sb_src = f.read()
checks["SurvivorshipBiasHandler 使用 get_active_stocks"] = (
    "get_active_stocks" in sb_src and "def get_universe" in sb_src
)

# 1f. get_daily_raw 和 get_daily_adjusted 均已实现
checks["get_daily_raw/adjusted 已实现"] = (
    "def get_daily_raw" in de_src and "def get_daily_adjusted" in de_src
)

# 1g. get_index_constituents 已实现
checks["get_index_constituents 已实现"] = "def get_index_constituents" in de_src

# 1h. get_active_stocks PIT 查询已实现
checks["get_active_stocks PIT 查询已实现"] = (
    "def get_active_stocks" in de_src
    and "stock_basic_history" in de_src
    and "eff_date" in de_src
)

# 1i. sync_progress 有 UPSERT 断点续跑
checks["sync_progress 断点续跑 UPSERT"] = (
    "ON CONFLICT (ts_code, table_name)" in de_src
    and "GREATEST(sync_progress.last_sync_date" in de_src
)

# 1j. run_data_quality_check 有新增检查项
checks["run_data_quality_check 新增3项检查"] = (
    "volume_amount_inconsistent" in de_src
    and "index_date_invalid" in de_src
    and "pit_universe_size_suspicious" in de_src
)

# ================================================================
# 2. 数据结构验证
# ================================================================
print("\n" + "=" * 60)
print("2. DATABASE VERIFICATION")
print("=" * 60)

import duckdb
db_path = "data/stock_data.duckdb"
if os.path.exists(db_path):
    conn = duckdb.connect(db_path)
    
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    print(f"Tables ({len(tables)}): {tables}")
    
    table_checks = {
        "stock_basic_history": ["ts_code", "eff_date", "symbol", "name", "exchange"],
        "daily_bar_raw": ["ts_code", "trade_date", "open", "high", "low", "close"],
        "daily_bar_adjusted": ["ts_code", "trade_date", "qfq_close", "hfq_close", "adj_factor"],
        "index_constituents_history": ["index_code", "ts_code", "in_date", "out_date"],
        "sync_progress": ["ts_code", "table_name", "last_sync_date", "status"],
        "data_quality_alert": ["alert_type", "ts_code", "trade_date", "detail"],
        "adj_factor_log": ["ts_code", "trade_date", "adj_factor_old", "adj_factor_new"],
    }
    
    for tbl, expected_cols in table_checks.items():
        if tbl in tables:
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()]
            missing = [c for c in expected_cols if c not in cols]
            if missing:
                print(f"  [FAIL] {tbl}: missing columns {missing}")
                checks[f"表 {tbl} 字段完整"] = False
            else:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"  [PASS] {tbl}: {cnt} records")
                checks[f"表 {tbl} 字段完整"] = True
        else:
            print(f"  [FAIL] {tbl}: not found")
            checks[f"表 {tbl} 字段完整"] = False
    
    # 验证复权字段
    adj_data = conn.execute(
        "SELECT COUNT(*) FROM daily_bar_adjusted WHERE qfq_close IS NOT NULL"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
    print(f"  qfq_close fill rate: {adj_data}/{total} ({adj_data/total*100:.1f}%)")
    checks["daily_bar_adjusted 复权字段可用"] = (adj_data > 0 and adj_data == total)
    
    # 验证 PIT 查询结果
    pit_stocks_2020 = conn.execute("""
        SELECT COUNT(DISTINCT h.ts_code) FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE eff_date <= DATE '2020-01-01'
            GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.list_date <= DATE '2020-01-01'
          AND (h.delist_date IS NULL OR h.delist_date > DATE '2020-01-01')
    """).fetchone()[0]
    print(f"  PIT stocks at 2020-01-01: {pit_stocks_2020}")
    checks["PIT 股票池规模合理 (2020)"] = (1000 < pit_stocks_2020 < 6000)
    
    pit_stocks_2024 = conn.execute("""
        SELECT COUNT(DISTINCT h.ts_code) FROM (
            SELECT ts_code, MAX(eff_date) as latest_eff
            FROM stock_basic_history WHERE eff_date <= DATE '2024-06-01'
            GROUP BY ts_code
        ) latest
        JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
        WHERE h.list_date <= DATE '2024-06-01'
          AND (h.delist_date IS NULL OR h.delist_date > DATE '2024-06-01')
    """).fetchone()[0]
    print(f"  PIT stocks at 2024-06-01: {pit_stocks_2024}")
    checks["PIT 股票池规模合理 (2024)"] = (1000 < pit_stocks_2024 < 6000)
    
    conn.close()
else:
    print(f"  Database not found: {db_path}")
    for tbl in ["stock_basic_history", "daily_bar_raw", "daily_bar_adjusted"]:
        checks[f"表 {tbl} 字段完整"] = False

# ================================================================
# 3. 功能验证
# ================================================================
print("\n" + "=" * 60)
print("3. FUNCTIONAL VERIFICATION")
print("=" * 60)

try:
    from scripts.data_engine import DataEngine, DEFAULT_START_DATE
    print(f"  DEFAULT_START_DATE = {DEFAULT_START_DATE}")
    checks["DEFAULT_START_DATE 正确读取"] = True
except Exception as e:
    import traceback; traceback.print_exc()
    print(f"  [FAIL] Import error: {e}")
    checks["DEFAULT_START_DATE 正确读取"] = False

try:
    engine = DataEngine()
    print(f"  engine.start_date = {engine.start_date}")
    
    stocks_2020 = engine.get_active_stocks("2020-01-01")
    print(f"  get_active_stocks('2020-01-01'): {len(stocks_2020)} stocks")
    if stocks_2020:
        print(f"    samples: {stocks_2020[:5]}")
    checks["get_active_stocks 运行正常"] = (len(stocks_2020) > 1000)
    
    stocks_2024 = engine.get_active_stocks("2024-06-01")
    print(f"  get_active_stocks('2024-06-01'): {len(stocks_2024)} stocks")
    
    cons = engine.get_index_constituents("000300.SH", "2020-01-01")
    print(f"  get_index_constituents('000300.SH', '2020-01-01'): {len(cons)} records")
    if not cons.empty:
        print(f"    columns: {list(cons.columns)}")
        print(f"    head:\n{cons.head(3).to_string()}")
    checks["get_index_constituents 运行正常"] = (not cons.empty)
    
    df_raw = engine.get_daily_raw("000001.SZ", start_date="2024-01-01", end_date="2024-01-10")
    print(f"  get_daily_raw('000001.SZ'): {len(df_raw)} records")
    if not df_raw.empty:
        print(f"    sample: open={df_raw.iloc[0]['open']}, close={df_raw.iloc[0]['close']}")
    checks["get_daily_raw 运行正常"] = (not df_raw.empty)
    
    df_adj = engine.get_daily_adjusted("000001.SZ", start_date="2024-01-01", end_date="2024-01-10")
    print(f"  get_daily_adjusted('000001.SZ'): {len(df_adj)} records")
    if not df_adj.empty:
        qfq = df_adj.iloc[0].get("qfq_close")
        adj_f = df_adj.iloc[0].get("adj_factor")
        hfq = df_adj.iloc[0].get("hfq_close")
        print(f"    qfq_close={qfq}, adj_factor={adj_f}, hfq_close={hfq}")
    checks["get_daily_adjusted 复权字段可用"] = (
        not df_adj.empty and df_adj.iloc[0].get("qfq_close") is not None
    )
    
except Exception as e:
    import traceback; traceback.print_exc()
    print(f"  [FAIL] Functional test error: {e}")
    for k in ["get_active_stocks 运行正常", "get_index_constituents 运行正常",
              "get_daily_raw 运行正常", "get_daily_adjusted 复权字段可用"]:
        checks[k] = False

# ================================================================
# 4. Schema vs Code 对齐验证
# ================================================================
print("\n" + "=" * 60)
print("4. SCHEMA vs CODE ALIGNMENT")
print("=" * 60)

schema_path = "scripts/stock_history_schema.sql"
with open(schema_path, "r", encoding="utf-8") as f:
    schema_src = f.read()

pg_patterns = ["nextval('sync_seq'", "nextval('alert_seq'", "nextval('action_seq'", "nextval('update_seq'"]
pg_found = [p for p in pg_patterns if p in schema_src]
if pg_found:
    print(f"  [FAIL] Schema contains PostgreSQL syntax: {pg_found}")
    checks["Schema 无 PostgreSQL 语法"] = False
else:
    print(f"  [PASS] Schema uses DuckDB-compatible syntax")
    checks["Schema 无 PostgreSQL 语法"] = True

schema_sync = "ts_code        TEXT       NOT NULL" in schema_src
checks["sync_progress schema 对齐"] = schema_sync
print(f"  [{'PASS' if schema_sync else 'FAIL'}] sync_progress schema")

schema_adj = "adj_factor_old" in schema_src
checks["adj_factor_log schema 对齐"] = schema_adj
print(f"  [{'PASS' if schema_adj else 'FAIL'}] adj_factor_log schema")

schema_dqa = "alert_type" in schema_src
checks["data_quality_alert schema 对齐"] = schema_dqa
print(f"  [{'PASS' if schema_dqa else 'FAIL'}] data_quality_alert schema")

# ================================================================
# 5. 汇总
# ================================================================
print("\n" + "=" * 60)
print("5. SUMMARY")
print("=" * 60)
all_passed = True
for name, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    if not passed:
        all_passed = False
    print(f"  [{status}] {name}")

print()
if all_passed:
    print("FINAL VERDICT: ALL CHECKS PASSED - Schema v5.1 closure complete")
else:
    failed = [k for k, v in checks.items() if not v]
    print(f"FINAL VERDICT: {len(failed)} CHECKS FAILED:")
    for k in failed:
        print(f"  - {k}")
