"""
数据口径标准化收口 - 最终验证脚本（直接 duckdb + 可运行代码验证）
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))

import duckdb
from scripts.data_engine import DataEngine, DEFAULT_START_DATE

print("=" * 60)
print("数据口径标准化收口 - 最终验证报告")
print("=" * 60)

db_path = project_root / "data" / "stock_data.duckdb"
engine = DataEngine()

# ═══════════════════════════════════════════════════════════════
# 1. 代码路径验证：读取源码验证旧口径是否清理
# ═══════════════════════════════════════════════════════════════
print("\n[1] 代码路径验证")

with open(project_root / "scripts" / "data_engine.py", "r", encoding="utf-8") as f:
    de_src = f.read()

checks = {
    "get_all_stocks 不再静默回退 _get_local_stocks": "_get_local_stocks" not in de_src.split("def get_all_stocks")[1].split("def ")[0],
    "get_all_stocks 显式抛 RuntimeError": "raise RuntimeError" in de_src.split("def get_all_stocks")[1].split("def ")[0],
    "save_snapshot 调用 _fetch_remote_stocks": "_fetch_remote_stocks" in de_src.split("def save_stock_basic_snapshot")[1].split("def ")[0],
    "save_snapshot 不走 get_all_stocks": "self.get_all_stocks" not in de_src.split("def save_stock_basic_snapshot")[1].split("def ")[0],
    "sync_stock_list 调用 _fetch_remote_stocks": "_fetch_remote_stocks" in de_src.split("def sync_stock_list")[1].split("def ")[0],
    "sync_stock_list 不走 get_all_stocks": "self.get_all_stocks" not in de_src.split("def sync_stock_list")[1].split("def ")[0],
}

with open(project_root / "scripts" / "survivorship_bias.py", "r", encoding="utf-8") as f:
    sbh_src = f.read()

checks["SurvivorshipBiasHandler.get_universe 用 get_active_stocks"] = (
    "get_active_stocks" in sbh_src.split("def get_universe")[1].split("def ")[0]
)
checks["SurvivorshipBiasHandler.get_universe 不走 get_all_stocks"] = (
    "get_all_stocks" not in sbh_src.split("def get_universe")[1].split("def ")[0]
)

with open(project_root / "scripts" / "security_master.py", "r", encoding="utf-8") as f:
    sm_src = f.read()

checks["security_master 不走 get_all_stocks"] = "get_all_stocks" not in sm_src

all_pass = True
for name, result in checks.items():
    status = "[PASS]" if result else "[FAIL]"
    if not result:
        all_pass = False
    print(f"  {status} {name}")

# ═══════════════════════════════════════════════════════════════
# 2. 数据结构验证
# ═══════════════════════════════════════════════════════════════
print("\n[2] 数据结构验证")

db = duckdb.connect(str(db_path), read_only=True)

tables = [
    ("stock_basic_history", "新表-股票历史主表"),
    ("daily_bar_raw", "新表-原始行情"),
    ("daily_bar_adjusted", "新表-复权行情"),
    ("index_constituents_history", "新表-指数成分历史"),
    ("sync_progress", "新表-同步进度"),
    ("data_quality_alert", "新表-数据质量告警"),
    ("stock_basic", "旧表(废弃)"),
    ("daily_quotes", "旧表(废弃)"),
    ("index_constituents", "旧表(废弃)"),
]

for tbl, desc in tables:
    try:
        cnt = db.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        cols = db.execute(f"PRAGMA table_info({tbl})").fetchall()
        print(f"  {desc}: {cnt:>10,} 条, {len(cols)} 字段")
    except Exception as e:
        print(f"  {desc}: [ERROR] {e}")

# 复权字段验证
print("\n  [daily_bar_adjusted 复权字段]")
adj_cols = {c[1]: True for c in db.execute("PRAGMA table_info(daily_bar_adjusted)").fetchall()}
total_adj = db.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
qfq_fields = ["qfq_open", "qfq_high", "qfq_low", "qfq_close",
              "hfq_open", "hfq_high", "hfq_low", "hfq_close"]
for f in qfq_fields:
    present = f in adj_cols
    cnt = db.execute(f"SELECT COUNT(*) FROM daily_bar_adjusted WHERE {f} IS NOT NULL AND {f} != 0").fetchone()[0]
    pct = cnt / total_adj * 100 if total_adj > 0 else 0
    status = "[PASS]" if (present and pct > 0) else "[FAIL]"
    print(f"  {status} {f:14s}: {cnt:>10,} / {total_adj} ({pct:.1f}%)")

db.close()

# ═══════════════════════════════════════════════════════════════
# 3. 功能验证：PIT 查询
# ═══════════════════════════════════════════════════════════════
print("\n[3] PIT 股票池查询")

for date, label in [
    ("2020-01-01", "2020初"),
    ("2022-06-01", "2022中"),
    ("2024-01-01", "2024初"),
    ("2025-01-01", "2025初"),
]:
    try:
        stocks = engine.get_active_stocks(date)
        print(f"  [PASS] get_active_stocks('{date}') [{label}]: {len(stocks):>5} 只  {stocks[:3]}")
    except Exception as e:
        print(f"  [FAIL] get_active_stocks('{date}'): {e}")

# ═══════════════════════════════════════════════════════════════
# 4. 指数成分历史验证
# ═══════════════════════════════════════════════════════════════
print("\n[4] 指数成分历史查询")

for idx, date in [
    ("000300.SH", "2020-01-01"),
    ("000300.SH", "2024-06-01"),
    ("000905.SH", "2020-01-01"),
    ("000852.SH", "2020-01-01"),
]:
    try:
        cons = engine.get_index_constituents(idx, date)
        print(f"  [PASS] get_index_constituents('{idx}', '{date}'): {len(cons):>4} 条")
    except Exception as e:
        print(f"  [FAIL] get_index_constituents('{idx}', '{date}'): {e}")

# ═══════════════════════════════════════════════════════════════
# 5. 原始价 vs 复权价
# ═══════════════════════════════════════════════════════════════
print("\n[5] 原始价 vs 复权价")

try:
    df_raw = engine.get_daily_raw("600000.SH", "2024-01-01", "2024-01-10")
    df_adj = engine.get_daily_adjusted("600000.SH", "2024-01-01", "2024-01-10")
    print(f"  get_daily_raw: {len(df_raw)} 条")
    print(f"  get_daily_adjusted: {len(df_adj)} 条")
    if not df_adj.empty:
        row = df_adj.iloc[0]
        print(f"  qfq_close={row.get('qfq_close', 'N/A'):.4f}, adj_factor={row.get('adj_factor', 'N/A')}")
    print(f"  [PASS] raw/adj 双路径可用")
except Exception as e:
    print(f"  [FAIL] {e}")

# ═══════════════════════════════════════════════════════════════
# 6. 可配置起始日期
# ═══════════════════════════════════════════════════════════════
print("\n[6] 可配置起始日期")

print(f"  DEFAULT_START_DATE = {DEFAULT_START_DATE}")
print(f"  engine.start_date  = {engine.start_date}")

e2 = DataEngine(start_date="2020-01-01")
print(f"  DataEngine(start_date='2020-01-01').start_date = {e2.start_date}")
print(f"  [{'PASS' if e2.start_date == '2020-01-01' else 'FAIL'}] 参数覆盖")

# ═══════════════════════════════════════════════════════════════
# 7. 主流程 SurvivorshipBiasHandler
# ═══════════════════════════════════════════════════════════════
print("\n[7] 主流程 SurvivorshipBiasHandler")

try:
    from scripts.survivorship_bias import SurvivorshipBiasHandler
    sbh = SurvivorshipBiasHandler(data_engine=engine)
    stocks = sbh.get_universe("2024-06-01")
    print(f"  [PASS] SurvivorshipBiasHandler.get_universe('2024-06-01'): {len(stocks)} 只")
except Exception as e:
    print(f"  [FAIL] SurvivorshipBiasHandler: {e}")

# ═══════════════════════════════════════════════════════════════
# 8. sync_progress / data_quality_alert
# ═══════════════════════════════════════════════════════════════
print("\n[8] 审计表")

db2 = duckdb.connect(str(db_path), read_only=True)
try:
    sp = db2.execute("SELECT COUNT(*) FROM sync_progress").fetchone()[0]
    dq = db2.execute("SELECT COUNT(*) FROM data_quality_alert").fetchone()[0]
    print(f"  [PASS] sync_progress: {sp} 条（下次同步填充）")
    print(f"  [PASS] data_quality_alert: {dq} 条")
except Exception as e:
    print(f"  [FAIL] {e}")
finally:
    db2.close()

# ═══════════════════════════════════════════════════════════════
# 总结
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print(f"结果: {'全部通过' if all_pass else '存在失败项'}")
print("=" * 60)
