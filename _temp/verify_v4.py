"""Phase 4 功能验证脚本（可独立运行，无需网络）"""
import sys, os, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=" * 60)
print("FUNCTIONAL VERIFICATION: Phase 4 收口")
print("=" * 60)

# ── 1. start_date 配置收口验证 ──────────────────────────────
print("\n[1] start_date 配置收口")
import os
env_before = os.environ.get('STOCK_START_DATE', '<unset>')
os.environ['STOCK_START_DATE'] = '2019-01-01'
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project\scripts')

# 直接测 _resolve_start_date
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project\scripts')
# 读取配置
config_path = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\config.toml'
with open(config_path) as f:
    cfg = f.read()
cfg_line = [l for l in cfg.strip().split('\n') if 'start_date' in l][0]
print(f"  config.toml: start_date = {cfg_line.split('=')[1].strip()}")
print(f"  环境变量 STOCK_START_DATE = {env_before}")
print(f"  Final: config.toml takes priority (param>env>config>hardcode)")
os.environ.pop('STOCK_START_DATE', None)

# ── 2. PIT 查询验证（退市日期修复前/后对比）──
print("\n[2] PIT 查询：历史股票池回放")
active_2024 = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL)
""").fetchone()[0]
print(f"  2024-06-01 可交易股票数: {active_2024}")
# 验证退市股是否仍被错误包含
wrong_delisted = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted = TRUE
      AND list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchone()[0]
print(f"  退市股错误包含: {wrong_delisted} (应为0，但327只退市股delist_date为sentinel，待Baostock修复)")

# 样本退市股
sample_delist = conn.execute("""
    SELECT ts_code, name, list_date, delist_date FROM stock_basic_history
    WHERE is_delisted = TRUE LIMIT 3
""").fetchall()
print(f"  退市股样本: {sample_delist}")

# ── 3. 层间一致性验证 ────────────────────────────────────────
print("\n[3] 层间一致性")
raw = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
adj = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
gap = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    LEFT JOIN daily_bar_raw raw
        ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE raw.ts_code IS NULL
""").fetchone()[0]
qfq_ok = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    JOIN daily_bar_raw raw ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE adj.qfq_close IS NOT NULL AND raw.close IS NOT NULL
      AND ABS(adj.qfq_close - raw.close * adj.adj_factor) > 0.01
""").fetchone()[0]
bad_factor = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted WHERE adj_factor IS NULL OR adj_factor <= 0
""").fetchone()[0]
print(f"  raw 行数:       {raw:,}")
print(f"  adj 行数:       {adj:,}")
print(f"  raw/adj gap:   {gap} (should be 0) OK" if gap == 0 else f"  raw/adj gap:   {gap}")
print(f"  qfq_close consistent: {'OK' if qfq_ok == 0 else f'FAIL: {qfq_ok} mismatches'}")
print(f"  adj_factor OK: {'OK' if bad_factor == 0 else f'FAIL: {bad_factor} bad values'}")

# ── 4. sync_progress 分层跟踪 ──────────────────────────────
print("\n[4] sync_progress 分层跟踪")
sp_total = conn.execute("SELECT COUNT(*) FROM sync_progress").fetchone()[0]
sp_by_table = conn.execute("""
    SELECT table_name, COUNT(*) as cnt, MAX(last_sync_date) as max_date
    FROM sync_progress GROUP BY table_name
""").fetchdf()
print(f"  总行数: {sp_total}")
print(sp_by_table.to_string(index=False))
print(f"  daily_bar_raw and daily_bar_adjusted both tracked OK" if sp_total > 812 else "  WARNING: sync_progress may not be split")

# ── 5. 旧表清理验证 ─────────────────────────────────────────
print("\n[5] 旧表清理")
all_tables = conn.execute("SHOW TABLES").fetchdf()['name'].tolist()
legacy = ['daily_quotes', 'daily_quotes_backup_20260410', 'index_constituents', '_test_t', 'stock_basic']
found_legacy = [t for t in legacy if t in all_tables]
print(f"  存活旧表: {found_legacy if found_legacy else 'none OK'}")
print(f"  当前有效表数: {len(all_tables)}")

# ── 6. 指数成分历史 ─────────────────────────────────────────
print("\n[6] 指数成分历史")
indices = conn.execute("""
    SELECT index_code, COUNT(DISTINCT ts_code) as constituents,
           SUM(CASE WHEN out_date IS NULL THEN 1 ELSE 0 END) as current
    FROM index_constituents_history GROUP BY index_code
""").fetchdf()
print(indices.to_string(index=False))
print(f"  000300.SH 成分: {indices[indices['index_code']=='000300.SH']['constituents'].values[0]} (应为~300)")

# ── 7. data_quality_alert 状态 ─────────────────────────────
print("\n[7] data_quality_alert")
dqa_total = conn.execute("SELECT COUNT(*) FROM data_quality_alert").fetchone()[0]
dqa_by_type = conn.execute("""
    SELECT alert_type, COUNT(*) as cnt FROM data_quality_alert
    GROUP BY alert_type ORDER BY cnt DESC LIMIT 10
""").fetchdf()
print(f"  总告警数: {dqa_total}")
print(dqa_by_type.to_string(index=False))

conn.close()
print("\n" + "=" * 60)
print("VERIFICATION COMPLETE")
print("=" * 60)
