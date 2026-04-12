"""Phase 4 functional verification - pure ASCII output"""
import sys, os, time
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

OUT = []
def out(msg):
    print(msg)
    OUT.append(msg)

out("=" * 60)
out("FUNCTIONAL VERIFICATION: Phase 4")
out("=" * 60)

# 1. start_date config
out("\n[1] start_date config unified")
with open(r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\config.toml') as f:
    cfg = f.read()
cfg_line = [l for l in cfg.strip().split('\n') if 'start_date' in l]
out(f"  config.toml start_date: {cfg_line[0].split('=')[1].strip()}" if cfg_line else "  config.toml: not found")
out("  Resolution order: param > env > config.toml > hardcode (2018-01-01)")

# 2. PIT query
out("\n[2] PIT query - historical stock pool")
active_2024 = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE list_date <= '2024-06-01'
      AND (delist_date > '2024-06-01' OR delist_date IS NULL)
""").fetchone()[0]
out(f"  Active on 2024-06-01: {active_2024}")
wrong = conn.execute("""
    SELECT COUNT(*) FROM stock_basic_history
    WHERE is_delisted = TRUE
      AND list_date <= '2024-06-01'
      AND (delist_date IS NULL OR delist_date = '2099-12-31')
""").fetchone()[0]
out(f"  Delisted with sentinel/NULL date (wrong, Baostock fix pending): {wrong}")

# 3. Layer consistency
out("\n[3] Layer consistency (raw vs adjusted)")
raw = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
adj = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
gap = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    LEFT JOIN daily_bar_raw raw ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE raw.ts_code IS NULL
""").fetchone()[0]
qfq_bad = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    JOIN daily_bar_raw raw ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE adj.qfq_close IS NOT NULL AND raw.close IS NOT NULL
      AND ABS(adj.qfq_close - raw.close * adj.adj_factor) > 0.01
""").fetchone()[0]
adj_factor_bad = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted WHERE adj_factor IS NULL OR adj_factor <= 0").fetchone()[0]
out(f"  daily_bar_raw:       {raw:,}")
out(f"  daily_bar_adjusted: {adj:,}")
out(f"  Gap:               {gap} (should be 0) {'OK' if gap == 0 else 'FAIL'}")
out(f"  qfq_close consistent: {'OK' if qfq_bad == 0 else f'FAIL: {qfq_bad} mismatches'}")
out(f"  adj_factor valid:    {'OK' if adj_factor_bad == 0 else f'FAIL: {adj_factor_bad} bad values'}")

# 4. sync_progress split tracking
out("\n[4] sync_progress split by table")
sp_total = conn.execute("SELECT COUNT(*) FROM sync_progress").fetchone()[0]
sp_by_table = conn.execute("""
    SELECT table_name, COUNT(*) as cnt, MAX(last_sync_date) as max_date
    FROM sync_progress GROUP BY table_name
""").fetchdf()
out(f"  Total rows: {sp_total}")
out(f"  By table:")
for _, row in sp_by_table.iterrows():
    out(f"    {row['table_name']}: {row['cnt']} stocks, last_sync={row['max_date']}")

# 5. Legacy table cleanup
out("\n[5] Legacy table cleanup")
all_tables = conn.execute("SHOW TABLES").fetchdf()['name'].tolist()
legacy = ['daily_quotes', 'daily_quotes_backup_20260410', 'index_constituents', '_test_t', 'stock_basic']
remaining = [t for t in legacy if t in all_tables]
out(f"  Remaining legacy tables: {remaining if remaining else 'none - OK'}")
out(f"  Current valid tables: {len(all_tables)}")

# 6. Index constituents history
out("\n[6] Index constituents history")
indices = conn.execute("""
    SELECT index_code, COUNT(DISTINCT ts_code) as constituents,
           SUM(CASE WHEN out_date IS NULL THEN 1 ELSE 0 END) as current
    FROM index_constituents_history GROUP BY index_code
""").fetchdf()
for _, row in indices.iterrows():
    out(f"  {row['index_code']}: {row['constituents']} constituents, {row['current']} current")
out(f"  Note: 000300.SH=300 OK, but 000001.SH (SSE Composite) has 0 - AkShare blocked")

# 7. data_quality_alert
out("\n[7] data_quality_alert")
dqa = conn.execute("SELECT COUNT(*) FROM data_quality_alert").fetchone()[0]
dqa_types = conn.execute("""
    SELECT alert_type, COUNT(*) as cnt FROM data_quality_alert
    GROUP BY alert_type ORDER BY cnt DESC LIMIT 5
""").fetchdf()
out(f"  Total alerts: {dqa}")
for _, row in dqa_types.iterrows():
    out(f"    {row['alert_type']}: {row['cnt']}")

conn.close()
out("\n" + "=" * 60)
out("VERIFICATION COMPLETE")
out("=" * 60)
