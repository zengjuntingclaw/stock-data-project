import sys, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'

conn = duckdb.connect(DB, read_only=True)

print("=== Phase 5 Final Verification ===")

# Layer consistency
raw = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
adj = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
gap = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    LEFT JOIN daily_bar_raw raw ON raw.ts_code=adj.ts_code AND raw.trade_date=adj.trade_date
    WHERE raw.ts_code IS NULL
""").fetchone()[0]
qfq_bad = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    JOIN daily_bar_raw raw ON raw.ts_code=adj.ts_code AND raw.trade_date=adj.trade_date
    WHERE adj.qfq_close IS NOT NULL AND raw.close IS NOT NULL
      AND ABS(adj.qfq_close - raw.close * adj.adj_factor) > 0.01
""").fetchone()[0]
adj_factor_bad = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted WHERE adj_factor IS NULL OR adj_factor <= 0").fetchone()[0]

print(f"daily_bar_raw:      {raw:,}")
print(f"daily_bar_adjusted: {adj:,}")
print(f"Gap:                {gap} (should be 0)")
print(f"qfq_close bad:     {qfq_bad} (should be 0)")
print(f"adj_factor bad:     {adj_factor_bad} (should be 0)")

# Trade dates
dates = conn.execute("""
    SELECT trade_date, COUNT(*) FROM daily_bar_adjusted
    WHERE trade_date >= '2026-04-07'
    GROUP BY trade_date ORDER BY trade_date
""").fetchdf()
print(f"\nNew trading days:")
print(dates.to_string(index=False))

# sync_progress
sp = conn.execute("""
    SELECT table_name, COUNT(*) as cnt, MAX(last_sync_date) as max_date
    FROM sync_progress GROUP BY table_name
""").fetchdf()
print(f"\nsync_progress:")
print(sp.to_string(index=False))

# data_quality_alert
dqa = conn.execute("SELECT COUNT(*) FROM data_quality_alert").fetchone()[0]
print(f"\ndata_quality_alert: {dqa}")

# stock_basic_history status
r = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN is_delisted=TRUE THEN 1 ELSE 0 END) as delisted,
           SUM(CASE WHEN is_st=TRUE THEN 1 ELSE 0 END) as st,
           SUM(CASE WHEN is_delisted=FALSE AND is_st=FALSE THEN 1 ELSE 0 END) as active_normal,
           SUM(CASE WHEN is_delisted=FALSE AND is_st=TRUE THEN 1 ELSE 0 END) as active_st
    FROM stock_basic_history
""").fetchone()
print(f"\nstock_basic_history:")
print(f"  Total: {r[0]}, Active normal: {r[3]}, Active ST: {r[4]}, Delisted: {r[1]}, ST(all): {r[2]}")

# All tables
tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
print(f"\nAll tables ({len(tables)}): {tables}")
conn.close()
