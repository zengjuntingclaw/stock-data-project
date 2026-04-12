import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=== OHLC 违规明细 ===")
violations = conn.execute("""
    SELECT ts_code, trade_date, open, high, low, close, pct_chg
    FROM daily_bar_raw
    WHERE (high < low)
       OR (high < close AND close IS NOT NULL AND high IS NOT NULL)
       OR (high < open AND open IS NOT NULL AND high IS NOT NULL)
       OR (low > close AND close IS NOT NULL AND low IS NOT NULL)
       OR (low > open AND open IS NOT NULL AND low IS NOT NULL)
    LIMIT 10
""").fetchdf()
print(violations.to_string())
print(f"Total: {conn.execute('SELECT COUNT(*) FROM daily_bar_raw WHERE high < low').fetchone()[0]} rows with high < low")

print("\n=== 128行 Gap 明细（adj 有但 raw 无）===")
gap = conn.execute("""
    SELECT adj.ts_code, adj.trade_date, adj.close as adj_close
    FROM daily_bar_adjusted adj
    LEFT JOIN daily_bar_raw raw
        ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE raw.ts_code IS NULL
    LIMIT 10
""").fetchdf()
print(gap.to_string())
print(f"Gap total: {conn.execute('''
    SELECT COUNT(*) FROM daily_bar_adjusted adj
    LEFT JOIN daily_bar_raw raw
        ON raw.ts_code = adj.ts_code AND raw.trade_date = adj.trade_date
    WHERE raw.ts_code IS NULL
''').fetchone()[0]}")

print("\n=== sync_progress 样本 ===")
sp = conn.execute("""
    SELECT ts_code, table_name, last_sync_date, total_records, status
    FROM sync_progress
    LIMIT 10
""").fetchdf()
print(sp.to_string())

print("\n=== data_quality_alert 最新记录 ===")
dqa = conn.execute("""
    SELECT id, alert_type, detail, created_at
    FROM data_quality_alert
    ORDER BY created_at DESC
    LIMIT 10
""").fetchdf()
print(dqa.to_string())

conn.close()
