"""Fix layer gap (raw=1409322, adj=1409312, gap=10) + cleanup duplicate table"""
import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("=== Fix Layer Gap + Cleanup ===")

# Find gap rows (in raw but not in adjusted)
gap = conn.execute("""
    SELECT raw.ts_code, raw.trade_date, raw.close
    FROM daily_bar_raw raw
    LEFT JOIN daily_bar_adjusted adj
        ON adj.ts_code = raw.ts_code AND adj.trade_date = raw.trade_date
    WHERE adj.ts_code IS NULL
""").fetchdf()
print(f"Gap rows (raw without adj): {len(gap)}")
print(gap.head(10).to_string(index=False))

# Fix: delete gap rows from raw
if len(gap) > 0:
    for _, row in gap.iterrows():
        conn.execute("""
            DELETE FROM daily_bar_raw
            WHERE ts_code = ? AND CAST(trade_date AS VARCHAR) = ?
        """, [row['ts_code'], str(row['trade_date'])])
    conn.commit()
    print(f"Deleted {len(gap)} gap rows from daily_bar_raw")

# Cleanup stock_basic_history_v2
tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
if 'stock_basic_history_v2' in tables:
    count = conn.execute("SELECT COUNT(*) FROM stock_basic_history_v2").fetchone()[0]
    print(f"\nDropping duplicate: stock_basic_history_v2 ({count} rows)")
    conn.execute("DROP TABLE stock_basic_history_v2")
    conn.commit()
    print("  Dropped")
else:
    print("\nstock_basic_history_v2 already gone")

# Final check
tables_after = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
raw = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
adj = conn.execute("SELECT COUNT(*) FROM daily_bar_adjusted").fetchone()[0]
gap_final = conn.execute("""
    SELECT COUNT(*) FROM daily_bar_raw raw
    LEFT JOIN daily_bar_adjusted adj ON adj.ts_code=raw.ts_code AND adj.trade_date=raw.trade_date
    WHERE adj.ts_code IS NULL
""").fetchone()[0]
print(f"\nTables: {tables_after}")
print(f"raw: {raw}, adj: {adj}, gap: {gap_final}")

conn.close()
print("DONE")
