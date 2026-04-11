import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import duckdb

DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB, read_only=True)

print("=== index_constituents_history ===")
for idx in ['000300.SH', '000905.SH', '000852.SH']:
    cnt = conn.execute("""
        SELECT COUNT(*) FROM index_constituents_history WHERE index_code = ?
    """, [idx]).fetchone()[0]
    print(f"  {idx}: {cnt} constituents")

# Check total
total = conn.execute("SELECT COUNT(*) FROM index_constituents_history").fetchone()[0]
indices = conn.execute("SELECT COUNT(DISTINCT index_code) FROM index_constituents_history").fetchone()[0]
pairs = conn.execute("SELECT COUNT(DISTINCT index_code, ts_code) FROM index_constituents_history").fetchone()[0]
print(f"\nTotal: {total} rows, {indices} indices, {pairs} distinct pairs")
print(f"Expected for 000300: ~300, got: {conn.execute('SELECT COUNT(*) FROM index_constituents_history WHERE index_code=?', [\"000300.SH\"]).fetchone()[0]}")

conn.close()
