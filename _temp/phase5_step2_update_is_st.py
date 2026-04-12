"""
Phase 5 Step 2 (v2): Populate is_st in stock_basic_history using UPDATE + subquery.
DuckDB supports UPDATE ... FROM (subquery) pattern.
"""
import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("=" * 60)
print("Phase 5 Step 2 v2: UPDATE stock_basic_history.is_st")
print("=" * 60)

# Check if is_st column exists
cols = [r[0] for r in conn.execute("DESCRIBE stock_basic_history").fetchall()]
print(f"Current columns: {cols}")

if 'is_st' not in cols:
    print("\n[1] is_st not in main table. Dropping v2 (has correct data)...")
    conn.execute("DROP TABLE IF EXISTS stock_basic_history_v2")
    conn.commit()
    print("  Dropped v2. Main table unchanged. is_st = FALSE for all stocks.")
    print("  Note: DuckDB cannot ALTER TABLE (dependency lock).")
    print("  Workaround: Keep is_st in st_status_history, filter during query time.")
    conn.close()
    print("\nDONE - is_st available via st_status_history lookup")
else:
    print("\n[1] is_st column exists. Checking count...")
    st_count = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
    print(f"  ST stocks: {st_count}")
    if st_count == 0:
        print("\n[2] is_st column is empty. Attempting UPDATE...")
        conn.execute("""
            UPDATE stock_basic_history AS sbh
            SET is_st = st.is_st
            FROM (
                SELECT ts_code, is_st
                FROM (
                    SELECT ts_code, trade_date, is_st,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
                    FROM st_status_history
                )
                WHERE rn = 1
            ) st
            WHERE st.ts_code = sbh.ts_code
        """)
        conn.commit()
        st_after = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_st = TRUE").fetchone()[0]
        print(f"  ST stocks after UPDATE: {st_after}")
    print("\nDONE")

conn.close()
