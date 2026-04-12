import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb

DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)

print("Checking constraints on all relevant tables...")
# Check constraints on daily_bar_adjusted
bar_constraints = conn.execute("""
    SELECT constraint_type, constraint_text, referenced_table
    FROM duckdb_constraints()
    WHERE table_name IN ('daily_bar_adjusted', 'daily_bar_raw', 'stock_basic_history', 'stock_basic_history_v2')
""").fetchdf()
print("Constraints:")
print(bar_constraints.to_string(index=False))

# Check if stock_basic_history_v2 is a VIEW or TABLE
kind_v2 = conn.execute("""
    SELECT table_type FROM information_schema.tables WHERE table_name = 'stock_basic_history_v2'
""").fetchone()
print(f"\nstock_basic_history_v2 type: {kind_v2}")

kind_main = conn.execute("""
    SELECT table_type FROM information_schema.tables WHERE table_name = 'stock_basic_history'
""").fetchone()
print(f"stock_basic_history type: {kind_main}")

# Find views that reference stock_basic_history
print("\nSearching for views referencing stock_basic_history...")
views = conn.execute("SHOW TABLES").fetchdf()
print("All tables/views:", views.to_string(index=False))

# Try to DROP FK constraint then rename
print("\nAttempting fix...")
try:
    # First rename the old stock_basic_history to a safe name
    conn.execute("ALTER TABLE stock_basic_history RENAME TO stock_basic_history_old_backup")
    print("  Renamed stock_basic_history -> stock_basic_history_old_backup")
except Exception as e:
    print(f"  Rename failed: {e}")
    print("  Trying alternative approach...")

conn.commit()
conn.close()
