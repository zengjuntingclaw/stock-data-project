"""Quick fixes: test files + index constituents rebuild"""
import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
import re

# ==============================================================
# PART 1: Fix test_data_engine.py - daily_quotes -> daily_bar_adjusted
# ==============================================================
test_path = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/tests/test_data_engine.py'
with open(test_path, 'r', encoding='utf-8') as f:
    content = f.read()

before = len(re.findall(r'daily_quotes', content))
after = content.replace('daily_quotes', 'daily_bar_adjusted')
print(f'test_data_engine.py: {before} replacements')
with open(test_path, 'w', encoding='utf-8') as f:
    f.write(after)

# ==============================================================
# PART 2: Rebuild index_constituents_history (no duplicate snapshots)
# ==============================================================
import duckdb
DB = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/data/stock_data.duckdb'
conn = duckdb.connect(DB)

ich_count_before = conn.execute("SELECT COUNT(*) FROM index_constituents_history").fetchone()[0]
print(f'\nPart 2: Rebuilding index_constituents_history: {ich_count_before} rows before')

# Get distinct current constituents (no duplicates)
current = conn.execute("""
    SELECT DISTINCT index_code, ts_code, MIN(in_date) as earliest_in
    FROM index_constituents_history
    GROUP BY index_code, ts_code
""").fetchall()
print(f'  Distinct constituent pairs: {len(current)}')

# Clear and rebuild
conn.execute("DELETE FROM index_constituents_history")
for idx_code, ts_code, earliest_in in current:
    conn.execute("""
        INSERT OR IGNORE INTO index_constituents_history
        (index_code, ts_code, in_date, out_date, source)
        VALUES (?, ?, ?, NULL, 'snapshot_reconstruction')
    """, [idx_code, ts_code, earliest_in])

ich_count_after = conn.execute("SELECT COUNT(*) FROM index_constituents_history").fetchone()[0]
dups = conn.execute("""
    SELECT index_code, ts_code, COUNT(*) as cnt
    FROM index_constituents_history GROUP BY index_code, ts_code HAVING cnt > 1
""").fetchall()
print(f'  After: {ich_count_after} rows, duplicate pairs: {len(dups)}')

# ==============================================================
# PART 3: Check which delisted stocks have bad delist_date
# ==============================================================
print('\nPart 3: Delisted stock delist_date status')
delisted = conn.execute("""
    SELECT ts_code, delist_date,
           CASE WHEN delist_date IS NULL THEN 'NULL'
                WHEN delist_date = '2099-12-31' THEN 'SENTINEL'
                ELSE 'OK'
           END as status
    FROM stock_basic_history
    WHERE is_delisted = TRUE
    LIMIT 20
""").fetchall()
print(f'  Sample:')
for ts, dd, st in delisted:
    print(f'    {ts}: {dd} [{st}]')

null_date = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NULL").fetchone()[0]
sentinel = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date='2099-12-31'").fetchone()[0]
has_real = conn.execute("SELECT COUNT(*) FROM stock_basic_history WHERE is_delisted=TRUE AND delist_date IS NOT NULL AND delist_date!='2099-12-31'").fetchone()[0]
print(f'\n  NULL: {null_date}, SENTINEL: {sentinel}, HAS_REAL_DATE: {has_real}')

conn.close()
print('\nQuick fixes complete')
