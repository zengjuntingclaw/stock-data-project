import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)

print('=== 新表存在性检查 ===')
new_tables = ['stock_basic_history', 'daily_bar_raw', 'daily_bar_adjusted',
              'index_constituents_history', 'sync_progress', 'data_quality_alert']
for t in new_tables:
    exists = t in [r[0] for r in db.execute('SHOW TABLES').fetchall()]
    print(f'  {t}: {"存在" if exists else "缺失"}')

print()
print('=== stock_basic_history 表结构 ===')
cols = db.execute('PRAGMA table_info(stock_basic_history)').fetchall()
for c in cols:
    print(f'  {c[1]}: {c[2]}')

print()
print('=== 记录数统计 ===')
stats = [
    ('stock_basic_history', 'SELECT COUNT(*) FROM stock_basic_history'),
    ('daily_bar_raw', 'SELECT COUNT(*) FROM daily_bar_raw'),
    ('daily_bar_adjusted', 'SELECT COUNT(*) FROM daily_bar_adjusted'),
    ('index_constituents_history', 'SELECT COUNT(*) FROM index_constituents_history'),
    ('sync_progress', 'SELECT COUNT(*) FROM sync_progress'),
    ('data_quality_alert', 'SELECT COUNT(*) FROM data_quality_alert'),
]
for name, sql in stats:
    try:
        cnt = db.execute(sql).fetchone()[0]
        print(f'  {name}: {cnt:,}')
    except Exception as e:
        print(f'  {name}: 查询失败 - {e}')

print()
print('=== daily_bar_raw 表结构 ===')
cols = db.execute('PRAGMA table_info(daily_bar_raw)').fetchall()
for c in cols:
    print(f'  {c[1]}: {c[2]}')

print()
print('=== daily_bar_adjusted 表结构 ===')
cols = db.execute('PRAGMA table_info(daily_bar_adjusted)').fetchall()
for c in cols:
    print(f'  {c[1]}: {c[2]}')

print()
print('=== index_constituents_history 表结构 ===')
cols = db.execute('PRAGMA table_info(index_constituents_history)').fetchall()
for c in cols:
    print(f'  {c[1]}: {c[2]}')

db.close()
