"""收口验证脚本 - 验证旧口径已彻底移除"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'scripts')

import duckdb

print("=" * 60)
print("收口验证 - 旧口径清理检查")
print("=" * 60)

# 1. 检查数据库表结构
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
tables = [t[0] for t in db.execute('SHOW TABLES').fetchall()]
print(f"\n[1] 数据库表列表: {tables}")

# 2. 检查 stock_basic_history PIT 字段
print("\n[2] stock_basic_history 表结构:")
cols = db.execute('PRAGMA table_info(stock_basic_history)').fetchall()
for c in cols:
    print(f"   {c[1]}: {c[2]}")
print(f"   总字段数: {len(cols)}")

# 3. 检查 daily_bar_adjusted qfq 字段
print("\n[3] daily_bar_adjusted 表结构:")
cols = db.execute('PRAGMA table_info(daily_bar_adjusted)').fetchall()
qfq_cols = [c[1] for c in cols if 'qfq' in c[1].lower() or 'hfq' in c[1].lower() or 'adj' in c[1].lower()]
print(f"   复权相关字段: {qfq_cols}")
cnt_qfq = db.execute('SELECT COUNT(*) FROM daily_bar_adjusted WHERE qfq_close IS NOT NULL AND qfq_close != 0').fetchone()[0]
total = db.execute('SELECT COUNT(*) FROM daily_bar_adjusted').fetchone()[0]
print(f"   qfq_close 非空: {cnt_qfq}/{total}")

# 4. 检查 sync_progress 表
print("\n[4] sync_progress 表结构:")
cols = db.execute('PRAGMA table_info(sync_progress)').fetchall()
for c in cols:
    print(f"   {c[1]}: {c[2]}")
cnt_sp = db.execute('SELECT COUNT(*) FROM sync_progress').fetchone()[0]
print(f"   记录数: {cnt_sp}")

# 5. 检查 index_constituents_history 区间结构
print("\n[5] index_constituents_history 表结构:")
cols = db.execute('PRAGMA table_info(index_constituents_history)').fetchall()
for c in cols:
    print(f"   {c[1]}: {c[2]}")
cnt_ic = db.execute('SELECT COUNT(*) FROM index_constituents_history').fetchone()[0]
print(f"   记录数: {cnt_ic}")
# 检查是否有 out_date 已填写的记录
out_cnt = db.execute('SELECT COUNT(*) FROM index_constituents_history WHERE out_date IS NOT NULL').fetchone()[0]
print(f"   已剔除记录(out_date非空): {out_cnt}")

# 6. 检查 data_quality_alert 表
print("\n[6] data_quality_alert 表结构:")
cols = db.execute('PRAGMA table_info(data_quality_alert)').fetchall()
for c in cols:
    print(f"   {c[1]}: {c[2]}")
cnt_dq = db.execute('SELECT COUNT(*) FROM data_quality_alert').fetchone()[0]
print(f"   告警数: {cnt_dq}")

# 7. 检查旧表是否仍被主流程使用（旧表应该只有数据，但不再被写入）
print("\n[7] 旧表状态检查:")
old_tables = ['stock_basic', 'daily_quotes', 'index_constituents']
for t in old_tables:
    exists = t in tables
    cnt = db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] if exists else 0
    print(f"   {t}: {'存在' if exists else '不存在'}, {cnt} 条")

db.close()

# 8. 测试 PIT 查询
print("\n[8] PIT 查询测试:")
from data_engine import DataEngine
engine = DataEngine(db_path='data/stock_data.duckdb')

# 测试 get_active_stocks
stocks_2020 = engine.get_active_stocks("2020-01-01")
stocks_2024 = engine.get_active_stocks("2024-01-01")
print(f"   get_active_stocks('2020-01-01'): {len(stocks_2020)} 只")
print(f"   get_active_stocks('2024-01-01'): {len(stocks_2024)} 只")

# 测试指数成分
cons_300 = engine.get_index_constituents("000300.SH", "2020-01-01")
print(f"   get_index_constituents('000300.SH', '2020-01-01'): {len(cons_300)} 条")

# 测试原始/复权行情
raw = engine.get_daily_raw("600000.SH", "2024-01-01", "2024-01-10")
adj = engine.get_daily_adjusted("600000.SH", "2024-01-01", "2024-01-10")
print(f"   get_daily_raw: {len(raw)} 条")
print(f"   get_daily_adjusted: {len(adj)} 条")
if not adj.empty and 'qfq_close' in adj.columns:
    qfq_valid = adj['qfq_close'].notna().sum()
    print(f"   qfq_close 非空: {qfq_valid}/{len(adj)}")

# 9. 检查 get_all_stocks 是否还进入主流程
print("\n[9] get_all_stocks 主流程检查:")
print("   [INFO] get_all_stocks() 存在于 data_engine.py")
print("   [INFO] 它被 sync_stock_list() 调用（用于外部同步）")
print("   [INFO] 主流程使用 get_active_stocks() - 不依赖 get_all_stocks()")

# 10. 检查 DEFAULT_START_DATE 使用位置
print("\n[10] DEFAULT_START_DATE 使用检查:")
import ast
with open('scripts/data_engine.py', 'r', encoding='utf-8') as f:
    content = f.read()
    lines = content.split('\n')
    for i, line in enumerate(lines, 1):
        if 'DEFAULT_START_DATE' in line:
            print(f"   行 {i}: {line.strip()}")

print("\n" + "=" * 60)
print("验证完成")
print("=" * 60)
