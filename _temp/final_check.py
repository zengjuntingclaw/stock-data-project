
import sys, re, os
sys.path.insert(0, '.')

# ── 1. 代码路径检查 ─────────────────────────────────────
with open('scripts/data_engine.py', encoding='utf-8') as f:
    body = f.read()

# 去掉注释避免误判
body_nc = re.sub(r'#[^\n]*', '', body)

print("=== 1. 代码路径验证 ===")

# 检查1: DEFAULT_START_DATE
has_env = 'os.environ.get' in body and 'STOCK_START_DATE' in body
print(f"[1a] DEFAULT_START_DATE 环境变量可配置: {has_env}")

# 检查2: get_all_stocks 无静默回退到 _get_local_stocks
gas = re.search(r'def get_all_stocks\([^)]*\):', body)
if gas:
    end = body.find('\n    def ', gas.end())
    gas_body = body[gas.start():end if end != -1 else len(body)]
    gas_body_nc = re.sub(r'#[^\n]*', '', gas_body)
    has_local = '_get_local_stocks' in gas_body_nc
    has_error = 'RuntimeError' in gas_body
    print(f"[1b] get_all_stocks 无 _get_local_stocks 回退: {not has_local} (RuntimeError={has_error})")

# 检查3: stock_basic 表已删除
has_old_table = 'CREATE TABLE IF NOT EXISTS stock_basic' in body
print(f"[1c] stock_basic 旧表已从 __init_schema__ 删除: {not has_old_table}")

# 检查4: daily_bar_adjusted 含 qfq/hfq 字段
adj_create = re.search(r'CREATE TABLE IF NOT EXISTS daily_bar_adjusted \([^)]+\)', body, re.DOTALL)
if adj_create:
    adj_text = adj_create.group()
    has_qfq = 'qfq_close' in adj_text and 'qfq_open' in adj_text
    has_hfq = 'hfq_close' in adj_text and 'hfq_open' in adj_text
    print(f"[1d] daily_bar_adjusted 含 qfq 字段: {has_qfq}")
    print(f"[1e] daily_bar_adjusted 含 hfq 字段: {has_hfq}")

# 检查5: INSERT 语句写入 qfq/hfq
has_qfq_insert = 'qfq_close' in body and 'INSERT INTO daily_bar_adjusted' in body
print(f"[1f] INSERT 写入 qfq/hfq: {has_qfq_insert}")

# 检查6: 主流程用 get_active_stocks
uses_pit = '_data_engine.get_active_stocks' in open('scripts/survivorship_bias.py', encoding='utf-8').read()
print(f"[1g] SurvivorshipBiasHandler 使用 get_active_stocks: {uses_pit}")

# 检查7: 主流程传入 data_engine
be_body = open('scripts/backtest_engine_v3.py', encoding='utf-8').read()
uses_de = 'SurvivorshipBiasHandler(data_engine=data_engine)' in be_body
print(f"[1h] ProductionBacktestEngine 传入 data_engine: {uses_de}")

# 检查8: 10项数据质量
checks = ['duplicate_rows','ohlc_violation','pct_chg_extreme','adj_factor_jump',
          'zero_volume_non_suspend','delisted_with_future_data','adj_raw_ratio_invalid',
          'volume_amount_inconsistent','index_date_invalid','pit_universe_size_suspicious']
cnt = sum(1 for c in checks if c in body)
print(f"[1i] 数据质量检查项: {cnt}/10")

# 检查9: UPSERT
has_upsert = 'ON CONFLICT (ts_code, table_name) DO UPDATE' in body
print(f"[1j] sync_progress UPSERT: {has_upsert}")

print()
print("=== 2. 实际 DB 表字段检查 ===")
try:
    import duckdb
    db = 'data/stock_data.duckdb'
    if not os.path.exists(db):
        print("DB 不存在，跳过 DB 检查")
    else:
        conn = duckdb.connect(db)
        cur = conn.cursor()

        # daily_bar_adjusted 字段
        cur.execute("PRAGMA table_info('daily_bar_adjusted')")
        cols = [r[1] for r in cur.fetchall()]
        has_qfq_db = any('qfq' in c for c in cols)
        has_hfq_db = any('hfq' in c for c in cols)
        print(f"[2a] DB daily_bar_adjusted 字段数: {len(cols)}")
        print(f"[2b] DB 含 qfq 字段: {has_qfq_db} - {cols}")
        print(f"[2c] DB 含 hfq 字段: {has_hfq_db}")
        print(f"[2d] DB daily_bar_raw 字段: {[r[1] for r in cur.execute(\"PRAGMA table_info('daily_bar_raw')\").fetchall()]}")

        # stock_basic 表
        try:
            cur.execute("SELECT COUNT(*) FROM stock_basic")
            cnt = cur.fetchone()[0]
            print(f"[2e] stock_basic 表存在记录数: {cnt}")
        except:
            print("[2e] stock_basic 表不存在: OK")

        # stock_basic_history
        try:
            cnt = conn.execute(\"SELECT COUNT(*) FROM stock_basic_history\").fetchone()[0]
            print(f"[2f] stock_basic_history 记录数: {cnt}")
        except:
            print("[2f] stock_basic_history 表不存在")

        conn.close()
except Exception as e:
    print(f"DB 检查失败: {e}")

print()
print("=== 3. 功能验证 ===")
try:
    from scripts.data_engine import DataEngine
    de = DataEngine(db_path='data/stock_data.duckdb')

    stocks = de.get_active_stocks('2024-01-02')
    print(f"[3a] get_active_stocks('2024-01-02'): {len(stocks)} 只")
    if stocks:
        print(f"     示例: {stocks[:3]}")

    cons = de.get_index_constituents('000300.SH', '2024-01-02')
    print(f"[3b] get_index_constituents('000300.SH','2024-01-02'): {len(cons)} 条")

    raw = de.get_daily_raw('600000.SH', '2024-01-02', '2024-01-10')
    print(f"[3c] get_daily_raw('600000.SH'): {len(raw)} 条")
    if not raw.empty:
        print(f"     列: {list(raw.columns)}")

    adj = de.get_daily_adjusted('600000.SH', '2024-01-02', '2024-01-10')
    print(f"[3d] get_daily_adjusted('600000.SH'): {len(adj)} 条")
    if not adj.empty:
        has_qfq = 'qfq_close' in adj.columns
        has_hfq = 'hfq_close' in adj.columns
        print(f"     含 qfq_close: {has_qfq}, 含 hfq_close: {has_hfq}")
        if has_qfq:
            print(f"     qfq_close 示例: {adj['qfq_close'].dropna().head(3).tolist()}")

except Exception as e:
    print(f"功能测试失败: {e}")
    import traceback; traceback.print_exc()

print()
print("=== 验证完成 ===")
