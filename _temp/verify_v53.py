import re

with open('scripts/data_engine.py', encoding='utf-8') as f:
    body = f.read()

body_nc = re.sub(r'#.*', '', body)

has_env = 'os.environ.get' in body and 'STOCK_START_DATE' in body
print(f'[1] DEFAULT_START_DATE 环境变量可配置: {has_env}')

gas = re.search(r'def get_all_stocks\(.*?\n(?:.*?\n)*?(?=    def |$)', body, re.DOTALL)
if gas:
    gas_body = re.sub(r'#.*', '', gas.group())
    no_local = '_get_local_stocks' not in gas_body
    no_fallback = 'RuntimeError' in gas_body
    print(f'[2] get_all_stocks 无静默回退: {no_local and no_fallback}')

udd = re.search(r'# 全量：使用 get_active_stocks\(\).*?symbols = .*?get_active_stocks', body, re.DOTALL)
print(f'[3] update_daily_data + fetch_financial_data 走 get_active_stocks: {bool(udd)}')

checks_found = [
    'duplicate_rows', 'ohlc_violation', 'pct_chg_extreme',
    'adj_factor_jump', 'zero_volume_non_suspend', 'delisted_with_future_data',
    'adj_raw_ratio_invalid', 'volume_amount_inconsistent',
    'index_date_invalid', 'pit_universe_size_suspicious'
]
cnt = sum(c in body for c in checks_found)
print(f'[4] run_data_quality_check 10项: {cnt}/10')

has_upsert = 'ON CONFLICT (ts_code, table_name) DO UPDATE' in body
print(f'[5] sync_progress 断点续跑 UPSERT: {has_upsert}')

with open('scripts/survivorship_bias.py', encoding='utf-8') as f:
    sbh = f.read()
uses_pit = '_data_engine.get_active_stocks' in sbh
print(f'[6] SurvivorshipBiasHandler 使用 get_active_stocks: {uses_pit}')

with open('scripts/backtest_engine_v3.py', encoding='utf-8') as f:
    be = f.read()
uses_de = 'SurvivorshipBiasHandler(data_engine=data_engine)' in be
print(f'[7] ProductionBacktestEngine 传入 data_engine: {uses_de}')

with open('scripts/stock_history_schema.sql', encoding='utf-8') as f:
    sql = f.read()
required_tables = ['stock_basic_history', 'daily_bar_raw', 'daily_bar_adjusted',
                   'index_constituents_history', 'trade_calendar', 'corporate_actions',
                   'st_status_history', 'financial_data', 'data_quality_alert',
                   'sync_progress', 'update_log']
print()
print('=== Schema 对齐 ===')
for t in required_tables:
    print(f'  {t}: {"CREATE TABLE IF NOT EXISTS " + t in sql}')

# 验证 schema v2.1
print(f'\n[8] Schema v2.1: {"Schema v2.1" in sql}')

# 验证 get_active_stocks PIT 查询
pit_q = re.search(r'eff_date.*MAX.*eff_date|MAX.*eff_date.*GROUP BY', body, re.DOTALL)
print(f'[9] get_active_stocks 含 MAX(eff_date) PIT: {bool(pit_q)}')

# 验证 get_index_constituents
gic = re.search(r'def get_index_constituents', body)
print(f'[10] get_index_constituents 方法存在: {bool(gic)}')

# 验证日线分离
dbr = 'daily_bar_raw' in body
dba = 'daily_bar_adjusted' in body
dbr_col = 'qfq_close' in body and 'hfq_close' in body
print(f'[11] daily_bar_raw + daily_bar_adjusted 分离: {dbr and dba}')
print(f'[12] 复权字段 qfq/hfq 存在: {dbr_col}')
