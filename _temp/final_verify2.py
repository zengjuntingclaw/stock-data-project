"""最终交付验证脚本"""
import sys
sys.path.insert(0, '.')
print("FINAL_VERIFICATION_START")
import duckdb
db = duckdb.connect('data/stock_data.duckdb', read_only=True)
tables = db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()
for t in tables:
    cnt = db.execute(f'SELECT COUNT(*) FROM "{t[0]}"').fetchone()[0]
    print(f'TABLE:{t[0]}:{cnt}')
db.close()
from scripts.data_engine import DataEngine
from scripts.exchange_mapping import build_ts_code
de = DataEngine(data_dir='data')
for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
    r = build_ts_code(inp)
    print(f'BTS:{inp}:{r}:OK' if r==exp else f'BTS:{inp}:{r}:FAIL')
stocks = de.get_active_stocks('2024-06-01')
print(f'PIT:2024-06-01:{len(stocks)}:{stocks[:3]}')
df_idx = de.get_index_constituents('000300.XSHG', '2024-06-01')
print(f'IDX:000300:2024-06-01:{len(df_idx)}')
df_raw = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
print(f'RAW:000001.SZ:{len(df_raw)}')
df_adj = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
print(f'ADJ:000001.SZ:{len(df_adj)}')
from scripts.survivorship_bias import SurvivorshipBiasHandler
sbh = SurvivorshipBiasHandler(data_engine=de)
pit = sbh.get_universe(__import__('datetime').datetime(2024,6,1), include_delisted=False)
print(f'SBH:2024-06-01:{len(pit)}')
print("FINAL_VERIFICATION_END")
