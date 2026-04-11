"""最终交付验证脚本"""
import sys, os
sys.path.insert(0, '.')
import duckdb
import datetime
from scripts.data_engine import DataEngine
from scripts.exchange_mapping import build_ts_code
from scripts.survivorship_bias import SurvivorshipBiasHandler

lines = []
lines.append("FINAL_VERIFICATION_START")

db = duckdb.connect('data/stock_data.duckdb', read_only=True)
tables = db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()
for t in tables:
    cnt = db.execute(f'SELECT COUNT(*) FROM "{t[0]}"').fetchone()[0]
    lines.append(f'TABLE:{t[0]}:{cnt}')
db.close()

for inp, exp in [('600000','600000.SH'),('000001','000001.SZ'),('688001','688001.SH'),('430001','430001.BJ'),('920001','920001.BJ')]:
    r = build_ts_code(inp)
    lines.append(f'BTS:{inp}:{r}:OK' if r==exp else f'BTS:{inp}:{r}:FAIL')

de = DataEngine(data_dir='data')
stocks = de.get_active_stocks('2024-06-01')
lines.append(f'PIT:2024-06-01:{len(stocks)}')

df_idx = de.get_index_constituents('000300.XSHG', '2024-06-01')
lines.append(f'IDX:000300:2024-06-01:{len(df_idx)}')

df_raw = de.get_daily_raw('000001.SZ', '2024-06-01', '2024-06-05')
lines.append(f'RAW:000001.SZ:{len(df_raw)}')

df_adj = de.get_daily_adjusted('000001.SZ', '2024-06-01', '2024-06-05')
lines.append(f'ADJ:000001.SZ:{len(df_adj)}')

sbh = SurvivorshipBiasHandler(data_engine=de)
pit = sbh.get_universe(datetime.datetime(2024,6,1), include_delisted=False)
lines.append(f'SBH:2024-06-01:{len(pit)}')

lines.append("FINAL_VERIFICATION_END")

with open('_temp/verify_result.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))
