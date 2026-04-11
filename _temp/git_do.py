import subprocess

# Check git log
r1 = subprocess.run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'log', '--oneline', '-5'], capture_output=True, text=True)
print("=== GIT LOG ===")
print(r1.stdout)

# Check key sections of data_engine.py from HEAD
r2 = subprocess.run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', 'HEAD:scripts/data_engine.py'], capture_output=True, text=True)
lines = r2.stdout.split('\n')
print("=== KEY LINES IN data_engine.py (HEAD) ===")
for i, l in enumerate(lines):
    if any(k in l for k in ['DEFAULT_START_DATE', '_get_local_stocks', 'stock_basic', 'qfq_close', 'hfq_close', 'get_all_stocks', 'DROP TABLE IF EXISTS']):
        print(f'{i+1}: {l}')

print("\n=== stock_history_schema.sql HEAD ===")
r3 = subprocess.run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', 'HEAD:scripts/stock_history_schema.sql'], capture_output=True, text=True)
slines = r3.stdout.split('\n')
for i, l in enumerate(slines):
    if 'daily_bar_adjusted' in l or 'qfq_' in l or 'hfq_' in l or 'Schema' in l or 'v2' in l:
        print(f'{i+1}: {l}')
