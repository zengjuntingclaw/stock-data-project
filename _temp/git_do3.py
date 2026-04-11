import subprocess

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

def run(args, outfile):
    r = subprocess.run(args, capture_output=True, timeout=30)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:500])

# 1. Stage scripts directory
run(['git', '-C', repo, 'add', 'scripts/'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stage1.txt')
print("Staged scripts")

# 2. Stage other files
run(['git', '-C', repo, 'add', 'main_v2_production.py', 'run_backtest.py', 'run_tests.py', 'pyproject.toml', 'requirements.txt', 'README.md'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stage2.txt')
print("Staged other files")

# 3. Status
run(['git', '-C', repo, 'status', '--porcelain'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stage3.txt')
print("Checked status")

# 4. Commit
r = subprocess.run(['git', '-C', repo, 'commit', '-m', 
    'feat: schema v5 数据口径标准化重构\n'
    '- PIT 股票池 (get_active_stocks)\n'
    '- 指数成分历史区间表\n'
    '- raw/adjusted 双层行情分离\n'
    '- 8 qfq/hfq 复权字段\n'
    '- DROP stock_basic + 环境变量配置\n'
    '- 10 项数据质量检查\n'
    '- 断点续跑 UPSERT'],
    capture_output=True, timeout=30)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stage4.txt', 'w', encoding='utf-8') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])
print("Commit done")

# 5. Push
r = subprocess.run(['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, timeout=30)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stage5.txt', 'w', encoding='utf-8') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])
print("Push done")

# 6. Log
r = subprocess.run(['git', '-C', repo, 'log', '--oneline', '-3'],
    capture_output=True, timeout=10)
print("LOG:", r.stdout)

print('ALL DONE')
