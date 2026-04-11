import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Find all files to stage
files_to_add = []
for root, dirs, files in os.walk(os.path.join(repo, 'scripts')):
    dirs[:] = [d for d in dirs if d != '__pycache__']
    for f in files:
        if not f.endswith('.pyc'):
            full = os.path.join(root, f)
            rel = os.path.relpath(full, repo).replace('\\', '/')
            files_to_add.append(rel)

for f in ['main_v2_production.py', 'run_backtest.py', 'run_tests.py', 
           'pyproject.toml', 'requirements.txt', 'README.md',
           'data_quality_report.json']:
    fp = os.path.join(repo, f)
    if os.path.exists(fp):
        files_to_add.append(f)

print(f'Files to add: {len(files_to_add)}')
for f in files_to_add[:5]:
    print(' ', f)

# Stage
r = subprocess.run(
    ['git', '-C', repo, 'add'] + files_to_add,
    capture_output=True, stdin=subprocess.DEVNULL
)
print('add exit:', r.returncode, 'stdout:', r.stdout[:100])

# Verify what's staged
r = subprocess.run(
    ['git', '-C', repo, 'diff', '--cached', '--stat'],
    capture_output=True, stdin=subprocess.DEVNULL, timeout=30
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/staged.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:200])
print('diff --cached exit:', r.returncode, 'len:', len(r.stdout))

# Commit
commit_msg = 'feat: schema v5 数据口径标准化重构\n\n' \
    '- PIT 股票池 (get_active_stocks)\n' \
    '- 指数成分历史区间表\n' \
    '- raw/adjusted 双层行情分离\n' \
    '- 8 qfq/hfq 复权字段\n' \
    '- DROP stock_basic + 环境变量配置\n' \
    '- 10 项数据质量检查\n' \
    '- 断点续跑 UPSERT\n'

r = subprocess.run(
    ['git', '-C', repo, 'commit', '-m', commit_msg],
    capture_output=True, stdin=subprocess.DEVNULL, timeout=60
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/comm2.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:300])
print('commit exit:', r.returncode, 'stdout:', r.stdout[:200])

# Push
r = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, stdin=subprocess.DEVNULL, timeout=60
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/push2.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:300])
print('push exit:', r.returncode, 'stdout:', r.stdout[:200])

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/log2.txt', 'w') as f:
    f.write(r.stdout)
print('log:', r.stdout)

print('ALL_DONE')
