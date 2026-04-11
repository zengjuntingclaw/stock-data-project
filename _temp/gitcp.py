import subprocess, os, sys

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Check what's staged
r1 = subprocess.run(
    ['git', '-C', repo, 'ls-files', '--stage', 'scripts/'],
    capture_output=True, timeout=10
)
# Write immediately
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/f1.txt', 'w') as f:
    f.write('STAGED:\n')
    f.write(r1.stdout)
    f.write('\n')

# Commit
r2 = subprocess.run(
    ['git', '-C', repo, 'commit', '-m', 'feat: schema v5 数据口径标准化重构'],
    capture_output=True, timeout=60, stdin=subprocess.DEVNULL
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/f2.txt', 'w') as f:
    f.write(f'exit:{r2.returncode}\n')
    f.write(r2.stdout)
    f.write(r2.stderr[:200])

# Push
r3 = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, timeout=60, stdin=subprocess.DEVNULL
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/f3.txt', 'w') as f:
    f.write(f'exit:{r3.returncode}\n')
    f.write(r3.stdout)
    f.write(r3.stderr[:200])

# Log
r4 = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/f4.txt', 'w') as f:
    f.write(r4.stdout)
