import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Test: add ONE specific file
test_file = 'scripts/data_engine.py'
r = subprocess.run(
    ['git', '-C', repo, 'add', test_file],
    capture_output=True, stdin=subprocess.DEVNULL
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/debug1.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(f'stdout:{r.stdout}\n')
    f.write(f'stderr:{r.stderr[:200]}\n')

# Verify
r = subprocess.run(
    ['git', '-C', repo, 'diff', '--cached', '--stat'],
    capture_output=True, stdin=subprocess.DEVNULL, timeout=15
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/debug2.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-3'],
    capture_output=True, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/debug3.txt', 'w') as f:
    f.write(r.stdout)
