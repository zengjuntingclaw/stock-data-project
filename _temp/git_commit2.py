import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Stage files
files = ['scripts/data_engine.py']
r = subprocess.run(
    ['git', '-C', repo, 'add'] + files,
    capture_output=True, env=env
)
print('add exit:', r.returncode)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r_a.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')

# Commit
r = subprocess.run(
    ['git', '-C', repo, 'commit', '-m', 'feat: schema v5 数据口径标准化重构'],
    capture_output=True, env=env
)
print('commit exit:', r.returncode, r.stdout[:100], r.stderr[:100])
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r_b.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:300])

# Push
r = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, env=env
)
print('push exit:', r.returncode, r.stdout[:100], r.stderr[:100])
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r_c.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:300])

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, env=env
)
print('log:', r.stdout)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r_d.txt', 'w') as f:
    f.write(r.stdout)

print('DONE')
