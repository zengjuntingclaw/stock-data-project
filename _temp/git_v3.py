import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env.pop('GIT_ASKPASS', None)
env.pop('GIT_SSH_COMMAND', None)
env['GIT_TERMINAL_PROMPT'] = '0'
env['GCM_DEBUG'] = ''

# Stage
r = subprocess.run(
    ['git', '-C', repo, 'add', 'scripts/'],
    capture_output=True, env=env, timeout=30
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v3_1.txt', 'w') as f:
    f.write(f'add exit:{r.returncode}\n')
    f.write(r.stdout[:200])
    f.write(r.stderr[:200])

# Commit
r = subprocess.run(
    ['git', '-C', repo, 'commit', '-m', 'feat: schema v5 数据口径标准化重构'],
    capture_output=True, env=env, timeout=120
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v3_2.txt', 'w') as f:
    f.write(f'commit exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])

# Push
r = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, env=env, timeout=120
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v3_3.txt', 'w') as f:
    f.write(f'push exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, env=env, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v3_4.txt', 'w') as f:
    f.write(r.stdout)

print('ALL_DONE_V3')
