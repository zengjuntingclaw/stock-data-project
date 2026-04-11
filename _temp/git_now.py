import subprocess, time

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Run git commit directly
r = subprocess.run(
    ['git', '-C', repo, 'commit', '-m', 
     'feat: schema v5 数据口径标准化重构'],
    capture_output=True, timeout=60
)
print('commit exit:', r.returncode)
print('commit stdout:', r.stdout[:200])
print('commit stderr:', r.stderr[:200])

with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/cresult.txt', 'w') as f:
    f.write(r.stdout)
    f.write(r.stderr[:500])

# Run git push
r = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, timeout=60
)
print('push exit:', r.returncode)
print('push stdout:', r.stdout[:200])
print('push stderr:', r.stderr[:200])

with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/presult.txt', 'w') as f:
    f.write(r.stdout)
    f.write(r.stderr[:500])

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, timeout=10
)
print('log:', r.stdout)

print('DONE')
