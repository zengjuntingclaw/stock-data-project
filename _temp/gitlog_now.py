import subprocess

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-10'],
    capture_output=True, timeout=15
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/log_now.txt', 'w') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('STDERR:\n')
    f.write(r.stderr[:200])

r = subprocess.run(
    ['git', '-C', repo, 'status', '--porcelain'],
    capture_output=True, timeout=15
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/status_now.txt', 'w') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('STDERR:\n')
    f.write(r.stderr[:200])

print('done')
