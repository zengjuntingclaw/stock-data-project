import subprocess, os, time

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Check if scripts is in the index
r = subprocess.run(
    ['git', '-C', repo, 'ls-files', '-s'],
    capture_output=True, env=env, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/q1.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    lines = r.stdout.decode('utf-8', errors='replace').split('\n')
    f.write(f'lines:{len(lines)}\n')
    for l in lines[:10]:
        f.write(l + '\n')
print('q1 done')

# Check index.lock
lock = os.path.join(repo, '.git', 'index.lock')
if os.path.exists(lock):
    with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/q2.txt', 'w') as f:
        f.write('index.lock EXISTS\n')
    print('lock exists')
else:
    with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/q2.txt', 'w') as f:
        f.write('no lock\n')
    print('no lock')

# Check index file size
index = os.path.join(repo, '.git', 'index')
if os.path.exists(index):
    size = os.path.getsize(index)
    with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/q3.txt', 'w') as f:
        f.write(f'index size: {size}\n')
    print(f'index size: {size}')

print('quick_done')
