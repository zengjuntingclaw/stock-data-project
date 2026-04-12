import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Check if scripts/ is in .gitignore
r = subprocess.run(
    ['git', '-C', repo, 'check-ignore', '-v', 'scripts/data_engine.py'],
    capture_output=True, env=env, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/d1.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:200])

# Check gitignore file content
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/.gitignore') as f:
    content = f.read()
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/d2.txt', 'w') as f:
    f.write(content)

# Check if scripts/ exists on disk
scripts_path = 'c:/Users/zengj/.qclaw/workspace/stock_data_project/scripts'
if os.path.exists(scripts_path):
    files = os.listdir(scripts_path)
    with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/d3.txt', 'w') as f:
        f.write(f'exists, {len(files)} files\n')
        for fn in files[:5]:
            f.write(f'  {fn}\n')
else:
    with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/d3.txt', 'w') as f:
        f.write('NOT FOUND\n')

# Try git add with -v (verbose) to see what's happening
r = subprocess.run(
    ['git', '-C', repo, 'add', '-v', 'scripts/'],
    capture_output=True, env=env, timeout=30
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/d4.txt', 'w') as f:
    f.write(f'add -v exit:{r.returncode}\n')
    f.write(r.stdout)
    f.write(r.stderr[:200])

print('diag_done')
