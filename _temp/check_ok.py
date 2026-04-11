import subprocess

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Check if scripts are staged
r = subprocess.run(
    ['git', '-C', repo, 'ls-files', '--stage', 'scripts/'],
    capture_output=True, timeout=30
)
print('ls-files exit:', r.returncode)
print('stdout len:', len(r.stdout))
print('first 500:', r.stdout[:500])
print('stderr:', r.stderr[:200])

# Check what's in HEAD
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, timeout=15
)
print('log exit:', r.returncode)
print('log:', r.stdout)

with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/check_ok.txt', 'w') as f:
    f.write(f'ls-files exit: {r.returncode}\n')
    f.write(f'stdout len: {len(r.stdout)}\n')
    f.write(r.stdout[:500])
    f.write(f'\nlog exit: {r.returncode}\n')
    f.write(r.stdout)

print('done')
