import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Try git add -A to add everything
r = subprocess.run(
    ['git', '-C', repo, 'add', '-A'],
    capture_output=True, timeout=120
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/diag2_1.txt', 'wb') as f:
    f.write(b'exit: ' + str(r.returncode).encode() + b'\n')
    f.write(b'stdout_len: ' + str(len(r.stdout)).encode() + b'\n')
    f.write(b'stderr_len: ' + str(len(r.stderr)).encode() + b'\n')
    if r.stdout: f.write(b'stdout:\n' + r.stdout[:200] + b'\n')
    if r.stderr: f.write(b'stderr:\n' + r.stderr[:500] + b'\n')

# Now check diff --cached
r = subprocess.run(
    ['git', '-C', repo, 'diff', '--cached', '--stat'],
    capture_output=True, timeout=30
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/diag2_2.txt', 'wb') as f:
    f.write(b'exit: ' + str(r.returncode).encode() + b'\n')
    f.write(b'stdout_len: ' + str(len(r.stdout)).encode() + b'\n')
    f.write(b'stderr_len: ' + str(len(r.stderr)).encode() + b'\n')
    if r.stdout: f.write(b'stdout:\n' + r.stdout[:500] + b'\n')
    if r.stderr: f.write(b'stderr:\n' + r.stderr[:200] + b'\n')

print('diag2_done')
