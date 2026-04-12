import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Try git add with explicit stderr capture
r = subprocess.run(
    ['git', '-C', repo, 'add', 'scripts/'],
    capture_output=True, env=env, timeout=60
)
# Write binary to avoid encoding issues
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/stderr.txt', 'wb') as f:
    f.write(b'exit: ' + str(r.returncode).encode() + b'\n')
    f.write(b'stdout_len: ' + str(len(r.stdout)).encode() + b'\n')
    f.write(b'stderr_len: ' + str(len(r.stderr)).encode() + b'\n')
    if r.stderr:
        f.write(b'stderr:\n')
        f.write(r.stderr[:500])
    if r.stdout:
        f.write(b'stdout:\n')
        f.write(r.stdout[:500])

print('stderr_done')
