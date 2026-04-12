import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Try using shell=True to run git through cmd.exe
cmd_add = f'git -C "{repo}" add scripts/'
r = subprocess.run(cmd_add, shell=True, capture_output=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/s1.txt', 'w') as f:
    f.write(f'add exit:{r.returncode}\n')
    f.write(r.stdout[:100])
    f.write(r.stderr[:200])
print('add done')

# Commit
msg = 'feat: schema v5 data warehouse refactoring'
cmd_commit = f'git -C "{repo}" commit -m "{msg}"'
r = subprocess.run(cmd_commit, shell=True, capture_output=True, timeout=60)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/s2.txt', 'w') as f:
    f.write(f'commit exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])
print('commit done')

# Push
cmd_push = f'git -C "{repo}" push origin master'
r = subprocess.run(cmd_push, shell=True, capture_output=True, timeout=60)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/s3.txt', 'w') as f:
    f.write(f'push exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])
print('push done')

# Log
r = subprocess.run(f'git -C "{repo}" log --oneline -5', shell=True, capture_output=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/s4.txt', 'w') as f:
    f.write(r.stdout)
print('log done')

print('SHELL_DONE')
