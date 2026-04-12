import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Run ALL git operations in a single bash command
script = f'''
cd "{repo}" && git add scripts/ && git commit -m "feat: schema v5 data warehouse refactoring" && git push origin master && git log --oneline -3
'''

r = subprocess.run(
    ['bash', '-c', script] if os.name != 'nt' else 
    ['cmd', '/c', f'cd /d "{repo}" && git add scripts/ && git commit -m "feat: schema v5 data warehouse refactoring" && git push origin master && git log --oneline -3'],
    capture_output=True, env=env, timeout=180
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/all_git.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])
print('ALL_GIT_DONE')
