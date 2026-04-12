import subprocess

r = subprocess.run(
    ['c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_shell_script.bat'],
    capture_output=True, timeout=180,
    cwd='c:/Users/zengj/.qclaw/workspace/stock_data_project'
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/bat_result.txt', 'w') as f:
    f.write(f'exit:{r.returncode}\n')
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])
print('BAT_DONE')
