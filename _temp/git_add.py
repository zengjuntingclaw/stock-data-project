import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:3000])
    return r.stdout

# Stage and check status
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'add', 'scripts/', 'docs/', 'tests/', 'main_v2_production.py', 'run_backtest.py', 'run_tests.py', 'pyproject.toml', 'requirements.txt', 'README.md'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_add.txt')
print("Add result:", out[:200])

# Check status after add
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'status', '--porcelain'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_status.txt')
print("Status:", out[:1000])

print('done')
