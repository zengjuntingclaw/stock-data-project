import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('=== STDOUT ===\n')
        f.write(r.stdout)
        f.write('\n=== STDERR ===\n')
        f.write(r.stderr[:3000])

run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'status', '--porcelain'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_status2.txt')
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'diff', '--stat', 'HEAD', '--', 'scripts/'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_diff.txt')
print('done')
