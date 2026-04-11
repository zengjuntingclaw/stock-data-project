import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:2000])

# Check what's in HEAD:scripts/
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'ls-tree', 'HEAD', 'scripts/'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_3.txt')

# Check HEAD version of data_engine.py
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', 'HEAD:scripts/data_engine.py'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_4.txt')

# Show full git log
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'log', '--oneline', '--all', '-20'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_5.txt')

# Show branches
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'branch', '-a', '-v'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_6.txt')

print('done')
