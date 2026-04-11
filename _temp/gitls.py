import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:2000])
    return r.stdout

# Check what's in HEAD
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', '--name-only', '--pretty=format:', 'HEAD'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_git_files.txt')

# Check gitignore
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'check-ignore', '-v', 'scripts/data_engine.py'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_gitignore.txt')

# Full repo tree
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'ls-tree', '-r', '--name-only', 'HEAD'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_git_tree.txt')

print('done')
