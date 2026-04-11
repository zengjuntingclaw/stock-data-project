import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:2000])
    return r.stdout

# Check what's in HEAD for scripts/
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'ls-tree', 'HEAD', 'scripts/'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_a.txt')
print("scripts in HEAD:", out[:500])

# Check what's in HEAD for the whole tree
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'ls-tree', 'HEAD'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_b.txt')
print("HEAD tree:", out[:1000])

# Check working tree scripts
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'status', 'scripts/', '--porcelain'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_c.txt')
print("scripts status:", out[:500])

# Check full HEAD file list
out = run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', '--name-only', '--pretty=format:', 'HEAD'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/out_d.txt')
print("HEAD files:", out[:2000])

print('done')
