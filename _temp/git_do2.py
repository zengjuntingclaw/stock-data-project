import subprocess

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:2000])

run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'status', '--porcelain'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/gitlog.txt')
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'diff', '--stat', 'HEAD'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/gitlog2.txt')
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'diff', '--stat'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/gitlog')
print('done')
