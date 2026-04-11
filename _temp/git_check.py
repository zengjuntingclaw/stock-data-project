import subprocess, sys

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write(r.stdout)
        if r.stderr:
            f.write('\n=== STDERR ===\n')
            f.write(r.stderr)

run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'log', '--oneline', '-5'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_0.txt')
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', 'HEAD:scripts/data_engine.py'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_1.txt')
run(['git', '-C', 'c:/Users/zengj/.qclaw/workspace/stock_data_project', 'show', 'HEAD:scripts/stock_history_schema.sql'],
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_out_2.txt')
print('done')
