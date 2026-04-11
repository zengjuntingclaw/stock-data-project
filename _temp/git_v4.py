import subprocess, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'
msg_file = 'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/msg.txt'
env = os.environ.copy()
env['GIT_TERMINAL_PROMPT'] = '0'

# Write commit message to file (ASCII only to avoid encoding issues)
with open(msg_file, 'w', encoding='utf-8') as f:
    f.write('feat: schema v5 data warehouse refactoring\n\n')
    f.write('- PIT stock universe (get_active_stocks)\n')
    f.write('- Index constituents history table\n')
    f.write('- raw/adjusted dual-layer price separation\n')
    f.write('- 8 qfq/hfq fields\n')
    f.write('- DROP stock_basic + env config\n')
    f.write('- 10 data quality checks\n')
    f.write('- UPSERT resumable sync\n')

# Stage
r = subprocess.run(
    ['git', '-C', repo, 'add', '.'],
    capture_output=True, env=env, timeout=30
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v4_1.txt', 'w') as f:
    f.write(f'add exit:{r.returncode}\n')
    f.write(r.stdout[:100])
    f.write(r.stderr[:100])

# Commit using file
r = subprocess.run(
    ['git', '-C', repo, 'commit', '--file=' + msg_file],
    capture_output=True, env=env, timeout=120
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v4_2.txt', 'w') as f:
    f.write(f'commit exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])

# Push
r = subprocess.run(
    ['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, env=env, timeout=120
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v4_3.txt', 'w') as f:
    f.write(f'push exit:{r.returncode}\n')
    f.write(r.stdout[:300])
    f.write(r.stderr[:300])

# Log
r = subprocess.run(
    ['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, env=env, timeout=10
)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/v4_4.txt', 'w') as f:
    f.write(r.stdout)

print('V4_DONE')
