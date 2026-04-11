import subprocess

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# Check what's staged
r = subprocess.run(['git', '-C', repo, 'diff', '--cached', '--stat'],
    capture_output=True, timeout=30)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/check_staged.txt', 'w') as f:
    f.write(r.stdout)
    f.write(r.stderr[:200])

# Commit what's staged
commit_msg = 'feat: schema v5 数据口径标准化重构\n\n- PIT 股票池 (get_active_stocks)\n- 指数成分历史区间表\n- raw/adjusted 双层行情分离\n- 8 qfq/hfq 复权字段\n- DROP stock_basic + 环境变量配置\n- 10 项数据质量检查\n- 断点续跑 UPSERT\n'
r = subprocess.run(['git', '-C', repo, 'commit', '-m', commit_msg],
    capture_output=True, timeout=30)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/commit_result.txt', 'w') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])

# Push
r = subprocess.run(['git', '-C', repo, 'push', 'origin', 'master'],
    capture_output=True, timeout=30)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/push_result.txt', 'w') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:500])

# Log
r = subprocess.run(['git', '-C', repo, 'log', '--oneline', '-5'],
    capture_output=True, timeout=10)
print('LOG:', r.stdout)

print('DONE')
