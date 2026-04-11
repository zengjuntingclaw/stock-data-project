import subprocess, sys, os

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

# 1. Stage files
r = subprocess.run(['git', '-C', repo, 'add', 'scripts/', 'docs/', 'tests/', 'main_v2_production.py', 'run_backtest.py', 'run_tests.py', 'pyproject.toml', 'requirements.txt', 'README.md', 'data_quality_report.json'], 
    capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_push.txt', 'w', encoding='utf-8') as f:
    f.write('=== ADD STDOUT ===\n')
    f.write(r.stdout)
    f.write('\n=== ADD STDERR ===\n')
    f.write(r.stderr[:500])

# 2. Status
r = subprocess.run(['git', '-C', repo, 'status', '--porcelain'], capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_push2.txt', 'w', encoding='utf-8') as f:
    f.write('=== STATUS STDOUT ===\n')
    f.write(r.stdout)
    f.write('\n=== STATUS STDERR ===\n')
    f.write(r.stderr[:500])

# 3. Commit
r = subprocess.run(['git', '-C', repo, 'commit', '-m', 
    'feat: 数据口径标准化重构 - PIT支持、原始/复权价格分离\n\n'
    '- 统一 ts_code 生成逻辑 (build_ts_code)\n'
    '- 新增 PIT 股票池查询 (get_active_stocks)\n'
    '- 新增指数成分股历史区间表 (get_index_constituents)\n'
    '- 分离原始价格表 (daily_bar_raw) 和复权价格表 (daily_bar_adjusted)\n'
    '- 新增 8 个 qfq/hfq 复权字段\n'
    '- 移除旧 stock_basic 表（DROP TABLE IF EXISTS）\n'
    '- DEFAULT_START_DATE 环境变量可配置\n'
    '- 10 项数据质量检查\n'
    '- ON CONFLICT DO UPDATE 断点续跑'],
    capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_push3.txt', 'w', encoding='utf-8') as f:
    f.write('=== COMMIT STDOUT ===\n')
    f.write(r.stdout)
    f.write('\n=== COMMIT STDERR ===\n')
    f.write(r.stderr[:500])

# 4. Push
r = subprocess.run(['git', '-C', repo, 'push', 'origin', 'master'], capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/git_push4.txt', 'w', encoding='utf-8') as f:
    f.write('=== PUSH STDOUT ===\n')
    f.write(r.stdout)
    f.write('\n=== PUSH STDERR ===\n')
    f.write(r.stderr[:500])

print('done')
