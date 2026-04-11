import subprocess

repo = 'c:/Users/zengj/.qclaw/workspace/stock_data_project'

def run(args, outfile):
    r = subprocess.run(args, capture_output=True)
    with open(outfile, 'w', encoding='utf-8') as f:
        f.write('STDOUT:\n')
        f.write(r.stdout)
        f.write('\nSTDERR:\n')
        f.write(r.stderr[:1000])
    return r.stdout

# 1. Stage all key files
files = [
    'scripts/data_engine.py',
    'scripts/exchange_mapping.py',
    'scripts/stock_history_schema.sql',
    'scripts/security_master.py',
    'scripts/data_validator.py',
    'scripts/data_qa_pipeline.py',
    'scripts/survivorship_bias.py',
    'main_v2_production.py',
    'run_backtest.py',
    'run_tests.py',
    'pyproject.toml',
    'requirements.txt',
    'README.md',
]

# Also check what else is in scripts/
import os
scripts_dir = os.path.join(repo, 'scripts')
all_script_files = []
for f in os.listdir(scripts_dir):
    fp = os.path.join(scripts_dir, f)
    if os.path.isfile(fp) and not f.startswith('__pycache__'):
        all_script_files.append(f'scripts/{f}')

print("Scripts files:", all_script_files)

# Also check tests, docs
for d in ['tests', 'docs']:
    dd = os.path.join(repo, d)
    if os.path.exists(dd):
        for f in os.listdir(dd):
            fp = os.path.join(dd, f)
            if os.path.isfile(fp):
                all_script_files.append(f'{d}/{f}')

print("All files to add:", all_script_files)

run(['git', '-C', repo, 'add'] + all_script_files,
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r1.txt')

# Check staged
out = run(['git', '-C', repo, 'status', '--porcelain'], 
    'c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r2.txt')
print("Status after add:")
print(out)

# Commit
commit_msg = '''feat: 数据口径标准化重构 - PIT支持、原始/复权价格分离

变更范围:
- 统一 ts_code 生成逻辑 (build_ts_code)
- 新增 PIT 股票池查询 (get_active_stocks) - 历史 eff_date 过滤
- 新增指数成分股历史区间表 (get_index_constituents)
- 分离原始价格表 (daily_bar_raw) 和复权价格表 (daily_bar_adjusted)
- daily_bar_adjusted 含 8 个 qfq/hfq 复权字段
- 移除旧 stock_basic 表 (DROP TABLE IF EXISTS)
- DEFAULT_START_DATE 环境变量可配置 (STOCK_START_DATE)
- 10 项数据质量检查
- ON CONFLICT DO UPDATE 断点续跑
- 主流程 SurvivorshipBiasHandler 已接入新数据层
'''
r = subprocess.run(['git', '-C', repo, 'commit', '-m', commit_msg], capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r3.txt', 'w', encoding='utf-8') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:1000])
print("Commit result:", r.stdout[:200], r.stderr[:200])

# Push
r = subprocess.run(['git', '-C', repo, 'push', 'origin', 'master'], capture_output=True, text=True)
with open('c:/Users/zengj/.qclaw/workspace/stock_data_project/_temp/r4.txt', 'w', encoding='utf-8') as f:
    f.write('STDOUT:\n')
    f.write(r.stdout)
    f.write('\nSTDERR:\n')
    f.write(r.stderr[:1000])
print("Push result:", r.stdout[:200], r.stderr[:200])

# Verify
r = subprocess.run(['git', '-C', repo, 'log', '--oneline', '-3'], capture_output=True, text=True)
print("Final log:", r.stdout)

print('done')
