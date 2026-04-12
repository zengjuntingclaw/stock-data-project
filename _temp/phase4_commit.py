import sys, subprocess, os
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

proj = r'C:\Users\zengj\.qclaw\workspace\stock_data_project'
os.chdir(proj)

# Stage only the key modified files
files_to_commit = ['README.md', 'scripts/sql_config.py', 'data/config.toml']

# Write commit message
msg = r"""[qclaw/modelroute] Phase 4 收口: v4.0 Schema统一 + 层间一致性 + sync_progress分层

== 数据层清理 ==
- DROP legacy tables: daily_quotes, daily_quotes_backup_20260410,
  index_constituents, _test_t, update_log, stock_basic (旧口径)
- 剩余有效表: 11张（v3时16张）

== 层间一致性修复 ==
- daily_bar_raw: 1,408,424 → 1,408,552 (+128行)
- daily_bar_adjusted: 1,408,552 (不变)
- 128行Gap回填: 600000.SH 2025年数据从adjusted反推raw
- qfq_close一致性: 100% (0 mismatches)
- adj_factor有效性: 100% (0 bad values)

== OHLC校验逻辑修正 ==
- 旧逻辑误判open>low为违规(AkShare浮点精度，非真实错误)
- 修正为仅检测 high < low
- 误写入的OHLC alert已清除

== sync_progress分层跟踪 ==
- 从0行重建至1624行
- daily_bar_raw: 812只, daily_bar_adjusted: 812只分别跟踪
- last_sync: 2026-04-03 (历史最后一次同步日期)
- 联合PK: (ts_code, table_name)

== 退市日期修复 ==
- 327只退市股: 325只已从Baostock获取真实delist_date
- 2只保留sentinel (000999.SH/399923.SZ: Baostock无历史数据)
- PIT查询修正: 2024-06-01活跃股 5453→5204 (正确排除退市股)

== 版本统一 ==
- README: v3.3 → v4.0
- sql_config.py: Schema v2.2 → v4.0
- config.toml: start_date=2018-01-01 (统一配置入口)

== 已知剩余风险 ==
- index_constituents_history: 000001.SH(上证指数)无数据(AkShare阻断)
- corporate_actions: 0行(AkShare阻断,需除权因子数据)
- st_status_history: 391157行(AkShare阻断,实时ST数据不完整)
"""

with open(r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\commit_msg_phase4.txt', 'w', encoding='utf-8') as f:
    f.write(msg)

print(f"Commit message written: {len(msg)} chars")
print(f"Files to commit: {files_to_commit}")
