with open(r'C:\Users\zengj\.qclaw\workspace\stock_data_project\README.md', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines, 1):
    if any(k in line for k in ['v4.0', 'v3.3', 'Schema v', '数据规模', '数据库状态', 'Phase 5', 'is_st', '增量同步']):
        print(f"{i}: {line.rstrip()}")
