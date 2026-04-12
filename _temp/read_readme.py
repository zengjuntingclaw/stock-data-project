import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
p = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\README.md'
with open(p, 'r', encoding='utf-8') as f:
    content = f.read()
lines = content.split('\n')
for i, line in enumerate(lines[:80]):
    print(f"{i+1:3d}: {line}")
