import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts')
p = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\README.md'
with open(p, 'r', encoding='utf-8') as f:
    content = f.read()
# Write to a GBK-safe output file
out = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\readme_out.txt'
with open(out, 'w', encoding='utf-8') as f:
    lines = content.split('\n')
    for i, line in enumerate(lines[:80]):
        f.write(f"{i+1:3d}: {line}\n")
print(f"Written {len(lines[:80])} lines to {out}")
# Print just key version lines
for i, line in enumerate(lines[:80]):
    if any(k in line for k in ['v3', 'Schema', 'schema', 'v4', 'VERSION', '版本']):
        print(f"  [{i+1}] {line}")
