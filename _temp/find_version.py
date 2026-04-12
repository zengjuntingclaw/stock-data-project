import sys, re
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
p = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\scripts\sql_config.py'
with open(p, 'r', encoding='utf-8') as f:
    content = f.read()
# Find version comments
for m in re.finditer(r'(Schema\s+[vv]?\d+[.:]\d+|v\d+[.:]\d+)', content):
    print(f"  pos {m.start()}: {repr(m.group()[:50])}")
# Also find the __version__ or version at top
for line in content.split('\n')[:10]:
    if 'version' in line.lower() or 'Version' in line:
        print(f"  Top: {line}")
