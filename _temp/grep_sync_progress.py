import os, re

proj = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\scripts'
results = {}
for fname in os.listdir(proj):
    if not fname.endswith('.py'):
        continue
    fpath = os.path.join(proj, fname)
    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    matches = [(i+1, line) for i, line in enumerate(content.split('\n')) if 'sync_progress' in line]
    if matches:
        results[fname] = matches

if results:
    for fname, matches in results.items():
        print(f"\n=== {fname} ===")
        for lineno, line in matches:
            print(f"  {lineno}: {line.strip()}")
else:
    print("No sync_progress references found in scripts/")
