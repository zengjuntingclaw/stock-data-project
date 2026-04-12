import sys, re
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')

# Update README version
readme_path = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\README.md'
with open(readme_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find version lines
for m in re.finditer(r'v\d+\.\d+|Schema\s+\d+\.\d+', content):
    print(f"Found at {m.start()}: {repr(m.group())}")

# Replace old version strings
updated = re.sub(r'v3\.3\s*\(Schema\s+v?\d+\.\d+\)', 'v4.0 (Schema v4.0)', content)
updated = re.sub(r'v3\.0\s*\(Schema\s+v?\d+\.\d+\)', 'v4.0 (Schema v4.0)', updated)
if updated != content:
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(updated)
    print("README.md: version updated to v4.0")
else:
    print("README.md: version already up to date or pattern not found")
    # Find actual version line
    for i, line in enumerate(content.split('\n')[:20]):
        if 'v3' in line or 'v4' in line or 'Version' in line or '版本' in line:
            print(f"  Line {i+1}: {repr(line[:80])}")
