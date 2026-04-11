"""Replace daily_quotes with daily_bar_adjusted in data_engine.py"""
import re

path = 'C:/Users/zengj/.qclaw/workspace/stock_data_project/scripts/data_engine.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

before = len(re.findall(r'daily_quotes', content))
after = content.replace('daily_quotes', 'daily_bar_adjusted')
after_count = len(re.findall(r'daily_quotes', after))

print(f'Before: {before}, After remaining: {after_count}')

# Show changed lines
lines_before = content.split('\n')
lines_after = after.split('\n')
changed = [(i+1, la, lb) for i,(la,lb) in enumerate(zip(lines_before, lines_after)) if la!=lb]
print(f'Changed lines: {len(changed)}')
for ln, old, new in changed[:5]:
    print(f'  Line {ln}: REMOVED [{old[:80]}]')
    print(f'         ADDED  [{new[:80]}]')

with open(path, 'w', encoding='utf-8') as f:
    f.write(after)

print('File written successfully')

# Verify
with open(path, 'r', encoding='utf-8') as f:
    verify = f.read()
remaining = len(re.findall(r'daily_quotes', verify))
new_count = len(re.findall(r'daily_bar_adjusted', verify))
print(f'Verification: daily_quotes remaining={remaining}, daily_bar_adjusted count={new_count}')
