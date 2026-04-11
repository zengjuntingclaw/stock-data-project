import os, sys

base = r"C:\Users\zengj\.workbuddy\plugins\marketplaces\cb_teams_marketplace\plugins\finance-data\skills\finance-data-retrieval\references"

# List first level
items = os.listdir(base)
print("Top-level dirs:", items[:20])

# Find files with 'daily' in name
matches = []
for root, dirs, files in os.walk(base):
    for f in files:
        if 'daily' in f.lower():
            matches.append(os.path.join(root, f))

print("Daily files:", matches[:10])

# Check for 港股数据 directory
hk_path = os.path.join(base, "港股数据")
if os.path.exists(hk_path):
    print("港股Data files:", os.listdir(hk_path)[:10])
