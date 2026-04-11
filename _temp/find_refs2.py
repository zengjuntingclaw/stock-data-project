import os, sys

base = r"C:\Users\zengj\.workbuddy\plugins\marketplaces\cb_teams_marketplace\plugins\finance-data\skills\finance-data-retrieval\references"

out = []

# List first level
items = os.listdir(base)
out.append("Top-level dirs: " + str(items[:20]))

# Find files with 'daily' in name
matches = []
for root, dirs, files in os.walk(base):
    for f in files:
        if 'daily' in f.lower():
            matches.append(os.path.relpath(os.path.join(root, f), base))

out.append("Daily files: " + str(matches[:10]))

# Check for 港股数据 directory
hk_path = os.path.join(base, "港股数据")
if os.path.exists(hk_path):
    out.append("港股Data files: " + str(os.listdir(hk_path)[:10]))

# Check for 股票数据 directory
stock_path = os.path.join(base, "股票数据")
if os.path.exists(stock_path):
    out.append("股票Data subdirs: " + str(os.listdir(stock_path)[:20]))

result = "\n".join(out)
with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\ref_result.txt", "w", encoding="utf-8") as f:
    f.write(result)
print(result)
