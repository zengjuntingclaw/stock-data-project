import os

base = r"C:\Users\zengj\.workbuddy\plugins\marketplaces\cb_teams_marketplace\plugins\finance-data\skills\finance-data-retrieval\references"

# List all md files in 行情数据
hk_dir = os.path.join(base, "港股数据")
a_dir = os.path.join(base, "股票数据", "行情数据")

out = []
out.append("港股Data files: " + str(os.listdir(hk_dir)))
out.append("A股行情 files: " + str(os.listdir(a_dir)))

# Read rt_k doc
rt_k = os.path.join(base, "股票数据", "行情数据", "rt_k.md")
if os.path.exists(rt_k):
    with open(rt_k, encoding="utf-8") as f:
        out.append("\n=== rt_k.md ===\n" + f.read()[:2000])

# Read hk_daily doc
hk_daily = os.path.join(base, "港股数据", "港股日线行情.md")
if os.path.exists(hk_daily):
    with open(hk_daily, encoding="utf-8") as f:
        out.append("\n=== 港股日线行情.md ===\n" + f.read()[:2000])

result = "\n".join(out)
with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\ref_result2.txt", "w", encoding="utf-8") as f:
    f.write(result)
print(result)
