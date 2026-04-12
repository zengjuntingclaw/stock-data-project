# -*- coding: utf-8 -*-
"""验证 HEAD commit 中的 scripts/data_engine.py 内容"""
import subprocess
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

results = {}

def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    return r

# 1. 检查 HEAD commit 中的 data_engine.py
r = run("git show HEAD:scripts/data_engine.py")
key_lines = []
for line in r.stdout.splitlines():
    if any(k in line for k in ["DEFAULT_START_DATE", "STOCK_START_DATE", "get_all_stocks", "_get_local_stocks", "RuntimeError", "DROP TABLE IF EXISTS stock_basic", "qfq_close", "hfq_close"]):
        key_lines.append(f"  {line}[:120]")

with open("_temp/head_data_engine.txt", "w", encoding="utf-8") as f:
    f.write(r.stdout)

results["head_data_engine_key"] = key_lines
print(f"HEAD data_engine.py length: {len(r.stdout)}")
print(f"Key lines from HEAD:\n" + "\n".join(key_lines[:20]))

# 2. 检查 HEAD commit 中的 stock_history_schema.sql
r = run("git show HEAD:scripts/stock_history_schema.sql")
with open("_temp/head_schema.txt", "w", encoding="utf-8") as f:
    f.write(r.stdout)

schema_lines = []
for line in r.stdout.splitlines():
    if any(k in line for k in ["daily_bar_adjusted", "qfq_close", "hfq_close", "v2", "Schema v"]):
        schema_lines.append(f"  {line[:120]}")

results["head_schema_key"] = schema_lines
print(f"\nHEAD schema key lines:\n" + "\n".join(schema_lines[:20]))

# 3. 检查磁盘 vs HEAD 的 diff
r = run("git diff HEAD -- scripts/data_engine.py | head -80")
with open("_temp/disk_vs_head.txt", "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"\ngit diff HEAD scripts/data_engine.py (first 80 lines): {len(r.stdout)} chars")
if r.stdout.strip():
    print(r.stdout[:2000])
else:
    print("  (no differences - disk matches HEAD)")

# 4. 磁盘 data_engine.py 关键行
with open("scripts/data_engine.py", "r", encoding="utf-8") as f:
    disk_content = f.read()

disk_lines = []
for line in disk_content.splitlines():
    if any(k in line for k in ["DEFAULT_START_DATE", "STOCK_START_DATE", "get_all_stocks", "_get_local_stocks", "RuntimeError", "DROP TABLE IF EXISTS stock_basic", "qfq_close", "hfq_close"]):
        disk_lines.append(f"  {line[:120]}")

results["disk_key"] = disk_lines
print(f"\nDisk data_engine.py key lines:\n" + "\n".join(disk_lines[:20]))

# 5. 对比结论
print("\n" + "="*60)
print("对比结论:")
print("="*60)
head_set = set(key_lines)
disk_set = set(disk_lines)
print(f"HEAD 关键行数: {len(head_set)}")
print(f"Disk 关键行数: {len(disk_set)}")
print(f"相同: {len(head_set & disk_set)}")
print(f"仅 HEAD: {len(head_set - disk_set)}")
print(f"仅 Disk: {len(disk_set - head_set)}")
if head_set - disk_set:
    print(f"\n仅 HEAD 有 (意味着磁盘版本没有):")
    for l in sorted(head_set - disk_set):
        print(f"  {l}")
if disk_set - head_set:
    print(f"\n仅 Disk 有 (意味着 HEAD 版本没有):")
    for l in sorted(disk_set - head_set):
        print(f"  {l}")
