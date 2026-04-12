# -*- coding: utf-8 -*-
"""找出磁盘比 HEAD 多出的内容"""
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"

with open(os.path.join(workspace, "_temp", "head_data_engine.py"), "rb") as f:
    head = f.read()
with open(os.path.join(workspace, "_temp", "disk_data_engine.py"), "rb") as f:
    disk = f.read()

# 找出不同之处（用字节级别diff）
import difflib
head_lines = head.decode("utf-8", errors="replace").splitlines()
disk_lines = disk.decode("utf-8", errors="replace").splitlines()

# 找head中缺少的行（在disk中存在但head不存在）
head_set = set()
for line in head_lines:
    if line.strip():
        head_set.add(line.strip())

extra_in_disk = []
for i, line in enumerate(disk_lines):
    stripped = line.strip()
    if stripped and stripped not in head_set:
        extra_in_disk.append((i+1, line[:100]))

print(f"HEAD lines: {len(head_lines)}, Disk lines: {len(disk_lines)}")
print(f"Extra lines in disk: {len(extra_in_disk)}")
print("\n磁盘中独有内容（前30条）:")
for lineno, line in extra_in_disk[:30]:
    print(f"  L{lineno}: {line}")

# 用 difflib 找 block differences
differ = difflib.unified_diff(head_lines, disk_lines, lineterm="", n=0)
diff_lines = []
count = 0
for line in differ:
    diff_lines.append(line)
    count += 1
    if count > 50:
        break

print(f"\nunified_diff (前50行):")
for line in diff_lines[:50]:
    print(f"  {line[:120]}")
