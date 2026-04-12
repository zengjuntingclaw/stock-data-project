# -*- coding: utf-8 -*-
"""直接提取 HEAD 中的关键代码"""
import subprocess
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

# 写 HEAD 中的 data_engine.py 到文件
r = subprocess.run(
    "git show HEAD:scripts/data_engine.py",
    capture_output=True, shell=True, cwd=workspace
)
out_file = os.path.join(workspace, "_temp", "head_data_engine.py")
with open(out_file, "wb") as f:
    f.write(r.stdout)
print(f"HEAD data_engine.py written: {len(r.stdout)} bytes")

# 写 HEAD 中的 stock_history_schema.sql
r = subprocess.run(
    "git show HEAD:scripts/stock_history_schema.sql",
    capture_output=True, shell=True, cwd=workspace
)
out_file2 = os.path.join(workspace, "_temp", "head_schema.sql")
with open(out_file2, "wb") as f:
    f.write(r.stdout)
print(f"HEAD schema.sql written: {len(r.stdout)} bytes")

# 写磁盘版本
with open("scripts/data_engine.py", "rb") as f:
    disk_content = f.read()
disk_file = os.path.join(workspace, "_temp", "disk_data_engine.py")
with open(disk_file, "wb") as f:
    f.write(disk_content)
print(f"Disk data_engine.py written: {len(disk_content)} bytes")

# 写磁盘 schema
with open("scripts/stock_history_schema.sql", "rb") as f:
    disk_schema = f.read()
disk_schema_file = os.path.join(workspace, "_temp", "disk_schema.sql")
with open(disk_schema_file, "wb") as f:
    f.write(disk_schema)
print(f"Disk schema.sql written: {len(disk_schema)} bytes")

print("\n所有文件已写入 _temp/")
