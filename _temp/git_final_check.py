# -*- coding: utf-8 -*-
"""最终验证：scripts/ 是否在 HEAD"""
import subprocess
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    return r

# 1. git log --oneline -10
r = run("git log --oneline -10")
print(f"=== git log --oneline -10 ===\n{r.stdout}")

# 2. git ls-tree HEAD scripts/
r = run("git ls-tree HEAD scripts/")
print(f"=== git ls-tree HEAD scripts/ ===\nstdout: '{r.stdout}'")

# 3. git ls-tree HEAD
r = run("git ls-tree HEAD")
print(f"=== git ls-tree HEAD ===\n{chr(10).join(r.stdout.splitlines()[:10])}")

# 4. git diff --cached -- scripts/
r = run("git diff --cached -- scripts/data_engine.py")
with open("_temp/staged_scripts_diff.txt", "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"\n=== git diff --cached scripts/data_engine.py ===")
print(f"length: {len(r.stdout)} chars")
if r.stdout.strip():
    # 显示关键行
    lines = r.stdout.splitlines()
    for line in lines:
        if any(k in line for k in ["DEFAULT_START", "STOCK_START", "get_all_stocks", "RuntimeError", "DROP TABLE", "qfq_close", "hfq_close", "+", "-"]):
            print(f"  {line[:150]}")
    print(f"\n  ... 共 {len(lines)} 行")
else:
    print("  (empty - 暂存区内容与工作区相同)")

# 5. git diff -- scripts/ (未暂存的修改)
r = run("git diff -- scripts/data_engine.py | head -50")
print(f"\n=== git diff -- scripts/data_engine.py (未暂存) ===")
print(f"length: {len(r.stdout)} chars")
if r.stdout.strip():
    print("磁盘有修改但未暂存")
else:
    print("磁盘与暂存区完全匹配")

# 6. staged files 总览
r = run("git diff --cached --name-only")
print(f"\n=== 暂存区文件列表 ===")
staged = [f for f in r.stdout.splitlines() if f.strip()]
print(f"共 {len(staged)} 个文件:")
for f in staged:
    print(f"  {f}")

# 7. 关键文件内容对比
print("\n=== 验证暂存区 vs HEAD ===")
r = run("git show HEAD:scripts/data_engine.py 2>&1")
if r.returncode != 0 or not r.stdout.strip():
    print("HEAD 中没有 scripts/data_engine.py — 确认是新文件，从未提交")
else:
    print(f"HEAD 中有 data_engine.py，长度 {len(r.stdout)}")
