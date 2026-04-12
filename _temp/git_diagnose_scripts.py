# -*- coding: utf-8 -*-
"""诊断 git 对 scripts/ 目录的处理"""
import subprocess
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

results = []

def run(cmd, label):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    results.append(f"\n=== {label} ===")
    results.append(f"cmd: {cmd}")
    results.append(f"exit: {r.returncode}")
    results.append(f"stdout ({len(r.stdout)} chars): {r.stdout[:300]}")
    results.append(f"stderr ({len(r.stderr)} chars): {r.stderr[:300]}")
    return r

# 1. 检查 git status -s 对 scripts/ 的显示
run("git status -s", "git status -s (short)")

# 2. 检查 scripts/ 是否在 .gitignore
run("git check-ignore -v scripts/", "git check-ignore scripts/")

# 3. 尝试 git add 显式文件
run("git add scripts/data_engine.py", "git add scripts/data_engine.py (explicit)")

# 4. 检查 git add 后的暂存区
run("git diff --cached --name-only", "git diff --cached --name-only")

# 5. 检查 scripts/ 下有多少文件
if os.path.exists("scripts"):
    py_files = [f for f in os.listdir("scripts") if f.endswith(".py")]
    results.append(f"\n=== scripts/ 文件列表 ===")
    results.append(f"共 {len(py_files)} 个 .py 文件: {py_files[:5]}")
else:
    results.append("\n=== scripts/ 目录不存在 ===")

# 6. 检查 git status --porcelain -u 强制显示未跟踪
run("git status --porcelain -u", "git status --porcelain -u (强制显示未跟踪)")

# 7. 检查 GIT_DIR 环境
run("echo $GIT_DIR", "GIT_DIR 环境变量")

# 8. 尝试 git add 绝对路径
abs_scripts = os.path.abspath("scripts")
run(f"git add {abs_scripts}", f"git add 绝对路径 {abs_scripts}")

for r in results:
    print(r)
