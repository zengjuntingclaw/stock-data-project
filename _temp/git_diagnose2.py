# -*- coding: utf-8 -*-
"""深度诊断 git scripts/ 问题"""
import subprocess
import os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

def run(cmd, label=""):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    return r

# 关键诊断
r = run("git status", "git status (full)")
status_file = os.path.join(workspace, "_temp", "git_status_full.txt")
with open(status_file, "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"git status full output written to {status_file}, length: {len(r.stdout)}")

r = run("git status -uall", "git status -uall (all untracked)")
status_all_file = os.path.join(workspace, "_temp", "git_status_uall.txt")
with open(status_all_file, "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"git status -uall written to {status_all_file}, length: {len(r.stdout)}")

# 检查 scripts/ 是否在忽略列表
r = run("git ls-files --others --ignored --exclude-standard -- scripts/", "git ls-files --others --ignored -- scripts/")
ignored_file = os.path.join(workspace, "_temp", "git_ignored_scripts.txt")
with open(ignored_file, "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"scripts/ ignored output: '{r.stdout}'")

# 检查 git ls-files 对 scripts/ 的输出
r = run("git ls-files scripts/", "git ls-files scripts/")
ls_file = os.path.join(workspace, "_temp", "git_ls_scripts.txt")
with open(ls_file, "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"git ls-files scripts/: '{r.stdout}'")

# 检查 git config
r = run("git config --list", "git config --list")
config_file = os.path.join(workspace, "_temp", "git_config.txt")
with open(config_file, "w", encoding="utf-8") as f:
    f.write(r.stdout)
print(f"git config written, lines: {len(r.stdout.splitlines())}")

# 检查 sparse-checkout
r = run("git sparse-checkout list", "git sparse-checkout list")
print(f"sparse-checkout: '{r.stdout}'")

# 检查 core.excludesFile
r = run("git config core.excludesFile", "core.excludesFile")
print(f"core.excludesFile: '{r.stdout.strip()}'")

# 读取全局 excludesFile
excludes_path = r.stdout.strip()
if excludes_path and os.path.exists(excludes_path):
    with open(excludes_path, "r", encoding="utf-8") as f:
        excludes_content = f.read()
    ef_file = os.path.join(workspace, "_temp", "global_gitignore.txt")
    with open(ef_file, "w", encoding="utf-8") as f:
        f.write(excludes_content)
    print(f"global gitignore written: {ef_file}")
    if "scripts" in excludes_content.lower():
        print("  !!! 'scripts' found in global gitignore !!!")
