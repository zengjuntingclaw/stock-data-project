# -*- coding: utf-8 -*-
"""清理 git 暂存区，只保留验证结果"""
import subprocess, os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    return r

# 1. Unstage all _temp/ files
r = run("git reset HEAD _temp/")
print(f"git reset HEAD _temp/: exit={r.returncode}, stdout={r.stdout[:200]}, stderr={r.stderr[:200]}")

# 2. Check what's left staged
r = run("git diff --cached --name-only")
print(f"\nAfter reset, staged files ({len(r.stdout.splitlines())}):")
for f in r.stdout.splitlines():
    print(f"  {f}")

# 3. Add only the verification result
r2 = run("git add _temp/verify_v5_result.txt _temp/verify_v5.py")
print(f"\ngit add verify files: exit={r2.returncode}")

# 4. Check staged again
r = run("git diff --cached --name-only")
print(f"\nNow staged files ({len(r.stdout.splitlines())}):")
for f in r.stdout.splitlines():
    print(f"  {f}")

# 5. Commit
r = run("git commit -m 'test: add schema v2.2 verification artifacts'")
print(f"\ngit commit: exit={r.returncode}")
print(f"stdout: {r.stdout[:500]}")
print(f"stderr: {r.stderr[:200]}")
