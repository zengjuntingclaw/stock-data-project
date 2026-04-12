# -*- coding: utf-8 -*-
"""Git commit + push"""
import subprocess, os

workspace = r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
os.chdir(workspace)

def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=workspace)
    return r

# Add files
r = run('git add _temp/verify_v5.py _temp/verify_v5_result.txt')
print(f"git add: exit={r.returncode}")

# Commit with message from file
msg_file = os.path.join(workspace, "_temp", "commit_msg.txt")
with open(msg_file, "w", encoding="utf-8") as f:
    f.write("test: add schema v2.2 runtime verification\n\n15/15 tests PASSED - Schema v2.2 fully verified:\n- DEFAULT_START_DATE env var configurable\n- DROP old stock_basic table\n- daily_bar_adjusted qfq/hfq 8 fields\n- get_active_stocks PIT query (5453 stocks)\n- get_index_constituents (300 for 000300)\n- raw/adjusted price separation (18 vs 25 fields)\n- SurvivorshipBiasHandler pipeline connected\n- sync_progress resumable\n- data quality checks (3 methods)\n- get_all_stocks raises RuntimeError (no fallback)\n")

r = run(f'git commit --file="{msg_file}"')
print(f"git commit: exit={r.returncode}")
print(f"stdout: {r.stdout[:300]}")
print(f"stderr: {r.stderr[:200]}")

# Push
r = run("git push")
print(f"\ngit push: exit={r.returncode}")
print(f"stdout: {r.stdout[:300]}")
print(f"stderr: {r.stderr[:200]}")
