import subprocess, sys, time
pids = [18636, 9580, 4696]
for pid in pids:
    r = subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True, text=True)
    print(f"PID {pid}: {r.stdout.strip() or 'not found'}")
print("Done")
