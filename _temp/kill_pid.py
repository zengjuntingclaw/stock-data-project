import subprocess, time
for pid in [4696, 4432, 2624]:
    subprocess.run(['taskkill', '/F', '/PID', str(pid)],
        capture_output=True, text=True)
    print(f'Killed {pid}')
print('Done')
