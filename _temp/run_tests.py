"""Run unittest"""
import subprocess, sys

result = subprocess.run(
    [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests'],
    cwd='C:/Users/zengj/.qclaw/workspace/stock_data_project',
    capture_output=True
)
stdout = result.stdout.decode('utf-8', errors='replace')
stderr = result.stderr.decode('utf-8', errors='replace')
print(stdout[-4000:])
if stderr:
    print("STDERR:", stderr[-2000:])
print("Return code:", result.returncode)
