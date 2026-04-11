import subprocess, os
r = subprocess.run(['git', 'log', '--oneline', '-5'], capture_output=True, text=True)
with open('_temp/gitlog_check.txt', 'w') as f: f.write(r.stdout)
r2 = subprocess.run(['git', 'status', '--short'], capture_output=True, text=True)
with open('_temp/gitstat_check.txt', 'w') as f: f.write(r2.stdout)
