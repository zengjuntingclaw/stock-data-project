import subprocess, sys, json, os, tempfile

# Write a runner script
runner = r"""
import subprocess, sys, json, os

script = r"C:\Users\zengj\.workbuddy\plugins\marketplaces\cb_teams_marketplace\plugins\finance-data\skills\neodata-financial-search\scripts\query.py"
query_text = "腾讯和贵州茅台今天涨了多少？"

# Try to get token via connect_cloud_service approach
# We'll call it as a module
try:
    from scripts.neodata_token import get_token
    token = get_token()
except:
    token = os.environ.get("NEODATA_TOKEN", "")

result = subprocess.run(
    [sys.executable, script, "--query", query_text, "--token", token, "--data-type", "api"],
    capture_output=True, text=True, encoding="utf-8", errors="replace",
    cwd=r"c:\Users\zengj\.qclaw\workspace\stock_data_project"
)

out_file = r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\neodata_result.txt"
with open(out_file, "w", encoding="utf-8") as f:
    f.write("STDOUT:\\n" + result.stdout[:8000])
    f.write("\\n\\nSTDERR:\\n" + result.stderr[:2000])
    f.write(f"\\n\\nRC={result.returncode}")

print("Written to", out_file)
print("RC:", result.returncode)
"""

with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\neodata_v2.py", "w", encoding="utf-8") as f:
    f.write(runner)

# The real approach: call API directly with requests using fresh token
# First, let's write a direct API caller that gets token from connect_cloud_service
direct_caller = r"""
import json, sys, os, urllib.request, urllib.parse, urllib.error

# Try requests first
try:
    import requests
    HAS_REQUESTS = True
except:
    HAS_REQUESTS = False
    print("requests not available", file=sys.stderr)

# Get token from env
token = os.environ.get("NEODATA_TOKEN", "")
if not token:
    print("No NEODATA_TOKEN in env", file=sys.stderr)
    sys.exit(1)

url = "https://copilot.tencent.com/agenttool/v1/neodata"
query_text = "腾讯和贵州茅台今天涨了多少？"

payload = {
    "query": query_text,
    "channel": "neodata",
    "sub_channel": "workbuddy",
}

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}

out_file = r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\neodata_direct.txt"

if HAS_REQUESTS:
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    result_text = f"Status: {resp.status_code}\n{resp.text[:5000]}"
else:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result_text = f"Status: {resp.status}\n{resp.read().decode('utf-8')[:5000]}"
    except urllib.error.HTTPError as e:
        result_text = f"HTTP Error: {e.code}\n{e.read().decode('utf-8')[:2000]}"
    except Exception as ex:
        result_text = f"Error: {ex}"

with open(out_file, "w", encoding="utf-8") as f:
    f.write(result_text)
print("Written to", out_file)
print(result_text[:500])
"""

with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\neodata_direct.py", "w", encoding="utf-8") as f:
    f.write(direct_caller)

print("Scripts written")
