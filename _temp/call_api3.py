import json, urllib.request

def call_api(api_name, params, fields=""):
    url = "https://www.codebuddy.cn/v2/tool/financedata"
    payload = {"api_name": api_name, "params": params}
    if fields:
        payload["fields"] = fields
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

# 腾讯港股
hk = call_api("hk_daily", {"ts_code": "00700.HK"},
               "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")

# 贵州茅台A股
maotai = call_api("daily", {"ts_code": "600519.SH"},
                  "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")

out = {"腾讯": hk, "茅台": maotai}
with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\api_both.txt", "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

# Format nicely
def fmt(code, data):
    fields = data["data"]["fields"]
    items = data["data"]["items"]
    lines = []
    lines.append(f"代码: {code}  |  {' | '.join(fields)}")
    for row in items[:5]:
        lines.append(" | ".join(str(x) for x in row))
    return "\n".join(lines)

print("=== 腾讯 (00700.HK) ===")
print(fmt("00700.HK", hk))
print("\n=== 贵州茅台 (600519.SH) ===")
print(fmt("600519.SH", maotai))
