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

# 仅查腾讯近5日
hk = call_api("hk_daily", {"ts_code": "00700.HK"},
               "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount")

out = json.dumps(hk, ensure_ascii=False, indent=2)
with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\hk_only.txt", "w", encoding="utf-8") as f:
    f.write(out)
print(out[:2000])
