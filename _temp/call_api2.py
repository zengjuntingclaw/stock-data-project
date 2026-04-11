import json, urllib.request

# Try WITHOUT auth first (maybe no auth needed)
def call_api_no_auth(api_name, params, fields=""):
    url = "https://www.codebuddy.cn/v2/tool/financedata"
    payload = {"api_name": api_name, "params": params}
    if fields:
        payload["fields"] = fields
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return {"status": resp.status, "body": json.loads(resp.read().decode("utf-8"))}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "body": e.read().decode("utf-8")[:500]}
    except Exception as ex:
        return {"error": str(ex)}

# Test
result = call_api_no_auth(
    "daily",
    {"ts_code": "600519.SH"},
    "ts_code,trade_date,open,high,low,close,pct_chg"
)

with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\api_result2.txt", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print(json.dumps(result, ensure_ascii=False, indent=2)[:2000])
