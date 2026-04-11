import subprocess, sys, json, os, urllib.request

TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJteWZFenA3ODNLaV9KQ3g4Vm5jM1hfaXg2alpyYjZDZjVPTWtHWk1QSTNzIn0.eyJleHAiOjE4MDU2OTE2MjMsImlhdCI6MTc3NTg5NDkzNSwiYXV0aF90aW1lIjoxNzc0MTU1NjIzLCJqdGkiOiI3ZDM0ODQ2OS05ZDMwLTQxNTgtODc4OC1jOTZiZGVlNjk4MjUiLCJpc3MiOiJodHRwczovL3d3dy5jb2RlYnVkZHkuY24vYXV0aC9yZWFsbXMvY29waWxvdCIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI0NmRjY2ZjNy0wOWE4LTRhOWUtYjczZC0zMjFiZTc5OGQ3ZjQiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJjb25zb2xlIiwic2lkIjoiNTVlNjIyMzMtY2FiNy00YTQyLWFlOGItODllZThmNzM5NWQxIiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdGx0LXJvbGVzIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgb2ZmbGluZV9hY2Nlc3MgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5pY2tuYW1lIjoi5pu-5L-K5bqtIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiMTM1MzgzMTI0NDgifQ.vxW7xLLexNJx_mTu5eiE4aUCvTv-JweH1n7gJZYOF81pyMy9gO4PV0Y6tOiOFidCLRFocPea8hyxqww1e3_hdwNtHxpYbwZcGNpcekN9NKqQs_Q0OeNvvrjUd0O7rs5tru3cc2j4xeUa1PzjvdksMt9zTzPz_LNWybf-hpQwX5RHMLsXxAnm1MN48zhgdZx3wHB88cSKURCTVYxh4a9aLk2zqbWB54ni8tCjsOW32_frEUvp8ZAA9iBgQ9_6JREhyyuc1dxTF7_Z4I3PnbZ8mdIUDIFoVnL9L-tqtT4JIXg_2c4b8G6YC1V5PbadDKu82k5EMZyUgSzFEa-UyMg25Q"

def call_api(api_name, params, fields=""):
    url = "https://www.codebuddy.cn/v2/tool/financedata"
    payload = {
        "api_name": api_name,
        "params": params,
    }
    if fields:
        payload["fields"] = fields

    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = resp.read().decode("utf-8")
            return json.loads(result)
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "body": e.read().decode("utf-8")}
    except Exception as ex:
        return {"error": str(ex)}

# Query 1: 腾讯港股 00700.HK 最近5个交易日
hk_result = call_api(
    "hk_daily",
    {"ts_code": "00700.HK"},
    "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount"
)

# Query 2: 贵州茅台 A股 600519.SH 最近5个交易日
a_result = call_api(
    "daily",
    {"ts_code": "600519.SH"},
    "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount"
)

out = {
    "腾讯(00700.HK)": hk_result,
    "贵州茅台(600519.SH)": a_result,
}

with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\api_result.txt", "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

print("Done, written to _temp/api_result.txt")
print(json.dumps(out, ensure_ascii=False, indent=2)[:3000])
