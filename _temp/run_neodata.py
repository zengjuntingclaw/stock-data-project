import subprocess, sys, json, os

script = r"C:\Users\zengj\.workbuddy\plugins\marketplaces\cb_teams_marketplace\plugins\finance-data\skills\neodata-financial-search\scripts\query.py"
query = "腾讯和贵州茅台今天涨了多少？"
token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJteWZFenA3ODNLaV9KQ3g4Vm5jM1hfaXg2alpyYjZDZjVPTWtHWk1QSTNzIn0.eyJleHAiOjE4MDU2OTE2MjMsImlhdCI6MTc3NTg5NDkzNSwiYXV0aF90aW1lIjoxNzc0MTU1NjIzLCJqdGkiOiI3ZDM0ODQ2OS05ZDMwLTQxNTgtODc4OC1jOTZiZGVlNjk4MjUiLCJpc3MiOiJodHRwczovL3d3dy5jb2RlYnVkZHkuY24vYXV0aC9yZWFsbXMvY29waWxvdCIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI0NmRjY2ZjNy0wOWE4LTRhOWUtYjczZC0zMjFiZTc5OGQ3ZjQiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJjb25zb2xlIiwic2lkIjoiNTVlNjIyMzMtY2FiNy00YTQyLWFlOGItODllZThmNzM5NWQxIiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdGx0LXJvbGVzIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgb2ZmbGluZV9hY2Nlc3MgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5pY2tuYW1lIjoi5pu-5L-K5bqtIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiMTM1MzgzMTI0NDgifQ.vxW7xLLexNJx_mTu5eiE4aUCvTv-JweH1n7gJZYOF81pyMy9gO4PV0Y6tOiOFidCLRFocPea8hyxqww1e3_hdwNtHxpYbwZcGNpcekN9NKqQs_Q0OeNvvrjUd0O7rs5tru3cc2j4xeUa1PzjvdksMt9zTzPz_LNWybf-hpQwX5RHMLsXxAnm1MN48zhgdZx3wHB88cSKURCTVYxh4a9aLk2zqbWB54ni8tCjsOW32_frEUvp8ZAA9iBgQ9_6JREhyyuc1dxTF7_Z4I3PnbZ8mdIUDIFoVnL9L-tqtT4JIXg_2c4b8G6YC1V5PbadDKu82k5EMZyUgSzFEa-UyMg25Q"

result = subprocess.run(
    [sys.executable, script, "--query", query, "--token", token],
    capture_output=True, text=True, encoding="utf-8", errors="replace"
)

with open(r"c:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\neodata_out.txt", "w", encoding="utf-8") as f:
    f.write("=== STDOUT ===\n" + result.stdout)
    f.write("\n=== STDERR ===\n" + result.stderr)
    f.write(f"\n=== RETURN CODE ===\n{result.returncode}")

print("Done, output written to _temp/neodata_out.txt")
