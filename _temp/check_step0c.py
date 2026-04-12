import json, os
state_file = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\delist_state.json'
if os.path.exists(state_file):
    with open(state_file) as f:
        d = json.load(f)
    print(f"Results so far: {len(d.get('results', {}))}")
else:
    print("No state file found yet")
