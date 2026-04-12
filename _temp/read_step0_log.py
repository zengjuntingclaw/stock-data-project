import sys
sys.stdout.reconfigure(encoding='utf-8')
path = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\_temp\step0_log.txt'
try:
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    print(repr(content[:500]))
    print("---")
    print(content[:500])
except Exception as e:
    print(f"Error: {e}")
