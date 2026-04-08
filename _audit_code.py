"""全面审查关键代码文件"""
import os

base = "C:/Users/zengj/.qclaw/workspace/stock_data_project"
key_files = [
    "scripts/data_engine.py",
    "scripts/data_store.py",
    "scripts/versioned_storage.py",
    "scripts/backtest_engine_v3.py",
    "scripts/execution_engine_v3.py",
    "scripts/survivorship_bias.py",
    "scripts/trading_rules.py",
    "scripts/data_validator.py",
    "scripts/production_data_system.py",
]

print("=== 关键文件行数 ===")
total_lines = 0
for f in key_files:
    path = os.path.join(base, f)
    if os.path.exists(path):
        lines = len(open(path, encoding='utf-8').readlines())
        total_lines += lines
        print(f"  {f}: {lines} 行")
    else:
        print(f"  {f}: 不存在")
print(f"  总计: {total_lines} 行")

# 检查ts_code映射相关代码
print()
print("=== ts_code/exchange映射代码搜索 ===")
keywords = ["BJ", "北交所", "83", "43", ".SH", ".SZ", "exchange", "market_map", "build_ts"]
for f in key_files:
    path = os.path.join(base, f)
    if not os.path.exists(path):
        continue
    content = open(path, encoding='utf-8').read()
    for kw in keywords:
        if kw.lower() in content.lower():
            lines = content.split('\n')
            hits = [(i+1, l.strip()) for i, l in enumerate(lines) if kw in l]
            if hits:
                print(f"\n--- {f} 中 '{kw}' ---")
                for ln, line in hits[:5]:
                    print(f"  L{ln}: {line[:120]}")

# 检查字段名混用
print()
print("=== 字段名混用检查 ===")
field_keywords = ["is_suspend", "is_suspended", "trade_date", "dt", "date_field"]
for f in key_files:
    path = os.path.join(base, f)
    if not os.path.exists(path):
        continue
    content = open(path, encoding='utf-8').read()
    for kw in field_keywords:
        if kw in content:
            lines = content.split('\n')
            hits = [(i+1, l.strip()) for i, l in enumerate(lines) if kw in l and not l.strip().startswith('#')]
            if hits:
                print(f"\n--- {f} 中 '{kw}' ---")
                for ln, line in hits[:3]:
                    print(f"  L{ln}: {line[:100]}")

# 检查checkpoint相关
print()
print("=== checkpoint相关代码 ===")
checkpoint_keywords = ["pending_settlement", "checkpoint", "save_checkpoint", "load_checkpoint"]
for f in key_files:
    path = os.path.join(base, f)
    if not os.path.exists(path):
        continue
    content = open(path, encoding='utf-8').read()
    for kw in checkpoint_keywords:
        if kw in content.lower():
            lines = content.split('\n')
            hits = [(i+1, l.strip()) for i, l in enumerate(lines) if kw.lower() in l.lower() and not l.strip().startswith('#')]
            if hits:
                print(f"\n--- {f} 中 '{kw}' ---")
                for ln, line in hits[:5]:
                    print(f"  L{ln}: {line[:100]}")
