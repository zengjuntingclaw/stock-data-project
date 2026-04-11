"""Quick test for leading zeros"""
import sys
sys.path.insert(0, '.')

from scripts.exchange_mapping import build_ts_code, classify_exchange

test_cases = [
    ('1', 'SZ'),
    ('000001', 'SZ'),
    ('6881', 'SH'),
    ('8', 'BJ'),
    ('92', None),
]

print("Testing leading zeros:")
for symbol, expected in test_cases:
    try:
        exchange, board = classify_exchange(symbol)
        ts_code = build_ts_code(symbol)
        print(f"  {repr(symbol):12} -> exchange={exchange}, board={board}, ts_code={ts_code}")
    except Exception as e:
        print(f"  {repr(symbol):12} -> ERROR: {e}")
