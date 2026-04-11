# -*- coding: utf-8 -*-
"""Final Verification Script for Data Schema Refactoring"""

import sys
import os

os.chdir('c:/Users/zengj/.qclaw/workspace/stock_data_project')
sys.path.insert(0, '.')

from scripts.exchange_mapping import build_ts_code
from scripts.data_engine import DataEngine
import duckdb

# Test 1: build_ts_code
print('=' * 60)
print('Test 1: build_ts_code Unification')
print('-' * 60)
tests = [
    ('600000', '600000.SH', 'SH mainboard'),
    ('688001', '688001.SH', 'KCB/STAR board'),
    ('000001', '000001.SZ', 'SZ mainboard'),
    ('300001', '300001.SZ', 'Chinext/GEM'),
    ('430001', '430001.BJ', 'BJ old stocks'),
    ('830001', '830001.BJ', 'BJ 2024 segment'),
    ('920001', '920001.BJ', 'BJ 2024 new codes'),
]
all_pass = True
for symbol, expected, desc in tests:
    result = build_ts_code(symbol)
    status = 'PASS' if result == expected else 'FAIL'
    if status == 'FAIL':
        all_pass = False
    print('  [{0}] {1} -> {2} (expected {3}) [{4}]'.format(status, symbol, result, expected, desc))
print('  Result: All tests passed = {0}'.format(all_pass))

# Test 2: DataEngine methods
print('')
print('=' * 60)
print('Test 2: DataEngine Methods')
print('-' * 60)
engine = DataEngine()
methods = ['get_active_stocks', 'get_index_constituents', 'get_daily_raw',
           'get_daily_adjusted', 'get_universe_at_date', 'sync_index_constituents',
           'save_stock_basic_snapshot', 'get_stocks_as_of', 'get_pit_stock_pool', 'get_sync_status']
for m in methods:
    exists = hasattr(engine, m)
    print('  [OK] {0}: {1}'.format(m, 'exists' if exists else 'MISSING'))

# Test 3: Tables
print('')
print('=' * 60)
print('Test 3: Database Tables')
print('-' * 60)
con = duckdb.connect('data/stock_data.duckdb', read_only=True)
tables = ['stock_basic', 'stock_basic_history', 'daily_bar_raw', 'daily_bar_adjusted',
          'index_constituents_history', 'sync_progress', 'st_status_history',
          'trade_calendar', 'corporate_actions']
for t in tables:
    query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{0}'".format(t)
    exists = con.execute(query).fetchone()[0] > 0
    print('  [OK] {0}: {1}'.format(t, 'exists' if exists else 'MISSING'))

# Test 4: Data counts
print('')
print('=' * 60)
print('Test 4: Data Counts')
print('-' * 60)
counts = [
    ('stock_basic_history', 'stock_basic_history'),
    ('daily_bar_raw', 'daily_bar_raw'),
    ('daily_bar_adjusted', 'daily_bar_adjusted'),
    ('index_constituents_history', 'index_constituents_history'),
    ('corporate_actions', 'corporate_actions'),
]
for table_name, alias in counts:
    try:
        cnt = con.execute('SELECT COUNT(*) FROM {0}'.format(table_name)).fetchone()[0]
        print('  [OK] {0}: {1:,} records'.format(alias, cnt))
    except Exception as e:
        print('  [X] {0}: ERROR - {1}'.format(alias, e))

# Test 5: Functional calls (using correct YYYY-MM-DD format)
print('')
print('=' * 60)
print('Test 5: Functional Calls (YYYY-MM-DD format)')
print('-' * 60)
try:
    stocks = engine.get_active_stocks('2024-01-01')
    print('  [OK] get_active_stocks(2024-01-01): {0} stocks'.format(len(stocks)))
except Exception as e:
    print('  [X] get_active_stocks: {0}'.format(e))

try:
    df = engine.get_index_constituents('000300.SH', '2024-01-01')
    print('  [OK] get_index_constituents(000300.SH, 2024-01-01): {0} stocks'.format(len(df)))
except Exception as e:
    print('  [X] get_index_constituents: {0}'.format(e))

try:
    df_raw = engine.get_daily_raw('000001.SZ', '2024-01-01', '2024-01-10')
    print('  [OK] get_daily_raw(000001.SZ, 2024-01-01-2024-01-10): {0} records'.format(len(df_raw)))
except Exception as e:
    print('  [X] get_daily_raw: {0}'.format(e))

try:
    df_adj = engine.get_daily_adjusted('000001.SZ', '2024-01-01', '2024-01-10')
    print('  [OK] get_daily_adjusted(000001.SZ, 2024-01-01-2024-01-10): {0} records'.format(len(df_adj)))
except Exception as e:
    print('  [X] get_daily_adjusted: {0}'.format(e))

con.close()
print('')
print('=' * 60)
print('Verification Complete!')
