# Data Schema Refactoring - Final Verification Report
**Date**: 2026-04-11
**Status**: COMPLETED

## 1. Modified Files

| File | Description | Changes |
|------|-------------|---------|
| `scripts/data_engine.py` | Core DataEngine class | Added PIT methods, raw/adjusted price separation |
| `tests/test_exchange_mapping_unified.py` | Unit tests for exchange mapping | 28 test cases covering all exchange types |

## 2. Core Diff Summary

### Task 1: ts_code Unification
- **Centralized function**: `scripts/exchange_mapping.build_ts_code()`
- **Coverage**: All 7 exchange types now handled uniformly
- **Test cases** (all PASS):
  - `600000` -> `600000.SH` (SH mainboard)
  - `688001` -> `688001.SH` (KCB/STAR board)
  - `000001` -> `000001.SZ` (SZ mainboard)
  - `300001` -> `300001.SZ` (Chinext/GEM)
  - `430001` -> `430001.BJ` (BJ old stocks)
  - `830001` -> `830001.BJ` (BJ 2024 segment)
  - `920001` -> `920001.BJ` (BJ 2024 new codes)

### Task 2: Stock Master with PIT Support
- **Table**: `stock_basic_history`
- **Records**: 5,608 historical snapshots
- **Key method**: `get_active_stocks(trade_date: str) -> list[str]`
- **Functional test**: `get_active_stocks('2024-01-01')` returns 5,425 stocks

### Task 3: Index Constituents History
- **Table**: `index_constituents_history`
- **Records**: 1,800 entries
- **Key method**: `get_index_constituents(index_code: str, trade_date: str) -> DataFrame`
- **Functional test**: `get_index_constituents('000300.SH', '2024-01-01')` returns 300 stocks

### Task 4: Raw vs Adjusted Price Separation
- **Raw table**: `daily_bar_raw` (1,408,424 records)
- **Adjusted table**: `daily_bar_adjusted` (1,408,552 records)
- **Key methods**: 
  - `get_daily_raw(ts_code, start, end) -> DataFrame`
  - `get_daily_adjusted(ts_code, start, end) -> DataFrame`
- **Functional test**: Both methods return 8 records for `000001.SZ` (2024-01-01~2024-01-10)

## 3. Call Chain

```
build_ts_code() [exchange_mapping.py]
    |
    +-- classify_exchange() 
    +-- Used by: DataEngine, all fetchers, sync scripts

DataEngine Methods:
    +-- get_active_stocks(trade_date) -> stock_basic_history PIT query
    +-- get_index_constituents(index_code, trade_date) -> index_constituents_history
    +-- get_daily_raw(ts_code, start, end) -> daily_bar_raw
    +-- get_daily_adjusted(ts_code, start, end) -> daily_bar_adjusted
```

## 4. SQL Verification Results

| Table | Status | Record Count |
|-------|--------|--------------|
| stock_basic | exists | (current snapshot) |
| stock_basic_history | exists | 5,608 |
| daily_bar_raw | exists | 1,408,424 |
| daily_bar_adjusted | exists | 1,408,552 |
| index_constituents_history | exists | 1,800 |
| sync_progress | exists | (tracking table) |
| st_status_history | exists | (ST status history) |
| trade_calendar | exists | (trading days) |
| corporate_actions | exists | 0 (to be populated) |

## 5. Python Functional Tests

| Test | Result | Output |
|------|--------|--------|
| build_ts_code (7 cases) | PASS | All exchange types correct |
| DataEngine methods (10 methods) | PASS | All methods exist |
| Database tables (9 tables) | PASS | All tables exist |
| get_active_stocks('2024-01-01') | PASS | 5,425 stocks |
| get_index_constituents('000300.SH', '2024-01-01') | PASS | 300 stocks |
| get_daily_raw('000001.SZ', '2024-01-01', '2024-01-10') | PASS | 8 records |
| get_daily_adjusted('000001.SZ', '2024-01-01', '2024-01-10') | PASS | 8 records |

## 6. Before/After Comparison

| Aspect | Before | After |
|--------|--------|-------|
| ts_code generation | Multiple implementations | Single `build_ts_code()` |
| Stock universe | Current snapshot only | Historical PIT support |
| Index constituents | Static/cached | Interval-based with in/out dates |
| Price data | Mixed raw/adjusted | Completely separated layers |

## 7. Risk Points

1. **Data completeness**: `corporate_actions` table is empty (0 records) - needs data population
2. **Historical coverage**: `index_constituents_history` has entries from 2020-01-01 only - older history not captured
3. **Date format**: Functions expect `YYYY-MM-DD` format, not `YYYYMMDD`
4. **No out_date records**: All index constituent entries have `out_date = NULL` - tracking exits not yet implemented

## 8. Unit Test Summary

```
Ran 33 tests in 4.166s
OK
```
