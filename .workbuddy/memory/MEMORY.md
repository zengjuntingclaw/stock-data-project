# Stock Data Project - Memory

## Project Overview
- **Default workspace**: `C:\Users\zengj\.qclaw\workspace\stock_data_project`
- **Database**: DuckDB at `data/stock_data.duckdb`
- **Main modules**: `scripts/data_engine.py`, `scripts/exchange_mapping.py`

## Data Schema Refactoring (2026-04-11)

### Commit: 7b1baa8
```
feat: 数据口径标准化重构 - PIT支持、原始/复权价格分离
- 统一 ts_code 生成逻辑 (build_ts_code)
- 新增 PIT 股票池查询 (get_active_stocks)
- 新增指数成分股历史区间表 (get_index_constituents)
- 分离原始价格表 (daily_bar_raw) 和复权价格表 (daily_bar_adjusted)
- 新增 33 个单元测试，全部通过
```

### Key Changes
1. **ts_code unification**: All code paths now use `build_ts_code()` from `exchange_mapping.py`
2. **PIT support**: `stock_basic_history` table for historical stock universe queries
3. **Index constituents**: `index_constituents_history` with `in_date/out_date` interval tracking
4. **Price separation**: `daily_bar_raw` (原始价) and `daily_bar_adjusted` (复权价) completely separated

### Key Methods
- `get_active_stocks(trade_date: str)` - PIT stock universe query
- `get_index_constituents(index_code: str, trade_date: str)` - Historical index constituents
- `get_daily_raw(ts_code, start, end)` - Original prices
- `get_daily_adjusted(ts_code, start, end)` - Adjusted prices

### Date Format
All date parameters use `YYYY-MM-DD` format (e.g., `'2024-01-01'`)

### Known Data Gaps
- `corporate_actions`: 0 records (needs population)
- `index_constituents_history`: Data from 2020-01-01 only

## Technical Notes
- PowerShell output may be wrapped in CLIXML format - read files directly for clean output
- Unit tests: 33 tests, all passing
- Auto-commit 习惯: 完成任务后主动提交和 push，不需要用户每次提醒
- 模块导入路径: 使用 `try: from scripts.xxx import yyy except: from xxx import yyy` 兼容相对/绝对导入

## Schema Refactoring v2 (2026-04-11 19:31)

### Commit: ac5796e
```
refactor: 补全标准化schema + 修复模块导入路径
```

### stock_history_schema.sql v2.0
标准化表结构已固化到 schema 文件：
- `stock_basic_history` - PIT 股票主数据
- `daily_bar_raw` - 原始行情（不复权）
- `daily_bar_adjusted` - 复权行情（含 adj_factor）
- `index_constituents_history` - 指数成分历史（区间型）
- `sync_progress` - 增量同步进度
- `data_quality_alert` - 数据质量告警
- 旧表 `stock_basic/daily_quotes/index_constituents` 已标注废弃

### Import Path Fix
- `data_engine.py`: data_validator 导入兼容
- `data_validator.py`: data_engine 常量导入兼容
- `survivorship_bias.py`: data_engine 函数导入兼容

## Critical Bug Fix (2026-04-11 下午)
### Commit: 3761df1
`scripts/data_engine.py` - PIT 查询严重 bug 修复

**Bug 1: get_active_stocks 缺少 eff_date 过滤**
- 旧代码直接查询 stock_basic_history 表，没有考虑 eff_date 过滤
- 导致查询 2024-06-01 时只返回 1-2 条记录（最新 eff_date 的变更事件）
- 修复：使用 JOIN + GROUP BY，对每个 ts_code 取 eff_date <= trade_date 的最新记录
```python
df = self.query("""
    SELECT h.ts_code FROM (
        SELECT ts_code, MAX(eff_date) as latest_eff
        FROM stock_basic_history WHERE eff_date <= CAST(? AS DATE)
        GROUP BY ts_code
    ) latest
    JOIN stock_basic_history h ON h.ts_code = latest.ts_code AND h.eff_date = latest.latest_eff
    WHERE h.list_date <= CAST(? AS DATE)
      AND (h.delist_date IS NULL OR h.delist_date > CAST(? AS DATE))
""", (trade_date, trade_date, trade_date))
```

**Bug 2: get_index_constituents index_code 格式不兼容**
- 传入 `.XSHG` 格式但表存储 `.SH` 格式，导致查不到数据
- 修复：标准化 index_code 格式（`.XSHG` → `.SH`）

**Bug 3: get_index_constituents LEFT JOIN 旧表**
- JOIN `stock_basic` 而不是 `stock_basic_history`
- 修复：改为 JOIN stock_basic_history 的子查询

### stock_basic_history 表特性
- 表中 5608 条记录 = 变更事件日志（不是每日快照）
- 每个 eff_date 只有 1-2 条记录（对应那一天的状态变更）
- eff_date 分布：2019:203, 2020:396, 2021:483, 2022:345, 2023:236, 2024:77, 2025:90
- 总计 5608 条 = 历史所有股票的状态变更累计

### index_constituents_history 表特性
- 1800 条 = 3 个指数（000300/000905/000852）的 2020-01-01 初始快照
- index_code 存储格式：`.SH` 后缀（如 `000300.SH`）
- out_date 全为 NULL（2020 年之后没有成分调整记录）
- 当前只能查询 2020-01-01 时的历史快照（数据局限性）

### 旧表状态（未清理）
- `stock_basic`: 5608 条（仍在被 data_qa_pipeline.py 等模块使用）
- `daily_quotes`: 1408552 条（仍在被多个模块使用）
- `index_constituents`: 0 条
- 这些旧表暂未删除，因为有其他模块依赖它们

### DataEngine 初始化
- 构造函数参数：`db_path`（不是 `data_dir`！）
- 正确用法：`DataEngine(db_path='data/stock_data.duckdb')`
