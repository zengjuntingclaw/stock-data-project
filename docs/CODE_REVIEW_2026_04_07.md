# 代码检视报告 (2026-04-07)

**项目：** A股多因子回测框架 v3.0  
**检视范围：** 全项目源码（scripts/ + tests/ + main_v2_production.py）  
**检视员：** WorkBuddy AI  

---

## 一、总体评价

该项目整体质量良好，工程化程度较高。已实现：未来函数治理（PIT对齐）、幸存者偏差处理、A股交易规则仿真、多线程数据下载、DuckDB列式存储、断点续传等核心生产级功能。代码结构清晰，模块拆分合理，测试覆盖了主要数据引擎路径。

以下按严重程度（P0–P3）列出问题。

---

## 二、P0 - 严重Bug（可能导致程序崩溃或错误结果）

### P0-1：`sync_st_status_history` 中 `LAG` 函数用法错误

**文件：** `scripts/data_engine.py` 第 547 行

```python
LAG(1) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) as prev_st
```

**问题：** `LAG(1)` 等价于 `LAG(常量1)`，实际应写 `LAG(is_st_flag)` 或具体字段名，这里逻辑上要追踪前一天是否是ST。但 daily_quotes 表没有 is_st 字段，整个查询逻辑存在语义错误：无法从 daily_quotes 推断当天是否是ST，只能知道该股存在交易记录。

**建议：** 改用 stock_basic.name 字段的变化来推断ST状态更替，或直接从 AkShare 的 `stock_zh_a_st_em` 接口获取ST历史。该方法目前无法产生有意义的 `is_new_st` 数据，需重构。

---

### P0-2：`performance.py` 的 `_dd_days` 计算有逻辑错误

**文件：** `scripts/performance.py` 第 35 行

```python
def _dd_days(self, r: pd.Series) -> int:
    cum, running_max = (1 + r).cumprod(), r.cumprod().expanding().max()
```

**问题：** `running_max` 使用了 `r.cumprod().expanding().max()`，但 `r` 是收益率序列（如 0.01, -0.02），`r.cumprod()` 不是净值序列（净值应为 `(1+r).cumprod()`）。`cum` 和 `running_max` 的计算基准不一致，导致回撤天数计算错误。

**建议：** 统一使用 `(1+r).cumprod()`：

```python
cum = (1 + r).cumprod()
running_max = cum.expanding().max()
dd = (cum - running_max) / running_max
```

---

### P0-3：`data_engine.py` 的 `_save_financial_data` 字段不匹配

**文件：** `scripts/data_engine.py` 第 732–752 行

```python
conn.execute("""
    INSERT OR REPLACE INTO financial_data
    (ts_code, ann_date, end_date, report_type, revenue,
     net_profit, total_assets, total_equity, roe, roa,
     eps, gross_margin, data_source)
    SELECT ...
        roe, roa, eps, gp, data_source
    FROM df
""")
```

**问题：** 数据库字段 `gross_margin`，但 DataFrame 中列名为 `gp`（来自 `_bs_profit_row` 和 `_ak_indicator_row`）。字段不对齐会导致插入失败或数据错位。另外，`financial_data` 表中没有 `eps` 和 `data_source` 字段（DDL中未定义），会导致插入报错。

**建议：**
1. 统一 DataFrame 中的列名为 `gross_margin`，或在 INSERT 时做映射。
2. 在 `financial_data` DDL 中补充 `eps` 和 `data_source` 字段，或从 INSERT 语句中移除。

---

### P0-4：`backtest_engine_v3.py` 中 `_get_market_data` 使用 `prev_close` 但 `daily_quotes` 没有该列

**文件：** `scripts/backtest_engine_v3.py` 第 267 行

```python
result = batch_df[["symbol", "open", "high", "low", "close", "volume", "prev_close"]].copy()
```

**问题：** `daily_quotes` 表中对应的列名为 `pre_close`（见 DDL 第 214 行），而此处引用了 `prev_close`，会抛出 `KeyError`。

**建议：** 改为 `pre_close`，或在 `get_batch_stock_data` 中统一重命名。

---

## 三、P1 - 重要问题（影响正确性或鲁棒性）

### P1-1：`data_engine.py` 中 `sync_stock_list` 使用裸字符串插入存在 SQL 注入风险

**文件：** `scripts/data_engine.py` 第 809–812 行

```python
conn.execute(
    f"INSERT INTO stock_basic ({','.join(write_cols)}) SELECT {','.join(write_cols)} FROM df"
)
```

**问题：** `write_cols` 列表由 DataFrame 列名过滤而来，理论上是安全的，但如果数据源（AkShare）返回带特殊字符的列名，有注入风险。`export_parquet` 中的同类代码也有此问题。

**建议：** 改用 `conn.register` + 固定 SELECT 列名的方式，或做更严格的列名白名单校验。

---

### P1-2：`TradingCalendar` 没有调用外部日历时无法识别调休工作日

**文件：** `scripts/trading_rules.py` 第 463–492 行

**问题：** 仅根据"是否周末"和"是否在 `_holidays` 集合中"判断交易日，但 A 股有调休制度（如某些周六需要上班补休，视作交易日）。硬编码的节假日列表也没有包含调休工作日信息，导致这些日期被错误标记为非交易日。

**建议：** 在 `add_holidays` 的同时，增加一个 `_trading_overrides` 集合存放调休工作日，`is_trading_day` 时先检查 overrides。或直接通过 `DataEngine.sync_calendar()` 从 AkShare 同步完整日历覆盖内置日历。

---

### P1-3：`_fetch_single_with_retry` 重试逻辑错误——每次重试都同时尝试 AkShare 和 Baostock

**文件：** `scripts/data_engine.py` 第 1011–1027 行

```python
for attempt in range(max_retries):
    if HAS_AKSHARE:
        df = self._fetch_akshare(...)
        if not df.empty:
            return df, "akshare"
    if HAS_BAOSTOCK:
        df = self._fetch_baostock(...)
        if not df.empty:
            return df, "baostock"
    if attempt < max_retries - 1:
        wait = (2 ** attempt) + random.uniform(0, 1)
        time.sleep(wait)
```

**问题：** 每轮循环都先尝试 AkShare，再尝试 Baostock，Baostock 成功后不再重试，但如果 Baostock 也失败，才会等待然后进入下一轮——下一轮仍然先试 AkShare。这导致对同一数据源的重复 Baostock 请求在指数退避前就发生了。更严重的是：如果 AkShare 成功，Baostock 不会被尝试，这是正确行为；但如果 AkShare 返回空而 Baostock 返回数据，后续循环仍然会再次尝试 AkShare——形成无谓的重复请求。

**建议：** 将重试逻辑分层：先对 AkShare 重试3次，失败后再对 Baostock 重试3次，而非混合循环。

---

### P1-4：`partitioned_storage.py` 的 `archive_to_parquet` 当 `year=None` 时返回 `None` 而不是 `Path`

**文件：** `scripts/partitioned_storage.py` 第 173–175 行

```python
if year is None:
    for y in range(current_year - self.HOT_YEARS - 5, current_year - self.HOT_YEARS):
        self.archive_to_parquet(table, y, compress)
    return  # 隐式返回 None
```

**问题：** 方法签名返回 `Path`，但 `year=None` 时返回 `None`，违反类型约定，调用方如果依赖返回值会报 `AttributeError`。

**建议：** 改为 `return None`（显式）并在类型注解中改为 `Optional[Path]`，或单独提供 `archive_all` 方法。

---

### P1-5：`survivorship_bias.py` 中 `_detect_code_changes` 是 O(n²) 复杂度

**文件：** `scripts/survivorship_bias.py` 第 330–347 行

**问题：** 两层嵌套循环遍历所有股票，全量历史股票约 6000+ 支，该方法复杂度为 O(n²)，在 `load_from_baostock` 完成后调用，可能耗时数分钟。

**建议：** 将候选股票按 `delist_date` 和 `list_date` 分组，只对时间窗口内的股票进行名称相似度比较，将复杂度降至 O(n log n)。

---

### P1-6：`data_engine.py` 中 `fetch_financial_data` 的 Baostock 财务抓取没有线程安全

**文件：** `scripts/data_engine.py` 第 652–666 行

```python
bs.login()
try:
    for year in range(start_year, end_year + 1):
        ...
finally:
    bs.logout()
```

**问题：** `_fetch_financial_single` 在单只股票时会调用 `bs.login()/logout()`。而上层 `fetch_financial_data` 使用 `ThreadPoolExecutor` 并发调用 `_fetch_financial_single`，多线程下 `bs.login()/logout()` 会互相干扰（Baostock 是单例全局状态）。

**建议：** 与 `update_daily_data` 的 Baostock 抓取保持一致，在每个线程内部的 `_fetch_baostock` 中独立管理 login/logout，或改为单线程串行处理财务数据。

---

## 四、P2 - 设计/代码质量问题

### P2-1：`performance.py` 代码过于紧凑，可读性极差

**文件：** `scripts/performance.py` 第 55–63 行（`BrinsonAttribution.analyze`）

```python
def analyze(self, pw, pr, bw, br):
    alloc = select = interact = 0
    for s in set(pw.index) | set(bw.index):
        wp, wb = pw.loc[s].sum() if s in pw.index else 0, ...
        rp = pr[pw.loc[s][pw.loc[s] > 0].index].mean() if ... else 0
        ...
    total = alloc + select + interact
```

**问题：** 将 Brinson 归因完整逻辑压缩在 8 行内，变量名单字母，完全没有注释。财务归因是核心功能，这种写法难以维护和 debug。

**建议：** 按 Brinson 模型三分量（Allocation、Selection、Interaction）各自单独计算，并加上说明注释。

---

### P2-2：`pit_aligner.py` 过于简单，缺少必要的鲁棒性处理

**文件：** `scripts/pit_aligner.py`

**问题：**
- `load()` 接受 `df` 但没有校验必需字段（`ann_date`, `end_date`, `symbol`）是否存在；
- `get_factor()` 中没有处理 `name` 列不存在时的情况，会抛出 `KeyError`；
- `validate()` 内层逻辑反转：`data.iloc[-1]['ann_date'] > d` 的条件下 `return False`，但按照 PIT 约束，已过滤 `ann_date <= d`，这个条件理论上永远不会成立，`validate` 方法实际上永远返回 `True`。

**建议：** 完善入参校验，修复 `validate` 的逻辑错误（应检查 `get_factor` 返回的数据中是否存在 `ann_date > d` 的记录）。

---

### P2-3：`backtest_engine_v3.py` 中 `_get_market_data` 的 `prev_close` 列名不一致

（已在 P0-4 中提及，此处补充说明）

**文件：** `scripts/backtest_engine_v3.py` 第 267 行

还有一处：`_is_blocked` 中 `r.get('prev_close', ...)` 与表字段 `pre_close` 不一致，会导致总是使用 fallback 值。

---

### P2-4：`data_engine.py` 中 `_fill_suspend_dates` 性能问题

**文件：** `scripts/data_engine.py` 第 1519–1522 行

```python
for mdate in missing_dates:
    mdt = pd.Timestamp(mdate)
    valid_before = df[pd.to_datetime(df["trade_date"]) < mdt]
```

**问题：** 对每个缺失日期都执行一次全量 DataFrame 过滤，时间复杂度 O(n*m)。当缺失日期多时（如长期停牌），性能显著下降。

**建议：** 对 `df` 按 `trade_date` 排序后，使用 `searchsorted` 或 `pd.merge_asof` 一次性完成前向填充。

---

### P2-5：`versioned_storage.py` 的连接池存在并发安全隐患

**文件：** `scripts/versioned_storage.py` 第 630–645 行

```python
if not read_only:
    with self._connection_pool_lock:
        if self._connection_pool:
            conn = self._connection_pool.pop()
            from_pool = True
```

**问题：** 只读连接不走连接池（每次都新建），写入连接才走连接池，但 DuckDB 不支持多写连接（会报 "already opened in write mode"）。如果连接池中的写连接被并发使用，会出现问题。

**建议：** DuckDB 在同一进程内推荐使用单一持久连接（非连接池），或者改用 WAL 模式 + 多连接读写分离。

---

## 五、P3 - 轻微问题 / 改进建议

### P3-1：`requirements.txt` 缺少版本上限约束

当前仅有下限约束（如 `pandas>=2.0.0`）。建议补充上限或使用精确版本（`==`），防止大版本升级引发兼容性问题。特别是 `akshare`，更新频繁，字段名经常变动。

### P3-2：`build_ts_code` 对北交所代码分配有误

**文件：** `scripts/data_engine.py` 第 108–113 行

```python
if sym6.startswith(("6", "5", "9", "688")):
    return f"{sym6}.SH"
else:
    return f"{sym6}.SZ"
```

**问题：** 北交所股票（8xxxxx/4xxxxx）被归为 `.SZ` 后缀，但北交所实际交易所为北京证券交易所，官方代码后缀为 `.BJ`（Tushare 标准）或 `.NQ`（不同系统）。用 `.SZ` 会导致与部分数据源不匹配。

**建议：** 根据目标数据源的代码规范统一，并在注释中说明。

### P3-3：`main_v2_production.py` 标注版本号与文件名不一致

文件名 `main_v2_production.py`，内部注释写的是 "v2.0 启动"，但 README 显示当前已是 v3.0。文件名和内部版本标识未同步。

### P3-4：`TradingFilter` 中 `Any` 类型未导入

**文件：** `scripts/trading_rules.py` 第 187–189 行

```python
def check_tradable(self, ..., market_data: Dict[str, Any], ...) -> Dict[str, Any]:
```

但文件顶部的 import 只导入了 `Dict, List, Optional, Tuple`，没有导入 `Any`，在 Python 严格类型检查模式下会报错。

### P3-5：测试缺少对执行引擎核心路径的覆盖

`tests/test_core.py` 主要测试数据类，没有覆盖 `ExecutionEngineV3` 的涨跌停顺延流程、T+1 冻结释放流程和资金结算逻辑。这三个模块是整个回测系统中最容易出错的部分。

---

## 六、问题汇总

| 级别 | 数量 | 主要涉及模块 |
|------|------|------------|
| P0（严重Bug） | 4 | data_engine, performance, backtest_engine |
| P1（重要问题） | 6 | data_engine, trading_rules, partitioned_storage, survivorship_bias |
| P2（设计质量） | 5 | performance, pit_aligner, backtest_engine, data_engine, versioned_storage |
| P3（轻微/改进）| 5 | requirements, data_engine, main, trading_rules, tests |

---

## 七、优先修复建议

**立即修复（P0）：**

1. `performance._dd_days`：修复 `running_max` 计算基准不一致的问题
2. `backtest_engine._get_market_data`：`prev_close` 改为 `pre_close`
3. `data_engine._save_financial_data`：对齐 `gross_margin`/`gp` 字段名，补充 DDL
4. `data_engine.sync_st_status_history`：修复无意义的 `LAG(1)` 逻辑

**近期修复（P1）：**

5. `trading_rules.TradingCalendar`：补充调休工作日机制
6. `data_engine._fetch_single_with_retry`：分层重试逻辑
7. `data_engine.fetch_financial_data`：Baostock 多线程安全问题

---

*报告生成时间：2026-04-07*
