# 代码优化分析报告

> 生成时间：2026-04-06  
> 分析范围：stock_data_project 全部源码文件  
> 分析方式：全面代码审查 + 静态分析

---

## 一、总览

| 文件 | 行数 | 状态 |
|------|------|------|
| `scripts/data_engine.py` | 1944 | ⚠️ 过大，需拆分 |
| `tests/test_data_engine.py` | 755 | ✅ 良好 |
| `scripts/execution_engine_v3.py` | 439 | ⚠️ 有 T+1 逻辑缺陷 |
| `scripts/backtest_engine_v3.py` | 434 | ❌ 序列化 bug |
| `scripts/trading_rules.py` | 323 | ⚠️ 节假日硬编码 |
| `scripts/survivorship_bias.py` | 255 | ❌ 死循环风险 |
| `scripts/sentinel_audit.py` | 249 | ✅ 正常 |
| `scripts/data_classes.py` | 137 | ✅ 正常 |
| `scripts/pit_aligner.py` | 42 | ✅ 正常 |
| `main_v2_production.py` | 99 | ✅ 正常 |

---

## 二、P0 — 必须修复（5 个）

### P0-1: `survivorship_bias.py:87` — 位运算导致潜在死循环

**位置**: `scripts/survivorship_bias.py` 第 87 行

**问题**:
```python
while (rs.error_code == '0') & rs.next():
```
`&` 是位运算符，在 Python 中 `(bool) & (call)` 的求值顺序不确定，可能导致 `rs.next()` 不被正确调用或 `error_code` 检查被跳过。

**修复**:
```python
while rs.next():
    if rs.error_code != '0':
        break
```

---

### P0-2: `data_engine.py:161-164` — 条件分支导致 `abnormal` 未定义

**位置**: `scripts/data_engine.py` 第 161-164 行

**问题**:
```python
if hasattr(df, "pct_chg"):          # DataFrame 始终有列属性，但这里检查的是属性不是列
    max_pct = df.apply(_max_pct, axis=1)
    abnormal = df[df["pct_chg"].abs() > max_pct]
if len(abnormal) > 0:               # abnormal 可能未定义！
```
- `hasattr(df, "pct_chg")` 检查的是 DataFrame 属性，不是列，始终返回 True（DataFrame 有 `__getattr__` 动态代理）
- 当内部条件为 False 时，`abnormal` 变量未定义，第 164 行 `len(abnormal)` 会抛 `NameError`

**修复**: 改用向量化操作，同时修复逻辑错误：
```python
if "pct_chg" in df.columns and "ts_code" in df.columns:
    codes = df["ts_code"].str[:6]
    max_pcts = codes.map(lambda x: detect_limit(x) * 100)
    abnormal = df[df["pct_chg"].abs() > max_pcts]
    if len(abnormal) > 0:
        issues.append({...})
```

---

### P0-3: `data_engine.py:156-162` — `df.apply(axis=1)` 性能问题

**位置**: `scripts/data_engine.py` 第 156-162 行

**问题**: `df.apply(_max_pct, axis=1)` 逐行遍历 DataFrame，效率极低。对万级数据量可造成显著延迟。

**修复**: 使用向量化 map（见 P0-2 修复方案），性能提升 10-100 倍。

---

### P0-4: `backtest_engine_v3.py:391` — 检查点序列化类型错误

**位置**: `scripts/backtest_engine_v3.py` 第 389-394 行

**问题**:
```python
state_data['execution'] = {
    'pending_settlements': [
        {'symbol': ps['symbol'], 'amount': ps['amount'], ...}
        for ps in getattr(self.execution, 'pending_settlements', [])
    ],
}
```
`self.execution.cash.pending_settlements` 是 `List[Tuple[datetime, float]]`，不是字典列表。代码用 `ps['symbol']` 访问元组会抛 `TypeError`。

**修复**:
```python
state_data['execution'] = {
    'cash': {
        'total': self.execution.cash.total,
        'available': self.execution.cash.available,
        'withdrawable': self.execution.cash.withdrawable,
    },
    'pending_settlements': [
        {'settle_date': ps[0].isoformat(), 'amount': ps[1]}
        for ps in self.execution.cash.pending_settlements
    ],
    'pending_withdrawals': [
        {'withdrawable_date': pw[0].isoformat(), 'amount': pw[1]}
        for pw in self.execution.cash.pending_withdrawals
    ],
}
```

---

### P0-5: `backtest_engine_v3.py:409` — 检查点恢复 Position 构造错误

**位置**: `scripts/backtest_engine_v3.py` 第 409 行

**问题**:
```python
positions={sym: Position(sym, **pos) for sym, pos in data['positions'].items()}
```
`BacktestState.to_dict()` 只序列化了 `{'shares': ..., 'avg_cost': ...}`，缺少 `available_shares` 字段。恢复后 `available_shares` 默认为 0，导致所有持仓无法卖出。

**修复**:
```python
positions={
    sym: Position(
        symbol=sym,
        shares=pos.get('shares', 0),
        available_shares=pos.get('available_shares', pos.get('shares', 0)),
        avg_cost=pos.get('avg_cost', 0.0)
    )
    for sym, pos in data['positions'].items()
}
```

同时修改 `BacktestState.to_dict()`，序列化 `available_shares`：
```python
'positions': {
    sym: {'shares': pos.shares, 'available_shares': pos.available_shares, 'avg_cost': pos.avg_cost}
    for sym, pos in self.positions.items()
},
```

---

## 三、P1 — 强烈建议（6 个）

### P1-1: `data_engine.py` — 1944 行单文件，严重违反单一职责

**建议拆分为模块包**:
```
scripts/data_engine/
├── __init__.py      # 导出 DataEngine 等公共接口
├── base.py          # DataEngine 基类
├── validator.py     # DataValidator
├── fetcher.py       # _fetch_akshare / _fetch_baostock / fetch_single
├── storage.py       # save_quotes / export_parquet
├── indicators.py    # compute_rolling_* 技术指标计算
├── calendar.py      # 交易日历相关
└── utils.py         # detect_board / detect_limit / build_ts_code
```

---

### P1-2: `trading_rules.py:86-96` 与 `data_engine.py:73-88` — 板块识别逻辑重复

**问题**: `AShareTradingRules.get_board()` 和 `detect_board()` 实现相同功能但独立维护。

**建议**: `trading_rules.py` 的 `get_board()` 内部调用 `detect_board()`，返回英文枚举值：
```python
@classmethod
def get_board(cls, symbol: str) -> str:
    from scripts.data_engine import detect_board
    board_map = {'主板': cls.BOARD_MAIN, '创业板': cls.BOARD_CHINEXT,
                 '科创板': cls.BOARD_STAR, '北交所': cls.BOARD_BSE}
    return board_map.get(detect_board(symbol), cls.BOARD_MAIN)
```

---

### P1-3: `data_engine.py:936-1022` 与 `1114-1275` — fetch_single 与 _fetch_* 代码重复

**问题**: `fetch_single()` 内部实现了列名标准化、涨跌停标记、复权因子计算等逻辑，与 `_fetch_akshare()` / `_fetch_baostock()` 高度重复。

**建议**: 将列名标准化、涨跌停标记、复权因子计算抽取为 `_normalize_columns()`、`_compute_adj_factor()` 等内部方法。

---

### P1-4: `execution_engine_v3.py:402-414` — T+1 持仓可卖逻辑过于简化

**位置**: 第 402-414 行

**问题**:
```python
def update_position_available(self, current_date: datetime):
    for sym, pos in self.positions.items():
        pos.available_shares = pos.shares  # 每日重置
```
每个交易日都把 `available_shares = shares`，但**没有跟踪今日新买入的冻结**。如果在同一天先执行卖出（释放 available_shares），再执行买入，会导致刚买入的股票立即可卖。

**建议**: 维护 `_today_bought` 映射，在每日开始时释放前日冻结，扣除今日新买入：
```python
def update_position_available(self, current_date: datetime):
    if not hasattr(self, '_frozen_shares'):
        self._frozen_shares = {}
    # 释放前一日冻结
    frozen = self._frozen_shares.pop(current_date - timedelta(days=1), {})
    for sym, shares in frozen.items():
        if sym in self.positions:
            self.positions[sym].available_shares += shares
    # 重置为 shares（减去今日买入冻结）
    for sym, pos in self.positions.items():
        pos.available_shares = pos.shares - self._frozen_shares.get(current_date, {}).get(sym, 0)
```

---

### P1-5: `trading_rules.py:186-279` — 节假日硬编码

**问题**: 2018-2026 年的节假日全部硬编码在类属性中，每年底需要手动添加新一年节假日。

**建议**: 改为从配置文件加载：
```python
# config/holidays.yaml
holidays:
  2026:
    - '2026-01-01'
    - '2026-02-16'
    ...
```

---

### P1-6: `execution_engine_v3.py:231-234` — ADV 限制参数含义不明确

**位置**: 第 231-234 行

**问题**: `adv_limit=0.1` 的含义需要读代码才能理解——是10%还是10倍？

**建议**: 重命名为 `adv_pct_limit` 并添加注释：
```python
adv_pct_limit: float = 0.1,  # 最多买入日均成交量的 10%
```

---

## 四、P2 — 建议改进（6 个）

### P2-1: 缺少 `.gitignore` 文件

建议添加：
```
__pycache__/
*.py[cod]
*.egg-info/
data/
*.duckdb
_output/
logs/
.env
.idea/
.vscode/
```

### P2-2: 缺少 `pytest.ini` / `pyproject.toml` 配置

建议添加 `pytest.ini`：
```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short
```

### P2-3: `main_v2_production.py:15` — 日志级别硬编码

建议支持环境变量：
```python
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger.add(sys.stdout, format="...", level=LOG_LEVEL)
```

### P2-4: 多处函数缺少类型注解

| 文件 | 方法 | 建议添加 |
|------|------|---------|
| `data_engine.py` | `save_quotes` | `-> None` |
| `execution_engine_v3.py` | `_is_blocked` | `-> bool` |
| `execution_engine_v3.py` | `_estimate_cost` | `-> float` |
| `survivorship_bias.py` | `get_universe` | `-> List[str]` |
| `trading_rules.py` | `add_holidays` | `-> None` |

### P2-5: `sentinel_audit.py` — 审查工具能力有限

建议升级为 AST 分析：
- 圈复杂度检测
- 未使用变量/导入检测
- 递归深度检测

### P2-6: `data_engine.py:1664-1668` — `compute_rolling_returns` 窗口函数列名歧义

**位置**: 第 1664-1668 行

**问题**: `PARTITION BY ts_code ORDER BY trade_date` 没有加表别名 `d.`，虽然当前在这个 CTE 中只有一张表不会报错，但与其他 compute_rolling_* 方法不一致（MA 和 volatility 已修复为 `d.ts_code`）。

**建议**: 统一添加 `d.` 别名。

---

## 五、P3 — 锦上添花（4 个）

### P3-1: 考虑使用 `ruff` 替代 `flake8`
### P3-2: 日志配置外部化（已在 P2-3 覆盖）
### P3-3: `TradingCostConfig` 中印花税逻辑与 `AShareTradingRules` 重复
### P3-4: 测试覆盖可增加：`execution_engine` T+1 完整流程、`backtest_engine` 端到端回测

---

## 六、测试覆盖分析

| 模块 | 当前覆盖 | 缺失 |
|------|---------|------|
| `data_classes.py` | ✅ 完整 | - |
| `data_engine.py` | ✅ 良好 (61 用例) | `sync_*` 方法、异常分支 |
| `trading_rules.py` | ✅ 良好 | 边界日期 |
| `execution_engine_v3.py` | ⚠️ 中等 | T+1 完整流程、风控逻辑 |
| `survivorship_bias.py` | ⚠️ 中等 | `_is_st` 方法 |
| `backtest_engine_v3.py` | ❌ 低 | 端到端回测、检查点恢复 |
| `performance.py` | ❌ 低 | 边界条件 |

---

## 七、安全性评估

| 方面 | 评级 | 说明 |
|------|------|------|
| SQL 注入 | ✅ 良好 | 使用参数化查询 + register 白名单 |
| 路径遍历 | ✅ 良好 | export_parquet 有路径安全检查 |
| 敏感信息 | ✅ 良好 | 无硬编码密钥 |
| eval 使用 | ✅ 无 | - |

---

## 八、优先级汇总

| 优先级 | 数量 | 影响范围 |
|--------|------|---------|
| P0 | 5 | 运行时崩溃/死循环/数据错误 |
| P1 | 6 | 功能缺陷/维护困难/性能 |
| P2 | 6 | 工程规范/可维护性 |
| P3 | 4 | 锦上添花 |

**建议处理顺序**: P0-1 → P0-2 → P0-4 → P0-5 → P1-1（重构拆分）→ P1-4（T+1 修复）
