# A股历史交易数据项目深度修复与重构报告

**执行日期**: 2026-04-09  
**执行标准**: "可以作为完整A股历史数据底座，用于量化选股和回测收益分析"  
**测试状态**: 72项测试全部通过（30项新测试+42项原有测试）

---

## 一、任务完成概览

| 任务 | 状态 | 关键交付 | 验证方式 |
|------|------|----------|----------|
| 1. 统一交易所代码映射 | ✅ 完成 | 修复920xxx北交所识别，所有入口统一 | 12个测试用例 |
| 2. Checkpoint一致性 | ✅ 完成 | save/load路径完全一致，T+1/T+2资金精确恢复 | 4个测试用例 |
| 3. 字段命名统一 | ✅ 完成 | 消除is_suspend/is_suspended混用 | 5个测试用例 |
| 4. 历史证券主表 | ✅ 完成 | 时点精确股票池，消除幸存者偏差 | 5个测试用例 |
| 5. 数据完整性 | ✅ 完成 | 完整性/一致性/价格合理性检查 | 3个测试用例+检查脚本 |
| 6. 代码质量 | ✅ 完成 | 统一入口、减少重复、防御性处理 | 集成测试 |
| 7. 回测可用性 | ✅ 完成 | 端到端工作流验证 | 1个集成测试 |

---

## 二、详细修改说明

### 任务1: 统一交易所代码映射，修复北交所代码误判

**问题诊断**:
- `data_engine.py`中的`build_ts_code()`错误地将920xxx识别为沪市（因为以9开头）
- `data_fetcher.py`中的BaostockSource有独立的映射逻辑，存在分叉风险
- 各模块自行实现代码映射，维护困难

**解决方案**:
1. **新建`exchange_mapping.py`** - 统一映射模块
   - `classify_exchange()`: 识别交易所和板块
   - `build_ts_code()`: 构造ts_code（关键修复920xxx→BJ）
   - `build_bs_code()`: 构造Baostock代码
   - 正则表达式严格匹配各交易所代码段

2. **修改`data_engine.py`**
   - `build_ts_code()`函数委托给`exchange_mapping.build_ts_code()`
   - 删除本地实现，确保所有入口一致

3. **修改`data_fetcher.py`**
   - AkShareSource和BaostockSource都使用`build_ts_code()`和`build_bs_code()`
   - 删除重复的映射逻辑

**关键修复验证**:
```python
# 修复前：920001会被误判为沪市（.SH）
# 修复后：正确识别为北交所（.BJ）
assert build_ts_code('920001') == '920001.BJ'
assert classify_exchange('920001') == ('BJ', '北交所')
```

---

### 任务2: 彻底修复Checkpoint保存/恢复一致性

**问题诊断**:
- `backtest_engine_v3.py`中的`_save_checkpoint()`和`_load_checkpoint()`使用不同的字段路径
- `pending_settlements`保存和恢复格式不一致
- 没有使用统一的状态管理模块

**解决方案**:
1. **新建`checkpoint_manager.py`** - 统一状态管理
   - `BacktestState`: 单一真相源状态类
   - `CashState`: 资金状态（含pending_settlements/pending_withdrawals）
   - `CheckpointManager`: 保存/加载管理器（JSON+Pickle双格式）
   - `extract_state_from_engine()`: 从引擎提取状态
   - `restore_engine_from_state()`: 恢复引擎状态

2. **修改`backtest_engine_v3.py`**
   - `_save_checkpoint()`: 使用`CheckpointManager`保存完整状态
   - `_load_checkpoint()`: 使用`CheckpointManager`恢复状态
   - 确保save和load使用完全相同的字段链路

**关键保证**:
- T+1可用资金、T+2可取资金状态完全一致
- 持仓、挂单、成交历史完整恢复
- 双格式存储：JSON（人类可读）+ Pickle（精确恢复）

---

### 任务3: 统一字段命名，避免数据层和回测层不一致

**问题诊断**:
- `is_suspend`/`is_suspended` 混用
- `trade_date`/`date`/`dt` 混用
- 数据抓取层和回测层字段名不一致

**解决方案**:
1. **新建`field_specs.py`** - 字段规范定义
   - `FieldNames`: 标准字段名常量类
   - `FIELD_ALIASES`: 旧字段名→新字段名映射
   - `FIELD_TYPES`: 字段类型定义
   - `normalize_column_names()`: 自动映射旧字段名
   - `standardize_df()`: 一站式标准化

2. **修改`data_fetcher.py`**
   - `_standardize_columns()`: 使用`standardize_df()`进行标准化
   - 自动处理中文列名映射和字段标准化

**字段统一映射**:
```python
FIELD_ALIASES = {
    "date": "trade_date",
    "is_suspended": "is_suspend",  # 关键统一
    "code": "symbol",
    "prev_close": "pre_close",
    # ...
}
```

---

### 任务4: 把股票池升级为"历史时点证券主表"

**问题诊断**:
- 原股票池是"当前上市股票+退市补丁"模式
- 无法精确重建历史时点的可交易股票池
- 存在幸存者偏差和上市日期穿越问题

**解决方案**:
1. **新建`security_master.py`** - 历史证券主表
   - `StockLifetime`: 股票全生命周期记录
   - `HistoricalSecurityMaster`: 主表管理器
   - `get_universe_on_date()`: 按日期重建股票池
   - `is_tradable()`: 判断指定日期是否可交易
   - 区分：未上市/已退市/停牌/正常可交易

**关键能力**:
```python
# 获取2024年6月的可交易股票池
universe = master.get_universe_on_date('2024-06-01', include_delisted=False)
tradable = universe[universe['is_tradable']]

# 检查个股状态
status = master.check_stock_status('000001', '2024-06-01')
# 返回：是否可交易、是否ST、上市日期、退市日期等
```

---

### 任务5: 提升历史行情数据完整性与回测可用性

**解决方案**:
1. **字段完整性**
   - 必需字段：symbol, trade_date, open, high, low, close, pre_close, volume, amount, is_suspend
   - 可选字段：limit_up, limit_down, is_st, adj_factor, turnover

2. **数据质量检查**
   - 新建`data_consistency_checker.py`
   - 检查项：交易所映射、字段一致性、数据完整性、价格合理性、日期连续性
   - 输出详细报告（JSON格式）

**检查脚本使用**:
```bash
python scripts/data_consistency_checker.py --data-path data/daily_quotes.parquet
```

---

### 任务6: 代码质量提升

**实现原则**:
1. **统一入口**: 所有交易所映射经`exchange_mapping.py`
2. **统一字段**: 所有字段名经`field_specs.py`
3. **统一状态**: 所有checkpoint经`checkpoint_manager.py`
4. **减少重复**: 提取公共函数，避免各模块重复实现
5. **防御性处理**: 空数据、异常日期、未知代码都有兜底

---

## 三、新增/修改文件清单

### 新建文件（5个）

| 文件 | 行数 | 职责 |
|------|------|------|
| `exchange_mapping.py` | ~350 | 统一交易所代码映射（关键修复920xxx） |
| `field_specs.py` | ~500 | 字段名规范与标准化 |
| `security_master.py` | ~750 | 历史时点证券主表 |
| `checkpoint_manager.py` | ~650 | Checkpoint状态管理（save/load一致性） |
| `data_consistency_checker.py` | ~550 | 数据质量检查脚本 |

### 修改文件（3个）

| 文件 | 修改内容 |
|------|----------|
| `data_engine.py` | `build_ts_code()`委托给`exchange_mapping`模块 |
| `data_fetcher.py` | 使用统一映射函数和字段标准化 |
| `backtest_engine_v3.py` | 使用`checkpoint_manager`进行状态保存/恢复 |

### 测试文件（1个）

| 文件 | 测试数 | 覆盖内容 |
|------|--------|----------|
| `test_deep_optimization.py` | 30 | 7大任务完整测试 |

---

## 四、测试覆盖详情

### 新测试用例（30个）全部通过 ✅

**交易所映射测试（12个）**:
- 沪市主板600/601/603/605 ✅
- 科创板688xxx ✅
- 深市主板000/001/002 ✅
- 创业板300/301 ✅
- 北交所老股43xxxx ✅
- 北交所新股83xxxx/87xxxx ✅
- **北交所2024代码920xxx** ⭐关键修复 ✅
- ts_code构造 ✅
- Baostock代码格式 ✅
- 涨跌停幅度 ✅
- 交易单位 ✅
- 股数规整 ✅

**字段名统一测试（5个）**:
- 停牌字段统一为is_suspend ✅
- 日期字段统一为trade_date ✅
- 代码字段统一为symbol ✅
- 必需字段补全 ✅
- 一站式标准化 ✅

**Checkpoint测试（4个）**:
- 资金状态序列化 ✅
- 完整状态往返 ✅
- 保存/加载一致性 ✅
- 双格式等价性 ✅

**历史证券主表测试（5个）**:
- 指定日期股票池 ✅
- 可交易筛选 ✅
- 历史重建 ✅
- 个股状态检查 ✅
- 920xxx在股票池中 ✅

**数据完整性测试（3个）**:
- 必需字段覆盖 ✅
- 字段类型定义 ✅
- 值保持 ✅

**集成测试（1个）**:
- 端到端工作流 ✅

### 原有测试（42个）全部通过 ✅
- `test_core.py`: 9项 ✅
- `test_execution_engine_v3.py`: 17项 ✅
- `test_production.py`: 16项 ✅

**总计**: 72项测试全部通过

---

## 五、风险与后续建议

### 已覆盖风险 ✅

| 风险 | 状态 | 说明 |
|------|------|------|
| 920xxx北交所识别 | ✅ 已修复 | 统一使用exchange_mapping模块 |
| Checkpoint不一致 | ✅ 已修复 | 使用checkpoint_manager统一路径 |
| 字段名混用 | ✅ 已修复 | 使用field_specs标准化 |
| 幸存者偏差 | ✅ 已修复 | 历史时点证券主表 |
| 数据完整性 | ✅ 已覆盖 | 数据一致性检查脚本 |

### 剩余风险（建议后续处理）

| 风险 | 严重程度 | 建议 |
|------|----------|------|
| 数据源稳定性 | 中 | 增加Tushare作为第三备选 |
| 财务数据PIT | 中 | 增加财务数据交叉验证 |
| 除权除息处理 | 中 | 增加复权因子校验机制 |
| 节假日数据 | 低 | 自动化交易日历更新 |
| 性能瓶颈 | 低 | 大数据量性能基准测试 |

### 后续优化建议

1. **数据源多元化**: 接入Tushare作为第三备选
2. **数据质量监控**: 建立自动化QA管道，每日检查数据完整性
3. **回测性能优化**: 对大规模回测进行性能分析和优化
4. **实盘对接准备**: 预留券商API接口，为实盘做准备
5. **文档完善**: 补充架构设计文档和使用手册

---

## 六、使用示例

### 交易所代码映射
```python
from scripts.exchange_mapping import build_ts_code, classify_exchange

# 正确识别920xxx为北交所
ts_code = build_ts_code('920001')  # '920001.BJ'
exchange, board = classify_exchange('920001')  # ('BJ', '北交所')
```

### 字段标准化
```python
from scripts.field_specs import standardize_df

# 原始数据（含旧字段名）
df = pd.DataFrame({
    'date': ['2024-01-01'],
    'code': ['000001'],
    'is_suspended': [False],
})

# 标准化
df_std = standardize_df(df)
# 结果: trade_date, symbol, is_suspend 等标准字段
```

### 历史证券主表
```python
from scripts.security_master import HistoricalSecurityMaster

master = HistoricalSecurityMaster()

# 获取2024年6月的可交易股票池
universe = master.get_universe_on_date('2024-06-01')
tradable = universe[universe['is_tradable']]

# 检查个股状态
status = master.check_stock_status('000001', '2024-06-01')
```

### Checkpoint管理
```python
from scripts.checkpoint_manager import CheckpointManager

manager = CheckpointManager("./checkpoints")
manager.save(state, "backtest_2024_01")
loaded_state = manager.load("./checkpoints/backtest_2024_01.json")
```

### 数据质量检查
```bash
# 检查数据文件
python scripts/data_consistency_checker.py --data-path data/daily_quotes.parquet --output report.json

# 使用示例数据测试
python scripts/data_consistency_checker.py
```

---

## 七、总结

本次深度修复与重构完成了7大任务，新建5个核心模块，修改3个现有文件，新增30个测试用例，与原有42个测试共同构成**72项测试全部通过**的质量保障体系。

**核心交付物**:
1. ✅ **统一交易所映射** - 修复920xxx北交所识别，所有入口一致
2. ✅ **Checkpoint一致性** - save/load路径完全一致，资金状态精确恢复
3. ✅ **字段规范统一** - 消除is_suspend/is_suspended混用
4. ✅ **历史证券主表** - 时点精确股票池，消除幸存者偏差
5. ✅ **数据质量检查** - 完整性/一致性/价格合理性自动检查
6. ✅ **代码质量提升** - 统一入口、减少重复、防御性处理
7. ✅ **完整测试覆盖** - 72项测试全部通过

**Git提交**: `ad621e9`  
**GitHub**: https://github.com/zengjuntingclaw/stock-data-project

项目现已具备**生产级A股历史数据底座**的能力，可用于完整历史数据获取、量化选股、回测收益分析。
