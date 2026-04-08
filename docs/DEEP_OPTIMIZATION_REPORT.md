# A股历史交易数据项目深度优化报告

**优化日期**: 2026-04-09  
**优化目标**: 将项目提升为"可用于完整历史数据获取、量化选股、回测收益分析"的可靠数据底座  
**测试状态**: 30项新测试 + 原有测试全部通过

---

## 一、任务完成概览

| 任务 | 状态 | 关键改动 | 测试覆盖 |
|------|------|----------|----------|
| 1. 交易所代码映射 | ✅ 完成 | 新建`exchange_mapping.py`，统一映射函数 | 12个测试用例 |
| 2. 字段名统一 | ✅ 完成 | 新建`field_specs.py`，定义权威字段规范 | 5个测试用例 |
| 3. Checkpoint修复 | ✅ 完成 | 新建`checkpoint_manager.py`，状态一致性保障 | 4个测试用例 |
| 4. 历史证券主表 | ✅ 完成 | 新建`security_master.py`，时点精确股票池 | 5个测试用例 |
| 5. 数据完整性 | ✅ 完成 | 字段类型定义、标准化流程 | 3个测试用例 |
| 6. 代码质量 | ✅ 完成 | 统一入口、减少重复、防御性处理 | 集成测试 |
| 7. 回测可用性 | ✅ 完成 | 端到端工作流验证 | 1个集成测试 |

---

## 二、详细修改说明

### 任务1: 修复交易所代码映射问题

**问题诊断**:
- 原`build_ts_code()`函数对`920xxx`北交所代码识别错误（会误判为沪市）
- 各模块自行实现代码映射规则，存在分叉风险

**解决方案**:
新建`scripts/exchange_mapping.py`，提供统一映射函数：

```python
# 核心函数
classify_exchange(symbol) -> (exchange, board)  # 识别交易所和板块
build_ts_code(symbol) -> "XXXXXX.EXCHANGE"      # 构造ts_code
build_bs_code(symbol) -> "xx.XXXXXX"            # 构造Baostock代码
get_price_limit_pct(symbol, date) -> float      # 获取涨跌停幅度
get_lot_size(symbol) -> (min_lot, step)         # 获取交易单位
round_shares(symbol, shares) -> int             # 规整股数
```

**关键修复**:
- `920xxx`代码段正确识别为北交所（BJ）
- `43xxxx/83xxxx/87xxxx`等北交所代码完整支持
- 所有数据入口复用同一套映射逻辑

**测试验证**:
```python
# 关键测试用例
assert classify_exchange('920001') == ('BJ', '北交所')  # 不再误判为SH
assert build_ts_code('920001') == '920001.BJ'
assert build_bs_code('920001') == 'bj.920001'
```

---

### 任务2: 统一停牌字段和数据字段命名

**问题诊断**:
- `is_suspend`/`is_suspended` 混用
- `trade_date`/`date`/`dt` 混用
- 各层字段名不一致导致数据传递错误

**解决方案**:
新建`scripts/field_specs.py`，定义权威字段规范：

```python
class FieldNames:
    SYMBOL = "symbol"           # 6位数字代码
    TS_CODE = "ts_code"         # 带后缀代码
    TRADE_DATE = "trade_date"   # 交易日期（标准）
    IS_SUSPEND = "is_suspend"   # 是否停牌（统一！）
    OPEN = "open"               # 开盘价
    # ... 完整字段定义

# 旧字段名 -> 新字段名映射（向后兼容）
FIELD_ALIASES = {
    "date": "trade_date",
    "is_suspended": "is_suspend",  # 关键映射
    # ...
}
```

**核心功能**:
- `normalize_column_names(df)`: 自动映射旧字段名到新字段名
- `ensure_columns(df, required)`: 补全缺失字段
- `standardize_df(df)`: 一站式标准化（列名+类型+默认值）

**关键保证**:
- 数据层、回测层、输出层使用完全一致的字段名
- 旧数据自动兼容，无需手动修改

---

### 任务3: 彻底修复Checkpoint/断点续跑状态保存

**问题诊断**:
- save和load使用不同字段链路
- `pending_settlements`路径复杂，容易丢失
- T+1/T+2资金状态恢复不一致

**解决方案**:
新建`scripts/checkpoint_manager.py`，实现状态管理单一真相源：

```python
@dataclass
class BacktestState:
    """回测完整状态（单一真相源）"""
    current_date: str                  # ISO格式
    cash: CashState                    # 资金状态（含pending_settlements）
    positions: Dict[str, PositionState]  # 持仓状态
    pending_orders: List[OrderState]   # 挂单队列
    trade_history: List[TradeState]    # 成交历史
    daily_records: List[Dict]          # 每日记录
    version: str = "2.0"               # 格式版本
```

**关键设计**:
1. **完整序列化**: 所有状态可序列化为JSON/Pickle
2. **路径一致性**: save和load使用完全相同的字段结构
3. **资金状态精确**: `pending_settlements`和`pending_withdrawals`完整保存
4. **双格式存储**: JSON（人类可读）+ Pickle（精确恢复）

**CheckpointManager功能**:
```python
manager = CheckpointManager("./checkpoints")
manager.save(state, "checkpoint_name")  # 同时生成.json和.pkl
loaded_state = manager.load("checkpoint.json")  # 自动识别格式
```

---

### 任务4: 历史时点证券主表

**问题诊断**:
- 原股票池是"当前快照+退市补丁"模式
- 无法精确重建历史时点的可交易股票池
- 存在幸存者偏差风险

**解决方案**:
新建`scripts/security_master.py`，实现时点精确的股票池管理：

```python
class HistoricalSecurityMaster:
    """历史证券主表管理器"""
    
    def get_universe_on_date(
        self, 
        date: Union[datetime, str],
        include_delisted: bool = False,
        include_st: bool = True,
        exchanges: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """获取指定日期的股票池"""
        
    def is_tradable(self, symbol: str, date) -> Tuple[bool, str]:
        """判断指定日期是否可交易，返回原因"""
        
    def check_stock_status(self, symbol, date) -> Dict:
        """检查个股完整状态（上市/退市/ST/停牌）"""
```

**股票状态分类**:
- `NOT_LISTED`: 未上市
- `NORMAL`: 正常交易
- `SUSPENDED`: 停牌
- `ST`/`STAR_ST`: ST/*ST
- `DELISTING`: 退市整理期（仍可交易）
- `DELISTED`: 已退市

**关键能力**:
- 按日期过滤出当时真实可交易的证券
- 区分"尚未上市"/"已退市"/"停牌"/"正常可交易"
- 支持退市股、ST股的完整历史追踪

---

### 任务5: 提升数据完整性和回测可用性

**解决方案**:
1. **字段类型定义**: `FIELD_TYPES`字典定义每个字段的数据类型
2. **必需字段清单**: `get_required_market_data_fields()`返回回测必需字段
3. **数据标准化流程**: `standardize_df()`一站式处理

**回测必需字段**:
```python
[
    "symbol", "trade_date",           # 标识
    "open", "high", "low", "close", "pre_close",  # 价格
    "volume", "amount",                # 量额
    "is_suspend",                      # 状态
]
```

**可选增强字段**:
```python
[
    "limit_up", "limit_down",          # 涨跌停价
    "is_limit_up", "is_limit_down",    # 是否涨跌停
    "is_st", "adj_factor",             # ST状态、复权因子
    "turnover", "avg_volume_20",       # 换手率、均量
]
```

---

### 任务6: 代码质量提升

**实现原则**:
1. **统一入口**: 所有交易所映射经`exchange_mapping.py`
2. **统一字段**: 所有字段名经`field_specs.py`
3. **统一校验**: 数据标准化流程统一
4. **减少重复**: 提取公共函数，避免各模块重复实现
5. **防御性处理**: 空数据、异常日期、未知代码都有兜底处理

---

## 三、新增/修改文件清单

### 新建文件（4个核心模块）

| 文件 | 行数 | 职责 |
|------|------|------|
| `scripts/exchange_mapping.py` | ~350 | 统一交易所代码映射 |
| `scripts/field_specs.py` | ~500 | 字段名规范与标准化 |
| `scripts/security_master.py` | ~750 | 历史证券主表管理 |
| `scripts/checkpoint_manager.py` | ~650 | Checkpoint状态管理 |

### 新增测试文件

| 文件 | 测试数 | 覆盖内容 |
|------|--------|----------|
| `tests/test_deep_optimization.py` | 30 | 7大任务完整测试 |

---

## 四、测试覆盖详情

### 新测试用例（30个）

**交易所映射测试（12个）**:
- `test_shanghai_main`: 沪市主板600/601/603/605
- `test_shanghai_star`: 科创板688xxx
- `test_shenzhen_main`: 深市主板000/001/002
- `test_shenzhen_chinext`: 创业板300/301
- `test_beijing_exchange_old`: 北交所老股43xxxx
- `test_beijing_exchange_new`: 北交所新股83xxxx/87xxxx
- `test_beijing_exchange_2024`: 北交所2024代码920xxx ⭐关键
- `test_ts_code_construction`: ts_code构造
- `test_baostock_code`: Baostock代码格式
- `test_price_limit`: 涨跌停幅度
- `test_lot_size`: 交易单位
- `test_share_rounding`: 股数规整

**字段名统一测试（5个）**:
- `test_suspend_field_unification`: 停牌字段统一为is_suspend
- `test_date_field_unification`: 日期字段统一为trade_date
- `test_symbol_field_unification`: 代码字段统一为symbol
- `test_required_fields_ensure`: 必需字段补全
- `test_full_standardization`: 一站式标准化

**Checkpoint测试（4个）**:
- `test_cash_state_serialization`: 资金状态序列化
- `test_full_state_roundtrip`: 完整状态往返
- `test_checkpoint_save_load`: 保存/加载一致性
- `test_pickle_json_equivalence`: 双格式等价性

**历史证券主表测试（5个）**:
- `test_universe_on_date`: 指定日期股票池
- `test_tradable_filtering`: 可交易筛选
- `test_historical_universe_reconstruction`: 历史重建
- `test_stock_status_check`: 个股状态检查
- `test_beijing_exchange_2024_in_universe`: 920xxx在股票池中

**数据完整性测试（3个）**:
- `test_required_fields_coverage`: 必需字段覆盖
- `test_field_types_defined`: 字段类型定义
- `test_data_standardization_preserves_values`: 值保持

**集成测试（1个）**:
- `test_end_to_end_workflow`: 端到端工作流

### 原有测试状态

| 测试文件 | 测试数 | 状态 |
|----------|--------|------|
| `test_core.py` | 9 | ✅ 全部通过 |
| `test_execution_engine_v3.py` | 17 | ✅ 全部通过 |
| `test_production.py` | 16 | ✅ 全部通过 |

**总计**: 30项新测试 + 42项原有测试 = **72项测试全部通过**

---

## 五、风险与后续建议

### 已覆盖风险

✅ **交易所代码映射**: 920xxx等北交所代码正确识别  
✅ **字段名一致性**: 全链路统一，无混用  
✅ **Checkpoint可靠性**: 状态保存/恢复一致  
✅ **历史股票池**: 时点精确，无幸存者偏差  
✅ **数据标准化**: 空数据、异常值有防御处理  

### 剩余风险（建议后续处理）

| 风险 | 严重程度 | 说明 | 建议 |
|------|----------|------|------|
| 数据源稳定性 | 中 | AkShare/Baostock可能不稳定 | 增加更多备选数据源（Tushare） |
| 财务数据PIT | 中 | 财务数据公告日对齐需验证 | 增加财务数据交叉验证测试 |
| 除权除息处理 | 中 | 复权因子计算需多源验证 | 增加复权因子校验机制 |
| 节假日数据 | 低 | 交易日历需定期更新 | 自动化交易日历更新 |
| 性能瓶颈 | 低 | 大数据量时性能待验证 | 增加性能基准测试 |

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
from scripts.field_specs import standardize_df, FieldNames

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
universe = master.get_universe_on_date('2024-06-01', include_delisted=False)
tradable = universe[universe['is_tradable']]

# 检查个股状态
status = master.check_stock_status('000001', '2024-06-01')
```

### Checkpoint管理

```python
from scripts.checkpoint_manager import CheckpointManager, BacktestState

manager = CheckpointManager("./checkpoints")

# 保存状态
manager.save(state, "backtest_2024_01")

# 加载并继续
loaded_state = manager.load("./checkpoints/backtest_2024_01.json")
```

---

## 七、总结

本次深度优化完成了7大任务，新建4个核心模块，新增30个测试用例，与原有42个测试共同构成**72项测试全部通过**的质量保障体系。

**核心交付物**:
1. ✅ 统一交易所映射（支持920xxx北交所代码）
2. ✅ 统一字段规范（消除is_suspend/is_suspended混用）
3. ✅ 可靠Checkpoint（状态保存/恢复一致）
4. ✅ 历史证券主表（时点精确的股票池）
5. ✅ 数据完整性保障（标准化流程）
6. ✅ 代码质量提升（统一入口、减少重复）
7. ✅ 完整测试覆盖（30项新测试+42项原有测试）

项目现已具备**生产级回测数据底座**的能力，可用于完整A股历史数据获取、量化选股、回测收益分析。
