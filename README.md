<div align="center">

# A股多因子回测框架

**准实盘级量化回测系统 v3.0**

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.9+-orange.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

专门为 A 股市场设计的生产级多因子回测框架，完整实现未来函数治理、T+1 交易模拟、涨跌停顺延、幸存者偏差处理等准实盘交易规则。

</div>

---

## 目录

- [核心特性](#核心特性)
- [项目架构](#项目架构)
- [快速开始](#快速开始)
- [核心模块](#核心模块)
- [回测流程](#回测流程)
- [未来函数治理](#未来函数治理)
- [A股交易规则实现](#a股交易规则实现)
- [绩效分析](#绩效分析)
- [数据源](#数据源)
- [测试](#测试)
- [文档](#文档)
- [技术栈](#技术栈)
- [许可证](#许可证)

---

## 核心特性

### 🔬 科学回测
- **未来函数零容忍** — PIT (Point-In-Time) 数据对齐，财务数据使用 `ann_date` 而非 `end_date`
- **幸存者偏差消除** — 全量历史股票池（含退市），严格按上市/退市日期过滤
- **分层回测** — 按因子分组构建多空组合，评估因子区分度

### 🏦 A股交易规则完整模拟
- **T+1 交易** — 当日买入不可卖出，次日开盘价成交
- **涨跌停顺延** — 涨停无法买入、跌停无法卖出，自动顺延至下一可交易日
- **100 股取整** — 严格遵守 A 股最小交易单位
- **ADV 流动性限制** — 单笔交易不超过日均成交量的可配置比例
- **印花税时间感知** — 自动识别 2023-08-28 前后的印花税率调整
- **ST/\*ST 过滤** — 严格过滤特别处理股票

### 📊 生产级工程
- **多线程数据下载** — AkShare + Baostock 双源自动切换，指数退避重试
- **DuckDB 列式存储** — 千万级行数据查询 <1s
- **数据质量监控** — 多源交叉验证、复权因子监测、异常数据自动标记
- **检查点恢复** — 回测状态序列化，崩溃后可从上次进度继续
- **交易日历** — 内置 2018-2027 年 A 股节假日数据

---

## 项目架构

```
stock_data_project/
├── main_v2_production.py          # 主程序入口
├── run_tests.py                   # 测试运行脚本
├── requirements.txt               # Python 依赖
│
├── scripts/                       # 核心模块
│   ├── __init__.py                # 模块导出 (v3.0)
│   ├── data_engine.py             # 数据引擎 — 多线程下载、DuckDB 存储、数据验证
│   ├── data_validator.py          # 数据验证器 — 多源交叉验证
│   ├── data_classes.py            # 数据类 — Order, Trade, Position, PerformanceMetrics
│   ├── execution_engine_v3.py     # 执行引擎 — T+1 交易模拟、涨跌停、ADV 限制
│   ├── backtest_engine_v3.py      # 回测引擎 — 分层回测、检查点恢复
│   ├── trading_rules.py           # 交易规则 — 涨跌停板、板块识别、交易日历
│   ├── survivorship_bias.py       # 幸存者偏差处理
│   ├── pit_aligner.py             # PIT 数据对齐
│   ├── performance.py             # 绩效分析 — 夏普、IR、Brinson 归因
│   ├── sentinel_audit.py          # 代码审查工具
│   └── stock_history_schema.sql   # 数据库 Schema
│
├── tests/                         # 测试
│   ├── conftest.py                # pytest 配置
│   ├── test_core.py               # 核心功能测试
│   ├── test_data_engine.py        # 数据引擎测试 (61 用例)
│   └── test_production.py         # 生产级测试
│
├── docs/                          # 文档
│   ├── PROJECT_OVERVIEW.md        # 项目概述
│   ├── DEVLOG.md                  # 开发日志
│   ├── PATCH_NOTES.md             # 补丁说明
│   ├── CODE_REVIEW.md             # 代码审查报告
│   ├── VULNERABILITY_REPORT.md    # 未来函数漏洞分析
│   ├── OPTIMIZATION_REPORT_V2.md  # 优化分析报告
│   └── v2.0_refactor_plan.md      # v2.0 重构计划
│
├── data/                          # 数据存储 (git ignored)
│   ├── stock_data.duckdb          # DuckDB 数据库
│   ├── price/                     # 价格数据
│   ├── finance/                   # 财务数据
│   ├── meta/                      # 元数据
│   └── v2/                        # v2 版本数据
│
├── _output/                       # 回测报告输出
└── logs/                          # 运行日志
```

### 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                 main_v2_production.py                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │  DataEngine   │  │ ExecutionEngineV3│  │ BacktestEngine │ │
│  │              │  │    v3            │  │     V3         │ │
│  │ +多线程下载   │  │                  │  │                │ │
│  │ +DuckDB存储  │  │ +T+1开盘价成交   │  │ +分层回测      │ │
│  │ +数据验证    │  │ +涨跌停顺延      │  │ +检查点恢复    │ │
│  │ +幸存者偏差  │  │ +ADV流动性限制   │  │ +未来函数治理  │ │
│  │ +增量更新    │  │ +100股取整       │  │ +分段分析      │ │
│  └──────┬───────┘  │ +印花税时间感知  │  └───────┬────────┘ │
│         │          └────────┬─────────┘          │          │
│         │                   │                    │          │
│  ┌──────┴──────┐   ┌───────┴────────┐  ┌───────┴────────┐ │
│  │DataValidator │   │TradingRules    │  │  Performance   │ │
│  │             │   │                │  │  Analyzer      │ │
│  │ +多源交叉    │   │ +涨跌停板      │  │                │ │
│  │   验证      │   │ +板块识别      │  │ +夏普/IR/Alpha  │ │
│  └─────────────┘   │ +交易日历      │  │ +Brinson归因   │ │
│                    └────────────────┘  └────────────────┘ │
│                                                             │
│  ┌──────────────┐  ┌──────────────────┐                    │
│  │PITDataAligner│  │ SurvivorshipBias │                    │
│  │             │  │    Handler       │                    │
│  │ +时点数据    │  │                  │                    │
│  │   对齐      │  │ +退市股票处理    │                    │
│  └─────────────┘  │ +ST状态过滤     │                    │
│                    └────────────────┘                    │
└─────────────────────────────────────────────────────────────┘
                           │
                    DuckDB + Parquet
```

---

## 快速开始

### 环境要求

- Python 3.9+
- 网络连接（首次运行需下载 A 股数据）

### 安装

```bash
# 克隆项目
git clone https://github.com/your-username/stock_data_project.git
cd stock_data_project

# 安装依赖
pip install -r requirements.txt
```

### 运行回测

```bash
# 运行默认回测 (2022-01-01 ~ 2024-12-31)
python main_v2_production.py
```

### 运行测试

```bash
# 运行全部测试
python run_tests.py

# 详细输出
python run_tests.py -v

# 只运行核心测试
python run_tests.py test_core

# 只运行生产级测试
python run_tests.py test_production
```

---

## 核心模块

### 1. DataEngine — 数据引擎

统一数据管理入口，负责数据获取、存储和验证。

```python
from scripts.data_engine import DataEngine

engine = DataEngine()

# 增量更新全市场数据（多线程，自动重试）
engine.incremental_update(max_workers=12)

# 获取指定日期行情
df = engine.get_daily_quotes(trade_date='2024-06-30')

# 获取股票列表（含退市）
stocks = engine.get_stock_list(include_delisted=True)

# 板块识别
from scripts.data_engine import detect_board, detect_limit
board = detect_board('300001')      # → '创业板'
limit = detect_limit('688001')      # → 0.20 (科创板 20%)
```

**关键能力：**

| 功能 | 说明 |
|------|------|
| 多线程下载 | `max_workers=12` 并发，指数退避重试 |
| 双源切换 | AkShare 主力 + Baostock 备援 |
| DuckDB 存储 | 列式存储，千万级查询 <1s |
| UPSERT | 重复数据自动覆盖，保证幂等 |
| 复权因子监测 | 变化 >5% 自动记录日志 |
| 断点续传 | 检测最新数据完整性，损坏自动回退 |

### 2. ExecutionEngineV3 — 执行引擎

A 股交易规则的完整模拟。

```python
from scripts.execution_engine_v3 import ExecutionEngineV3

engine = ExecutionEngineV3(
    initial_cash=10_000_000,    # 初始资金 1000 万
    commission_rate=0.0003,     # 佣金 0.03%
    min_commission=5.0,         # 最低佣金 5 元
    slippage_rate=0.001,        # 滑点 0.1%
    adv_limit=0.1,              # ADV 流动性限制 10%
)

# 生成订单（自动处理 T+1、涨跌停、100 股取整）
orders = engine.generate_orders(
    target_weights={'000001.SZ': 0.05, '600519.SH': 0.08},
    prices={'000001.SZ': 12.5, '600519.SH': 1850.0},
    current_date=datetime(2024, 6, 30)
)

# 执行交易
trades = engine.execute(orders, prices, current_date)
```

**交易规则实现：**

| 规则 | 实现 |
|------|------|
| T+1 | 当日买入冻结，次日开盘释放 |
| 涨跌停顺延 | 不可交易时自动顺延至下一交易日 |
| 100 股取整 | 买入/卖出数量向下取整到 100 的倍数 |
| ADV 限制 | 单笔不超过 20 日均成交量的 10% |
| 印花税 | 2023-08-28 前后自动区分税率 |
| 待结算资金 | 已卖出但 T+1 未到账的资金不参与买入 |

### 3. BacktestEngineV3 — 回测引擎

完整的生产级回测框架。

```python
from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig

config = BacktestConfig(
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2024, 12, 31),
    initial_capital=10_000_000,
)

engine = ProductionBacktestEngine(config=config, data_engine=data)

# 定义策略函数
def my_strategy(factor_data, date):
    """输入因子数据，输出目标权重字典"""
    if factor_data.empty:
        return {}
    # 等权买入因子排名前 50 的股票
    n = min(50, len(factor_data))
    weight = 1.0 / n
    return {sym: weight for sym in factor_data.index[:n]}

# 运行回测
result = engine.run(my_strategy)
```

**回测特性：**

- **检查点恢复** — 回测状态定期序列化，崩溃后可继续
- **分段分析** — 按年度分解绩效，识别极端行情表现
- **每日记录** — 完整的每日净值、持仓、交易流水

### 4. TradingRules — A 股交易规则

```python
from scripts.trading_rules import AShareTradingRules, TradingCalendar

# 板块识别（委托到 data_engine.detect_board，消除重复逻辑）
board = AShareTradingRules.get_board('688001')   # → BOARD_STAR
board = AShareTradingRules.get_board('300001')   # → BOARD_CHINEXT
board = AShareTradingRules.get_board('600519')   # → BOARD_MAIN
board = AShareTradingRules.get_board('430047')   # → BOARD_BSE

# 涨跌停价格
limit = AShareTradingRules.get_price_limit('300001', datetime(2024, 6, 30))  # → 0.20

# 交易日历
cal = TradingCalendar()
cal.is_trading_day(datetime(2024, 10, 1))      # → False (国庆)
cal.is_trading_day(datetime(2024, 10, 8))      # → True
next_day = cal.next_trading_day(datetime(2024, 9, 30))  # → 2024-10-08

# 覆盖 2018-2027 年 A 股节假日
```

### 5. PITDataAligner — 时点数据对齐

消除财务数据的未来函数。

```python
from scripts.pit_aligner import PITDataAligner

aligner = PITDataAligner()
aligner.load(financial_df)  # 包含 ann_date 列

# 获取指定日期可用的财务数据（严格 <= ann_date）
roe = aligner.get_factor('roe', datetime(2024, 6, 30))
# 只返回 2024-06-30 之前已公告的 ROE 数据

# 验证 PIT 有效性
is_valid = aligner.validate('roe', [datetime(2024, 3, 31), datetime(2024, 6, 30)])
```

### 6. PerformanceAnalyzer — 绩效分析

```python
from scripts.performance import EnhancedPerformanceAnalyzer

analyzer = EnhancedPerformanceAnalyzer(risk_free=0.03)
metrics = analyzer.calculate(
    returns=strategy_returns,
    benchmark=hs300_returns,
    turnover=turnover_series
)

print(f"年化收益: {metrics.annual_return:.2%}")
print(f"夏普比率: {metrics.sharpe_ratio:.2f}")
print(f"最大回撤: {metrics.max_drawdown:.2%}")
print(f"信息比率: {metrics.information_ratio:.2f}")
print(f"Alpha:    {metrics.alpha:.2%}")
print(f"Beta:     {metrics.beta:.2f}")
print(f"最长回撤恢复天数: {metrics.max_dd_days}")

# Brinson 归因分析
from scripts.performance import BrinsonAttribution
attr = BrinsonAttribution().analyze(
    pw=portfolio_weights, pr=portfolio_returns,
    bw=benchmark_weights, br=benchmark_returns
)
# → {'total': 0.03, 'allocation': 0.01, 'selection': 0.015, 'interaction': 0.005}
```

---

## 回测流程

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  数据准备    │ →  │  因子计算    │ →  │  信号生成    │ →  │  订单执行    │
│             │    │             │    │             │    │             │
│ 下载全市场   │    │ 使用T-1及   │    │ 因子排序     │    │ T+1开盘价    │
│ 日线数据    │    │ 之前数据     │    │ 分组打分     │    │ 成交         │
│ 财务数据    │    │ PIT对齐     │    │ 目标权重     │    │ 涨跌停顺延   │
│ 节假日历    │    │ 去极值      │    │             │    │ 100股取整    │
│             │    │ 中性化      │    │             │    │ ADV限制     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ↓
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  报告输出    │ ←  │  绩效评估    │ ←  │  风险分析    │ ←  │  每日结算    │
│             │    │             │    │             │    │             │
│ 净值曲线    │    │ 年化收益    │    │ 最大回撤    │    │ 更新持仓    │
│ 分组收益    │    │ 夏普比率    │    │ 回撤恢复期   │    │ T+1冻结释放 │
│ 归因分析    │    │ 信息比率    │    │ Beta/Alpha  │    │ 待结算资金  │
│ IC分析      │    │ Calmar      │    │ Brinson归因  │    │ 检查点保存  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### 调仓时序（关键）

```
T-1 日之前的数据 → T 日收盘后计算因子 → T 日生成目标权重 → T+1 日开盘执行交易
                      ↑                                        ↑
                 严格使用历史数据                          开盘价 + 滑点成交
                 不可使用 T 日收盘价                      涨停股自动顺延
```

---

## 未来函数治理

回测中最常见也最致命的问题就是未来函数（Look-ahead Bias）。本框架从三个维度彻底消除：

### 1. 财务数据 PIT 对齐

```python
# ❌ 错误做法：使用 end_date（报告期）
SELECT roe FROM financial_data WHERE end_date = '2024-03-31'
# 2024Q1 财报可能在 2024-04-30 才公告，T 日时根本不可用！

# ✅ 正确做法：使用 ann_date（公告日）
SELECT roe FROM financial_data WHERE ann_date <= '2024-03-31'
# 只使用 T 日之前已公告的数据
```

### 2. 因子计算时间窗口

```python
# ❌ 错误做法
momentum = prices.pct_change(20).loc['2024-06-30']  # 可能包含未来数据

# ✅ 正确做法
hist = prices[prices.index <= '2024-06-30']
momentum = hist.pct_change(20).iloc[-1]  # 严格只用历史数据
```

### 3. 交易执行时序

```python
# T 日收盘后生成信号 → T+1 日开盘执行
# 成交价 = 开盘价 + 滑点（非收盘价）
# 涨停股无法买入 → 顺延至下一个可交易日
```

---

## A股交易规则实现

### 涨跌停板制度

| 板块 | 代码前缀 | 涨跌停幅度 |
|------|---------|-----------|
| 主板 | 60xxxx, 00xxxx | ±10% |
| 创业板 | 300xxx | ±20% |
| 科创板 | 688xxx | ±20% |
| 北交所 | 8xxxxx / 4xxxxx | ±30% |

### 交易成本

| 费用 | 买入 | 卖出 | 说明 |
|------|------|------|------|
| 佣金 | ✓ | ✓ | max(金额 × 0.03%, 5 元) |
| 印花税 | ✗ | ✓ | 2023-08-28 前: 0.1%；后: 0.05% |
| 滑点 | ✓ | ✓ | 默认 0.1% |

### T+1 持仓冻结

```
第 T 日：
  1. 释放前日买入冻结 → available_shares = shares
  2. 执行当日卖出 → available_shares 减少
  3. 执行当日买入 → shares 增加，available_shares 不变（新买入冻结）
  4. 检查点保存 → 持仓状态序列化

第 T+1 日：
  1. 释放 T 日买入冻结 → T 日买入的股票变为可卖
  2. 重复上述流程
```

---

## 绩效分析

### 核心指标

| 指标 | 说明 | 计算方式 |
|------|------|---------|
| 年化收益率 | 策略年化回报 | empyrical.annual_return |
| 年化波动率 | 收益率标准差 | empyrical.annual_volatility |
| 夏普比率 | 风险调整收益 | (R-Rf) / σ |
| 最大回撤 | 最大峰谷跌幅 | empyrical.max_drawdown |
| Calmar 比率 | 收益/最大回撤 | 年化收益 / 最大回撤 |
| 信息比率 | 超额收益/跟踪误差 | E(Rp-Rb) / TE |
| 跟踪误差 | 与基准偏离 | σ(Rp-Rb) × √252 |
| Beta | 市场敏感度 | 滚动回归 |
| Alpha | 超额收益 | CAPM 残差 |
| 最长回撤恢复 | 回撤持续天数 | 向量化计算 |
| 平均换手率 | 月均调仓比例 | 单边换手 / 组合市值 |

### Brinson 归因

将组合超额收益分解为：

```
超额收益 = 行业配置 + 个股选择 + 交互作用
```

---

## 数据源

| 数据源 | 用途 | 特点 |
|--------|------|------|
| **AkShare** | 主力数据源 | 免费、无需 API Key、覆盖全面 |
| **Baostock** | 备援数据源 | 证券宝开源、历史数据完整 |

**双源容错机制：**
1. AkShare 请求失败 → 指数退避重试（最多 3 次）
2. 重试仍失败 → 自动切换 Baostock
3. Baostock 也失败 → 记录错误日志，跳过该股票
4. 下载数据自动进行多源交叉验证

### 数据存储

```
DuckDB (主力) ←── SQL 查询、聚合分析、Window 函数
     ↕
Parquet  (导出) ←── 快速加载、长期归档、跨平台共享
```

| 格式 | 100 万行读取 | 100 万行写入 | 文件大小 |
|------|------------|------------|---------|
| CSV | 8.5s | 12s | 180MB |
| Parquet | 0.8s | 1.2s | 45MB |
| DuckDB | 0.3s | 0.5s | 50MB |

---

## 测试

```bash
# 运行全部测试
python run_tests.py

# 运行特定测试模块
python run_tests.py test_core
python run_tests.py test_data_engine
python run_tests.py test_production
```

### 测试覆盖

| 模块 | 覆盖状态 | 说明 |
|------|---------|------|
| data_classes.py | ✅ 完整 | Order, Trade, Position, CostConfig |
| data_engine.py | ✅ 良好 | 61 用例，含数据验证 |
| trading_rules.py | ✅ 良好 | 涨跌停、交易日历 |
| execution_engine_v3.py | ⚠️ 中等 | T+1 流程、风控逻辑 |
| survivorship_bias.py | ⚠️ 中等 | ST 状态、退市处理 |
| backtest_engine_v3.py | ⚠️ 基础 | 检查点恢复、端到端 |

---

## 文档

| 文档 | 说明 |
|------|------|
| [PROJECT_OVERVIEW](docs/PROJECT_OVERVIEW.md) | 项目概述与核心功能详解 |
| [DEVLOG](docs/DEVLOG.md) | 开发日志 |
| [PATCH_NOTES](docs/PATCH_NOTES.md) | v1.1/v2.0 核心补丁说明 |
| [CODE_REVIEW](docs/CODE_REVIEW.md) | 代码自检报告（10 个问题修复） |
| [VULNERABILITY_REPORT](docs/VULNERABILITY_REPORT.md) | 未来函数高危漏洞分析 |
| [OPTIMIZATION_REPORT_V2](docs/OPTIMIZATION_REPORT_V2.md) | P0-P3 优化分析报告 |
| [v2.0_refactor_plan](docs/v2.0_refactor_plan.md) | v2.0 工业级重构计划 |

---

## 技术栈

| 类别 | 技术 | 说明 |
|------|------|------|
| 语言 | Python 3.9+ | 类型注解、dataclass |
| 数据获取 | AkShare / Baostock | 免费 A 股数据源 |
| 数据存储 | DuckDB | 列式数据库，OLAP 优化 |
| 数据处理 | pandas / numpy | DataFrame 操作、向量化计算 |
| 统计分析 | scipy | 回归、统计检验 |
| 绩效计算 | empyrical | 专业量化绩效指标 |
| 可视化 | matplotlib / seaborn | 图表生成 |
| 日志 | loguru | 结构化日志 |
| 测试 | pytest | 单元测试框架 |

---

## 版本历史

### v3.0 (2026-04-06)
- 修复 5 个 P0 运行时 Bug（位运算死循环、NameError、TypeError、持仓丢失）
- 拆分 DataValidator 到独立模块
- 消除板块识别重复逻辑
- 完善 T+1 持仓冻结逻辑
- 添加 2027 年预估节假日

### v2.0 (2026-04-05)
- 准实盘仿真升级
- 开盘价成交 + 滑点模型
- 一字板过滤 + 单票独立执行
- IC 衰减分析
- 印花税时间感知

### v1.1 (2026-04-05)
- 多线程数据下载 + Baostock 备援
- 断点续传 + UPSERT
- 幸存者偏差处理
- 涨跌停顺延逻辑

### v1.0 (2026-04-03)
- 项目初始化
- 基础回测框架
- 数据引擎 + 因子计算

---

## 许可证

MIT License

---

<div align="center">

**Built with ❤️ for quantitative finance**

</div>
