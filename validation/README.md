# 验证数据系统（Validation Data System）

## 概述

本目录包含用于验证数据质量和回测引擎正确性的 ground truth 数据。

## 目录结构

```
validation/
├── ground_truth/              # 已确认正确数据（Parquet 格式）
│   ├── quotes.parquet         # 行情验证集（10只股票 × 5个关键日期）
│   ├── corporate_actions.parquet  # 除权除息验证集
│   └── financials.parquet     # 财务验证集（可选）
├── expected_results/          # 回测期望输出（JSON 格式）
│   ├── portfolio_001.json     # 价值投资组合
│   └── portfolio_002.json     # 不同市场环境组合
├── scripts/                   # 生成和验证脚本
│   ├── generate_ground_truth.py
│   ├── run_validation.py
│   └── compare_results.py
└── reports/                   # 验证报告输出
```

## Ground Truth 选股原则

### quotes.parquet 验证集

**股票池（10只）：**
| 类别 | 股票代码 | 股票名称 |
|------|---------|---------|
| 大盘 | 000001 | 平安银行 |
| 大盘 | 600000 | 浦发银行 |
| 小盘 | 002415 | 海康威视 |
| 小盘 | 300059 | 东方财富 |
| 创业板 | 300750 | 宁德时代 |
| 创业板 | 300014 | 亿纬锂能 |
| 科创板 | 688981 | 中芯国际 |
| 科创板 | 688111 | 金山办公 |
| 北交所 | 832566 | 梓撞科技 |
| 北交所 | 430047 | 诺思兰德 |

**关键日期（5个）：**
1. **正常交易日**: 2024-01-15（无特殊事件）
2. **除权除息日**: 2024-06-20（分红派息）
3. **涨跌停日**: 2024-03-15（触及涨停/跌停）
4. **停牌日**: 2024-09-10（停牌复牌）
5. **上市首日**: 2023-01-03（新股上市）

### corporate_actions.parquet 验证集

已人工核实的除权除息记录，用于验证 adj_factor 计算正确性。

## 使用方法

### 1. 生成 Ground Truth 数据

```bash
cd validation/scripts
python generate_ground_truth.py --interactive
```

交互式生成验证数据，支持手动确认每条记录。

### 2. 运行验证

```bash
# 完整验证（数据 + 回测）
python run_validation.py --full

# 仅验证数据质量
python run_validation.py --data-only

# 仅验证回测引擎
python run_validation.py --backtest-only
```

### 3. 查看报告

验证报告输出到 `validation/reports/validation_YYYYMMDD.json`

## 容差规则

| 指标 | 允许误差 | 说明 |
|------|---------|------|
| open/high/low/close | ±0.5% | 价格相对误差 |
| adj_factor | ±0.01 | 复权因子绝对误差 |
| volume | ±1% | 成交量相对误差 |
| 收益率 | ±0.1% | 回测收益相对误差 |
| 夏普比率 | ±0.1 | 夏普绝对误差 |

## 集成到 CI/CD

建议在每次 git commit 前运行验证：

```bash
# .git/hooks/pre-commit
cd stock_data_project/validation/scripts
python run_validation.py --data-only
```

## 更新日志

- 2026-04-13: 初始版本，设计 A+B 合并方案

---

*最后更新: 2026-04-13*
