# A股多因子回测框架 - 项目概述

## 项目完成状态

✅ **已完成** - 2026-04-03

## 项目结构

```
stock_data_project/
├── main.py                      # 主程序入口
├── run.bat                      # Windows启动脚本
├── example.py                   # 使用示例
├── requirements.txt             # Python依赖
├── README.md                    # 完整文档
├── scripts/                     # 核心模块
│   ├── config.py               # 配置参数
│   ├── data_fetcher.py         # 数据获取（AkShare）
│   ├── data_manager.py         # 数据管理（DuckDB）
│   ├── factor_processor.py     # 因子计算与预处理
│   ├── backtester.py           # 回测引擎
│   └── report_generator.py     # 报告生成
├── data/                        # 数据存储
│   ├── stock_data.duckdb       # DuckDB数据库
│   ├── price/                  # Parquet价格数据
│   ├── finance/                # 财务数据
│   └── meta/                   # 元数据
├── _output/                     # 回测报告输出
├── logs/                        # 运行日志
└── docs/                        # 文档
    ├── README.md               # 使用说明
    └── DEVLOG.md               # 开发日志
```

## 核心功能

### 1. 数据获取模块 (data_fetcher.py)
- ✅ 使用 AkShare 获取数据（免费，无需API Key）
- ✅ 支持增量更新
- ✅ 自动处理前复权/后复权
- ✅ 标记停牌期
- ✅ 获取交易日历

### 2. 数据管理模块 (data_manager.py)
- ✅ DuckDB 数据库管理
- ✅ 交易日历查询
- ✅ 每月最后交易日查询
- ✅ 可交易股票过滤（ST、停牌、涨跌停、上市天数）
- ✅ 数据校验

### 3. 因子处理模块 (factor_processor.py)
- ✅ 5个基础因子：
  - 动量因子（20日收益率）
  - 波动率因子（20日标准差）
  - 市值因子（流通市值）
  - 反转因子（5日反转）
  - 流动性因子（Amihud指标）
- ✅ 预处理流水线：
  1. MAD去极值
  2. 行业中性化
  3. 市值中性化
  4. Z-Score标准化
- ✅ IC计算（Rank IC、ICIR）

### 4. 回测引擎 (backtester.py)
- ✅ 月度调仓
- ✅ 分层回测（可配置分组数）
- ✅ 多空组合
- ✅ 交易成本（佣金、印花税、滑点）
- ✅ 绩效计算：
  - 年化收益率
  - 年化波动率
  - 夏普比率
  - 最大回撤
  - 胜率
  - Calmar比率

### 5. 报告生成模块 (report_generator.py)
- ✅ 净值曲线图
- ✅ 分组收益柱状图
- ✅ 多空收益分析图
- ✅ IC分析图
- ✅ 文本报告
- ✅ 控制台摘要

## 关键设计

### 未来函数规避
```python
# 财务数据：使用 ann_date（公告日）而非 end_date（报告期）
SELECT * FROM financial_data 
WHERE ann_date <= '2023-06-30'  -- 只使用已公告的数据

# 调仓时机：T日计算，T+1日执行
rebalance_date = "2023-06-30"   # T日：计算因子
execution_date = "2023-07-03"   # T+1日：执行交易
```

### 因子预处理流水线
```python
# 标准流程
factor = raw_factor
factor = winsorize_mad(factor, threshold=3.0)      # 去极值
factor = neutralize_industry(factor, industry_map)  # 行业中性化
factor = neutralize_market_cap(factor, market_cap)  # 市值中性化
factor = standardize_zscore(factor)                 # 标准化
```

### 月度调仓逻辑
```python
# 1. 获取每月最后一个交易日
last_dates = dm.get_last_trade_date_of_month(start, end)

# 2. 在T日计算因子
for date in last_dates:
    factor = calc_factor(date)  # 使用T-1日及之前数据
    
# 3. 在T+1日开盘执行
execution_dates = get_next_trade_days(last_dates)
```

## 运行方式

### 快速开始
```bash
# Windows 双击运行
run.bat

# 或命令行
python main.py
```

### 运行示例
```bash
python example.py
```

### 自定义因子
```python
from scripts.data_manager import DataManager
from scripts.factor_processor import FactorProcessor
from main import run_factor_backtest

def my_factor(trade_date):
    dm = DataManager()
    fp = FactorProcessor(dm)
    
    # 计算因子...
    factor = ...
    
    # 预处理
    return fp.process_factor_pipeline(factor, industry_map, market_cap)

# 运行回测
results = run_factor_backtest(dm, "自定义因子", my_factor, "2023-01-01", "2024-01-01")
```

## 回测配置

```python
# scripts/config.py

BACKTEST_CONFIG = {
    "start_date": "2023-01-01",
    "initial_capital": 1_000_000,  # 初始资金100万
    "commission_rate": 0.0003,     # 佣金 0.03%
    "stamp_tax_rate": 0.001,       # 印花税 0.1%
    "slippage_rate": 0.001,        # 滑点 0.1%
    "min_trading_days": 60,        # 最少上市60天
}

FACTOR_CONFIG = {
    "mad_threshold": 3.0,          # MAD阈值
    "neutralize_industry": True,   # 行业中性化
    "neutralize_market_cap": True, # 市值中性化
    "standardize": True,           # 标准化
}
```

## 输出文件

每次回测生成以下文件（保存在 `_output/` 目录）：

1. `{因子名}_绩效表.csv` - 各分组绩效指标
2. `{因子名}_净值曲线.png` - 分组净值对比
3. `{因子名}_分组收益.png` - 年化收益/夏普比率柱状图
4. `{因子名}_多空收益.png` - 多空组合分析
5. `{因子名}_回测报告.txt` - 完整文本报告

## 性能指标

| 操作 | 时间 |
|------|------|
| 首次下载（沪深300，3年数据） | 10-30分钟 |
| 增量更新 | 1-5分钟 |
| 因子计算（单次） | 1-3秒 |
| 回测执行 | 1-5秒 |
| 报告生成 | 5-10秒 |

## 技术栈

- **Python**: 3.9+
- **数据获取**: AkShare（免费，无需注册）
- **数据存储**: DuckDB（列式数据库）
- **数据处理**: pandas, numpy
- **统计分析**: scipy
- **可视化**: matplotlib, seaborn
- **日志**: loguru

## 依赖库

```
pandas>=2.0.0
numpy>=1.24.0
akshare>=1.12.0
duckdb>=0.9.0
matplotlib>=3.7.0
seaborn>=0.12.0
loguru>=0.7.0
tqdm>=4.65.0
scipy>=1.10.0
```

## 注意事项

1. **首次运行** 会下载完整数据，耗时约10-30分钟
2. **AkShare请求频率** 间隔设为0.3秒，避免被封
3. **测试数据** 默认使用沪深300成分股
4. **数据更新** 每次运行自动增量更新
5. **报告位置** 保存在 `_output/` 目录

## 扩展方向

### 短期优化
- [ ] 添加沪深300基准对比
- [ ] 详细的换手率分析
- [ ] 因子相关性矩阵

### 中期扩展
- [ ] 更多因子（估值、质量、成长）
- [ ] 财务数据完整版
- [ ] 参数敏感性测试

### 长期规划
- [ ] 因子挖掘工具
- [ ] 机器学习因子
- [ ] 风险模型
- [ ] 组合优化

## License

MIT License

---

**项目创建时间**: 2026-04-03  
**框架版本**: v1.0.0  
**作者**: OpenClaw
