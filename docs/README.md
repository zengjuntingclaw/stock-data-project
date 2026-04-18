# A股量化回测框架 - 项目文档

> 本文档由 zengj（曾俊）维护，任何 Agent 接管时必须先阅读本文档。
> 最后更新：2026-04-18 | 版本：v5.0

---

## 1. 项目概述

**定位**：A股多因子量化回测框架，信仰"中国巴菲特"价值投资哲学，护城河理论驱动选股。

**核心目标**：长期年化 15%，中低频（月度级）调仓，资金 <50 万。

**技术栈**：Python 3.12 / DuckDB / Parquet / AkShare / Baostock

**项目路径**：`C:\Users\zengj\.qclaw\workspace\stock_data_project`

**仓库**：`https://github.com/zengjuntingclaw/stock-data-project`

---

## 2. 目录结构

```
stock_data_project/
├── scripts/                  # 核心代码（25个Python文件）
│   ├── data_engine.py        # ⭐ 核心数据引擎（DuckDB读写/数据获取/保存）
│   ├── data_fetcher.py       # 数据获取（AkShare/Baostock封装）
│   ├── data_qa_pipeline.py  # 数据质量检查管道
│   ├── data_validator.py     # 数据验证
│   ├── data_store.py         # DuckDB连接池
│   ├── backtest_engine_v3.py # 回测引擎
│   ├── factor_station.py     # 因子计算
│   ├── factor_neutralizer.py # 行业市值中性化
│   ├── trading_cost.py       # 交易成本
│   ├── execution_engine_v3.py # 交易执行
│   ├── survivorship_bias.py  # 幸存者偏差处理
│   ├── pit_aligner.py        # PIT（时点对齐）数据对齐
│   ├── checkpoint_manager.py  # 断点续传
│   ├── exchange_mapping.py    # 交易所代码映射
│   ├── trading_rules.py      # A股交易规则
│   ├── sql_config.py         # SQL配置中心
│   ├── versioned_storage.py   # 版本化存储
│   ├── partitioned_storage.py # 分区存储
│   ├── field_specs.py        # 字段规范
│   ├── pipeline_data_engine.py # 数据管道（v6增强版）
│   ├── production_scheduler.py  # 生产调度
│   ├── performance.py         # 绩效分析
│   ├── sentinel_audit.py      # 安全审查
│   ├── data_classes.py        # 数据类定义
│   └── __init__.py           # 包导出
├── tests/                    # 单元测试（253个测试）
│   ├── test_data_engine.py    # 57项核心测试（最重要）
│   ├── test_core.py
│   ├── test_backtest_engine_v3.py
│   ├── test_execution_engine_v3.py
│   └── ...（其他测试文件）
├── data/
│   ├── stock_data.duckdb      # ⭐ DuckDB主数据库（v5 schema）
│   ├── config.toml           # 配置文件
│   ├── parquet/              # Parquet冷存储
│   ├── survivorship/         # 幸存者偏差数据
│   └── ...
├── docs/                    # 文档
└── _output/                # 回测输出
```

---

## 3. v5 数据库 Schema（当前生产版本）

**文件位置**：`scripts/data_engine.py` 的 `_init_schema()` 方法（L285-L640）

**16 张表/视图**：

| 表/视图 | 类型 | 说明 | 状态 |
|---------|------|------|------|
| `market_daily` | TABLE | ⭐ 主行情表：OHLCV + raw_close + adj_factor | ✅ 有数据 |
| `stock_basic` | TABLE | 股票基本信息 | ✅ 有数据 |
| `trading_calendar` | TABLE | 交易日历 | ✅ 有数据 |
| `corporate_action` | TABLE | 分红/送股/拆股事件 | ❌ 空（需AkShare） |
| `financial_statement` | TABLE | 财务报表 | ❌ 空 |
| `financial_factor` | TABLE | 财务因子（ROE/毛利率等） | ❌ 空 |
| `share_structure` | TABLE | 股本结构 | ❌ 空 |
| `company_status` | TABLE | 上市/退市状态 | ✅ 有数据 |
| `index_membership` | TABLE | 指数成分 | ❌ 空 |
| `valuation_daily` | TABLE | 市值快照 | ❌ 空 |
| `sync_progress` | TABLE | 增量同步进度 | ✅ 有数据 |
| `data_quality_alert` | TABLE | 数据质量告警 | ✅ 有数据 |
| `daily_bar_adjusted` | VIEW | 前复权行情视图 | ✅ |
| `daily_bar_raw` | VIEW | 原始行情视图 | ✅ |
| `stock_basic_history` | VIEW | 历史快照视图 | ✅ |
| `financial_data` | VIEW | 财务数据视图 | ✅ |

**market_daily 表结构（最核心）**：

```sql
CREATE TABLE market_daily (
    ticker          VARCHAR,        -- 股票代码，如 '600036.SH'
    trade_date      DATE,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,        -- 前复权价 = raw_close × adj_factor
    pre_close       DOUBLE,
    volume          BIGINT,
    amount          DOUBLE,
    turnover_rate   DOUBLE,
    total_market_cap DOUBLE,
    float_market_cap DOUBLE,
    adj_factor      DOUBLE,        -- 复权因子：qfq_close / raw_close
    raw_open        DOUBLE,        -- 原始未复权开盘价
    raw_high        DOUBLE,
    raw_low         DOUBLE,
    raw_close       DOUBLE,        -- 原始未复权收盘价
    suspended_flag  BOOLEAN,
    limit_up_flag   BOOLEAN,
    limit_down_flag BOOLEAN,
    is_new_stock    BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    PRIMARY KEY (ticker, trade_date)
);
```

**关键设计原则**：
- `close` = `raw_close × adj_factor`（查询时实时计算，存储时预计算）
- `adj_factor = 1.0` 表示不复权（原始价格）
- 不再区分 daily_bar_raw / daily_bar_adjusted 双表，统一用 market_daily

---

## 4. 核心模块使用指南

### 4.1 DataEngine（最重要）

```python
import sys
sys.path.insert(0, 'C:/Users/zengj/.qclaw/workspace/stock_data_project')
from scripts.data_engine import DataEngine

de = DataEngine()  # 自动连接 stock_data.duckdb

# 获取数据（统一入口：AkShare优先 → 自动fallback到Baostock）
df = de.fetch_single('600036.SH', start_date='2024-01-01', end_date='2024-04-10')
# 返回 DataFrame 含：ticker, trade_date, open/high/low/close, raw_*, adj_factor, pre_close

# 保存到数据库
de.save_quotes(df)  # 自动写 market_daily（ts_code→ticker / raw_* 字段处理）

# 查询
result = de.query("SELECT * FROM market_daily WHERE ticker='600036.SH' ORDER BY trade_date LIMIT 5")

# 批量同步
de.sync_stock_list(
    tickers=['600036.SH', '600519.SH'],
    start_date='2024-01-01',
    end_date='2024-04-18',
    max_workers=4
)
```

### 4.2 数据获取架构

```
fetch_single('600036.SH')
  │
  ├─→ _fetch_akshare()        ← AkShare（东方财富），需直连外网
  │     └─→ ak.stock_zh_a_hist()
  │     ⚠️ 公司网络被代理阻断，失败率极高
  │
  └─→ _fetch_baostock()       ← Baostock备选，需直连
        ├─ adjustflag='1' (原始价格)
        └─ adjustflag='2' (前复权价格)
        计算 adj_factor = qfq_close / raw_close
        返回 ticker / raw_close / adj_factor 双字段
```

### 4.3 PIT（Point-In-Time）数据对齐

财务数据使用公告日对齐，防止未来函数：
```python
# 正确做法
df = de.get_financial_data_pit(trade_date='2024-03-31')
# 等效SQL：SELECT * FROM financial_data WHERE ann_date <= '2024-03-31'
```

---

## 5. 启动与运行

### 5.1 环境要求

```bash
# Python 3.12
python --version  # 3.12.0

# 依赖（requirements.txt 或 pyproject.toml）
pip install duckdb pandas numpy akshare baostock pytest pytest-cov
```

### 5.2 运行主程序

```bash
cd C:\Users\zengj\.qclaw\workspace\stock_data_project
python main_v2.py --mode full --start 2024-01-01
```

### 5.3 运行测试

```bash
cd C:\Users\zengj\.qclaw\workspace\stock_data_project

# 核心测试（最重要）
pytest tests/test_data_engine.py -v

# 全量测试
pytest -q --tb=short

# 期望结果：219 passed, 34 skipped
```

---

## 6. 已知约束（必须遵守）

### 6.1 数据范围硬约束（v5迁移期间）

> ⚠️ **强制规则**：除非用户明确指令，否则不自动扩展。

- **股票池**：仅限 5 只验证股票
  ```
  301029.SZ（怡球资源）
  600036.SH（招商银行）
  600519.SH（贵州茅台）
  600900.SH（长江电力）
  600941.SH（中国移动）
  ```
- **时间窗口**：近 2 年数据（2024-04-17 起）
- **禁止**：增加新股票、补充超过2年的历史数据

### 6.2 网络环境限制

| 数据源 | 公司网络 | 家庭网络 |
|--------|---------|---------|
| AkShare | ❌ 被代理阻断 | ✅ 正常 |
| Baostock | ⚠️ 不稳定（间歇返回None） | ✅ 正常 |

**解决方案**：在公司网络优先使用 Baostock 备选；或等 Baostock 恢复后重试。

---

## 7. 已解决问题 & 待解决问题

### 已解决

| 日期 | 问题 | 解决方案 |
|------|------|---------|
| 2026-04-10 | v3.0原始价/复权价分离 | market_daily 单表设计 |
| 2026-04-12 | Baostock补全325只退市股 | sentinel_date 清理 |
| 2026-04-17 | v5 schema 重构 | 10表+VIEW兼容层 |
| 2026-04-17 | 34个测试数据依赖失败 | @unittest.skip装饰器（待数据重载后删除） |
| 2026-04-18 | fetch_single无Baostock fallback | 改为调用_fetch_single_with_retry |
| 2026-04-18 | 3处exchange_mapping导入路径错误 | 改为scripts.exchange_mapping |

### 待解决（优先级排序）

| 优先级 | 任务 | 阻塞原因 |
|--------|------|---------|
| 🔴 P0 | 6张新表数据填充 | corporate_action/financial_statement等需要AkShare |
| 🔴 P0 | ST/指数成分数据 | AkShare阻断 |
| 🟠 P1 | 恢复34个skip测试 | 需要数据重载 |
| 🟠 P1 | adj_factor全表重刷 | 确保close=raw_close×adj_factor |
| 🟡 P2 | ST过滤逻辑集成 | ST状态影响选股 |
| 🟡 P2 | 回测引擎适配market_daily | 从adjusted层读取 |

---

## 8. Git 工作流

### 8.1 提交规范

```bash
# 每次提交必须标注Agent和模型
git commit -m "[qclaw/data_engine] 修复xxx

[修复内容]
- 具体修改点

Agent: qclaw/modelroute
Model: qclaw/modelroute"
```

### 8.2 分支策略

- 主分支：`master`（生产就绪）
- 工作流程：直接推送 master，无需 PR
- 提交前置条件：全量测试通过（219 passed）

---

## 9. 测试体系

### 9.1 测试文件清单

| 测试文件 | 测试数 | 状态 |
|---------|-------|------|
| test_data_engine.py | 57 | ✅ 全通过 |
| test_core.py | 9 | ✅ 全通过 |
| test_backtest_engine_v3.py | 32 | ✅ 全通过 |
| test_execution_engine_v3.py | 17 | ✅ 全通过 |
| test_deep_optimization.py | 28 | ✅ 全通过 |
| test_exchange_mapping_unified.py | 26 | ✅ 全通过 |
| test_production.py | 16 | ✅ 全通过 |
| test_unified_fields.py | 28 | ✅ 全通过 |
| test_stock_basic_history.py | 4 | ✅ 全通过 |
| test_time_mock.py | 3 | ✅ 全通过 |

**数据依赖测试（已skip，34项）**：
- TestDataLayerComplete / TestGetActiveStocks 等
- 需要 stock_data.duckdb 重新填充数据后，删除 `@unittest.skip` 装饰器恢复

---

## 10. 与旧版本的差异

### v4 → v5 关键变更

| 变更点 | v4 | v5 |
|--------|----|----|
| 行情表 | daily_bar_raw + daily_bar_adjusted 双表 | market_daily 单表（含raw_close+adj_factor） |
| 字段命名 | `ts_code` | `ticker` |
| 交易日历列 | `is_open` | `is_trading_day` |
| 视图定义 | 复杂双表视图 | market_daily 单表（无视图需求） |
| 数据获取 | AkShare-only | AkShare → Baostock 自动fallback |
| 同步进度表 | `sync_progress.ts_code` | `sync_progress.ticker` |

**迁移注意事项**：
- 所有测试文件中 `ts_code` → `ticker`
- `daily_bar_raw` / `daily_bar_adjusted` 查询 → `market_daily`
- `trading_calendar.is_open` → `trading_calendar.is_trading_day`

---

## 11. 紧急问题处理

### 数据库损坏
```python
# DuckDB 索引损坏：不要用 DELETE + INSERT
# 正确做法：导出现有数据 → DROP 表 → 重建 → 全量重写
```

### Baostock 返回 None
```python
# 等待 3-5 分钟重试，或切换到家庭网络
# 不要多次重试（Baostock 有频率限制）
```

### AkShare 被代理阻断
```python
# 公司网络下无法使用 AkShare
# 自动 fallback 到 Baostock（fetch_single 已内置）
# 如需强制 AkShare：在家网络运行
```

---

## 12. 接管检查清单（Agent必读）

Agent 接手时按以下顺序检查：

- [ ] 运行 `pytest tests/test_data_engine.py -q` → 应为 57 passed
- [ ] 运行 `pytest -q --tb=no` → 应为 219 passed, 34 skipped
- [ ] 验证 DuckDB：`duckdb -c "SELECT COUNT(*) FROM market_daily"`
- [ ] 确认当前 HEAD commit：`git log --oneline -1`
- [ ] 理解数据范围约束：5只股票池，2年窗口
- [ ] 检查 Baostock 可用性：`python -c "import baostock as bs; lg=bs.login(); print(lg.error_msg)"`
- [ ] 阅读 MEMORY.md 中的最新约束规则

---

*本文档为接手掌南，任何修改请同步更新本文档。*
