# Phase 5 全量数据质量验证方案

> **目标**：5只代表性股票的全量数据端到端验证
> **核心理念**：独立第三방核查，不信任任何单一数据源
> **策略**：价格数据走 Baostock × AkShare 双源交叉；财务指标走 Tushare / 东方财富 / 网易财经 三源核查

---

## 一、5只代表性股票选择

| # | 代码 | 名称 | 选择理由 |
|---|------|------|----------|
| 1 | 000001.SZ | 平安银行 | 主板蓝筹，2018至今多次分红派息 |
| 2 | 600519.SH | 贵州茅台 | 主板核心资产，高ROE代表 |
| 3 | 300750.SZ | 宁德时代 | 创业板注册制，2018-06上市 |
| 4 | 688981.SH | 中芯国际 | 科创板，2020-07上市，历史短 |
| 5 | 430685.BJ | 锦波生物 | 北交所，创新器械，波动大 |

---

## 二、验证维度分解

### 维度A：价格数据（行情）—— Baostock × AkShare 双源核查

#### A1. 原始价格 raw_close

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| A1a: 数据完整性 | 统计5只股票历史交易日数量 | 与实际交易日历误差 ≤ 5天 |
| A1b: 停牌日正确 | 对比 trade_calendar，停牌日 volume=0 | volume=0 且 close=前收盘 |
| A1c: OHLC 逻辑 | high≥low, high≥open/close, low≤open/close | 0 violations |
| A1d: 涨跌停判断 | 查历史涨跌停日期，手动核 | 误差 = 0 |
| A1e: 极值检测 | pct_chg > 20% or < -20%（主板） | 逐条人工确认 |

**执行**：对5只股票，对比 Baostock raw_close 与 AkShare stock_zh_a_hist raw_close
- 差异率 > 1% → 报警
- 差异率 > 5% → 标记为 FAIL，需根因追溯

#### A2. 复权因子 adj_factor

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| A2a: 归一化 | adj_factor[latest] = 1.0 | exactly 1.0 |
| A2b: 单调性 | adj_factor 单调递增（或不变） | 0 violations |
| A2c: Baostock 精确匹配 | adj_factor[i] = (raw[i]/qfq[i]) / (raw[latest]/qfq[latest]) | 误差 < 0.001% |
| A2d: 零除检查 | adj_factor > 0 且 < 1000 | 0 violations |

**执行**：
```python
adj_bs = df_qfq['close'].values / df_raw['close'].values
adj_norm = adj_bs / adj_bs[-1]  # 归一化到最新=1.0
error = abs(adj_stored - adj_norm) / adj_norm * 100
max(error) < 0.001%  # PASS
```

#### A3. 复权价格验证

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| A3a: 复权价 = raw × adj_factor | 从 DuckDB 读取 daily_bar_adjusted | 误差 < 0.01% |
| A3b: Baostock qfq 对比 | our qfq_close vs Baostock qfq_close | 误差 < 0.5% |
| A3c: AkShare 复权价对比 | our qfq_close vs AkShare 前复权 close | 误差 < 1% |
| A3d: 近期日期误差 | 2024-01-01 至 latest | 误差 < 0.1%（重点关注） |

**执行**：
```python
# our_qfq = raw × adj_factor
our_qfq = raw['close'] * adj_factor
bs_qfq = df_qfq['close']
ak_qfq = akshare_data['前复权 close']
print(f'vs Baostock: avg_err={abs(our_qfq - bs_qfq).mean():.4f}, max_err={abs(our_qfq - bs_qfq).max():.4f}')
print(f'vs AkShare:  avg_err={abs(our_qfq - ak_qfq).mean():.4f}, max_err={abs(our_qfq - ak_qfq).max():.4f}')
```

#### A4: 复权因子连续性验证（关键！）

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| A4a: 层间 gap 检验 | daily_bar_adjusted 层间 diff 应为 0（复权消除波动） | max gap < 0.5% |
| A4b: dividend event 边界 | 复权因子在事件日前后应有明显跳变 | 逐个人工确认 |
| A4c: 长期复权一致性 | 2018-2020 长期复权价 vs 2024-2026 | 相对误差 < 1% |

---

### 维度B：财务数据（基本面指标）—— 三源核查

#### B1. 数据来源说明

| 指标 | 来源 | 频率 | 滞后 |
|------|------|------|------|
| ROE | 净利润/净资产 | 季度 | 财报公布后 |
| ROA | 净利润/总资产 | 季度 | 财报公布后 |
| PE | 总市值/净利润（TTM） | 日频 | T+1 |
| PB | 总市值/净资产 | 日频 | T+1 |
| 营收增速 | (本期-上期)/上期 | 季度 | - |
| 净利润增速 | 同上 | 季度 | - |
| 毛利率 | (营收-成本)/营收 | 季度 | - |

#### B2. 三源核查方案

**数据源**：
- Source 1: Baostock `query_finance_stock_sina`（实时财务数据）
- Source 2: AkShare `stock_financial_analysis_indicator`
- Source 3: Tushare（需 token，有积分限制）

**核查方法**（以 ROE 为例）：
```python
# 对5只股票，抽查4个季度
quarters = ['2024Q2', '2024Q1', '2023Q4', '2023Q3']
for code in stocks:
    bs_roe  = get_baostock_roe(code, quarters)
    ak_roe  = get_akshare_roe(code, quarters)
    # ts_roe  = get_tushare_roe(code, quarters)  # 备用
    
    # 三源取中位数（去除极端值）
    median_roe = np.nanmedian([bs_roe, ak_roe], axis=0)
    
    # 与数据库比对
    db_roe = get_db_roe(code, quarters)
    err = abs(db_roe - median_roe) / median_roe * 100
    if err > 5:
        FAIL(f'{code} ROE@{quarter} 差异 {err:.2f}%')
```

#### B3. PE/PB 市盈率验证

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| B3a: PE 合理范围 | PE > 0 且 < 500（剔除负值和异常） | 99% 在范围内 |
| B3b: PB 合理范围 | PB > 0 且 < 50 | 99% 在范围内 |
| B3c: PE × PB 合理性 | 与同行业均值对比 | 偏差 < 50% |
| B3d: 公告日 PIT | ann_date ≤ as_of_date（无未来函数） | 0 violations |

#### B4. ROE/ROA 合理性验证

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| B4a: ROE 范围 | 0 < ROE < 50%（银行可至100%+） | 95% 在合理范围 |
| B4b: 连续性 | 季度间 ROE 变化 < 30%（排除大洗澡） | 逐条人工确认 |
| B4c: 与净利润符号一致 | 亏损时 ROE 应为负 | 0 violations |

---

### 维度C：时点一致性（PIT — Point In Time）

#### C1. 公告日 vs 交易日对齐

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| C1a: 财务数据可见性 | ann_date 之后才能在 as_of_date 查询到 | 0 未来函数违规 |
| C1b: PIT 查询边界 | 查询 T 日数据，只应看到 ann_date ≤ T 的财务数据 | 0 violations |
| C1c: stock_basic_history PIT | 2020-01-01 查到的股票列表 vs 实际上市股票 | 误差 = 0 |

**执行伪代码**：
```python
# 对 000001.SZ，在 2024-01-01 时点查询
pit_data = query("""
    SELECT * FROM financial_data
    WHERE ts_code='000001.SZ'
      AND ann_date <= '2024-01-01'
      AND as_of_date <= '2024-01-01'
""")
# 这些数据在 2024-01-01 之前就公告了 → PASS
# 如果有 ann_date > '2024-01-01' 的数据 → FAIL（未来函数泄漏）
```

#### C2. stock_basic_history 时点一致性

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| C2a: 上市日对齐 | list_date <= as_of_date < delist_date | 0 violations |
| C2b: 退市日正确 | delisted stock: delist_date 应在最后交易日之后 | 误差 = 0 |
| C2c: ST 状态 | 2024-01-01 是 ST 的股票，是否真的 ST 过？ | 人工抽查 |

---

### 维度D：数据库写入完整性

#### D1. DuckDB 一致性

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| D1a: 行数完整 | daily_bar_raw 行数 vs Baostock 历史数据行数 | 误差 < 5 行 |
| D1b: adj_factor 读写一致性 | 写入 DuckDB 的 adj_factor vs 内存计算值 | 误差 < 1e-9 |
| D1c: daily_bar_adjusted 正确性 | adj_close = raw_close × adj_factor | 误差 < 1e-6 |
| D1d: 最新日期验证 | DuckDB 最新日期 vs 实际最新交易日 | 误差 = 0 |

#### D2. 数据新鲜度

| 检查项 | 方法 | 合格标准 |
|--------|------|----------|
| D2a: 最新数据日期 | 最新 trade_date 应 >= 2026-04-10 | FAIL 如果太旧 |
| D2b: 增量同步能力 | 删除1只股票数据，重跑增量，检查完整性 | 数据完全恢复 |

---

## 三、验证执行计划

### 阶段一：数据拉取（Baostock + AkShare）

```python
# 伪代码框架
stocks = ['000001.SZ', '600519.SH', '300750.SZ', '688981.SH', '430685.BJ']
results = {}
for stock in stocks:
    # Baostock: raw + qfq
    bs_raw = fetch_baostock(stock, flag='3')   # 不复权
    bs_qfq = fetch_baostock(stock, flag='2')   # 前复权
    bs_hfq = fetch_baostock(stock, flag='1')   # 后复权（备用）
    
    # AkShare: raw + qfq + hfq
    ak_raw = fetch_akshare(stock, adjust='qfq')  # 前复权
    ak_hfq = fetch_akshare(stock, adjust='hfq') # 后复权
    
    # Tushare: 财务数据（ROE、ROA、PE等）
    fin = fetch_tushare_financial(stock)
    
    results[stock] = {
        'bs_raw': bs_raw, 'bs_qfq': bs_qfq,
        'ak_raw': ak_raw, 'fin': fin
    }
```

### 阶段二：逐项自动化验证

```python
def verify_adj_factor(bs_raw, bs_qfq, db_adj_factor):
    adj = bs_raw['close'] / bs_qfq['close']
    adj_norm = adj / adj[-1]  # 归一化到最新=1.0
    error = abs(db_adj_factor - adj_norm) / adj_norm * 100
    return {'max_err': error.max(), 'pass': error.max() < 0.001}

def verify_financial(db_roe, bs_roe, ak_roe, ts_roe=None):
    sources = [x for x in [bs_roe, ak_roe, ts_roe] if x is not None]
    if len(sources) < 2:
        return {'status': 'SKIP', 'reason': 'insufficient sources'}
    median = np.nanmedian(sources, axis=0)
    err = abs(db_roe - median) / median * 100
    return {'max_err': err.max(), 'pass': err.max() < 5}
```

### 阶段三：人工抽查清单

- [ ] **000001.SZ**: 2018-02-07 分红事件，确认 adj_factor 跳变正确
- [ ] **600519.SH**: 2019-06-27 分红，确认前复权价连续性
- [ ] **300750.SZ**: 2018-06-11 上市首日，确认价格从0开始
- [ ] **688981.SH**: 科创板上市初期无涨跌幅，确认极端价格合法
- [ ] **430685.BJ**: 北交所高波动，确认极端 pct_chg 合理性
- [ ] **所有股票**: 2024-01-01 的 ROE/ROA 抽查（至少5个季度）

---

## 四、验收标准

| 类别 | 关键指标 | 目标 | 硬性门槛 |
|------|----------|------|----------|
| 原始价格 | A1c OHLC 合规 | 100% | 0 violations |
| 复权因子 | A2c Baostock 匹配 | < 0.001% | < 0.1% |
| 复权价格 | A3b Baostock 对比 | < 0.5% | < 2% |
| 复权价格 | A3c AkShare 对比 | < 1% | < 5% |
| 财务 ROE | B2 三源中位数误差 | < 5% | < 15% |
| 财务 PE/PB | B3a/b 合理范围 | 99% | 90% |
| PIT 一致性 | C1a 公告日合规 | 100% | 0 violations |
| 停牌日 | A1b 停牌日正确 | 100% | - |
| 写一致性 | D1b adj_factor 读写 | 0 误差 | < 1e-6 |

**通过条件**：所有硬性门槛全部达标，自动化检查 PASS

---

## 五、工具与脚本规划

```
scripts/
├── phase5_verify.py          # Stage 1（已存在）：adj_factor 单股票验证 ✅
├── phase5_verify_v2.py        # [TODO] 价格数据全维度验证（覆盖 A1-A4）
├── phase5_verify_financial.py  # [TODO] 财务数据三源核查（B1-B4）
├── phase5_verify_pit.py       # [TODO] PIT 一致性验证（C1-C2）
└── phase5_verify_full.py      # [TODO] 全量自动化汇总报告（D1-D2）
```

---

## 六、已知风险与处理

| 风险 | 影响 | 处理方案 |
|------|------|----------|
| AkShare 公司网络阻断 | 无法用 AkShare 交叉验证价格 | 切换到 Baostock hfq 作为第二源 |
| Tushare 无 token | 无法三源核查财务数据 | 使用 Baostock + 东方财富网页双源 |
| Baostock 限速 | 查询失败率高 | 加 sleep + 指数退避 + 失败重试3次 |
| 000001.SZ 历史分红复杂 | adj_factor 可能多次跳变 | 人工逐个确认事件日期 |

---

## 七、执行流程

```
[Phase 5.1] 价格数据验证
  └─ phase5_verify_v2.py
      ├─ A1: 数据完整性 + OHLC ✅
      ├─ A2: adj_factor 归一化 + 单调性 ✅
      ├─ A3: 双源 qfq 价格对比（Baostock vs AkShare）⚠️
      └─ A4: 复权因子层间 gap 检验 ✅

[Phase 5.2] 财务数据验证
  └─ phase5_verify_financial.py
      ├─ B1: Baostock 财务指标拉取 ⚠️
      ├─ B2: 双源 ROE/ROA 核查（Baostock vs 东方财富）⚠️
      ├─ B3: PE/PB 合理范围检验 ✅
      └─ B4: ROE/ROA 合理性 + PIT 边界 ✅

[Phase 5.3] PIT 一致性验证
  └─ phase5_verify_pit.py
      ├─ C1: 财务数据公告日 PIT 边界 ✅
      └─ C2: stock_basic_history 时点一致性 ✅

[Phase 5.4] 数据库写入完整性
  └─ phase5_verify_full.py
      ├─ D1: DuckDB 行数 + adj_factor 读写一致性 ✅
      └─ D2: 数据新鲜度 + 增量同步能力 ✅

[Phase 5.5] 人工抽查 + 最终报告
  └─ phase5_report.md
      ├─ 汇总所有检查结果
      ├─ FAIL 项根因分析
      └─ 验收签字（全部 PASS 或已知风险备注）
```

> ⚠️ = 受网络环境影响，可能需要降级或换源
> ✅ = 可本地执行，无网络依赖
