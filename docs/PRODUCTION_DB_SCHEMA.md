# A股量化策略数据库 - 生产级数据要求

## 项目目标
保存生产级别的A股量化策略数据库

## 目标表结构

### 1. financial_factors (财务因子表)
| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR | 股票代码 |
| report_period_end | DATE | 报告期末 |
| announcement_date | DATE | 公告日 |
| available_date | DATE | 可用日（公告日+1，PIT对齐） |
| roe | DOUBLE | 净资产收益率 |
| roic | DOUBLE | 投入资本回报率 |
| gross_margin | DOUBLE | 毛利率 |
| asset_liability_ratio | DOUBLE | 资产负债率 |
| net_profit | DOUBLE | 净利润 |
| operating_cash_flow | DOUBLE | 经营活动现金流净额 |

### 2. daily_bars (行情表)
| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR | 股票代码 |
| trade_date | DATE | 交易日 |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| volume | BIGINT | 成交量 |
| amount | DOUBLE | 成交额 |
| adj_factor | DOUBLE | 复权因子 |
| suspended_flag | BOOLEAN | 停牌标记 |
| limit_up_flag | BOOLEAN | 涨停标记 |
| limit_down_flag | BOOLEAN | 跌停标记 |

### 3. company_status (公司状态表)
| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR | 股票代码 |
| listed_date | DATE | 上市日 |
| delisted_date | DATE | 退市日（NULL=在交易） |
| st_status | VARCHAR | ST状态 |
| tradable_flag | BOOLEAN | 是否可交易标记 |

### 4. dividends (分红表)
| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | VARCHAR | 股票代码 |
| announcement_date | DATE | 分红公告日 |
| ex_dividend_date | DATE | 除权除息日 |
| pay_date | DATE | 派息日 |
| cash_dividend_per_share | DOUBLE | 每股现金分红 |
| bonus_share_per_share | DOUBLE | 每股送股 |
| transfer_share_per_share | DOUBLE | 每股转增 |

## 状态
- [ ] financial_factors - 待创建
- [ ] daily_bars - 待创建
- [ ] company_status - 待创建
- [ ] dividends - 待创建

---
*Created: 2026-04-17*