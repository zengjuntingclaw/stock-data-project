"""
Phase 5 验证方案：小数据量逐股票验证
========================================
思路：从单只股票开始验证完整管道（Baostock → DuckDB → adj_factor → PIT查询），
      验证通过后再批量扩展。

三阶段：
  Stage 1: 单只股票端到端验证  (今晚)
  Stage 2: 10只股票批量验证    (Baostock恢复后)
  Stage 3: 全量数据质量扫描    (主库重建后)

Stage 1 选股：000001.SZ（平安银行）
  - 理由：主板代表性、历年分红多、adj_factor变化多、数据连续性好
  - 验证点：
    1. Baostock原始数据能获取
    2. daily_bar_raw 写入成功（trade_date/open/high/low/close/vol/amount/adj_factor）
    3. daily_bar_adjusted 复权价正确（对比 Baostock 前复权 API）
    4. adj_factor 累计乘积 = 最新 adj_factor
    5. OHLC 内部一致性（high >= low, high >= open/close 等）
    6. PIT 查询：2024-01-01 能查到，2020-01-01 查到

Stage 2 选股（10只，覆盖全板块）：
  - 主板大盘: 000001.SZ, 600000.SH
  - 主板小盘: 000858.SZ（五粮液）, 600519.SH（茅台）
  - 创业板: 300750.SZ（宁德时代）, 300059.SZ（东方财富）
  - 科创板: 688981.SH（中芯国际）, 688111.SH（金山办公）
  - 北交所: 430047.BJ（诺思兰德）, 832566.BJ（梓橦科技）

  验证点：
    1. 全板块数据获取（SH/SZ/BJ）
    2. adj_factor 变化次数合理（主板少，科创板/创业板多有分红送股）
    3. 北交所数据（特殊板，无涨跌幅限制）
    4. 跨股票 PIT 查询一致性

Stage 3 全量扫描：
  - 所有455只股票数据完整性
  - adj_factor 分布统计
  - 缺失日期检测（trade_calendar 对齐）
  - OHLC 异常值检测

运行方式：
  python scripts/phase5_verify.py --stage 1      # 单只股票验证
  python scripts/phase5_verify.py --stage 2      # 10只股票批量
  python scripts/phase5_verify.py --stage 3      # 全量扫描
  python scripts/phase5_verify.py --stock 000001.SZ  # 指定股票

预期时长：
  Stage 1: ~1分钟（1只股票）
  Stage 2: ~10分钟（10只股票，有限速等待）
  Stage 3: ~30分钟（455只，有限速等待）
"""

STAGE1_STOCKS = ['000001.SZ']
STAGE2_STOCKS = ['000001.SZ', '600000.SH', '000858.SZ', '600519.SH',
                 '300750.SZ', '300059.SZ', '688981.SH', '688111.SH',
                 '430047.BJ', '832566.BJ']
