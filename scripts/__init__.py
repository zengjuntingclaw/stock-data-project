"""stock_data_project - A股多因子回测框架 v3.1 生产级"""

# 数据类
from scripts.data_classes import (
    Order, Trade, OrderStatus, OrderSide,
    PerformanceMetrics, TradingCostConfig, StockStatus
)

# 交易规则
from scripts.trading_rules import AShareTradingRules, TradingCalendar

# 执行引擎
from scripts.execution_engine_v3 import ExecutionEngineV3, CashAccount, Position

# 幸存者偏差治理
from scripts.survivorship_bias import SurvivorshipBiasHandler, StockLifetime

# PIT数据对齐
from scripts.pit_aligner import PITDataAligner

# 回测引擎
from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig

# 绩效分析
from scripts.performance import EnhancedPerformanceAnalyzer, BrinsonAttribution

# 数据引擎（主入口 v6 - 统一管道）
from scripts.data_engine import DataEngine, detect_board, detect_limit, build_ts_code
from scripts.pipeline_data_engine import (
    PipelineDataEngine,
    EnhancedDataEngine,
    ProductionDataEngine,
    RuntimeController,
    DataRouter,
    QualityEngine,
    StorageManager,
    BacktestValidator,
    create_pipeline_engine,
)

# 数据存储层（保留作为底层存储）
from scripts.data_store import DataStore, ConnectionPool
from scripts.data_fetcher import DataFetcher, DataSource, AkShareSource, BaostockSource

# 数据验证器
from scripts.data_validator import DataValidator

__version__ = "3.1.0"
__all__ = [
    # 数据类
    "Order", "Trade", "OrderStatus", "OrderSide",
    "PerformanceMetrics", "TradingCostConfig", "StockStatus",
    # 交易规则
    "AShareTradingRules", "TradingCalendar",
    # 执行引擎
    "ExecutionEngineV3", "CashAccount", "Position",
    # 幸存者偏差
    "SurvivorshipBiasHandler", "StockLifetime",
    # PIT对齐
    "PITDataAligner",
    # 回测引擎
    "ProductionBacktestEngine", "BacktestConfig",
    # 绩效分析
    "EnhancedPerformanceAnalyzer", "BrinsonAttribution",
    # 数据引擎
    "DataEngine", "detect_board", "detect_limit", "build_ts_code",
    # 统一数据管道 (v6 - 推荐入口)
    "PipelineDataEngine",
    "EnhancedDataEngine",
    "ProductionDataEngine",
    "RuntimeController",
    "DataRouter",
    "QualityEngine",
    "StorageManager",
    "BacktestValidator",
    "create_pipeline_engine",
    # 数据存储层
    "DataStore", "ConnectionPool",
    "DataFetcher", "DataSource", "AkShareSource", "BaostockSource",
    # 数据验证器
    "DataValidator",
]
