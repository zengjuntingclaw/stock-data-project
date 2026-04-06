"""stock_data_project - A股多因子回测框架 v3.0 生产级"""

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

# 数据引擎（v1兼容）
from scripts.data_engine import DataEngine

__version__ = "3.0.0"
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
    "DataEngine",
]
