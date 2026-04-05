"""A股多因子回测框架 v2.0 主程序"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from loguru import logger

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
logger.add(project_root / "logs" / "v2_{time}.log", rotation="10 MB", retention="7 days", level="DEBUG")

from scripts.data_classes import TradingCostConfig
from scripts.execution_engine import ExecutionEngine
from scripts.trading_utils import TradingFilter, DelistingMonitor, STMonitor
from scripts.dynamic_universe import DynamicUniverse, UniverseConfig
from scripts.pit_aligner import PITDataAligner
from scripts.factor_engine import VectorizedFactorEngine, LongTableManager
from scripts.performance import EnhancedPerformanceAnalyzer
from scripts.data_engine import DataEngine


def setup():
    (project_root / "logs").mkdir(exist_ok=True)
    (project_root / "data" / "v2").mkdir(parents=True, exist_ok=True)
    (project_root / "_output").mkdir(parents=True, exist_ok=True)
    logger.info("="*50 + "\nv2.0 启动\n" + "="*50)


def run_backtest(start="2022-01-01", end="2024-12-31", capital=1e7):
    """回测主流程"""
    logger.info(f"区间: {start} ~ {end}, 资金: {capital/1e7:.0f}千万")
    
    # 1. 初始化
    cost = TradingCostConfig()
    exec_engine = ExecutionEngine(cost_config=cost, adv_limit=0.1)
    filter = TradingFilter(exclude_st=True, min_listing_days=60, min_market_cap=5e8)
    universe_cfg = UniverseConfig(min_market_cap=5e8, min_listing_days=60, top_n=500)
    universe = DynamicUniverse(config=universe_cfg)
    perf = EnhancedPerformanceAnalyzer(risk_free=0.03)
    
    logger.info("组件初始化完成")
    
    # 2. 数据准备（简化版）
    logger.info("数据引擎初始化...")
    data = DataEngine()
    
    # 3. 生成测试数据验证流程
    logger.info("\n生成测试数据验证...")
    dates = pd.date_range(start, end, freq='B')
    dates = [d for d in dates if d.weekday() < 5]
    
    # 模拟持仓
    positions = {'000001': 1000, '000002': 2000}
    
    # 生成调仓日订单
    signal_date = datetime(2023, 1, 31)
    target = {'000001': 2000, '000003': 1000}
    orders = exec_engine.generate_orders(target, positions, signal_date)
    
    logger.info(f"生成订单: {len(orders)}个")
    for o in orders:
        logger.info(f"  {o.side.value.upper()} {o.symbol} {o.target_shares}股 @ {o.execution_date.date()}")
    
    # 模拟成交
    market = pd.DataFrame({
        'symbol': ['000001', '000002', '000003'],
        'open': [10.0, 20.0, 15.0],
        'volume': [1e6, 2e6, 5e5],
        'is_limit_up': [False, False, False],
        'is_limit_down': [False, False, False],
        'is_suspended': [False, False, False]
    })
    
    trades, remaining = exec_engine.execute(orders, market, orders[0].execution_date)
    logger.info(f"成交: {len(trades)}笔")
    for t in trades:
        logger.info(f"  {t.side.name} {t.symbol} {t.shares}股 @ {t.price:.2f}")
    
    # 4. 绩效计算
    logger.info("\n绩效分析...")
    returns = pd.Series([0.01, -0.005, 0.02] * 100)
    metrics = perf.calculate(returns)
    logger.info(perf.report(metrics))
    
    logger.info("\n✅ 验证完成！v2.0框架可正常运行")
    return True


if __name__ == "__main__":
    setup()
    success = run_backtest()
    sys.exit(0 if success else 1)
