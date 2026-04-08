"""A股多因子回测框架 v3.1 主程序"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict
import pandas as pd
import numpy as np

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from loguru import logger

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}", level="INFO")
logger.add(project_root / "logs" / "v3_{time}.log", rotation="10 MB", retention="7 days", level="DEBUG")

from scripts.data_classes import TradingCostConfig
from scripts.execution_engine_v3 import ExecutionEngineV3, CashAccount, Position
from scripts.pit_aligner import PITDataAligner
from scripts.performance import EnhancedPerformanceAnalyzer
from scripts.data_engine import DataEngine
from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig


def setup():
    (project_root / "logs").mkdir(exist_ok=True)
    (project_root / "data" / "v2").mkdir(parents=True, exist_ok=True)
    (project_root / "_output").mkdir(parents=True, exist_ok=True)
    logger.info("="*50 + "\nv3.1 启动\n" + "="*50)


def run_backtest(start="2022-01-01", end="2024-12-31", capital=1e7):
    """回测主流程"""
    logger.info(f"区间: {start} ~ {end}, 资金: {capital/1e7:.0f}千万")
    
    # 1. 初始化组件
    data = DataEngine()
    
    # 2. 配置回测引擎
    config = BacktestConfig(
        start_date=datetime.strptime(start, "%Y-%m-%d"),
        end_date=datetime.strptime(end, "%Y-%m-%d"),
        initial_capital=capital,
    )
    
    # 3. 简单策略：等权买入前N只
    def simple_strategy(factor_data: pd.DataFrame, date: datetime) -> Dict[str, float]:
        if factor_data.empty:
            return {}
        n = min(50, len(factor_data))
        weight = 1.0 / n
        return {sym: weight for sym in factor_data.index[:n]}
    
    # 4. 运行回测
    logger.info("启动回测引擎...")
    engine = ProductionBacktestEngine(config=config, data_engine=data, factor_engine=None)
    result = engine.run(simple_strategy)
    
    if 'error' in result:
        logger.error(f"回测失败: {result['error']}")
        logger.info("使用模拟数据验证框架...")
        _verify_with_mock_data()
        return False
    
    perf = result.get('performance', {})
    logger.info(f"总收益: {perf.get('total_return', 0):.2%}")
    logger.info(f"夏普: {perf.get('sharpe_ratio', 0):.2f}")
    logger.info(f"最大回撤: {perf.get('max_drawdown', 0):.2%}")
    logger.info(f"交易笔数: {result.get('trades', 0)}")
    logger.info("回测完成!")
    return True


def _verify_with_mock_data():
    """使用模拟数据验证框架核心组件可运行"""
    from scripts.trading_rules import AShareTradingRules, TradingCalendar
    from scripts.survivorship_bias import SurvivorshipBiasHandler
    
    # 验证交易规则
    rules_ok = AShareTradingRules.get_board('600000') == AShareTradingRules.BOARD_MAIN
    rules_ok &= AShareTradingRules.get_price_limit('300001', datetime(2020, 8, 24)) == 0.20
    
    # 验证日历
    cal = TradingCalendar()
    cal_ok = not cal.is_trading_day(datetime(2024, 1, 1))  # 元旦
    cal_ok &= cal.is_trading_day(datetime(2024, 1, 2))      # 周二
    
    # 验证执行引擎
    engine = ExecutionEngineV3(initial_cash=1e6)
    exec_ok = engine.cash.available == 1e6
    
    if rules_ok and cal_ok and exec_ok:
        logger.info("框架核心组件验证通过")
    else:
        logger.warning("框架验证发现问题")


if __name__ == "__main__":
    setup()
    success = run_backtest()
    sys.exit(0 if success else 1)
