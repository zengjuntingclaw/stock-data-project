"""单元测试 - v2.0核心模块"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
import pandas as pd
import numpy as np

# ========== data_classes ==========
from scripts.data_classes import (
    Order, Trade, OrderStatus, OrderSide, 
    PerformanceMetrics, TradingCostConfig, StockStatus
)

def test_order_remaining_shares():
    order = Order('000001', OrderSide.BUY, 1000, datetime.now(), datetime.now(), filled_shares=300)
    assert order.remaining_shares == 700
    print("test_order_remaining_shares PASSED")

def test_trade_net_amount():
    trade = Trade('T1', '000001', OrderSide.BUY, 100, 10.0, datetime.now(), 0.3, 0.0, 1.0)
    assert abs(trade.total_cost - 1.3) < 0.01
    assert abs(trade.net_amount - (-1001.3)) < 0.01
    print("test_trade_net_amount PASSED")

def test_trading_cost_config():
    cfg = TradingCostConfig()
    # 买入 10000元, 佣金率0.03%但最低5元
    c, s, sl = cfg.calculate_cost(OrderSide.BUY, 10000)
    assert abs(c - 5.0) < 0.01  # min_commission=5.0
    assert s == 0.0  # 买入不收印花税
    assert abs(sl - 10.0) < 0.01  # 0.1%滑点
    
    # 卖出 10000元
    c, s, sl = cfg.calculate_cost(OrderSide.SELL, 10000)
    assert abs(c - 5.0) < 0.01
    assert abs(s - 10.0) < 0.01  # 0.1%印花税
    assert abs(sl - 10.0) < 0.01
    
    # 大额时按比例
    c, s, sl = cfg.calculate_cost(OrderSide.BUY, 1e6)
    assert abs(c - 300.0) < 0.01  # 超过最低佣金
    print("test_trading_cost_config PASSED")

# ========== execution_engine ==========
from scripts.execution_engine import ExecutionEngine

def test_generate_orders():
    engine = ExecutionEngine()
    target = {'000001': 2000, '000002': 1000}
    current = {'000001': 1000, '000003': 3000}
    orders = engine.generate_orders(target, current, datetime(2023, 1, 31))
    
    assert len(orders) == 3
    buy_orders = [o for o in orders if o.side == OrderSide.BUY]
    sell_orders = [o for o in orders if o.side == OrderSide.SELL]
    assert len(buy_orders) == 2
    assert len(sell_orders) == 1
    assert sell_orders[0].symbol == '000003'
    assert sell_orders[0].target_shares == 3000
    print('test_generate_orders PASSED')

def test_execute_buy_order():
    engine = ExecutionEngine()
    market = pd.DataFrame({
        'symbol': ['000001'],
        'open': [10.0],
        'volume': [1e7],
        'is_limit_up': [False],
        'is_limit_down': [False],
        'is_suspended': [False]
    })
    
    order = Order('000001', OrderSide.BUY, 1000, datetime(2023,1,31), datetime(2023,2,1))
    trades, remaining = engine.execute([order], market, datetime(2023,2,1))
    
    assert len(trades) == 1
    assert trades[0].shares == 1000
    assert trades[0].price == 10.0
    assert len(remaining) == 0
    print("test_execute_buy_order PASSED")

def test_execute_blocked_by_limit_up():
    engine = ExecutionEngine()
    market = pd.DataFrame({
        'symbol': ['000001'],
        'open': [10.0],
        'volume': [1e7],
        'is_limit_up': [True],  # 涨停
        'is_limit_down': [False],
        'is_suspended': [False]
    })
    
    order = Order('000001', OrderSide.BUY, 1000, datetime(2023,1,31), datetime(2023,2,1))
    trades, remaining = engine.execute([order], market, datetime(2023,2,1))
    
    assert len(trades) == 0  # 无法成交
    assert len(remaining) == 1
    assert remaining[0].execution_date > datetime(2023,2,1)  # 顺延
    print("test_execute_blocked_by_limit_up PASSED")

# ========== trading_utils ==========
from scripts.trading_utils import TradingFilter, DelistingMonitor, STMonitor

def test_trading_filter():
    f = TradingFilter(exclude_st=True, exclude_suspended=True, min_listing_days=60, min_market_cap=1e8)
    market = pd.DataFrame({
        'symbol': ['000001', '000002', '000003', '000004'],
        'is_st': [False, True, False, False],
        'is_suspended': [False, False, True, False],
        'list_date': [datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2023,6,1)],
        'total_mv': [1e9, 1e9, 1e9, 1e9]
    })
    
    result = f.filter(['000001','000002','000003','000004'], market, datetime(2024,1,1))
    # 000001: 正常 -> 通过
    # 000002: ST -> 排除
    # 000003: 停牌 -> 排除
    # 000004: 上市天数 = 2024-2023=1年=365天 > 60 -> 通过(但市值1e9 > 1e8也通过)
    assert '000001' in result
    assert '000002' not in result
    assert '000003' not in result
    # 000004: 2024-1-1 - 2023-6-1 = 214天 > 60, 市值1e9 > 1e8 -> 应该通过
    assert '000004' in result
    print("test_trading_filter PASSED")

def test_delisting_monitor():
    m = DelistingMonitor(warning_days=20)
    m.load(pd.DataFrame({'symbol': ['000001'], 'delist_date': [datetime(2024,2,1)]}))
    
    ok, days = m.check('000001', datetime(2024,1,15))
    assert ok == True  # 距离17天
    assert days == 17
    
    ok, days = m.check('000001', datetime(2024,1,1))
    assert ok == False  # 距离31天
    
    ok, days = m.check('000002', datetime(2024,1,15))
    assert ok == False  # 不在列表
    print("test_delisting_monitor PASSED")

def test_st_monitor():
    m = STMonitor()
    m.load(pd.DataFrame({'symbol': ['000001'], 'start_date': [datetime(2024,1,1)], 'end_date': [None]}))
    
    assert m.get_status('000001', datetime(2024,6,1)) == StockStatus.ST
    assert m.get_status('000002', datetime(2024,6,1)) == StockStatus.NORMAL
    assert m.can_buy('000001', datetime(2024,6,1)) == False  # 禁止买入ST
    assert m.can_buy('000002', datetime(2024,6,1)) == True
    print("test_st_monitor PASSED")

# ========== performance ==========
from scripts.performance import EnhancedPerformanceAnalyzer

def test_performance_metrics():
    a = EnhancedPerformanceAnalyzer(risk_free=0.03)
    returns = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015] * 50)
    metrics = a.calculate(returns)
    
    assert metrics.total_return > 0
    assert metrics.annual_return > 0
    assert metrics.sharpe_ratio != 0
    assert metrics.max_drawdown < 0
    assert metrics.calmar_ratio != 0
    print("test_performance_metrics PASSED")

def test_performance_with_benchmark():
    a = EnhancedPerformanceAnalyzer(risk_free=0.03)
    returns = pd.Series([0.01, -0.005, 0.02, -0.01, 0.015] * 50)
    benchmark = pd.Series([0.005, -0.002, 0.01, -0.005, 0.008] * 50)
    
    metrics = a.calculate(returns, benchmark)
    
    assert not np.isnan(metrics.information_ratio)
    assert not np.isnan(metrics.tracking_error)
    assert not np.isnan(metrics.beta)
    print("test_performance_with_benchmark PASSED")

# ========== dynamic_universe ==========
from scripts.dynamic_universe import DynamicUniverse, UniverseConfig

def test_dynamic_universe():
    cfg = UniverseConfig(min_market_cap=1e8, min_listing_days=30)
    market = pd.DataFrame({
        'symbol': ['000001', '000002', '000003'],
        'total_mv': [5e8, 1e7, 1e9],
        'list_date': [datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)],
        'is_st': [False, False, False],
        'delist_date': [None, None, None]
    })
    
    u = DynamicUniverse(cfg)
    result = u.get_universe(datetime(2024,1,1), market)
    
    assert '000001' in result
    assert '000002' not in result  # 市值不足
    assert '000003' in result
    print("test_dynamic_universe PASSED")

# ========== pit_aligner ==========
from scripts.pit_aligner import PITDataAligner

def test_pit_aligner():
    df = pd.DataFrame({
        'symbol': ['000001', '000001'],
        'end_date': ['2023-09-30', '2023-12-31'],
        'ann_date': ['2023-10-31', '2024-01-31'],
        'roe': [0.10, 0.12]
    })
    
    aligner = PITDataAligner()
    aligner.load(df)
    
    # 2023-11-15 只能看到10月公告的数据
    factor = aligner.get_factor('roe', datetime(2023,11,15))
    assert factor['000001'] == 0.10  # 9月季报
    
    # 2024-02-01 可以看到12月公告的数据
    factor = aligner.get_factor('roe', datetime(2024,2,1))
    assert factor['000001'] == 0.12  # 12月季报
    print("test_pit_aligner PASSED")


def run_all_tests():
    print("\n" + "="*50)
    print("运行v2.0核心模块单元测试")
    print("="*50 + "\n")
    
    tests = [
        test_order_remaining_shares,
        test_trade_net_amount,
        test_trading_cost_config,
        test_generate_orders,
        test_execute_buy_order,
        test_execute_blocked_by_limit_up,
        test_trading_filter,
        test_delisting_monitor,
        test_st_monitor,
        test_performance_metrics,
        test_performance_with_benchmark,
        test_dynamic_universe,
        test_pit_aligner,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"FAILED: {test.__name__}: {e}")
            failed += 1

    print("\n" + "="*50)
    print(f"结果: {passed} 通过, {failed} 失败")
    print("="*50)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
