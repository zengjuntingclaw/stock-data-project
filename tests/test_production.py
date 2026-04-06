"""生产级组件测试"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
import pandas as pd
import numpy as np

from scripts.trading_rules import AShareTradingRules, TradingCalendar
from scripts.execution_engine_v3 import ExecutionEngineV3
from scripts.survivorship_bias import SurvivorshipBiasHandler


def test_trading_rules():
    """测试交易规则中心"""
    print("\n=== Testing TradingRules ===")
    
    # 板块识别
    assert AShareTradingRules.get_board('600000') == AShareTradingRules.BOARD_MAIN
    assert AShareTradingRules.get_board('000001') == AShareTradingRules.BOARD_MAIN
    assert AShareTradingRules.get_board('300001') == AShareTradingRules.BOARD_CHINEXT
    assert AShareTradingRules.get_board('688001') == AShareTradingRules.BOARD_STAR
    print("  Board detection: PASSED")
    
    # 涨跌停历史
    # 创业板2020-08-24前10%，后20%
    limit_old = AShareTradingRules.get_price_limit('300001', datetime(2020, 8, 23))
    limit_new = AShareTradingRules.get_price_limit('300001', datetime(2020, 8, 24))
    assert limit_old == 0.10, f"Expected 0.10, got {limit_old}"
    assert limit_new == 0.20, f"Expected 0.20, got {limit_new}"
    print(f"  Price limit history: {limit_old:.0%} -> {limit_new:.0%} PASSED")
    
    # 手数约束
    lot_main = AShareTradingRules.get_lot_size('600000', datetime(2024, 1, 1))
    lot_kcb = AShareTradingRules.get_lot_size('688001', datetime(2024, 1, 1))
    assert lot_main == (100, 100), f"Expected (100, 100), got {lot_main}"  # 主板100股起
    assert lot_kcb == (200, 1), f"Expected (200, 1), got {lot_kcb}"     # 科创板200股起，1股递增
    print(f"  Lot size: MAIN{lot_main}, STAR{lot_kcb} PASSED")
    
    # 印花税历史
    tax_old = AShareTradingRules.get_stamp_tax_rate(datetime(2023, 8, 27))
    tax_new = AShareTradingRules.get_stamp_tax_rate(datetime(2023, 8, 28))
    assert tax_old == 0.001
    assert tax_new == 0.0005
    print(f"  Stamp tax history: {tax_old:.1%} -> {tax_new:.1%} PASSED")
    
    # 股数规整
    rounded = AShareTradingRules.round_shares('600000', datetime(2024, 1, 1), 550)
    assert rounded == 500  # 规整到100的倍数
    print(f"  Share rounding: 550 -> {rounded} PASSED")


def test_trading_calendar():
    """测试交易日历"""
    print("\n=== Testing TradingCalendar ===")
    
    cal = TradingCalendar()
    
    # 周末非交易日
    assert not cal.is_trading_day(datetime(2024, 1, 6))  # 周六
    assert not cal.is_trading_day(datetime(2024, 1, 7))  # 周日
    print("  Weekend check: PASSED")
    
    # 节假日非交易日
    assert not cal.is_trading_day(datetime(2024, 1, 1))  # 元旦
    assert not cal.is_trading_day(datetime(2024, 5, 1))  # 劳动节
    print("  Holiday check: PASSED")
    
    # 工作日是交易日
    assert cal.is_trading_day(datetime(2024, 1, 2))  # 周二
    print("  Trading day check: PASSED")
    
    # 下一交易日
    next_day = cal.next_trading_day(datetime(2024, 1, 5))  # 周五->下周一
    assert next_day == datetime(2024, 1, 8)
    print(f"  Next trading day: {next_day.date()} PASSED")


def test_execution_engine_v3():
    """测试V3执行引擎"""
    print("\n=== Testing ExecutionEngineV3 ===")
    
    engine = ExecutionEngineV3(initial_cash=1e6)
    
    # 生成订单
    target = {'600000': 0.5, '300001': 0.3}
    prices = {'600000': 10.0, '300001': 20.0}
    date = datetime(2024, 1, 2)
    
    orders = engine.generate_orders(target, date, prices, total_value=1e6)
    assert len(orders) == 2
    print(f"  Order generation: {len(orders)} orders PASSED")
    
    # 检查股数规整（主板100股倍数）
    main_order = [o for o in orders if o.symbol == '600000'][0]
    assert main_order.target_shares % 100 == 0
    print(f"  Lot size constraint: {main_order.target_shares} PASSED")
    
    # 执行订单
    market = pd.DataFrame({
        'symbol': ['600000', '300001'],
        'open': [10.0, 20.0],
        'close': [10.5, 20.5],
        'volume': [1e7, 5e6],
        'avg_volume_20': [1e7, 5e6],
        'prev_close': [9.9, 19.8],
        'is_suspended': [False, False],
    })
    
    trades, remaining = engine.execute(orders, market, orders[0].execution_date)
    assert len(trades) > 0
    print(f"  Order execution: {len(trades)} trades PASSED")
    
    # 检查资金更新
    assert engine.cash.total < 1e6  # 有手续费扣除
    print(f"  Cash update: {engine.cash.total:,.0f} PASSED")


def test_survivorship_bias():
    """测试幸存者偏差处理器"""
    print("\n=== Testing SurvivorshipBiasHandler ===")
    
    handler = SurvivorshipBiasHandler()
    
    # 手动添加测试数据
    from scripts.survivorship_bias import StockLifetime
    handler._stocks['TEST001'] = StockLifetime(
        symbol='TEST001',
        name='Test Stock',
        list_date=datetime(2020, 1, 1),
        delist_date=datetime(2023, 6, 1),
    )
    handler._stocks['TEST002'] = StockLifetime(
        symbol='TEST002',
        name='Active Stock',
        list_date=datetime(2020, 1, 1),
        delist_date=None,
    )
    
    # 退市前可交易
    tradable, reason = handler.is_tradable('TEST001', datetime(2022, 1, 1))
    assert tradable
    print(f"  Tradable before delisting: PASSED")
    
    # 退市后不可交易
    tradable, reason = handler.is_tradable('TEST001', datetime(2024, 1, 1))
    assert not tradable
    print(f"  Not tradable after delisting: {reason} PASSED")
    
    # 股票池包含退市股
    universe = handler.get_universe(datetime(2022, 1, 1), include_delisted=True)
    assert 'TEST001' in universe
    assert 'TEST002' in universe
    print(f"  Universe with delisted: PASSED")
    
    # 股票池不包含退市股
    universe = handler.get_universe(datetime(2024, 1, 1), include_delisted=False)
    assert 'TEST001' not in universe
    assert 'TEST002' in universe
    print(f"  Universe without delisted: PASSED")


def run_all_tests():
    """运行所有测试"""
    print("="*60)
    print("生产级组件测试")
    print("="*60)
    
    tests = [
        test_trading_rules,
        test_trading_calendar,
        test_execution_engine_v3,
        test_survivorship_bias,
    ]
    
    passed = failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"结果: {passed} 通过, {failed} 失败")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
