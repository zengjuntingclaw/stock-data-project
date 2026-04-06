"""生产级组件测试 - 优化版

使用 unittest 标准库，无需额外依赖
"""
import unittest
from datetime import datetime
import pandas as pd
import numpy as np

from scripts.trading_rules import AShareTradingRules, TradingCalendar
from scripts.execution_engine_v3 import ExecutionEngineV3
from scripts.survivorship_bias import SurvivorshipBiasHandler, StockLifetime


# ========== TradingRules Tests ==========

class TestTradingRules(unittest.TestCase):
    """交易规则测试类"""
    
    def test_board_detection(self):
        """测试板块识别"""
        self.assertEqual(AShareTradingRules.get_board('600000'), AShareTradingRules.BOARD_MAIN)
        self.assertEqual(AShareTradingRules.get_board('000001'), AShareTradingRules.BOARD_MAIN)
        self.assertEqual(AShareTradingRules.get_board('300001'), AShareTradingRules.BOARD_CHINEXT)
        self.assertEqual(AShareTradingRules.get_board('688001'), AShareTradingRules.BOARD_STAR)
    
    def test_price_limit_history(self):
        """测试涨跌停历史变迁"""
        # 创业板改革前
        limit_old = AShareTradingRules.get_price_limit('300001', datetime(2020, 8, 23))
        self.assertEqual(limit_old, 0.10)
        # 创业板改革后
        limit_new = AShareTradingRules.get_price_limit('300001', datetime(2020, 8, 24))
        self.assertEqual(limit_new, 0.20)
    
    def test_lot_size(self):
        """测试手数约束"""
        # 主板
        lot_main = AShareTradingRules.get_lot_size('600000', datetime(2024, 1, 1))
        self.assertEqual(lot_main, (100, 100))
        # 科创板
        lot_kcb = AShareTradingRules.get_lot_size('688001', datetime(2024, 1, 1))
        self.assertEqual(lot_kcb, (200, 1))
    
    def test_stamp_tax_history(self):
        """测试印花税历史变迁"""
        # 减半前
        tax_old = AShareTradingRules.get_stamp_tax_rate(datetime(2023, 8, 27))
        self.assertEqual(tax_old, 0.001)
        # 减半后
        tax_new = AShareTradingRules.get_stamp_tax_rate(datetime(2023, 8, 28))
        self.assertEqual(tax_new, 0.0005)
    
    def test_share_rounding(self):
        """测试股数规整"""
        # 主板规整到100倍数
        rounded = AShareTradingRules.round_shares('600000', datetime(2024, 1, 1), 550)
        self.assertEqual(rounded, 500)
        # 科创板200股起，不足200返回0（函数设计：不足最小手数不交易）
        rounded_kcb = AShareTradingRules.round_shares('688001', datetime(2024, 1, 1), 150)
        self.assertEqual(rounded_kcb, 0)
        # 科创板200股起，足200正常
        rounded_kcb_ok = AShareTradingRules.round_shares('688001', datetime(2024, 1, 1), 250)
        self.assertEqual(rounded_kcb_ok, 250)


# ========== TradingCalendar Tests ==========

class TestTradingCalendar(unittest.TestCase):
    """交易日历测试类"""
    
    def setUp(self):
        """测试前置"""
        self.calendar = TradingCalendar()
    
    def test_weekend_not_trading(self):
        """测试周末非交易日"""
        self.assertFalse(self.calendar.is_trading_day(datetime(2024, 1, 6)))  # 周六
        self.assertFalse(self.calendar.is_trading_day(datetime(2024, 1, 7)))  # 周日
    
    def test_holiday_not_trading(self):
        """测试节假日非交易日"""
        self.assertFalse(self.calendar.is_trading_day(datetime(2024, 1, 1)))   # 元旦
        self.assertFalse(self.calendar.is_trading_day(datetime(2024, 5, 1)))   # 劳动节
    
    def test_trading_day(self):
        """测试正常交易日"""
        self.assertTrue(self.calendar.is_trading_day(datetime(2024, 1, 2)))  # 周二
    
    def test_next_trading_day(self):
        """测试下一交易日"""
        # 周五->下周一
        next_day = self.calendar.next_trading_day(datetime(2024, 1, 5))
        self.assertEqual(next_day, datetime(2024, 1, 8))


# ========== ExecutionEngineV3 Tests ==========

class TestExecutionEngineV3(unittest.TestCase):
    """V3执行引擎测试类"""
    
    def setUp(self):
        """测试前置"""
        self.engine = ExecutionEngineV3(initial_cash=1e6)
        self.market_data = pd.DataFrame({
            'symbol': ['000001', '300001', '688001'],
            'open': [10.0, 20.0, 30.0],
            'high': [10.5, 20.5, 30.5],
            'low': [9.5, 19.5, 29.5],
            'close': [10.2, 20.2, 30.2],
            'volume': [1e7, 5e6, 2e6],
            'avg_volume_20': [1e7, 5e6, 2e6],
            'prev_close': [10.0, 20.0, 30.0],
            'is_suspended': [False, False, False],
        })
    
    def test_order_generation(self):
        """测试订单生成"""
        target = {'000001': 0.5, '300001': 0.3}
        prices = {'000001': 10.0, '300001': 20.0}
        date = datetime(2024, 1, 2)
        
        orders = self.engine.generate_orders(target, date, prices, total_value=1e6)
        
        self.assertEqual(len(orders), 2)
        # 检查股数规整
        main_order = [o for o in orders if o.symbol == '000001'][0]
        self.assertEqual(main_order.target_shares % 100, 0)  # 主板100股倍数
    
    def test_order_execution(self):
        """测试订单执行"""
        from scripts.data_classes import Order, OrderSide
        date = datetime(2024, 1, 2)
        order = Order('000001', OrderSide.BUY, 1000, date, date)
        
        trades, remaining = self.engine.execute([order], self.market_data, date)
        
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].shares, 1000)
        self.assertEqual(trades[0].price, 10.0)
        self.assertEqual(len(remaining), 0)
    
    def test_cash_update_after_buy(self):
        """测试买入后资金更新"""
        from scripts.data_classes import Order, OrderSide
        date = datetime(2024, 1, 2)
        initial_cash = self.engine.cash.available
        
        order = Order('000001', OrderSide.BUY, 1000, date, date)
        self.engine.execute([order], self.market_data, date)
        
        # 买入后可用资金减少
        self.assertLess(self.engine.cash.available, initial_cash)
        # 总资产减少手续费
        self.assertLess(self.engine.cash.total, 1e6)


# ========== SurvivorshipBias Tests ==========

class TestSurvivorshipBias(unittest.TestCase):
    """幸存者偏差测试类"""
    
    def setUp(self):
        """测试前置"""
        self.handler = SurvivorshipBiasHandler()
        # 添加测试数据
        self.handler._stocks['TEST001'] = StockLifetime(
            symbol='TEST001',
            name='Test Stock',
            list_date=datetime(2020, 1, 1),
            delist_date=datetime(2023, 6, 1),
        )
        self.handler._stocks['TEST002'] = StockLifetime(
            symbol='TEST002',
            name='Active Stock',
            list_date=datetime(2020, 1, 1),
            delist_date=None,
        )
    
    def test_tradable_before_delisting(self):
        """测试退市前可交易"""
        tradable, reason = self.handler.is_tradable('TEST001', datetime(2022, 1, 1))
        self.assertTrue(tradable)
    
    def test_not_tradable_after_delisting(self):
        """测试退市后不可交易"""
        tradable, reason = self.handler.is_tradable('TEST001', datetime(2024, 1, 1))
        self.assertFalse(tradable)
        self.assertIn('退市', reason)
    
    def test_universe_with_delisted(self):
        """测试股票池包含退市股"""
        universe = self.handler.get_universe(datetime(2022, 1, 1), include_delisted=True)
        self.assertIn('TEST001', universe)
        self.assertIn('TEST002', universe)
    
    def test_universe_without_delisted(self):
        """测试股票池不包含退市股"""
        universe = self.handler.get_universe(datetime(2024, 1, 1), include_delisted=False)
        self.assertNotIn('TEST001', universe)
        self.assertIn('TEST002', universe)


# ========== Main ==========

if __name__ == "__main__":
    unittest.main(verbosity=2)
