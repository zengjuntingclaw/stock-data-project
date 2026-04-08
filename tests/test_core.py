"""核心模块测试 - 优化版

测试 data_classes 和基础执行引擎
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
from datetime import datetime

from scripts.data_classes import (
    Order, Trade, OrderStatus, OrderSide,
    TradingCostConfig, PerformanceMetrics
)


# ========== Data Classes Tests ==========

class TestOrder(unittest.TestCase):
    """Order 数据类测试"""
    
    def test_remaining_shares(self):
        """测试剩余股数计算"""
        order = Order(
            '000001', OrderSide.BUY, 1000,
            datetime.now(), datetime.now(),
            filled_shares=300
        )
        self.assertEqual(order.remaining_shares, 700)
    
    def test_order_creation_defaults(self):
        """测试订单创建默认值"""
        order = Order('000001', OrderSide.BUY, 1000, datetime.now(), datetime.now())
        self.assertEqual(order.status, OrderStatus.PENDING)
        self.assertEqual(order.filled_shares, 0)


class TestTrade(unittest.TestCase):
    """Trade 数据类测试"""
    
    def test_total_cost_calculation(self):
        """测试总成本计算"""
        trade = Trade(
            'T1', '000001', OrderSide.BUY,
            100, 10.0, datetime.now(),
            commission=0.3, stamp_tax=0.0, slippage=1.0
        )
        self.assertEqual(trade.total_cost, 1.3)  # 0.3 + 0 + 1.0
    
    def test_net_amount_buy(self):
        """测试买入净额"""
        trade = Trade(
            'T1', '000001', OrderSide.BUY,
            100, 10.0, datetime.now(),
            commission=0.3, stamp_tax=0.0, slippage=1.0
        )
        # 买入：-(金额 + 成本)
        expected = -(100 * 10.0 + 1.3)
        self.assertAlmostEqual(trade.net_amount, expected, places=2)
    
    def test_net_amount_sell(self):
        """测试卖出净额"""
        trade = Trade(
            'T1', '000001', OrderSide.SELL,
            100, 10.0, datetime.now(),
            commission=0.3, stamp_tax=1.0, slippage=1.0
        )
        # 卖出：金额 - 成本
        expected = 100 * 10.0 - 2.3
        self.assertAlmostEqual(trade.net_amount, expected, places=2)


class TestTradingCostConfig(unittest.TestCase):
    """交易成本配置测试"""
    
    def test_buy_cost_calculation(self):
        """测试买入成本计算"""
        cfg = TradingCostConfig()
        commission, stamp_tax, slippage = cfg.calculate_cost(OrderSide.BUY, 10000)
        
        self.assertEqual(commission, 5.0)  # min_commission
        self.assertEqual(stamp_tax, 0.0)   # 买入不收印花税
        self.assertEqual(slippage, 10.0)   # 0.1%
    
    def test_sell_cost_calculation(self):
        """测试卖出成本计算"""
        cfg = TradingCostConfig()
        commission, stamp_tax, slippage = cfg.calculate_cost(OrderSide.SELL, 10000)
        
        self.assertEqual(commission, 5.0)
        self.assertEqual(stamp_tax, 10.0)  # 0.1%印花税
        self.assertEqual(slippage, 10.0)
    
    def test_large_amount_commission(self):
        """测试大额交易佣金"""
        cfg = TradingCostConfig()
        commission, _, _ = cfg.calculate_cost(OrderSide.BUY, 1e6)
        
        self.assertAlmostEqual(commission, 300.0, places=2)  # 超过最低佣金，按比例


class TestPerformanceMetrics(unittest.TestCase):
    """绩效指标测试"""
    
    def test_metrics_creation(self):
        """测试指标创建"""
        metrics = PerformanceMetrics(
            total_return=0.5,
            annual_return=0.2,
            annual_volatility=0.15,
            sharpe_ratio=1.33,
            max_drawdown=-0.1,
            calmar_ratio=2.0,
            information_ratio=0.8,
            tracking_error=0.05,
            beta=1.0,
            alpha=0.02,
            max_dd_days=30,
            avg_turnover=0.5
        )
        
        self.assertEqual(metrics.total_return, 0.5)
        self.assertEqual(metrics.sharpe_ratio, 1.33)


# ========== Main ==========

if __name__ == "__main__":
    unittest.main(verbosity=2)
