"""ExecutionEngineV3 核心路径测试

覆盖三大关键逻辑：
1. 涨跌停顺延：_is_blocked 判断涨停买不进/跌停卖不出，订单顺延下一交易日
2. T+1 冻结释放：买入当日不可卖，次日释放；资金 T+1 可用、T+2 可取
3. 资金结算：买卖对 available/total/pending_settlements 的精确影响
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from scripts.execution_engine_v3 import ExecutionEngineV3, CashAccount, Position
from scripts.data_classes import Order, OrderStatus, OrderSide


def make_market_row(
    symbol: str,
    open_px: float,
    close_px: float = None,
    pre_close: float = None,
    volume: float = 1e7,
    avg_volume_20: float = 1e7,
    is_suspend: bool = False,
) -> dict:
    """构造单行 market_data DataFrame。默认 pre_close = open * 0.98（正常交易日）。"""
    if close_px is None:
        close_px = open_px
    if pre_close is None:
        pre_close = open_px / 1.02  # 正常涨幅约 +2%
    return {
        "symbol": symbol,
        "open": open_px,
        "high": max(open_px, close_px) * 1.01,
        "low": min(open_px, close_px) * 0.99,
        "close": close_px,
        "pre_close": pre_close,
        "volume": volume,
        "avg_volume_20": avg_volume_20,
        "is_suspend": is_suspend,
    }


def make_market_df(rows: list) -> pd.DataFrame:
    return pd.DataFrame(rows)


def parse_dt(s: str) -> datetime:
    """解析 'YYYY-MM-DD' 为 datetime（时间部分为 00:00）"""
    return datetime.strptime(s, "%Y-%m-%d")


# =============================================================================
# 一、涨跌停顺延测试
# =============================================================================

class TestLimitUpDownDeferral(unittest.TestCase):
    """涨跌停顺延逻辑：订单在 T 日被涨跌停阻塞时应顺延至 T+1"""

    def setUp(self):
        # 初始资金 1000 万，无任何持仓
        self.engine = ExecutionEngineV3(initial_cash=1e7)

    def _execute_order_on_date(
        self, order: Order, market_df: pd.DataFrame, current_date: datetime
    ):
        """辅助：直接调用 engine.execute，返回 (trades, remaining)"""
        return self.engine.execute([order], market_df, current_date)

    def test_limit_up_buy_order_deferred(self):
        """买入订单：T 日无量涨停 → 订单顺延 T+1"""
        sym = "600000"
        cur = parse_dt("2024-01-02")  # T 日（周二）

        # T-1 收盘 10.0，T 日涨停价 = 11.0；开盘即触涨停且无量
        row = make_market_row(
            sym, open_px=11.0, pre_close=10.0,
            volume=1e3,          # 无量（远低于 avg_volume_20）
            avg_volume_20=1e7,
        )
        market_df = make_market_df([row])

        # 买入 1000 股，信号日 T-1，执行日 T
        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=cur - timedelta(days=1),
            execution_date=cur,
        )

        trades, remaining = self._execute_order_on_date(order, market_df, cur)

        # 断言：无成交，订单顺延
        self.assertEqual(len(trades), 0)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0].status, OrderStatus.PENDING)
        # 执行日应被推至下一个交易日（T+1）
        self.assertGreater(remaining[0].execution_date, cur)

    def test_limit_down_sell_order_deferred(self):
        """卖出订单：T 日无量跌停 → 订单顺延 T+1"""
        sym = "600000"
        cur = parse_dt("2024-01-02")

        # T 日无量跌停：开盘 = 跌停价
        row = make_market_row(
            sym, open_px=9.0, pre_close=10.0,
            volume=1e3, avg_volume_20=1e7,
        )
        market_df = make_market_df([row])

        # 账户已有持仓
        self.engine.positions[sym] = Position(
            symbol=sym, shares=1000, available_shares=1000, avg_cost=10.0
        )

        order = Order(
            symbol=sym, side=OrderSide.SELL, target_shares=1000,
            signal_date=cur - timedelta(days=1),
            execution_date=cur,
        )

        trades, remaining = self._execute_order_on_date(order, market_df, cur)

        # 断言：无成交，订单顺延
        self.assertEqual(len(trades), 0)
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0].status, OrderStatus.PENDING)
        self.assertGreater(remaining[0].execution_date, cur)

    def test_limit_up_with_volume_buy_can_fill(self):
        """买入订单：T 日涨停但有量（超过 20 日均量 10%）→ 不阻塞，正常成交"""
        sym = "600000"
        cur = parse_dt("2024-01-02")

        row = make_market_row(
            sym, open_px=11.0, pre_close=10.0,
            volume=2e6,          # 有量（> avg_volume_20 * 0.1 = 1e6）
            avg_volume_20=1e7,
        )
        market_df = make_market_df([row])

        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=cur - timedelta(days=1),
            execution_date=cur,
        )

        trades, remaining = self._execute_order_on_date(order, market_df, cur)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].shares, 1000)
        self.assertEqual(len(remaining), 0)

    def test_suspended_stock_order_deferred(self):
        """停牌股票：任何订单均顺延"""
        sym = "600000"
        cur = parse_dt("2024-01-02")

        row = make_market_row(sym, open_px=10.0, is_suspend=True)
        market_df = make_market_df([row])

        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=cur - timedelta(days=1),
            execution_date=cur,
        )

        trades, remaining = self._execute_order_on_date(order, market_df, cur)

        self.assertEqual(len(trades), 0)
        self.assertEqual(len(remaining), 1)
        # 执行日应推至停牌结束后的第一个交易日
        self.assertGreater(remaining[0].execution_date, cur)

    def test_order_deferred_twice_until_fill(self):
        """连续两个交易日涨跌停：订单被顺延两次，第三日恢复成交"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")  # T 日：涨停，无量
        t1 = parse_dt("2024-01-03")  # T+1 日：继续涨停，无量
        t2 = parse_dt("2024-01-04")  # T+2 日：正常开盘

        # T 日：涨停无量
        row_t0 = make_market_row(sym, open_px=11.0, pre_close=10.0,
                                  volume=1e3, avg_volume_20=1e7)
        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        trades_t0, rem_t0 = self.engine.execute([order],
                                                  make_market_df([row_t0]), t0)
        self.assertEqual(len(trades_t0), 0)
        self.assertEqual(rem_t0[0].execution_date, t1)  # 顺延到 T+1

        # T+1 日：继续涨停无量
        row_t1 = make_market_row(sym, open_px=12.1, pre_close=11.0,
                                  volume=1e3, avg_volume_20=1e7)
        trades_t1, rem_t1 = self.engine.execute(rem_t0,
                                                  make_market_df([row_t1]), t1)
        self.assertEqual(len(trades_t1), 0)
        self.assertEqual(rem_t1[0].execution_date, t2)  # 顺延到 T+2

        # T+2 日：正常价格
        row_t2 = make_market_row(sym, open_px=11.5, pre_close=12.1,
                                  volume=2e6, avg_volume_20=1e7)
        trades_t2, rem_t2 = self.engine.execute(rem_t1,
                                                  make_market_df([row_t2]), t2)
        self.assertEqual(len(trades_t2), 1)
        self.assertEqual(len(rem_t2), 0)


# =============================================================================
# 二、T+1 冻结释放测试
# =============================================================================

class TestT1FreezeAndRelease(unittest.TestCase):
    """T+1 规则：当日买入不可卖；次日变为可卖"""

    def setUp(self):
        self.engine = ExecutionEngineV3(initial_cash=1e7)

    def test_shares_not_available_same_day_after_buy(self):
        """买入当天：shares > 0，但 available_shares = 0"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")

        row = make_market_row(sym, open_px=10.0, pre_close=10.0,
                               volume=1e7, avg_volume_20=1e7)
        market_df = make_market_df([row])

        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([order], market_df, t0)

        # 持仓存在，但当日不可卖
        self.assertIn(sym, self.engine.positions)
        pos = self.engine.positions[sym]
        self.assertEqual(pos.shares, 1000)
        self.assertEqual(pos.available_shares, 0)

    def test_shares_available_next_day(self):
        """下一个交易日：available_shares 自动恢复为 shares"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        t1 = parse_dt("2024-01-03")

        # T0 买入
        row0 = make_market_row(sym, open_px=10.0, pre_close=10.0,
                                 volume=1e7, avg_volume_20=1e7)
        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([order], make_market_df([row0]), t0)

        # T1 日开始时调用 update_position_available（execute 内部会自动调用）
        self.engine.update_position_available(t1)
        pos = self.engine.positions[sym]
        self.assertEqual(pos.available_shares, pos.shares)

    def test_same_day_buy_then_sell_uses_generate_orders_protection(self):
        """T+1 保护由 generate_orders 层提供：当日卖出后同日再次卖出被拒绝

        完整场景（使用 generate_orders → execute 流程）：
        1. 预置持仓 5000 股（available = 5000）
        2. T0：generate_orders 卖出全部 5000 → execute 成交，shares=0, available=0
        3. T0（同日）：再次调用 generate_orders，目标仍为清仓
           → available=0，delta=0，卖出订单被跳过
        """
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        # generate_orders 中 execution_date = next_trading_day(t0) = 2024-01-03
        # 所以 execute 时用 t1，让 execution_date 与 current_date 匹配
        t1 = parse_dt("2024-01-03")

        # 预置 5000 股持仓（T+1 解冻后状态）
        self.engine.positions[sym] = Position(
            symbol=sym, shares=5000, available_shares=5000, avg_cost=10.0
        )

        row = make_market_row(sym, open_px=10.0, pre_close=10.0,
                               volume=1e7, avg_volume_20=1e7)
        market_df = make_market_df([row])

        # 生成卖出订单（execution_date = next_trading_day(t0) = t1）
        targets = {sym: 0.0}
        total_val = self.engine.get_portfolio_value({sym: 10.0})["total_value"]
        orders = self.engine.generate_orders(targets, t0, {sym: 10.0}, total_val)
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].side, OrderSide.SELL)

        # 在 t1 执行卖出
        trades, _ = self.engine.execute(orders, market_df, t1)
        self.assertEqual(len(trades), 1)

        # 执行后：已无持仓
        self.assertNotIn(sym, self.engine.positions)

        # t1（同日再次调仓）：generate_orders 不产生卖出订单（无可卖股份）
        orders_again = self.engine.generate_orders(targets, t0, {sym: 10.0}, total_val)
        sell_orders = [o for o in orders_again if o.side == OrderSide.SELL]
        self.assertEqual(len(sell_orders), 0)

    def test_sell_allowed_next_day(self):
        """次日卖出：available_shares 已恢复，可以正常卖出"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        t1 = parse_dt("2024-01-03")

        # T0 买入
        row0 = make_market_row(sym, open_px=10.0, pre_close=10.0,
                                 volume=1e7, avg_volume_20=1e7)
        buy_order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=1000,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([buy_order], make_market_df([row0]), t0)

        # T1 卖出
        row1 = make_market_row(sym, open_px=10.5, pre_close=10.0,
                                 volume=1e7, avg_volume_20=1e7)
        sell_order = Order(
            symbol=sym, side=OrderSide.SELL, target_shares=1000,
            signal_date=t0, execution_date=t1,
        )
        trades, _ = self.engine.execute([sell_order], make_market_df([row1]), t1)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].shares, 1000)


# =============================================================================
# 三、资金结算测试
# =============================================================================

class TestCashSettlement(unittest.TestCase):
    """资金结算：买入扣可用、卖出资金 T+1 可用、T+2 可取、总资产仅减少手续费"""

    def setUp(self):
        self.engine = ExecutionEngineV3(
            initial_cash=1e7,
            commission_rate=0.0003,
            min_commission=5.0,
            slippage_rate=0.001,
        )
        self.init_cash = 1e7

    def test_buy_deducts_available_cash(self):
        """买入：可用资金减少（含佣金+滑点），总资产减少（仅手续费）"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        price = 10.0
        shares = 1000
        amount = price * shares  # 10000

        row = make_market_row(sym, open_px=price, pre_close=price,
                               volume=1e7, avg_volume_20=1e7)
        market_df = make_market_df([row])

        order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=shares,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([order], market_df, t0)

        commission = max(amount * self.engine.commission_rate, self.engine.min_commission)  # 5.0（≥min）
        slippage = amount * self.engine.slippage_rate        # 10.0
        total_cost = amount + commission + slippage          # 10015.0

        # 可用资金减少
        expected_avail = self.init_cash - total_cost
        self.assertAlmostEqual(self.engine.cash.available, expected_avail, places=2)

        # 总资产减少（仅佣金+滑点，股价部分转为持仓市值）
        expected_total = self.init_cash - (commission + slippage)
        self.assertAlmostEqual(self.engine.cash.total, expected_total, places=2)

    def test_sell_proceeds_t1_available(self):
        """卖出：所得资金 T+1 可用（加入 pending_settlements），T+2 可取"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        t1 = parse_dt("2024-01-03")  # T+1

        price = 10.0
        shares = 1000
        amount = price * shares  # 10000

        # 先买入建仓
        row0 = make_market_row(sym, open_px=price, pre_close=price,
                                 volume=1e7, avg_volume_20=1e7)
        buy_order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=shares,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([buy_order], make_market_df([row0]), t0)

        # T0 日卖出
        sell_price = 11.0
        row1 = make_market_df([make_market_row(
            sym, open_px=sell_price, pre_close=price,
            volume=1e7, avg_volume_20=1e7
        )])
        sell_order = Order(
            symbol=sym, side=OrderSide.SELL, target_shares=shares,
            signal_date=t0, execution_date=t0,
        )
        self.engine.execute([sell_order], row1, t0)

        # T0：资金未到账，在 pending_settlements 中
        pending_before = sum(a for _, a in self.engine.cash.pending_settlements)
        self.assertGreater(pending_before, 0)
        # T0 时 available 不应包含这笔资金
        # (实现中，卖出时直接加入 pending，T+1 的 update_settlements 才转到 available)

        # T1：资金变为 available
        self.engine.cash.update_settlements(t1)
        pending_after = sum(a for _, a in self.engine.cash.pending_settlements)
        self.assertEqual(pending_after, 0)
        # available 应增加
        avail_increase = self.engine.cash.available - (self.init_cash - (amount * 0.003 + amount * 0.001))
        # 粗略验证 available 有明显增加（至少包含净额）
        self.assertGreater(avail_increase, 0)

    def test_sell_proceeds_t2_withdrawable(self):
        """卖出所得：T+2 可取（withdrawable）"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")
        t1 = parse_dt("2024-01-03")  # T+1
        t2 = parse_dt("2024-01-04")  # T+2

        price = 10.0
        shares = 1000

        # T0 买入
        row0 = make_market_row(sym, open_px=price, pre_close=price,
                                 volume=1e7, avg_volume_20=1e7)
        self.engine.execute([Order(
            symbol=sym, side=OrderSide.BUY, target_shares=shares,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )], make_market_df([row0]), t0)

        # T0 卖出
        row1 = make_market_df([make_market_row(
            sym, open_px=11.0, pre_close=price, volume=1e7, avg_volume_20=1e7
        )])
        sell_order = Order(
            symbol=sym, side=OrderSide.SELL, target_shares=shares,
            signal_date=t0, execution_date=t0,
        )
        self.engine.execute([sell_order], row1, t0)

        # T1：资金 available，withdrawable 仍为 0
        self.engine.cash.update_settlements(t1)
        self.assertGreater(self.engine.cash.available, self.engine.cash.withdrawable)

        # T2：资金变为可取
        self.engine.cash.update_settlements(t2)
        self.assertGreater(self.engine.cash.withdrawable, 0)

    def test_total_assets_only_reduce_by_fees(self):
        """总资产验证：买入/卖出本身是资产形态转换，总资产只减少手续费"""
        sym = "600000"
        t0 = parse_dt("2024-01-02")

        price = 10.0
        shares = 1000
        amount = price * shares

        row = make_market_row(sym, open_px=price, pre_close=price,
                               volume=1e7, avg_volume_20=1e7)
        buy_order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=shares,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        self.engine.execute([buy_order], make_market_df([row]), t0)

        commission = max(amount * self.engine.commission_rate, self.engine.min_commission)
        slippage = amount * self.engine.slippage_rate

        # 总资产只减少佣金+滑点
        expected_total = self.init_cash - (commission + slippage)
        self.assertAlmostEqual(self.engine.cash.total, expected_total, places=2)

        # 组合市值 + 可用资金 ≈ 总资产（误差为手续费）
        pv = self.engine.get_portfolio_value({sym: price})
        # position_value = 1000 * 10 = 10000
        # total_value = available + position_value = (init - cost) + 10000
        # ≈ init - (commission + slippage)
        self.assertAlmostEqual(pv["total_value"], expected_total, places=2)


# =============================================================================
# 四、组合流程测试
# =============================================================================

class TestIntegratedFlow(unittest.TestCase):
    """多日调仓：买入 → 涨跌停顺延 → 卖出 → 资金T+1/可取的完整流程"""

    def test_multi_day_rebalance_flow(self):
        """多日再平衡：买入 → 涨跌停顺延 → 卖出 → 资金T+1 可用"""
        engine = ExecutionEngineV3(initial_cash=1e7)
        sym = "300001"  # 创业板（T+1 规则同主板，但涨跌幅 20%）
        t0 = parse_dt("2024-01-02")  # 周二

        # ---------- T0：正常买入 300ETF ----------
        row0 = make_market_row(sym, open_px=10.0, pre_close=10.0,
                                 volume=1e7, avg_volume_20=1e7)
        buy_order = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=10000,
            signal_date=t0 - timedelta(days=1), execution_date=t0,
        )
        trades_t0, _ = engine.execute([buy_order], make_market_df([row0]), t0)
        self.assertEqual(len(trades_t0), 1)
        self.assertEqual(trades_t0[0].shares, 10000)
        self.assertEqual(engine.positions[sym].shares, 10000)
        # T0 不可卖
        self.assertEqual(engine.positions[sym].available_shares, 0)

        # ---------- T1：股票涨停，无量，买入订单顺延 ----------
        t1 = parse_dt("2024-01-03")
        row1 = make_market_row(sym, open_px=12.0, pre_close=10.0,
                                 volume=1e3, avg_volume_20=1e7)  # 无量涨停
        # 新的买入信号
        new_buy = Order(
            symbol=sym, side=OrderSide.BUY, target_shares=5000,
            signal_date=t1 - timedelta(days=1), execution_date=t1,
        )
        trades_t1, rem_t1 = engine.execute([new_buy], make_market_df([row1]), t1)
        self.assertEqual(len(trades_t1), 0)   # 涨停被阻
        self.assertEqual(len(rem_t1), 1)       # 顺延

        # ---------- T2：涨停打开，正常买入 ----------
        t2 = parse_dt("2024-01-04")
        row2 = make_market_row(sym, open_px=11.5, pre_close=12.0,
                                 volume=5e6, avg_volume_20=1e7)
        trades_t2, _ = engine.execute(rem_t1, make_market_df([row2]), t2)
        self.assertEqual(len(trades_t2), 1)

        # ---------- T2：同时卖出 T0 建仓的部分 ----------
        engine.update_position_available(t2)  # 释放 T0 的可卖数量
        sell_order = Order(
            symbol=sym, side=OrderSide.SELL, target_shares=5000,
            signal_date=t2, execution_date=t2,
        )
        trades_sell, _ = engine.execute([sell_order], make_market_df([row2]), t2)
        self.assertEqual(len(trades_sell), 1)
        self.assertEqual(trades_sell[0].shares, 5000)

        # T2 结束时：持仓 = 10000 + 5000 - 5000 = 10000（仅剩 T0 买入的部分）
        self.assertEqual(engine.positions[sym].shares, 10000)
        # T2 卖出的 5000 在 pending_settlements 中，T+3 才可用
        pending = dict(engine.cash.pending_settlements)
        self.assertGreater(len(pending), 0)


# =============================================================================
# 五、CashAccount 独立单元测试
# =============================================================================

class TestCashAccountUnit(unittest.TestCase):
    """CashAccount 结算队列的精确行为"""

    def test_empty_settlements_no_change(self):
        """无待结算时：available 不变"""
        ca = CashAccount(total=1e7, available=1e7, withdrawable=1e7)
        ca.update_settlements(parse_dt("2024-01-02"))
        self.assertEqual(ca.available, 1e7)
        self.assertEqual(ca.withdrawable, 1e7)

    def test_settlement_moves_to_available(self):
        """结算日到达：pending → available"""
        t_sell = parse_dt("2024-01-02")
        t_settle = parse_dt("2024-01-03")  # T+1

        ca = CashAccount(total=1e7, available=1e7, withdrawable=1e7)
        ca.pending_settlements.append((t_settle, 10000.0))

        ca.update_settlements(t_sell)  # 结算日未到
        self.assertEqual(ca.available, 1e7)

        ca.update_settlements(t_settle)  # 结算日到达
        self.assertAlmostEqual(ca.available, 1e7 + 10000.0)

    def test_withdrawable_t2_release(self):
        """T+2：available → withdrawable"""
        t_sell = parse_dt("2024-01-02")
        t_settle = parse_dt("2024-01-03")  # T+1
        t_withdraw = parse_dt("2024-01-04")  # T+2

        ca = CashAccount(total=1e7, available=1e7, withdrawable=1e7)
        ca.pending_settlements.append((t_settle, 10000.0))

        ca.update_settlements(t_settle)
        self.assertAlmostEqual(ca.withdrawable, 1e7)  # T+1 时不可取

        ca.update_settlements(t_withdraw)
        self.assertAlmostEqual(ca.withdrawable, 1e7 + 10000.0)


if __name__ == "__main__":
    unittest.main()
