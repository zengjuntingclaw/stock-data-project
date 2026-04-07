"""生产级交易执行引擎

核心改进：
1. 集成TradingRules历史规则感知
2. 支持科创板/创业板差异化手数约束
3. 真实A股交易日历（节假日处理）
4. 完整的资金结算逻辑（T+1可用 vs T+0可取）
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from loguru import logger

from scripts.data_classes import Order, Trade, OrderStatus, OrderSide
from scripts.trading_rules import AShareTradingRules, TradingCalendar


@dataclass
class Position:
    """持仓明细"""
    symbol: str
    shares: int = 0                    # 总持仓
    available_shares: int = 0          # 可卖数量（T+1）
    avg_cost: float = 0.0              # 平均成本
    
    def market_value(self, price: float) -> float:
        """计算持仓市值（普通方法，@property不支持传入参数）"""
        return self.shares * price


@dataclass 
class CashAccount:
    """资金账户"""
    total: float = 0.0                 # 总资产
    available: float = 0.0             # 可用资金（可买股票）
    withdrawable: float = 0.0          # 可取资金（T+1卖出所得）
    frozen: float = 0.0                # 冻结资金（挂单未成交）
    
    # 待结算资金队列: [(settle_date, amount), ...]
    pending_settlements: List[Tuple[datetime, float]] = field(default_factory=list)
    
    # 待转为可取资金的队列: [(withdrawable_date, amount), ...]
    # T+1卖出 → T+2才可取
    pending_withdrawals: List[Tuple[datetime, float]] = field(default_factory=list)
    
    def update_settlements(self, current_date: datetime):
        """更新T+1资金结算
        
        T+1规则：卖出股票所得资金
        - T+1日：资金可用（可买股票）
        - T+2日：资金可取（能提现）
        """
        new_pending = []
        for settle_date, amount in self.pending_settlements:
            if settle_date <= current_date:
                # T+1日：资金可用（可买股票）
                self.available += amount
                # 资金T+2日可取，加入待取队列
                withdrawable_date = settle_date + timedelta(days=1)
                self.pending_withdrawals.append((withdrawable_date, amount))
            else:
                new_pending.append((settle_date, amount))
        self.pending_settlements = new_pending
        
        # 处理可取资金（T+2日释放）
        new_withdrawals = []
        for withdrawable_date, amount in self.pending_withdrawals:
            if withdrawable_date <= current_date:
                self.withdrawable += amount
            else:
                new_withdrawals.append((withdrawable_date, amount))
        self.pending_withdrawals = new_withdrawals


class ExecutionEngineV3:
    """
    生产级交易执行引擎 V3
    
    关键特性：
    - 历史规则感知（涨跌停、印花税、手数约束随时间变化）
    - 真实交易日历（跳过节假日）
    - 精确资金结算（T+1可用 vs T+0可取）
    - 板块差异化处理（主板/创业板/科创板/北交所）
    """
    
    def __init__(
        self,
        initial_cash: float = 1e7,
        commission_rate: float = 0.0003,
        min_commission: float = 5.0,
        slippage_rate: float = 0.001,
        adv_limit: float = 0.1,
    ):
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.slippage_rate = slippage_rate
        self.adv_limit = adv_limit  # ADV 流动性限制：单笔交易不超过日均成交量的此比例（默认10%）
        
        self.calendar = TradingCalendar()
        self.cash = CashAccount(total=initial_cash, available=initial_cash, withdrawable=initial_cash)
        self.positions: Dict[str, Position] = {}
        self.pending_orders: List[Order] = []
        self.trade_history: List[Trade] = []
        self.order_counter = 0
        
        logger.info(f"ExecutionEngineV3 initialized: cash={initial_cash:,.0f}")
    
    def generate_orders(
        self,
        target_weights: Dict[str, float],
        current_date: datetime,
        prices: Dict[str, float],
        total_value: Optional[float] = None,
    ) -> List[Order]:
        """
        根据目标权重生成订单
        
        Parameters
        ----------
        target_weights : Dict[str, float]
            目标权重 {symbol: weight}
        current_date : datetime
            当前日期（用于规则判断）
        prices : Dict[str, float]
            当前价格
        total_value : float, optional
            总资产，默认使用当前总资产
        """
        if total_value is None:
            position_value = sum(
                pos.shares * prices.get(sym, 0) 
                for sym, pos in self.positions.items()
            )
            # 总资产 = 可用资金 + 持仓市值 + 待结算资金（已卖出但T+1未到账）
            pending_amount = sum(amount for _, amount in self.cash.pending_settlements)
            total_value = self.cash.available + position_value + pending_amount
        
        orders = []
        signal_date = current_date
        exec_date = self.calendar.next_trading_day(current_date)
        
        for symbol, weight in target_weights.items():
            if symbol not in prices or prices[symbol] <= 0:
                continue
            
            target_value = total_value * weight
            target_shares = int(target_value / prices[symbol])
            
            # 应用手数约束
            target_shares = AShareTradingRules.round_shares(symbol, current_date, target_shares)
            
            current_shares = self.positions.get(symbol, Position(symbol)).shares
            delta = target_shares - current_shares
            
            if delta == 0:
                continue
            
            side = OrderSide.BUY if delta > 0 else OrderSide.SELL
            
            # 卖出检查：是否有足够可卖数量
            if side == OrderSide.SELL:
                available = self.positions.get(symbol, Position(symbol)).available_shares
                sell_shares = min(abs(delta), available)
                if sell_shares <= 0:
                    continue
                delta = -sell_shares
            
            orders.append(Order(
                symbol=symbol,
                side=side,
                target_shares=abs(delta),
                signal_date=signal_date,
                execution_date=exec_date,
            ))
        
        return orders
    
    def execute(
        self,
        orders: List[Order],
        market_data: pd.DataFrame,
        current_date: datetime,
    ) -> Tuple[List[Trade], List[Order]]:
        """
        执行订单
        
        Returns
        -------
        trades : List[Trade]
            成交记录
        remaining : List[Order]
            未完成的订单（顺延至下一交易日）
        """
        # 更新资金结算
        self.cash.update_settlements(current_date)
        
        # T+1持仓可卖数量更新（在每日开始时释放昨日买入的冻结）
        self.update_position_available(current_date)
        
        trades = []
        remaining = []
        
        for order in orders:
            if order.status != OrderStatus.PENDING:
                continue
            
            # 检查是否到执行日
            if current_date < order.execution_date:
                remaining.append(order)
                continue
            
            # 获取股票数据
            row = market_data[market_data['symbol'] == order.symbol]
            if row.empty:
                order.execution_date = self.calendar.next_trading_day(current_date)
                remaining.append(order)
                continue
            
            r = row.iloc[0]
            
            # 检查交易限制
            if self._is_blocked(order, r, current_date):
                order.execution_date = self.calendar.next_trading_day(current_date)
                remaining.append(order)
                continue
            
            # ADV流动性限制
            adv = r.get('avg_volume_20', r.get('volume', 0))
            max_shares = AShareTradingRules.round_shares(
                order.symbol, current_date, 
                int(adv * self.adv_limit)
            )
            exec_shares = min(order.remaining_shares, max_shares)
            
            if exec_shares <= 0:
                order.execution_date = self.calendar.next_trading_day(current_date)
                remaining.append(order)
                continue
            
            # 资金检查
            price = r['open']
            amount = exec_shares * price
            
            if order.side == OrderSide.BUY:
                # 买入需要可用资金
                total_cost = self._estimate_cost(order.side, amount, current_date)
                if self.cash.available < total_cost:
                    # 资金不足，部分成交或跳过
                    max_affordable = int(self.cash.available / (price * (1 + self.commission_rate + self.slippage_rate)))
                    exec_shares = AShareTradingRules.round_shares(order.symbol, current_date, max_affordable)
                    if exec_shares <= 0:
                        remaining.append(order)
                        continue
                    amount = exec_shares * price
            
            # 执行成交
            trade = self._create_trade(order, exec_shares, price, current_date)
            trades.append(trade)
            self.trade_history.append(trade)
            
            # 更新订单状态
            order.filled_shares += exec_shares
            if order.filled_shares >= order.target_shares:
                order.status = OrderStatus.FILLED
            else:
                order.status = OrderStatus.PARTIAL
                order.execution_date = self.calendar.next_trading_day(current_date)
                remaining.append(order)
            
            # 更新持仓和资金
            self._update_position_and_cash(order, trade)
        
        return trades, remaining
    
    def _is_blocked(self, order: Order, r: pd.Series, date: datetime) -> bool:
        """检查交易是否被限制（涨跌停判断）
        
        修复说明：
        - 订单在 T-1 日生成（T-1 收盘后调仓），market_data 是 T-1 日数据
        - 无法在 T-1 日预知 T 日的 high/low，只能用 pre_close 计算涨跌停板价格
        - 判断逻辑：T 日开盘价 = 涨停板价格 → 无量涨停无法买入；开盘价 = 跌停板价格 → 无量跌停无法卖出
        
        涨跌停规则：
        - 涨停：open = pre_close * (1 + limit) → 封板，成交量极小时无法买入
        - 跌停：open = pre_close * (1 - limit) → 封板，成交量极小时无法卖出
        - 有量涨跌停：可以排队成交
        """
        # 停牌
        if r.get('is_suspended', False):
            return True
        
        # 用前收盘价计算涨跌停板价格
        # 注意：daily_quotes 中字段名为 pre_close
        prev_close = r.get('pre_close', r.get('close', r['open']))
        if prev_close <= 0:
            return True  # 无效价格，当停牌处理
        
        price_limit = AShareTradingRules.get_price_limit(order.symbol, date)
        limit_up_price = prev_close * (1 + price_limit)
        limit_down_price = prev_close * (1 - price_limit)
        
        # T日开盘价（来自 T-1 日的 market_data，即 T 日开盘价）
        open_price = r.get('open', 0)
        volume = r.get('volume', 0)
        
        # 20日均量判断是否"无量"
        avg_volume = r.get('avg_volume_20', None)
        if avg_volume is None or avg_volume <= 0:
            # 无法判断，保守处理：只要触及涨跌停就阻塞
            is_low_volume = True
            logger.debug(f"avg_volume_20 missing for {order.symbol}, conservative block")
        else:
            is_low_volume = volume < avg_volume * 0.1  # 成交量低于20日均量10%
        
        if order.side == OrderSide.BUY:
            # 买入被阻：T日开盘价触及涨停板，且无量
            # tolerance: 涨停价误差0.1%（浮点精度）
            if open_price >= limit_up_price * 0.999 and is_low_volume:
                logger.debug(f"BUY blocked: {order.symbol} limit-up at open={open_price:.2f}, limit={limit_up_price:.2f}")
                return True
        else:
            # 卖出被阻：T日开盘价触及跌停板，且无量
            if open_price <= limit_down_price * 1.001 and is_low_volume:
                logger.debug(f"SELL blocked: {order.symbol} limit-down at open={open_price:.2f}, limit={limit_down_price:.2f}")
                return True
        
        return False
    
    def _estimate_cost(self, side: OrderSide, amount: float, date: datetime) -> float:
        """预估交易成本"""
        commission = max(amount * self.commission_rate, self.min_commission)
        slippage = amount * self.slippage_rate
        stamp_tax = 0.0
        if side == OrderSide.SELL:
            stamp_tax = amount * AShareTradingRules.get_stamp_tax_rate(date)
        return amount + commission + slippage + stamp_tax
    
    def _create_trade(
        self, order: Order, shares: int, price: float, date: datetime
    ) -> Trade:
        """创建成交记录"""
        amount = shares * price
        commission = max(amount * self.commission_rate, self.min_commission)
        slippage = amount * self.slippage_rate
        
        stamp_tax = 0.0
        if order.side == OrderSide.SELL:
            stamp_tax = amount * AShareTradingRules.get_stamp_tax_rate(date)
        
        self.order_counter += 1
        
        return Trade(
            order_id=f"ORD{date:%Y%m%d}_{self.order_counter:06d}",
            symbol=order.symbol,
            side=order.side,
            shares=shares,
            price=price,
            date=date,
            commission=commission,
            stamp_tax=stamp_tax,
            slippage=slippage,
        )
    
    def _update_position_and_cash(self, order: Order, trade: Trade):
        """更新持仓和资金
        
        资金会计规则：
        - 买入：可用资金减少（股票价值+佣金+滑点），总资产减少（仅手续费）
          买入的股票市值已体现在 position_value 中，不额外减少 total
        - 卖出：持仓减少，所得资金T+1可用
          总资产减少（仅手续费），卖出所得通过 pending_settlements 追踪
        """
        symbol = order.symbol
        
        if order.side == OrderSide.BUY:
            # 买入：减少可用资金（股票价值+费用），总资产不变
            # 资金从 available 转为持仓市值，只有手续费是损失
            total_cost = trade.shares * trade.price + trade.commission + trade.slippage
            self.cash.available -= total_cost
            # 总资产减少手续费（佣金+滑点）
            self.cash.total -= (trade.commission + trade.slippage)
            
            # 更新持仓
            if symbol not in self.positions:
                self.positions[symbol] = Position(symbol)
            
            pos = self.positions[symbol]
            # 计算新平均成本
            total_cost_basis = pos.avg_cost * pos.shares + trade.shares * trade.price
            pos.shares += trade.shares
            pos.avg_cost = total_cost_basis / pos.shares if pos.shares > 0 else 0
            # 买入的股票当日不可卖（T+1），次日结算时由 update_position_available 释放
            
        else:
            # 卖出：减少持仓，资金T+1可用
            if symbol in self.positions:
                pos = self.positions[symbol]
                pos.shares -= trade.shares
                pos.available_shares -= trade.shares
                
                if pos.shares <= 0:
                    del self.positions[symbol]
            
            # 卖出所得资金T+1可用（扣除手续费后的净额）
            net_proceeds = trade.shares * trade.price - trade.commission - trade.stamp_tax - trade.slippage
            settle_date = self.calendar.next_trading_day(trade.date)
            self.cash.pending_settlements.append((settle_date, net_proceeds))
            
            # 总资产仅减少手续费（卖出本身是资产形态转换，不是损失）
            self.cash.total -= (trade.commission + trade.stamp_tax + trade.slippage)
    
    def update_position_available(self, current_date: datetime):
        """T+1持仓可卖数量更新
        
        规则：
        - 每个交易日开始时，所有持仓的 available_shares 同步为 shares
        - 当日买入的股票不可卖（T+1），因为买入发生在本方法之后
        - 当日卖出的股票已从 available_shares 扣减
        """
        for sym, pos in self.positions.items():
            pos.available_shares = pos.shares
    
    def get_portfolio_value(self, prices: Dict[str, float]) -> Dict[str, float]:
        """获取组合市值
        
        total_value 计算逻辑：
        - 可用资金 + 持仓市值 + 待结算资金（T+1可用但尚未结算的资金）
        - 确保在卖出当天 total_value 不会因 pending_settlements 而虚减
        """
        position_value = sum(
            pos.shares * prices.get(sym, 0)
            for sym, pos in self.positions.items()
        )
        # 待结算资金（已卖出但尚未到账的部分）
        pending_amount = sum(amount for _, amount in self.cash.pending_settlements)
        
        total_value = self.cash.available + position_value + pending_amount
        
        return {
            'cash_available': self.cash.available,
            'cash_withdrawable': self.cash.withdrawable,
            'position_value': position_value,
            'pending_settlements': pending_amount,
            'total_value': total_value,
        }
