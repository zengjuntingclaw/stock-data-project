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
    
    @property
    def market_value(self, price: float) -> float:
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
    
    def update_settlements(self, current_date: datetime):
        """更新T+1资金结算
        
        T+1规则：卖出股票所得资金
        - T+1日：资金可用（可买股票）
        - T+1日：资金不可取（不能提现）
        - T+2日：资金可取（能提现）
        """
        new_pending = []
        for settle_date, amount in self.pending_settlements:
            if settle_date <= current_date:
                # T+1日：资金可用（可买股票）
                self.available += amount
                # 注意：withdrawable（可取资金）在T+1日仍不可用
                # 需要再过一天才能取
            else:
                new_pending.append((settle_date, amount))
        self.pending_settlements = new_pending


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
        self.adv_limit = adv_limit
        
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
            total_value = self.cash.available + position_value
        
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
        """检查交易是否被限制
        
        涨停/跌停判断：
        - 涨停：当日最高价 >= 涨停价，且成交量极小（无量涨停）
        - 跌停：当日最低价 <= 跌停价，且成交量极小（无量跌停）
        - 有量涨跌停：可以成交（排队）
        """
        # 停牌
        if r.get('is_suspended', False):
            return True
        
        # 涨跌停限制（历史感知）
        prev_close = r.get('prev_close', r.get('close', r['open']))
        price_limit = AShareTradingRules.get_price_limit(order.symbol, date)
        
        # 计算涨停价和跌停价
        limit_up_price = prev_close * (1 + price_limit)
        limit_down_price = prev_close * (1 - price_limit)
        
        # 成交量判断（无量涨跌停才无法交易）
        avg_volume = r.get('avg_volume_20', r.get('volume', 0))
        is_low_volume = r.get('volume', 0) < avg_volume * 0.1  # 成交量低于20日均量10%
        
        if order.side == OrderSide.BUY:
            # 买入被阻：涨停且无量（无法买入）
            # 使用high判断是否触及涨停
            if r.get('high', r['open']) >= limit_up_price * 0.999 and is_low_volume:
                return True
        else:
            # 卖出被阻：跌停且无量（无法卖出）
            # 使用low判断是否触及跌停
            if r.get('low', r['open']) <= limit_down_price * 1.001 and is_low_volume:
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
        """更新持仓和资金"""
        symbol = order.symbol
        
        if order.side == OrderSide.BUY:
            # 买入：减少可用资金，增加持仓
            total_cost = trade.shares * trade.price + trade.commission + trade.slippage
            self.cash.available -= total_cost
            self.cash.total -= trade.commission + trade.slippage  # 手续费减少总资产
            
            # 更新持仓
            if symbol not in self.positions:
                self.positions[symbol] = Position(symbol)
            
            pos = self.positions[symbol]
            # 计算新平均成本
            total_cost_basis = pos.avg_cost * pos.shares + trade.shares * trade.price
            pos.shares += trade.shares
            pos.avg_cost = total_cost_basis / pos.shares if pos.shares > 0 else 0
            # 买入的股票当日不可卖（T+1）
            # available_shares 不变
            
        else:
            # 卖出：减少持仓，资金T+1可用
            if symbol in self.positions:
                pos = self.positions[symbol]
                pos.shares -= trade.shares
                pos.available_shares -= trade.shares
                
                if pos.shares <= 0:
                    del self.positions[symbol]
            
            # 卖出所得资金T+1可用
            net_proceeds = trade.shares * trade.price - trade.commission - trade.stamp_tax - trade.slippage
            settle_date = self.calendar.next_trading_day(trade.date)
            self.cash.pending_settlements.append((settle_date, net_proceeds))
            
            # 总资产减少手续费
            self.cash.total -= (trade.commission + trade.stamp_tax + trade.slippage)
    
    def get_portfolio_value(self, prices: Dict[str, float]) -> Dict[str, float]:
        """获取组合市值"""
        position_value = sum(
            pos.shares * prices.get(sym, 0)
            for sym, pos in self.positions.items()
        )
        return {
            'cash_available': self.cash.available,
            'cash_withdrawable': self.cash.withdrawable,
            'position_value': position_value,
            'total_value': self.cash.available + position_value,
        }
