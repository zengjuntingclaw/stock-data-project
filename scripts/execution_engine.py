"""交易执行引擎"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
from loguru import logger
from scripts.data_classes import Order, Trade, OrderStatus, OrderSide, TradingCostConfig

class ExecutionEngine:
    def __init__(self, cost_config: Optional[TradingCostConfig] = None, adv_limit: float = 0.1):
        self.cost_config = cost_config or TradingCostConfig()
        self.adv_limit = adv_limit
        self.pending_orders: Dict[str, Order] = {}
        self.trade_history: List[Trade] = []
        self.order_counter = 0
    
    def generate_orders(self, target: Dict[str, int], current: Dict[str, int], signal_date: datetime) -> List[Order]:
        orders = []
        exec_date = self._next_day(signal_date)
        for symbol in set(target) | set(current):
            delta = (target.get(symbol, 0) - current.get(symbol, 0)) // 100 * 100
            if delta:
                orders.append(Order(symbol, OrderSide.BUY if delta > 0 else OrderSide.SELL, abs(delta), signal_date, exec_date))
        return orders
    
    def execute(self, orders: List[Order], market: pd.DataFrame, date: datetime) -> Tuple[List[Trade], List[Order]]:
        trades, remaining = [], []
        for order in orders:
            if order.status != OrderStatus.PENDING or date < order.execution_date:
                remaining.append(order)
                continue
            row = market[market['symbol'] == order.symbol]
            if row.empty:
                order.execution_date = self._next_day(date)
                remaining.append(order)
                continue
            r = row.iloc[0]
            if self._is_blocked(order, r):
                order.execution_date = self._next_day(date)
                remaining.append(order)
                continue
            adv = r.get('adv_20', r.get('volume', 0))
            exec_shares = min(order.remaining_shares, int(adv * self.adv_limit / 100) * 100)
            if exec_shares <= 0:
                order.execution_date = self._next_day(date)
                remaining.append(order)
                continue
            price = r['open']
            amount = exec_shares * price
            c, s, sl = self.cost_config.calculate_cost(order.side, amount)
            trade = Trade(self._gen_id(), order.symbol, order.side, exec_shares, price, date, c, s, sl)
            trades.append(trade)
            self.trade_history.append(trade)
            order.filled_shares += exec_shares
            order.avg_price = (order.avg_price * (order.filled_shares - exec_shares) + price * exec_shares) / order.filled_shares
            order.status = OrderStatus.FILLED if order.filled_shares >= order.target_shares else OrderStatus.PARTIAL
            if order.status == OrderStatus.PARTIAL:
                order.execution_date = self._next_day(date)
                remaining.append(order)
        return trades, remaining
    
    def _is_blocked(self, order: Order, r: pd.Series) -> bool:
        if r.get('is_suspended'): return True
        if order.side == OrderSide.BUY and r.get('is_limit_up'): return True
        if order.side == OrderSide.SELL and r.get('is_limit_down'): return True
        return False
    
    def _next_day(self, d: datetime) -> datetime:
        nd = d + timedelta(days=1)
        while nd.weekday() >= 5: nd += timedelta(days=1)
        return nd
    
    def _gen_id(self) -> str:
        self.order_counter += 1
        return f"ORD{datetime.now():%Y%m%d%H%M%S}_{self.order_counter:04d}"
