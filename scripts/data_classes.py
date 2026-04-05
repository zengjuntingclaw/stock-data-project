"""数据类定义"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Tuple

class OrderStatus(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

class StockStatus(Enum):
    NORMAL = "normal"
    ST = "st"
    STAR_ST = "star_st"
    SUSPENDED = "suspended"
    DELISTING = "delisting"
    DELISTED = "delisted"

@dataclass
class Order:
    symbol: str
    side: OrderSide
    target_shares: int
    signal_date: datetime
    execution_date: datetime
    status: OrderStatus = OrderStatus.PENDING
    filled_shares: int = 0
    avg_price: float = 0.0
    reject_reason: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    
    @property
    def remaining_shares(self) -> int:
        return self.target_shares - self.filled_shares

@dataclass
class Trade:
    order_id: str
    symbol: str
    side: OrderSide
    shares: int
    price: float
    date: datetime
    commission: float
    stamp_tax: float
    slippage: float
    
    @property
    def total_cost(self) -> float:
        return self.commission + self.stamp_tax + self.slippage
    
    @property
    def net_amount(self) -> float:
        amount = self.shares * self.price
        return -(amount + self.total_cost) if self.side == OrderSide.BUY else amount - self.total_cost

@dataclass
class PerformanceMetrics:
    total_return: float
    annual_return: float
    annual_volatility: float
    sharpe_ratio: float
    max_drawdown: float
    calmar_ratio: float
    information_ratio: float
    tracking_error: float
    beta: float
    alpha: float
    max_dd_days: int
    avg_turnover: float
    yearly_metrics: Optional[dict] = None

@dataclass
class TradingCostConfig:
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001
    slippage_rate: float = 0.001
    
    def calculate_cost(self, side: OrderSide, amount: float) -> Tuple[float, float, float]:
        commission = max(amount * self.commission_rate, self.min_commission)
        stamp_tax = amount * self.stamp_tax_rate if side == OrderSide.SELL else 0.0
        slippage = amount * self.slippage_rate
        return commission, stamp_tax, slippage
