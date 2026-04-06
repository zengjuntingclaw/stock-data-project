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
    """
    交易成本配置（支持历史时间感知）
    
    A股印花税历史调整：
    - 2023-08-28之前：卖方 1‰
    - 2023-08-28及之后：卖方 0.5‰（减半征收）
    """
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001  # 默认值（旧税率）
    slippage_rate: float = 0.001
    
    # 印花税调整关键日期
    STAMP_TAX_REFORM_DATE = datetime(2023, 8, 28)
    STAMP_TAX_RATE_OLD = 0.001   # 1‰
    STAMP_TAX_RATE_NEW = 0.0005  # 0.5‰
    
    def get_stamp_tax_rate(self, trade_date: Optional[datetime] = None) -> float:
        """
        获取指定日期的印花税率
        
        Parameters
        ----------
        trade_date : datetime, optional
            交易日期。如不提供，返回当前配置的税率
        """
        if trade_date is None:
            return self.stamp_tax_rate
        
        # 2023-08-28起减半征收
        if trade_date >= self.STAMP_TAX_REFORM_DATE:
            return self.STAMP_TAX_RATE_NEW
        return self.STAMP_TAX_RATE_OLD
    
    def calculate_cost(self, side: OrderSide, amount: float, 
                       trade_date: Optional[datetime] = None) -> Tuple[float, float, float]:
        """
        计算交易成本
        
        Parameters
        ----------
        side : OrderSide
            买卖方向
        amount : float
            成交金额
        trade_date : datetime, optional
            交易日期（用于确定印花税率）
        """
        commission = max(amount * self.commission_rate, self.min_commission)
        stamp_tax = 0.0
        if side == OrderSide.SELL:
            stamp_tax_rate = self.get_stamp_tax_rate(trade_date)
            stamp_tax = amount * stamp_tax_rate
        slippage = amount * self.slippage_rate
        return commission, stamp_tax, slippage
