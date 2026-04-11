"""生产级回测引擎 V3

整合所有生产级组件：
- TradingRules: 历史规则感知
- ExecutionEngineV3: 精确交易执行
- SurvivorshipBiasHandler: 幸存者偏差治理
- PITDataAligner: 未来函数防护
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from loguru import logger

from scripts.data_classes import PerformanceMetrics, Order
from scripts.trading_rules import AShareTradingRules, TradingCalendar
from scripts.execution_engine_v3 import ExecutionEngineV3, CashAccount, Position
from scripts.survivorship_bias import SurvivorshipBiasHandler
from scripts.pit_aligner import PITDataAligner
from scripts.performance import EnhancedPerformanceAnalyzer
from scripts.checkpoint_manager import (
    CheckpointManager, BacktestState as CMBacktestState,
    CashState, PositionState, OrderState, TradeState,
    extract_state_from_engine, restore_engine_from_state
)


@dataclass
class BacktestConfig:
    """回测配置"""
    start_date: datetime
    end_date: datetime
    initial_capital: float = 1e7
    
    # 调仓频率
    rebalance_freq: str = 'ME'  # ME=月末, W-FRI=周五, D=每日
    
    # 交易成本
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    slippage_rate: float = 0.001
    
    # 风控参数
    max_position_weight: float = 0.10  # 单票最大权重
    min_position_weight: float = 0.005  # 单票最小权重（低于则清仓）
    
    # 股票池参数
    universe_size: int = 500  # 股票池大小
    min_market_cap: float = 5e8  # 最小市值5亿
    
    # 数据参数
    price_field: str = 'open'  # 成交价字段（open=开盘价, vwap=均价）


@dataclass
class BacktestState:
    """回测状态（可序列化，支持断点续传）"""
    current_date: datetime
    cash: CashAccount
    positions: Dict[str, Position]
    trade_history: List[Any]
    daily_values: List[Dict]
    pending_orders: List[Order] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'current_date': self.current_date.isoformat(),
            'cash': {
                'total': self.cash.total,
                'available': self.cash.available,
                'withdrawable': self.cash.withdrawable,
            },
            'positions': {
                sym: {'shares': pos.shares, 'available_shares': pos.available_shares, 'avg_cost': pos.avg_cost}
                for sym, pos in self.positions.items()
            },
        }


class ProductionBacktestEngine:
    """
    生产级回测引擎
    
    关键特性：
    1. 历史规则感知（涨跌停、印花税、手数约束）
    2. 真实交易日历（节假日处理）
    3. 幸存者偏差治理（含退市股）
    4. PIT数据对齐（无未来函数）
    5. 精确资金结算（T+1）
    6. 断点续传支持
    """
    
    def __init__(
        self,
        config: BacktestConfig,
        data_engine: Any,  # DataEngine实例
        factor_engine: Any,  # FactorStation实例
    ):
        self.config = config
        self.data_engine = data_engine
        self.factor_engine = factor_engine
        
        # 初始化组件
        self.calendar = TradingCalendar()
        self.execution = ExecutionEngineV3(
            initial_cash=config.initial_capital,
            commission_rate=config.commission_rate,
            min_commission=config.min_commission,
            slippage_rate=config.slippage_rate,
        )
        self.survivorship = SurvivorshipBiasHandler(data_engine=data_engine)
        self.pit_aligner = PITDataAligner()
        self.performance = EnhancedPerformanceAnalyzer()
        
        # 回测状态
        self.state: Optional[BacktestState] = None
        self.daily_records: List[Dict] = []
        
        logger.info(f"ProductionBacktestEngine initialized: {config.start_date.date()} ~ {config.end_date.date()}")
    
    def run(
        self,
        strategy: Callable[[pd.DataFrame, datetime], Dict[str, float]],
        checkpoint_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        运行回测
        
        Parameters
        ----------
        strategy : Callable
            策略函数，输入(因子数据, 当前日期)，输出{symbol: weight}
        checkpoint_path : str, optional
            检查点路径，支持断点续传
        
        Returns
        -------
        Dict : 回测结果
        """
        # 加载检查点或初始化
        if checkpoint_path and self._load_checkpoint(checkpoint_path):
            logger.info(f"Resumed from checkpoint: {checkpoint_path}")
        else:
            self._initialize()
        
        # 生成交易日序列
        trade_dates = self._generate_trade_dates()
        
        logger.info(f"Running backtest: {len(trade_dates)} trading days")
        
        for i, date in enumerate(trade_dates):
            if date < self.state.current_date:
                continue  # 跳过已处理的日期
            
            # 每日处理
            self._process_day(date, strategy)
            
            # 保存检查点（每100天保存一次，减少IO开销）
            if checkpoint_path and (i + 1) % 100 == 0:
                self._save_checkpoint(checkpoint_path)
                logger.debug(f"Checkpoint saved at {date.date()}")
            
            # 进度报告
            if (i + 1) % 50 == 0:
                progress = (i + 1) / len(trade_dates) * 100
                logger.info(f"Progress: {progress:.1f}% ({date.date()})")
        
        # 生成报告
        return self._generate_report()
    
    def _initialize(self):
        """初始化回测状态"""
        self.state = BacktestState(
            current_date=self.config.start_date,
            cash=self.execution.cash,
            positions=self.execution.positions,
            trade_history=[],
            daily_values=[],
            pending_orders=[],
        )
        
        # 加载历史股票池（幸存者偏差治理）
        try:
            self.survivorship.load_from_baostock(
                start_year=self.config.start_date.year,
                end_year=self.config.end_date.year,
            )
            stats = self.survivorship.get_statistics()
            logger.info(f"Initialized with {stats}")
        except Exception as e:
            logger.error(f"Failed to load survivorship data: {e}")
            logger.warning("Falling back to current stock universe - SURVIVORSHIP BIAS WARNING!")
            # 备选：使用当前股票（但会有幸存者偏差）
            # 这里应该有一个备选方案，比如从本地缓存加载
    
    def _generate_trade_dates(self) -> List[datetime]:
        """生成交易日序列"""
        dates = []
        current = self.config.start_date
        while current <= self.config.end_date:
            if self.calendar.is_trading_day(current):
                dates.append(current)
            current += timedelta(days=1)
        return dates
    
    def _process_day(
        self,
        date: datetime,
        strategy: Callable[[pd.DataFrame, datetime], Dict[str, float]],
    ):
        """处理单日
        
        Bug修复(P0-3)：pending_orders必须每个交易日执行，不止调仓日。
        根因：
        - 旧逻辑只在_should_rebalance()=True时执行，导致非调仓日未成交的订单永久挂起
        - 旧逻辑在调仓日生成新订单时未合并pending_orders，导致两部分订单孤立执行
        修复：
        - 步骤1：执行pending_orders（每个交易日必执行）
        - 步骤2：如果是调仓日，生成新订单并与remaining合并，再执行
        """
        self.state.current_date = date
        
        # 1. 获取当日市场数据
        market_data = self._get_market_data(date)
        if market_data is None or market_data.empty:
            return
        
        # === 步骤0(修复P0-3)：每个交易日先执行pending_orders ===
        if self.state.pending_orders:
            prices = market_data.set_index('symbol')['open'].to_dict()
            # 更新pending订单的执行日期为当天
            for order in self.state.pending_orders:
                order.execution_date = date
            trades, remaining = self.execution.execute(
                self.state.pending_orders, market_data, date
            )
            self.state.pending_orders = remaining
            self.state.trade_history.extend(trades)
        
        # 2. 检查是否需要调仓
        if self._should_rebalance(date):
            # 3. 获取因子数据（PIT对齐）
            factor_data = self._get_factor_data(date)
            
            # 4. 运行策略
            target_weights = strategy(factor_data, date)
            
            # 5. 权重约束
            target_weights = self._apply_weight_constraints(target_weights)
            
            # 6. 生成新订单（包含信号重算后的全量目标）
            prices = market_data.set_index('symbol')['open'].to_dict()
            new_orders = self.execution.generate_orders(
                target_weights, date, prices
            )
            
            # === 步骤7(修复P0-3)：新订单与remaining合并 ===
            # 若步骤0执行后产生了remaining(含涨跌停/流动性阻塞)，将其加入新订单
            all_orders = new_orders + self.state.pending_orders
            
            # 7. 执行合并后订单
            trades, remaining = self.execution.execute(all_orders, market_data, date)
            
            self.state.pending_orders = remaining
            self.state.trade_history.extend(trades)
        
        # 8. 记录每日状态
        self._record_daily_state(date, market_data)
    
    def _get_market_data(self, date: datetime) -> Optional[pd.DataFrame]:
        """获取市场数据 - 使用T-1日数据避免未来函数
        
        关键：策略在T日生成的信号，只能使用T-1日及之前的数据
        成交价使用T日开盘价（T+1执行），但数据获取用T-1日避免lookahead bias
        """
        # 获取上一交易日数据（避免未来函数）
        prev_date = self.calendar.prev_trading_day(date)
        prev_date_str = prev_date.strftime("%Y-%m-%d") if isinstance(prev_date, datetime) else str(prev_date)
        
        try:
            # 获取T-1日的股票池
            universe = self.survivorship.get_universe(prev_date, include_delisted=True)
            if not universe:
                return None
            
            # 批量获取数据（单条SQL替代N次查询，大幅减少数据库I/O）
            batch_df = self.data_engine.get_batch_stock_data(
                universe[:self.config.universe_size], prev_date_str
            )
            
            if batch_df.empty:
                return None
            
            # 标准化列名
            # 注意：daily_quotes DDL 中字段名为 pre_close，而非 prev_close
            result = batch_df[["symbol", "open", "high", "low", "close", "volume", "pre_close"]].copy()
            result["is_suspend"] = batch_df.get("is_suspend", False)
            return result
            
        except Exception as e:
            logger.error(f"Failed to get market data for {date}: {e}")
            return None
    
    def _get_factor_data(self, date: datetime) -> pd.DataFrame:
        """获取因子数据（PIT对齐 - Point In Time）
        
        PIT原则：使用公告日(ann_date)而非报告期(end_date)
        确保T日只能使用T日及之前已公告的数据
        """
        # 获取上一交易日（与market_data保持一致）
        prev_date = self.calendar.prev_trading_day(date)
        
        try:
            # 从pit_aligner获取PIT对齐的因子数据
            # 使用正确的方法名：get_factors（复数形式）
            factor_df = self.pit_aligner.get_factors(
                factor_names=['roe', 'pe_ttm', 'pb'],
                date=prev_date,
                symbols=None  # 获取全市场
            )
            
            if factor_df.empty:
                logger.warning(f"No factor data available for {prev_date.date()}")
                
            return factor_df
            
        except Exception as e:
            logger.error(f"Failed to get factor data for {date}: {e}")
            return pd.DataFrame()
    
    def _should_rebalance(self, date: datetime) -> bool:
        """判断是否调仓日"""
        if self.config.rebalance_freq == 'ME':
            # 月末
            next_day = date + timedelta(days=1)
            return next_day.month != date.month
        elif self.config.rebalance_freq == 'W-FRI':
            return date.weekday() == 4  # 周五
        elif self.config.rebalance_freq == 'D':
            return True
        return False
    
    def _apply_weight_constraints(self, weights: Dict[str, float]) -> Dict[str, float]:
        """应用权重约束"""
        # 过滤过小权重
        weights = {k: v for k, v in weights.items() 
                  if v >= self.config.min_position_weight}
        
        # 限制最大权重
        weights = {k: min(v, self.config.max_position_weight) 
                  for k, v in weights.items()}
        
        # 归一化
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        
        return weights
    
    def _record_daily_state(self, date: datetime, market_data: pd.DataFrame):
        """记录每日状态"""
        prices = market_data.set_index('symbol')['close'].to_dict()
        portfolio = self.execution.get_portfolio_value(prices)
        
        record = {
            'date': date,
            'total_value': portfolio['total_value'],
            'cash_available': portfolio['cash_available'],
            'position_value': portfolio['position_value'],
            'positions': len(self.execution.positions),
        }
        
        self.daily_records.append(record)
    
    def _generate_report(self) -> Dict[str, Any]:
        """生成回测报告"""
        if not self.daily_records:
            return {'error': 'No records generated'}
        
        df = pd.DataFrame(self.daily_records)
        df['return'] = df['total_value'].pct_change()
        
        # 计算绩效指标
        returns = df['return'].dropna(how='all')
        metrics = self.performance.calculate(returns)
        
        report = {
            'config': {
                'start_date': self.config.start_date.date().isoformat(),
                'end_date': self.config.end_date.date().isoformat(),
                'initial_capital': self.config.initial_capital,
            },
            'performance': {
                'total_return': metrics.total_return,
                'annual_return': metrics.annual_return,
                'sharpe_ratio': metrics.sharpe_ratio,
                'max_drawdown': metrics.max_drawdown,
                'calmar_ratio': metrics.calmar_ratio,
            },
            'trades': len(self.state.trade_history),
            'final_value': df['total_value'].iloc[-1],
            'daily_records': df.to_dict('records'),
        }
        
        logger.info(f"Backtest completed: Return={metrics.total_return:.2%}, Sharpe={metrics.sharpe_ratio:.2f}")
        
        return report
    
    def _dataclass_to_dict(self, obj):
        """递归序列化 dataclass（含 Enum/datetime 正确处理）"""
        from dataclasses import is_dataclass
        from enum import Enum
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Enum):
            return obj.name  # "BUY"/"SELL"/"PENDING" 等
        elif isinstance(obj, float):
            return obj
        elif isinstance(obj, int):
            return obj
        elif isinstance(obj, str):
            return obj
        elif isinstance(obj, (list, tuple)):
            return [self._dataclass_to_dict(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self._dataclass_to_dict(v) for k, v in obj.items()}
        elif is_dataclass(obj):
            result = {}
            for field_name in obj.__dataclass_fields__:
                val = getattr(obj, field_name)
                result[field_name] = self._dataclass_to_dict(val)
            return result
        return str(obj)  # fallback

    def _save_checkpoint(self, path: str):
        """保存检查点（使用统一的CheckpointManager，确保save/load路径一致）"""
        # 使用新的checkpoint_manager模块，确保状态保存/恢复一致性
        manager = CheckpointManager(Path(path).parent)
        
        # 从execution engine提取完整状态
        state = extract_state_from_engine(self.execution, self.state.current_date)
        
        # 补充BacktestState特有的字段
        state.pending_orders = [
            OrderState.from_order(o) for o in self.state.pending_orders
        ]
        state.trade_history = [
            TradeState(
                order_id=getattr(t, 'order_id', ''),
                symbol=t.symbol,
                side=t.side.name,
                shares=t.shares,
                price=t.price,
                date=t.date.isoformat() if isinstance(t.date, datetime) else t.date,
                commission=getattr(t, 'commission', 0.0),
                stamp_tax=getattr(t, 'stamp_tax', 0.0),
                slippage=getattr(t, 'slippage', 0.0),
            )
            for t in self.state.trade_history
        ]
        state.daily_records = self.daily_records
        
        # 保存（同时生成.json和.pkl）
        manager.save(state, Path(path).stem)
        logger.info(f"Checkpoint saved: {path}")

    def _load_checkpoint(self, path: str) -> bool:
        """加载检查点（使用统一的CheckpointManager，确保save/load路径一致）"""
        try:
            from scripts.data_classes import Order, OrderSide
            
            manager = CheckpointManager(Path(path).parent)
            state = manager.load(path)
            
            if state is None:
                logger.error(f"Failed to load checkpoint from {path}")
                return False
            
            # 恢复 execution engine 状态
            restore_engine_from_state(self.execution, state)
            
            # 恢复 BacktestState
            self.state = BacktestState(
                current_date=datetime.fromisoformat(state.current_date),
                cash=self.execution.cash,  # 使用恢复后的cash
                positions=self.execution.positions,  # 使用恢复后的positions
                pending_orders=[
                    o.to_order() for o in state.pending_orders
                ],
                trade_history=[],  # Trade历史从state.trade_history恢复（如果需要）
                daily_values=state.daily_records,
            )
            self.daily_records = state.daily_records
            
            logger.info(f"Checkpoint restored: date={state.current_date}, "
                       f"trades={len(state.trade_history)}, "
                       f"pending_orders={len(state.pending_orders)}, "
                       f"records={len(state.daily_records)}")
            return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
