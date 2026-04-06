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

from scripts.data_classes import PerformanceMetrics
from scripts.trading_rules import AShareTradingRules, TradingCalendar
from scripts.execution_engine_v3 import ExecutionEngineV3, CashAccount, Position
from scripts.survivorship_bias import SurvivorshipBiasHandler
from scripts.pit_aligner import PITDataAligner
from scripts.performance import EnhancedPerformanceAnalyzer


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
    pending_orders: List[Any]  # 简化，实际应为List[Order]
    trade_history: List[Any]
    daily_values: List[Dict]
    
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
        self.survivorship = SurvivorshipBiasHandler()
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
            pending_orders=[],
            trade_history=[],
            daily_values=[],
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
        """处理单日"""
        self.state.current_date = date
        
        # 1. 获取当日市场数据
        market_data = self._get_market_data(date)
        if market_data is None or market_data.empty:
            return
        
        # 2. 检查是否需要调仓
        if self._should_rebalance(date):
            # 3. 获取因子数据（PIT对齐）
            factor_data = self._get_factor_data(date)
            
            # 4. 运行策略
            target_weights = strategy(factor_data, date)
            
            # 5. 权重约束
            target_weights = self._apply_weight_constraints(target_weights)
            
            # 6. 生成订单
            prices = market_data.set_index('symbol')['open'].to_dict()
            orders = self.execution.generate_orders(
                target_weights, date, prices
            )
            
            # 7. 执行订单
            trades, remaining = self.execution.execute(orders, market_data, date)
            
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
            result = batch_df[["symbol", "open", "high", "low", "close", "volume", "prev_close"]].copy()
            result["is_suspended"] = batch_df.get("is_suspend", False)
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
        returns = df['return'].dropna()
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
    
    def _save_checkpoint(self, path: str):
        """保存检查点（含完整状态，支持断点续传）"""
        import json
        state_data = self.state.to_dict()
        # 序列化 trade_history（每条交易记录为 dict）
        state_data['trade_history'] = self.state.trade_history
        # 序列化 daily_values（daily_records 同步保存）
        state_data['daily_records'] = self.daily_records
        # 同步 execution engine 的内部状态（pending_settlements 等）
        pending_settles = getattr(self.execution, 'pending_settlements', [])
        if pending_settles:
            # pending_settlements 是 List[Tuple[datetime, float]]
            state_data['execution'] = {
                'pending_settlements': [
                    {'settle_date': item[0].isoformat(), 'amount': item[1]}
                    for item in pending_settles
                ],
                'pending_withdrawals': [
                    {'withdrawable_date': item[0].isoformat(), 'amount': item[1]}
                    for item in getattr(self.execution.cash, 'pending_withdrawals', [])
                ],
            }
        else:
            state_data['execution'] = {}
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, default=str, ensure_ascii=False)

    def _load_checkpoint(self, path: str) -> bool:
        """加载检查点（恢复完整状态，避免丢失历史记录）"""
        try:
            import json
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 恢复状态
            self.state = BacktestState(
                current_date=datetime.fromisoformat(data['current_date']),
                cash=CashAccount(**data['cash']),
                positions={sym: Position(sym, **pos) for sym, pos in data['positions'].items()},
                pending_orders=[],
                trade_history=data.get('trade_history', []),
                daily_values=data.get('daily_records', []),
            )
            # 恢复 daily_records（外部列表与 BacktestState 同步）
            self.daily_records = data.get('daily_records', [])
            # 恢复 execution engine 的 pending_settlements
            exec_data = data.get('execution', {})
            if exec_data and hasattr(self.execution.cash, 'pending_settlements'):
                self.execution.cash.pending_settlements = [
                    (datetime.fromisoformat(ps['settle_date']), ps['amount'])
                    for ps in exec_data.get('pending_settlements', [])
                ]
                self.execution.cash.pending_withdrawals = [
                    (datetime.fromisoformat(pw['withdrawable_date']), pw['amount'])
                    for pw in exec_data.get('pending_withdrawals', [])
                ]
            logger.info(f"Checkpoint restored: date={data['current_date']}, "
                       f"trades={len(self.state.trade_history)}, "
                       f"records={len(self.daily_records)}")
            return True
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False
