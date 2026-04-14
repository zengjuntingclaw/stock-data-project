"""
回测状态管理 - 可靠的Checkpoint/断点续跑
=========================================
核心设计原则：
1. 单一真相源：BacktestState 包含回测的所有状态
2. 完整序列化：所有状态必须可序列化/反序列化
3. 路径一致性：save和load使用完全相同的字段链路
4. 资金状态精确：T+1可用、T+2可取必须完全一致
"""

from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from pathlib import Path
import json
import pickle
from loguru import logger

# 导入数据类
from scripts.data_classes import Order, Trade, OrderStatus, OrderSide


# ============================================================================
# 状态类定义
# ============================================================================

@dataclass
class CashState:
    """现金账户状态（完整版，支持T+1/T+2精确恢复）"""
    total: float = 0.0                 # 总资产
    available: float = 0.0             # 可用资金（可买股票）
    withdrawable: float = 0.0          # 可取资金
    frozen: float = 0.0                # 冻结资金
    
    # T+1结算队列: [(settle_date_str, amount), ...]
    pending_settlements: List[Tuple[str, float]] = field(default_factory=list)
    
    # T+2可取队列: [(withdrawable_date_str, amount), ...]
    pending_withdrawals: List[Tuple[str, float]] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """序列化为字典"""
        return {
            'total': self.total,
            'available': self.available,
            'withdrawable': self.withdrawable,
            'frozen': self.frozen,
            'pending_settlements': self.pending_settlements,
            'pending_withdrawals': self.pending_withdrawals,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CashState':
        """从字典反序列化"""
        return cls(
            total=data.get('total', 0.0),
            available=data.get('available', 0.0),
            withdrawable=data.get('withdrawable', 0.0),
            frozen=data.get('frozen', 0.0),
            pending_settlements=[(item[0], float(item[1])) for item in data.get('pending_settlements', [])],
            pending_withdrawals=[(item[0], float(item[1])) for item in data.get('pending_withdrawals', [])],
        )


@dataclass
class PositionState:
    """持仓状态（完整版）"""
    symbol: str
    shares: int = 0                    # 总持仓
    available_shares: int = 0          # 可卖数量（T+1）
    avg_cost: float = 0.0              # 平均成本
    
    def to_dict(self) -> Dict:
        return {
            'symbol': self.symbol,
            'shares': self.shares,
            'available_shares': self.available_shares,
            'avg_cost': self.avg_cost,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PositionState':
        return cls(
            symbol=data['symbol'],
            shares=data.get('shares', 0),
            available_shares=data.get('available_shares', 0),
            avg_cost=data.get('avg_cost', 0.0),
        )


@dataclass
class OrderState:
    """订单状态（用于序列化）"""
    symbol: str
    side: str                          # 'BUY' or 'SELL'
    target_shares: int
    filled_shares: int
    status: str                        # OrderStatus.name
    signal_date: Optional[str] = None      # ISO格式日期
    execution_date: Optional[str] = None   # ISO格式日期
    avg_price: float = 0.0
    reject_reason: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'symbol': self.symbol,
            'side': self.side,
            'target_shares': self.target_shares,
            'filled_shares': self.filled_shares,
            'status': self.status,
            'signal_date': self.signal_date,
            'execution_date': self.execution_date,
            'avg_price': self.avg_price,
            'reject_reason': self.reject_reason,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'OrderState':
        return cls(**data)
    
    def to_order(self) -> Order:
        """转换为Order对象"""
        return Order(
            symbol=self.symbol,
            side=OrderSide[self.side],
            target_shares=self.target_shares,
            signal_date=datetime.fromisoformat(self.signal_date) if self.signal_date else None,
            execution_date=datetime.fromisoformat(self.execution_date) if self.execution_date else None,
            status=OrderStatus[self.status],
            filled_shares=self.filled_shares,
            avg_price=self.avg_price,
            reject_reason=self.reject_reason,
        )
    
    @classmethod
    def from_order(cls, order: Order) -> 'OrderState':
        """从Order对象创建"""
        return cls(
            symbol=order.symbol,
            side=order.side.name,
            target_shares=order.target_shares,
            filled_shares=order.filled_shares,
            status=order.status.name,
            signal_date=order.signal_date.isoformat() if order.signal_date else None,
            execution_date=order.execution_date.isoformat() if order.execution_date else None,
            avg_price=order.avg_price,
            reject_reason=order.reject_reason,
        )


@dataclass
class TradeState:
    """成交记录状态（用于序列化）"""
    order_id: str
    symbol: str
    side: str                          # 'BUY' or 'SELL'
    shares: int
    price: float
    date: str                          # ISO格式日期
    commission: float
    stamp_tax: float
    slippage: float
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TradeState':
        return cls(**data)


@dataclass
class BacktestState:
    """
    回测完整状态（单一真相源）
    
    包含回测的所有状态信息，用于checkpoint保存/恢复。
    """
    # 当前日期
    current_date: str                  # ISO格式日期
    
    # 资金状态
    cash: CashState
    
    # 持仓状态: {symbol: PositionState}
    positions: Dict[str, PositionState]
    
    # 挂单队列
    pending_orders: List[OrderState]
    
    # 成交历史
    trade_history: List[TradeState]
    
    # 每日记录
    daily_records: List[Dict]
    
    # 订单计数器（用于恢复 order_id 不冲突）
    order_counter: int = 0
    
    # 元数据
    version: str = "3.0"               # 状态格式版本
    created_at: Optional[str] = None    # ISO格式日期，None时自动填充
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = self._get_now()
    
    @classmethod
    def _get_now(cls) -> str:
        """获取当前时间（ISO格式），单测时可mock"""
        return datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        """完整序列化为字典"""
        return {
            'version': self.version,
            'created_at': self.created_at,
            'current_date': self.current_date,
            'cash': self.cash.to_dict(),
            'positions': {k: v.to_dict() for k, v in self.positions.items()},
            'pending_orders': [o.to_dict() for o in self.pending_orders],
            'trade_history': [t.to_dict() for t in self.trade_history],
            'daily_records': self.daily_records,
            'order_counter': self.order_counter,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BacktestState':
        """从字典完整反序列化"""
        return cls(
            version=data.get('version', '1.0'),
            created_at=data.get('created_at', ''),
            current_date=data['current_date'],
            cash=CashState.from_dict(data['cash']),
            positions={k: PositionState.from_dict(v) for k, v in data['positions'].items()},
            pending_orders=[OrderState.from_dict(o) for o in data.get('pending_orders', [])],
            trade_history=[TradeState.from_dict(t) for t in data.get('trade_history', [])],
            daily_records=data.get('daily_records', []),
            order_counter=data.get('order_counter', 0),
        )


# ============================================================================
# Checkpoint管理器
# ============================================================================

class CheckpointManager:
    """
    Checkpoint管理器
    
    提供可靠的保存/加载功能，确保状态完全一致。
    """
    
    CURRENT_VERSION = "3.0"
    
    def __init__(self, checkpoint_dir: str = "./checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, state: BacktestState, name: Optional[str] = None) -> str:
        """
        保存checkpoint
        
        Parameters
        ----------
        state : BacktestState
            回测状态
        name : str, optional
            checkpoint名称（默认使用时间戳）
        
        Returns
        -------
        str : 保存的文件路径
        """
        if name is None:
            name = f"checkpoint_{BacktestState._get_now()[:10]}"
        
        # 更新版本和创建时间
        state.version = self.CURRENT_VERSION
        state.created_at = BacktestState._get_now()
        
        # 保存为JSON（人类可读）
        json_path = self.checkpoint_dir / f"{name}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(state.to_dict(), f, indent=2, ensure_ascii=False, default=str)
        
        # 同时保存为pickle（精确恢复）
        pickle_path = self.checkpoint_dir / f"{name}.pkl"
        with open(pickle_path, 'wb') as f:
            pickle.dump(state, f)
        
        logger.info(f"Checkpoint saved: {json_path} (and .pkl)")
        return str(json_path)
    
    def load(self, path: str) -> Optional[BacktestState]:
        """
        加载checkpoint
        
        Parameters
        ----------
        path : str
            checkpoint文件路径（.json或.pkl）
        
        Returns
        -------
        BacktestState or None : 恢复的状态
        """
        path = Path(path)
        
        if not path.exists():
            # 尝试在checkpoint_dir中查找
            path = self.checkpoint_dir / path.name
        
        if not path.exists():
            logger.error(f"Checkpoint not found: {path}")
            return None
        
        try:
            if path.suffix == '.pkl':
                # 从pickle加载（精确恢复）
                with open(path, 'rb') as f:
                    state = pickle.load(f)
                logger.info(f"Checkpoint loaded from pickle: {path}")
                return state
            else:
                # 从JSON加载
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                state = BacktestState.from_dict(data)
                logger.info(f"Checkpoint loaded from JSON: {path}")
                return state
        
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """列出所有可用的checkpoint"""
        checkpoints = []
        
        for json_file in sorted(self.checkpoint_dir.glob("*.json")):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                checkpoints.append({
                    'name': json_file.stem,
                    'path': str(json_file),
                    'version': data.get('version', 'unknown'),
                    'created_at': data.get('created_at', 'unknown'),
                    'current_date': data.get('current_date', 'unknown'),
                    'cash_total': data.get('cash', {}).get('total', 0),
                    'positions_count': len(data.get('positions', {})),
                    'trades_count': len(data.get('trade_history', [])),
                })
            except Exception as e:
                logger.warning(f"Failed to read checkpoint {json_file}: {e}")
        
        return checkpoints
    
    def get_latest(self) -> Optional[str]:
        """获取最新的checkpoint路径"""
        checkpoints = self.list_checkpoints()
        if not checkpoints:
            return None
        
        # 按创建时间排序
        latest = sorted(checkpoints, key=lambda x: x['created_at'])[-1]
        return latest['path']


# ============================================================================
# 与ExecutionEngine的集成
# ============================================================================

def extract_state_from_engine(engine, current_date: datetime) -> BacktestState:
    """
    从ExecutionEngine提取完整状态
    
    Parameters
    ----------
    engine : ExecutionEngineV3
        执行引擎实例
    current_date : datetime
        当前日期
    
    Returns
    -------
    BacktestState : 完整状态
    """
    # 提取资金状态（⚠️ 必须完整包含 pending_withdrawals，否则 T+2 可取资金丢失）
    cash = CashState(
        total=engine.cash.total,
        available=engine.cash.available,
        withdrawable=engine.cash.withdrawable,
        frozen=engine.cash.frozen,
        pending_settlements=[
            (d.isoformat(), float(a))
            for d, a in engine.cash.pending_settlements
        ],
        # ⚠️ 修复：必须同时保存 pending_withdrawals（T+2可取队列）
        # 否则断点恢复后，部分资金会永久卡在"可用"状态无法提现
        pending_withdrawals=[
            (d.isoformat(), float(a))
            for d, a in engine.cash.pending_withdrawals
        ],
    )
    
    # 提取持仓状态
    positions = {}
    for symbol, pos in engine.positions.items():
        positions[symbol] = PositionState(
            symbol=pos.symbol,
            shares=pos.shares,
            available_shares=pos.available_shares,
            avg_cost=pos.avg_cost,
        )
    
    # 返回状态（pending_orders和trade_history由调用方填充）
    return BacktestState(
        current_date=current_date.isoformat(),
        cash=cash,
        positions=positions,
        pending_orders=[],  # 由调用方填充（见 backtest_engine_v3._save_checkpoint）
        trade_history=[],   # 由调用方填充（见 backtest_engine_v3._save_checkpoint）
        daily_records=[],
        order_counter=engine.order_counter,  # 恢复订单计数器，防止 order_id 冲突
    )


def restore_engine_from_state(engine, state: BacktestState) -> None:
    """
    从状态恢复ExecutionEngine
    
    Parameters
    ----------
    engine : ExecutionEngineV3
        执行引擎实例
    state : BacktestState
        回测状态
    """
    from scripts.execution_engine_v3 import CashAccount, Position
    
    # 恢复资金状态
    engine.cash = CashAccount(
        total=state.cash.total,
        available=state.cash.available,
        withdrawable=state.cash.withdrawable,
        frozen=state.cash.frozen,
        pending_settlements=[
            (datetime.fromisoformat(d), float(a))
            for d, a in state.cash.pending_settlements
        ],
        pending_withdrawals=[
            (datetime.fromisoformat(d), float(a))
            for d, a in state.cash.pending_withdrawals
        ],
    )
    
    # 恢复持仓
    engine.positions = {}
    for symbol, pos_state in state.positions.items():
        engine.positions[symbol] = Position(
            symbol=pos_state.symbol,
            shares=pos_state.shares,
            available_shares=pos_state.available_shares,
            avg_cost=pos_state.avg_cost,
        )

    # 恢复订单计数器（防止恢复后 order_id 与历史成交冲突）
    engine.order_counter = state.order_counter


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    import tempfile
    import shutil
    
    print("Checkpoint State Management Test:")
    print("=" * 60)
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    
    try:
        # 测试1: 创建状态
        print("\n1. Create BacktestState:")
        state = BacktestState(
            current_date='2024-01-15T00:00:00',
            cash=CashState(
                total=1000000.0,
                available=500000.0,
                withdrawable=300000.0,
                frozen=200000.0,
                pending_settlements=[('2024-01-16', 100000.0), ('2024-01-17', 50000.0)],
                pending_withdrawals=[('2024-01-17', 80000.0)],
            ),
            positions={
                '000001': PositionState('000001', 1000, 500, 10.0),
                '600000': PositionState('600000', 2000, 2000, 15.0),
            },
            pending_orders=[
                OrderState('000002', 'BUY', 500, 0, 'PENDING', '2024-01-14', '2024-01-15'),
            ],
            trade_history=[
                TradeState('T001', '000001', 'BUY', 1000, 10.0, '2024-01-10', 5.0, 0.0, 1.0),
            ],
            daily_records=[
                {'date': '2024-01-10', 'nav': 1000000},
                {'date': '2024-01-11', 'nav': 1005000},
            ],
            order_counter=42,
        )
        print(f"   Created: {len(state.positions)} positions, {len(state.pending_orders)} orders, order_counter={state.order_counter}")
        print("   [OK] Pass")
        
        # 测试2: 序列化/反序列化
        print("\n2. Serialization/Deserialization:")
        state_dict = state.to_dict()
        state2 = BacktestState.from_dict(state_dict)
        assert state2.cash.total == state.cash.total
        assert state2.positions['000001'].shares == 1000
        assert len(state2.cash.pending_settlements) == 2
        print(f"   After roundtrip: total={state2.cash.total}, available={state2.cash.available}")
        print("   [OK] Pass")
        
        # 测试3: Checkpoint保存/加载
        print("\n3. Checkpoint Save/Load:")
        manager = CheckpointManager(temp_dir)
        path = manager.save(state, "test")
        
        loaded_state = manager.load(path)
        assert loaded_state is not None
        assert loaded_state.cash.total == 1000000.0
        assert loaded_state.cash.pending_settlements[0][0] == '2024-01-16'
        print(f"   Loaded pending_settlements: {loaded_state.cash.pending_settlements}")
        print("   [OK] Pass")
        
        # 测试4: Pickle格式
        print("\n4. Pickle Format:")
        pickle_path = path.replace('.json', '.pkl')
        loaded_from_pickle = manager.load(pickle_path)
        assert loaded_from_pickle is not None
        assert loaded_from_pickle.positions['600000'].shares == 2000
        print("   [OK] Pass")
        
        # 测试5: 列出checkpoints
        print("\n5. List Checkpoints:")
        checkpoints = manager.list_checkpoints()
        print(f"   Found {len(checkpoints)} checkpoint(s)")
        assert len(checkpoints) >= 1
        print("   [OK] Pass")
        
        # 测试6: 版本兼容性检查
        print("\n6. Version Info:")
        print(f"   Current version: {state.version}")
        print(f"   Loaded version: {loaded_state.version}")
        print("   [OK] Pass")
        
        print("\n" + "=" * 60)
        print("All tests passed!")
        
    finally:
        # 清理临时目录
        shutil.rmtree(temp_dir)
