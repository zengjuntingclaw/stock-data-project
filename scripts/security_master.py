"""
历史证券主表 - 时点精确的股票池管理
=====================================
解决核心问题：回测时必须使用历史时点的真实可交易股票池，避免幸存者偏差。

功能：
1. 按日期重建历史证券主表（含上市/退市/暂停上市全生命周期）
2. 精确判断任意日期的股票可交易状态
3. 支持退市股、ST股、停牌股的完整历史追踪
4. 与DataEngine集成，提供统一的股票池查询接口
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
import json
import pandas as pd
import numpy as np
from loguru import logger

# 导入统一字段规范
from scripts.field_specs import FieldNames, standardize_df
from scripts.exchange_mapping import classify_exchange, build_ts_code


# ============================================================================
# 枚举定义
# ============================================================================

class ListingStatus(Enum):
    """上市状态"""
    NOT_LISTED = "not_listed"       # 未上市
    NORMAL = "normal"               # 正常交易
    SUSPENDED = "suspended"         # 停牌
    ST = "st"                       # ST特别处理
    STAR_ST = "star_st"             # *ST退市风险警示
    DELISTING = "delisting"         # 退市整理期
    DELISTED = "delisted"           # 已退市


class DelistReason(Enum):
    """退市原因"""
    MERGER = "merger"               # 吸收合并
    ACQUISITION = "acquisition"     # 被收购
    BANKRUPTCY = "bankruptcy"       # 破产清算
    VOLUNTARY = "voluntary"         # 主动退市
    MANDATORY_FINANCIAL = "mandatory_financial"  # 强制退市-财务指标
    MANDATORY_VIOLATION = "mandatory_violation"  # 强制退市-重大违法
    MANDATORY_OTHER = "mandatory_other"          # 强制退市-其他
    CODE_CHANGE = "code_change"     # 代码变更
    UNKNOWN = "unknown"


# ============================================================================
# 数据类定义
# ============================================================================

@dataclass
class StockLifetime:
    """
    股票全生命周期记录
    
    包含从上市到退市的完整信息，支持任意时点的状态查询。
    """
    symbol: str                     # 6位数字代码
    name: str                       # 股票名称
    ts_code: str                    # 带后缀的完整代码
    exchange: str                   # 交易所（SH/SZ/BJ）
    board: str                      # 板块
    
    # 关键日期
    list_date: datetime             # 上市日期（首次上市）
    delist_date: Optional[datetime] = None  # 退市日期
    
    # 状态历史（按时间排序的事件列表）
    status_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # ST历史
    st_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # 退市信息
    delist_reason: DelistReason = DelistReason.UNKNOWN
    successor_symbol: Optional[str] = None  # 吸收合并后的继承者
    predecessor_symbols: List[str] = field(default_factory=list)  # 被吸收的前任
    
    # 代码变更历史
    code_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    def get_status_on_date(self, date: Union[datetime, str]) -> ListingStatus:
        """
        获取指定日期的上市状态
        
        Parameters
        ----------
        date : datetime or str
            查询日期
        
        Returns
        -------
        ListingStatus : 该日期的状态
        """
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        # 未上市
        if date < self.list_date:
            return ListingStatus.NOT_LISTED
        
        # 已退市
        if self.delist_date and date >= self.delist_date:
            return ListingStatus.DELISTED
        
        # 检查状态历史（倒序查找最新的状态）
        current_status = ListingStatus.NORMAL
        for record in sorted(self.status_history, key=lambda x: x['date']):
            if date >= pd.to_datetime(record['date']):
                current_status = ListingStatus(record['status'])
        
        return current_status
    
    def is_tradable(self, date: Union[datetime, str]) -> Tuple[bool, str]:
        """
        判断指定日期是否可交易
        
        Returns
        -------
        Tuple[bool, str] : (是否可交易, 原因说明)
        """
        status = self.get_status_on_date(date)
        
        tradable_map = {
            ListingStatus.NORMAL: (True, "Normal trading"),
            ListingStatus.ST: (True, "ST stock tradable"),
            ListingStatus.STAR_ST: (True, "*ST stock tradable"),
            ListingStatus.NOT_LISTED: (False, "Not listed yet"),
            ListingStatus.SUSPENDED: (False, "Suspended"),
            ListingStatus.DELISTING: (True, "Delisting period tradable"),
            ListingStatus.DELISTED: (False, "Delisted"),
        }
        
        return tradable_map.get(status, (False, f"Unknown status: {status.value}"))
    
    def is_st_on_date(self, date: Union[datetime, str]) -> Tuple[bool, Optional[str]]:
        """判断指定日期是否为ST股"""
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        for record in self.st_history:
            start = pd.to_datetime(record['start_date'])
            end = record.get('end_date')
            end = pd.to_datetime(end) if end else None
            
            if start <= date and (end is None or date <= end):
                return True, record.get('type', 'ST')
        
        return False, None
    
    def to_dict(self) -> Dict:
        """序列化为字典"""
        return {
            'symbol': self.symbol,
            'name': self.name,
            'ts_code': self.ts_code,
            'exchange': self.exchange,
            'board': self.board,
            'list_date': self.list_date.isoformat() if self.list_date else None,
            'delist_date': self.delist_date.isoformat() if self.delist_date else None,
            'status_history': self.status_history,
            'st_history': self.st_history,
            'delist_reason': self.delist_reason.value,
            'successor_symbol': self.successor_symbol,
            'predecessor_symbols': self.predecessor_symbols,
            'code_changes': self.code_changes,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StockLifetime':
        """从字典反序列化"""
        return cls(
            symbol=data['symbol'],
            name=data['name'],
            ts_code=data['ts_code'],
            exchange=data['exchange'],
            board=data['board'],
            list_date=pd.to_datetime(data['list_date']),
            delist_date=pd.to_datetime(data['delist_date']) if data.get('delist_date') else None,
            status_history=data.get('status_history', []),
            st_history=data.get('st_history', []),
            delist_reason=DelistReason(data.get('delist_reason', 'unknown')),
            successor_symbol=data.get('successor_symbol'),
            predecessor_symbols=data.get('predecessor_symbols', []),
            code_changes=data.get('code_changes', []),
        )


# ============================================================================
# 历史证券主表管理器
# ============================================================================

class HistoricalSecurityMaster:
    """
    历史证券主表管理器
    
    管理所有股票的全生命周期数据，支持任意历史时点的股票池重建。
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Parameters
        ----------
        data_dir : str, optional
            数据持久化目录
        """
        # 股票全生命周期数据: {symbol: StockLifetime}
        self._stocks: Dict[str, StockLifetime] = {}
        
        # 按交易所索引
        self._by_exchange: Dict[str, Set[str]] = {'SH': set(), 'SZ': set(), 'BJ': set()}
        
        # 退市股票集合
        self._delisted: Set[str] = set()
        
        # 数据目录
        project_root = Path(__file__).resolve().parent.parent
        self.data_dir = Path(data_dir or project_root / 'data' / 'security_master')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载持久化数据
        self._load()
        
        logger.info(f"HistoricalSecurityMaster initialized: {len(self._stocks)} stocks")
    
    def _load(self):
        """加载持久化数据"""
        data_file = self.data_dir / 'security_master.json'
        if data_file.exists():
            try:
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                for item in data:
                    lifetime = StockLifetime.from_dict(item)
                    self._stocks[lifetime.symbol] = lifetime
                    self._by_exchange[lifetime.exchange].add(lifetime.symbol)
                    if lifetime.delist_date:
                        self._delisted.add(lifetime.symbol)
                
                logger.info(f"Loaded {len(self._stocks)} stocks from persistence")
            except Exception as e:
                logger.warning(f"Failed to load security master: {e}")
    
    def _save(self):
        """持久化数据"""
        try:
            data = [s.to_dict() for s in self._stocks.values()]
            data_file = self.data_dir / 'security_master.json'
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Saved {len(data)} stocks to security master")
        except Exception as e:
            logger.error(f"Failed to save security master: {e}")
    
    def add_stock(self, lifetime: StockLifetime) -> None:
        """
        添加股票生命周期记录
        
        Parameters
        ----------
        lifetime : StockLifetime
            股票生命周期记录
        """
        self._stocks[lifetime.symbol] = lifetime
        self._by_exchange[lifetime.exchange].add(lifetime.symbol)
        if lifetime.delist_date:
            self._delisted.add(lifetime.symbol)
    
    def add_stock_from_df(self, df: pd.DataFrame) -> int:
        """
        从DataFrame批量添加股票
        
        期望的列：
        - symbol: 股票代码
        - name: 股票名称
        - list_date: 上市日期
        - delist_date: 退市日期（可选）
        - exchange: 交易所（可选，可从代码推断）
        - board: 板块（可选，可从代码推断）
        """
        count = 0
        for _, row in df.iterrows():
            try:
                symbol = str(row['symbol']).zfill(6)
                exchange, board = classify_exchange(symbol)
                
                lifetime = StockLifetime(
                    symbol=symbol,
                    name=row.get('name', ''),
                    ts_code=build_ts_code(symbol),
                    exchange=exchange,
                    board=board,
                    list_date=pd.to_datetime(row['list_date']),
                    delist_date=pd.to_datetime(row['delist_date']) if pd.notna(row.get('delist_date')) else None,
                )
                
                self.add_stock(lifetime)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to add stock {row.get('symbol')}: {e}")
        
        if count > 0:
            self._save()
        
        return count
    
    def get_stock(self, symbol: str) -> Optional[StockLifetime]:
        """获取单个股票的生命周期记录"""
        return self._stocks.get(str(symbol).zfill(6))
    

    @classmethod
    def from_duckdb(cls, db_path: str, rebuild: bool = False) -> "HistoricalSecurityMaster":
        """
        从 DuckDB 初始化历史证券主表（内存缓存）

        用 DuckDB SQL 聚合 ST 事件，写入 security_master.json，
        再由 _load() 读入内存。比 iterrows 快 10 倍。
        策略：若无 rebuild=True 且 JSON 存在则直接加载；否则重新生成。

        注意：此方法是可选的。DuckDB 已原生支持 PIT 查询：
          - get_pit_stock_pool(trade_date)        → 任意历史股票池
          - get_stocks_with_st_status(trade_date) → 任意历史 ST 股
        """
        import json as _json
        master = cls()
        json_path = master.data_dir / "security_master.json"
        if json_path.exists() and not rebuild:
            master._load()
            if master._stocks:
                logger.info("from_duckdb: loaded %d stocks from %s", len(master._stocks), json_path)
                return master

        try:
            import duckdb as _duckdb
        except ImportError:
            logger.warning("from_duckdb: duckdb not installed, returning empty master")
            return master

        # DuckDB SQL：聚合每只股票的 ST 事件 JSON 数组
        conn = _duckdb.connect(str(db_path))
        query = """
        SELECT
            sb.ts_code, sb.symbol, sb.name, sb.exchange, sb.board,
            sb.list_date::VARCHAR AS list_date,
            sb.delist_date::VARCHAR AS delist_date,
            sb.is_delisted, sb.delist_reason,
            COALESCE(st_info.st_events, '[]') AS st_events
        FROM stock_basic_history sb
        LEFT JOIN (
            SELECT ts_code,
                   json_group_array(json_object(
                       'start_date', trade_date,
                       'is_st', is_st,
                       'is_new_st', COALESCE(is_new_st, FALSE),
                       'st_type', COALESCE(st_type, 'ST')
                   )) AS st_events
            FROM st_status_history
            GROUP BY ts_code
        ) st_info ON sb.ts_code = st_info.ts_code
        """
        rows = conn.execute(query).fetchdf()
        conn.close()
        del conn

        # 逐行构建 StockLifetime，写入 JSON 列表
        records = []
        for _, row in rows.iterrows():
            ts_code = str(row['ts_code'])
            symbol  = ts_code.split('.')[0] if '.' in ts_code else ts_code
            dr = DelistReason.UNKNOWN
            dr_val = str(row.get('delist_reason') or '').strip()
            if dr_val and dr_val.lower() not in ('', 'unknown', 'none'):
                try:
                    dr = DelistReason(dr_val)
                except ValueError:
                    pass

            # 解析 ST 事件
            st_events = _json.loads(row['st_events']) if row['st_events'] != '[]' else []
            st_history, status_history = [], []
            prev_st = False
            for rec in st_events:
                is_st = bool(rec.get('is_st'))
                if is_st:
                    st_history.append({
                        'start_date': str(rec.get('start_date', ''))[:10],
                        'end_date': None,
                        'type': rec.get('st_type', 'ST'),
                        'new_st': bool(rec.get('is_new_st')),
                    })
                if is_st and not prev_st:
                    status_history.append({'date': str(rec.get('start_date', ''))[:10], 'status': 'st'})
                elif not is_st and prev_st:
                    status_history.append({'date': str(rec.get('start_date', ''))[:10], 'status': 'normal'})
                prev_st = is_st

            # 线性补 end_date：预建 start_date->index 映射 + 一次遍历
            if st_history and st_events:
                from datetime import datetime, timedelta
                date_to_idx = {str(ev.get('start_date', ''))[:10]: j
                               for j, ev in enumerate(st_events)}
                next_remove_idx = None  # 下一个 is_st=False 的索引
                for j in range(len(st_events) - 1, -1, -1):
                    ev = st_events[j]
                    if not ev.get('is_st'):
                        next_remove_idx = j
                    elif next_remove_idx is not None:
                        sd = str(ev.get('start_date', ''))[:10]
                        nd = str(st_events[next_remove_idx].get('start_date', ''))[:10]
                        sh = next((h for h in st_history if h['start_date'] == sd), None)
                        if sh:
                            sh['end_date'] = (datetime.strptime(nd, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

            records.append({
                'symbol': symbol,
                'name': str(row.get('name', '')),
                'ts_code': ts_code,
                'exchange': str(row.get('exchange', 'SZ')),
                'board': str(row.get('board', 'Main')),
                'list_date': str(row['list_date'])[:10] if row.get('list_date') else None,
                'delist_date': str(row['delist_date'])[:10] if row.get('delist_date') else None,
                'status_history': status_history,
                'st_history': st_history,
                'delist_reason': dr.value,
                'successor_symbol': None,
                'predecessor_symbols': [],
                'code_changes': [],
            })

        # 写 JSON
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            _json.dump(records, f, indent=2, ensure_ascii=False)

        master._load()
        logger.info("from_duckdb: %d stocks -> %s", len(master._stocks), json_path)
        return master



    def get_universe_on_date(
        self, 
        date: Union[datetime, str],
        include_delisted: bool = False,
        include_st: bool = True,
        exchanges: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        获取指定日期的股票池
        
        Parameters
        ----------
        date : datetime or str
            目标日期
        include_delisted : bool
            是否包含已退市股票（退市整理期仍视为可交易）
        include_st : bool
            是否包含ST股票
        exchanges : List[str], optional
            筛选特定交易所（如 ['SH', 'SZ']）
        
        Returns
        -------
        pd.DataFrame : 股票池，包含以下列：
            - symbol, name, ts_code, exchange, board
            - list_date, delist_date
            - status: 该日期的状态
            - is_tradable: 是否可交易
            - not_tradable_reason: 不可交易原因
            - is_st: 是否ST
            - st_type: ST类型
        """
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        records = []
        
        for symbol, lifetime in self._stocks.items():
            # 交易所筛选
            if exchanges and lifetime.exchange not in exchanges:
                continue
            
            # 获取该日期的状态
            status = lifetime.get_status_on_date(date)
            is_tradable, reason = lifetime.is_tradable(date)
            is_st, st_type = lifetime.is_st_on_date(date)
            
            # 退市股票筛选
            if status == ListingStatus.DELISTED and not include_delisted:
                continue
            
            # ST股票筛选
            if is_st and not include_st:
                continue
            
            records.append({
                FieldNames.SYMBOL: lifetime.symbol,
                FieldNames.NAME: lifetime.name,
                FieldNames.TS_CODE: lifetime.ts_code,
                FieldNames.EXCHANGE: lifetime.exchange,
                FieldNames.BOARD: lifetime.board,
                FieldNames.LIST_DATE: lifetime.list_date,
                FieldNames.DELIST_DATE: lifetime.delist_date,
                'status': status.value,
                FieldNames.IS_TRADABLE: is_tradable,
                'not_tradable_reason': reason if not is_tradable else None,
                FieldNames.IS_ST: is_st,
                'st_type': st_type,
            })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values([FieldNames.EXCHANGE, FieldNames.SYMBOL])
        
        return df
    
    def get_tradable_stocks(
        self, 
        date: Union[datetime, str],
        exchanges: Optional[List[str]] = None,
    ) -> List[str]:
        """
        获取指定日期可交易的股票代码列表
        
        Returns
        -------
        List[str] : 可交易的股票代码列表（6位数字）
        """
        df = self.get_universe_on_date(date, include_delisted=False, include_st=True, exchanges=exchanges)
        if df.empty:
            return []
        return df[df[FieldNames.IS_TRADABLE]][FieldNames.SYMBOL].tolist()
    
    def check_stock_status(
        self, 
        symbol: str, 
        date: Union[datetime, str]
    ) -> Dict[str, Any]:
        """
        检查单个股票在指定日期的详细状态
        
        Returns
        -------
        Dict : 包含完整状态信息
        """
        lifetime = self.get_stock(symbol)
        if not lifetime:
            return {'exists': False, 'error': 'Stock not found'}
        
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        status = lifetime.get_status_on_date(date)
        is_tradable, reason = lifetime.is_tradable(date)
        is_st, st_type = lifetime.is_st_on_date(date)
        
        return {
            'exists': True,
            'symbol': lifetime.symbol,
            'name': lifetime.name,
            'date': date.strftime('%Y-%m-%d'),
            'status': status.value,
            'is_tradable': is_tradable,
            'not_tradable_reason': reason if not is_tradable else None,
            'is_st': is_st,
            'st_type': st_type,
            'list_date': lifetime.list_date.strftime('%Y-%m-%d'),
            'delist_date': lifetime.delist_date.strftime('%Y-%m-%d') if lifetime.delist_date else None,
            'exchange': lifetime.exchange,
            'board': lifetime.board,
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'total_stocks': len(self._stocks),
            'delisted_stocks': len(self._delisted),
            'by_exchange': {k: len(v) for k, v in self._by_exchange.items()},
        }


# ============================================================================
# 与DataEngine的集成接口
# ============================================================================

def create_security_master_from_engine(engine, start_year: int = 2010, end_year: int = 2024) -> HistoricalSecurityMaster:
    """
    从DataEngine创建历史证券主表
    
    Parameters
    ----------
    engine : DataEngine
        数据引擎实例
    start_year : int
        起始年份
    end_year : int
        结束年份
    
    Returns
    -------
    HistoricalSecurityMaster : 证券主表管理器
    """
    master = HistoricalSecurityMaster()
    
    # 从 DataEngine 获取当前交易日可交易股票池（PIT 查询，不含未来函数）
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        ts_codes = engine.get_active_stocks(today)
        if ts_codes:
            # 构造 DataFrame 用于 add_stock_from_df
            # get_active_stocks 返回 list[str]，需转 DataFrame 并补充字段
            df = pd.DataFrame({"symbol": ts_codes})
            # 补全 ts_code（用于后续处理）
            df["ts_code"] = df["symbol"]
            # 去掉后缀得到纯数字代码
            df["symbol"] = df["symbol"].str.split(".").str[0]
            count = master.add_stock_from_df(df)
            logger.info(f"Created security master with {count} stocks from DataEngine.get_active_stocks('{today}')")
    except Exception as e:
        logger.error(f"Failed to create security master from engine: {e}")
    
    return master


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    print("Historical Security Master Test:")
    print("=" * 60)
    
    # 创建测试数据
    master = HistoricalSecurityMaster(data_dir=None)  # 使用临时目录
    
    # 添加测试股票
    test_stocks = [
        StockLifetime(
            symbol='000001',
            name='Ping An Bank',
            ts_code='000001.SZ',
            exchange='SZ',
            board='Main',
            list_date=datetime(1991, 4, 3),
            delist_date=None,
        ),
        StockLifetime(
            symbol='600000',
            name='SPD Bank',
            ts_code='600000.SH',
            exchange='SH',
            board='Main',
            list_date=datetime(1999, 11, 10),
            delist_date=None,
        ),
        StockLifetime(
            symbol='688001',
            name='HuaXing',
            ts_code='688001.SH',
            exchange='SH',
            board='STAR',
            list_date=datetime(2019, 7, 22),
            delist_date=None,
        ),
        StockLifetime(
            symbol='920001',
            name='BJSE New',
            ts_code='920001.BJ',
            exchange='BJ',
            board='BJSE',
            list_date=datetime(2024, 1, 1),
            delist_date=None,
        ),
        StockLifetime(
            symbol='000002',
            name='Delisted Stock',
            ts_code='000002.SZ',
            exchange='SZ',
            board='Main',
            list_date=datetime(2000, 1, 1),
            delist_date=datetime(2020, 6, 1),
            delist_reason=DelistReason.MANDATORY_FINANCIAL,
        ),
    ]
    
    for stock in test_stocks:
        master.add_stock(stock)
    
    # 测试1: 获取股票池
    print("\n1. 2024 Universe Test:")
    universe = master.get_universe_on_date('2024-06-01', include_delisted=False)
    print(f"   Tradable stocks: {universe[universe['is_tradable']].shape[0]}")
    print(f"   Total stocks: {len(universe)}")
    assert len(universe) == 4  # Excluding delisted 000002
    print("   [OK] Pass")
    
    # 测试2: 包含退市股
    print("\n2. Include Delisted Test:")
    universe_all = master.get_universe_on_date('2024-06-01', include_delisted=True)
    print(f"   Total stocks (with delisted): {len(universe_all)}")
    assert len(universe_all) == 5
    print("   [OK] Pass")
    
    # 测试3: 历史时点查询
    print("\n3. Historical Universe Test:")
    universe_2010 = master.get_universe_on_date('2010-01-01')
    tradable_2010 = universe_2010[universe_2010['is_tradable']]
    print(f"   Tradable stocks in 2010: {len(tradable_2010)}")
    # 2010: 000001 and 600000 listed, 688001 and 920001 not listed
    assert len(tradable_2010) == 3  # 000001, 600000, 000002
    print("   [OK] Pass")
    
    # 测试4: 退市前可交易
    print("\n4. Before Delisting Test:")
    status = master.check_stock_status('000002', '2019-01-01')
    print(f"   000002 in 2019: {status['is_tradable']} ({status['status']})")
    assert status['is_tradable'] == True
    print("   [OK] Pass")
    
    # 测试5: 退市后不可交易
    print("\n5. After Delisting Test:")
    status = master.check_stock_status('000002', '2021-01-01')
    print(f"   000002 in 2021: {status['is_tradable']} ({status['not_tradable_reason']})")
    assert status['is_tradable'] == False
    print("   [OK] Pass")
    
    # 测试6: 北交所代码
    print("\n6. Beijing Exchange 920xxx Test:")
    stock_920 = master.get_stock('920001')
    print(f"   920001 exchange: {stock_920.exchange}")
    assert stock_920.exchange == 'BJ'
    print("   [OK] Pass")
    
    print("\n" + "=" * 60)
    print("All tests passed!")
    print(f"\nStatistics: {master.get_statistics()}")
