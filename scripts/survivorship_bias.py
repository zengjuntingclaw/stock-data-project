"""幸存者偏差治理模块 V2 - 增强版

核心问题：回测时只使用当前存活的股票，会高估策略表现（幸存者偏差）
解决方案：获取历史全量股票列表（含退市股），确保回测股票池的历史完整性

V2增强：
1. 自动处理退市与换码（吸收合并、代码变更）
2. 退市股票最后交易状态固化
3. 精确停牌标记（is_tradable布尔值）
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import bisect
import json
import pandas as pd
import numpy as np
import threading
from loguru import logger

# 复用 data_engine.py 中的统一交易所映射函数（避免代码重复）
from scripts.data_engine import build_ts_code

# Baostock 全局会话锁（Baostock 不支持并发，全局单会话）
_BS_LOCK = threading.Lock()


class DelistReason(Enum):
    """退市原因分类"""
    MERGER = "merger"               # 吸收合并
    ACQUISITION = "acquisition"     # 被收购
    BANKRUPTCY = "bankruptcy"       # 破产清算
    VOLUNTARY = "voluntary"         # 主动退市
    MANDATORY = "mandatory"         # 强制退市（财务/违法）
    CODE_CHANGE = "code_change"     # 代码变更（如股改）
    UNKNOWN = "unknown"


class SuspensionType(Enum):
    """停牌类型"""
    NORMAL = "normal"               # 正常交易
    SUSPEND = "suspend"             # 停牌
    LIMIT_UP = "limit_up"           # 涨停无法买入
    LIMIT_DOWN = "limit_down"       # 跌停无法卖出
    DELISTED = "delisted"           # 已退市
    NOT_YET_LISTED = "not_yet"      # 未上市


@dataclass
class StockLifetime:
    """股票生命周期 V2"""
    symbol: str
    name: str
    ts_code: str                    # 带后缀的完整代码
    list_date: datetime             # 上市日期
    delist_date: Optional[datetime] = None
    delist_reason: DelistReason = DelistReason.UNKNOWN
    successor_symbol: Optional[str] = None  # 吸收合并后的继承者代码
    predecessor_symbols: List[str] = field(default_factory=list)  # 被吸收的前任代码
    
    # 板块信息
    board: str = '主板'
    original_board: Optional[str] = None  # 原始板块（如从主板转科创板）
    
    # ST状态历史
    st_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # 代码变更历史
    code_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    # 最后交易状态（退市时固化）
    final_state: Optional[Dict[str, Any]] = None
    
    def is_active(self, date: datetime) -> bool:
        """检查指定日期是否处于上市状态"""
        if date < self.list_date:
            return False
        if self.delist_date and date >= self.delist_date:
            return False
        return True
    
    def get_st_status(self, date: datetime) -> Tuple[bool, Optional[str]]:
        """获取指定日期的ST状态"""
        for record in self.st_history:
            start = pd.to_datetime(record['start_date'])
            end = record.get('end_date')
            end = pd.to_datetime(end) if end else None
            
            if start <= date and (end is None or date <= end):
                return True, record.get('type', 'ST')
        
        return False, None


class SurvivorshipBiasHandler:
    """
    幸存者偏差处理器 V2
    
    增强功能：
    1. 自动处理退市与换码（吸收合并、代码变更）
    2. 退市股票最后交易状态固化
    3. 精确停牌处理（is_tradable布尔值）
    4. 代码变更映射追踪
    5. PIT历史查询支持（优先使用 stock_basic_history）
    """
    
    def __init__(self, data_dir: Optional[str] = None, data_engine=None):
        self._stocks: Dict[str, StockLifetime] = {}
        self._delisted: Set[str] = set()
        self._st_history: Dict[str, List[Tuple[datetime, Optional[datetime]]]] = {}
        
        # 代码变更映射：旧代码 -> 新代码
        self._code_change_map: Dict[str, str] = {}
        
        # 吸收合并映射：被吸收代码 -> 继承代码
        self._merger_map: Dict[str, str] = {}
        
        # DataEngine 引用（用于 PIT 查询）
        self._data_engine = data_engine
        
        # 数据目录（用于持久化退市信息）
        project_root = Path(__file__).resolve().parent.parent
        self.data_dir = Path(data_dir or project_root / 'data' / 'survivorship')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载已保存的退市信息
        self._load_persisted_data()
        
        logger.info("SurvivorshipBiasHandler V2 initialized")
    
    def _load_persisted_data(self):
        """加载持久化的退市数据"""
        delisted_file = self.data_dir / 'delisted_stocks.json'
        if delisted_file.exists():
            try:
                with open(delisted_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                for item in data:
                    lifetime = StockLifetime(
                        symbol=item['symbol'],
                        name=item['name'],
                        # ⚠️ 修复：统一使用 build_ts_code()，禁止硬编码 .SH/.SZ 逻辑
                        # 否则 4xxxxx/8xxxxx/920xxx 等北交所代码会被误判为 .SZ
                        ts_code=item.get('ts_code', build_ts_code(item['symbol'])),
                        list_date=pd.to_datetime(item['list_date']),
                        delist_date=pd.to_datetime(item['delist_date']) if item.get('delist_date') else None,
                        delist_reason=DelistReason(item.get('delist_reason', 'unknown')),
                        successor_symbol=item.get('successor_symbol'),
                        predecessor_symbols=item.get('predecessor_symbols', []),
                        board=item.get('board', '主板'),
                        final_state=item.get('final_state')
                    )
                    self._stocks[item['symbol']] = lifetime
                    if lifetime.delist_date:
                        self._delisted.add(item['symbol'])
                
                logger.info(f"Loaded {len(self._delisted)} delisted stocks from persistence")
            except Exception as e:
                logger.warning(f"Failed to load persisted data: {e}")
    
    def _save_persisted_data(self):
        """持久化退市数据"""
        data = []
        for symbol in self._delisted:
            lifetime = self._stocks.get(symbol)
            if lifetime:
                data.append({
                    'symbol': lifetime.symbol,
                    'name': lifetime.name,
                    'ts_code': lifetime.ts_code,
                    'list_date': lifetime.list_date.isoformat(),
                    'delist_date': lifetime.delist_date.isoformat() if lifetime.delist_date else None,
                    'delist_reason': lifetime.delist_reason.value,
                    'successor_symbol': lifetime.successor_symbol,
                    'predecessor_symbols': lifetime.predecessor_symbols,
                    'board': lifetime.board,
                    'final_state': lifetime.final_state
                })
        
        delisted_file = self.data_dir / 'delisted_stocks.json'
        with open(delisted_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Persisted {len(data)} delisted stocks")
    
    def load_from_akshare(self):
        """从AkShare加载历史全量股票"""
        try:
            import akshare as ak
            
            # 获取当前所有股票
            current = ak.stock_zh_a_spot_em()
            
            # 获取历史退市股票（需要特殊接口或本地数据）
            # 这里简化处理，实际应维护一个退市股票数据库
            logger.warning("退市股票数据需要额外数据源，当前仅加载现存股票")
            
            for _, row in current.iterrows():
                symbol = row['代码']
                self._stocks[symbol] = StockLifetime(
                    symbol=symbol,
                    name=row['名称'],
                    list_date=pd.to_datetime(row.get('上市日期', '2000-01-01')),
                    board=self._detect_board(symbol),
                )
            
            logger.info(f"Loaded {len(self._stocks)} stocks from AkShare")
            
        except Exception as e:
            logger.error(f"Failed to load from AkShare: {e}")
    
    def load_from_baostock(self, start_year: int = 2000, end_year: int = 2024):
        """从Baostock加载历史全量股票（含退市）- V2增强版（线程安全）"""
        try:
            import baostock as bs
            
            # Baostock 全局会话锁（线程安全）
            with _BS_LOCK:
                lg = bs.login()
                if lg.error_code != '0':
                    logger.error(f"Baostock login failed: {lg.error_msg}")
                    return
                
                try:
                    newly_delisted = []
                    
                    for year in range(start_year, end_year + 1):
                        # 查询每年最后一个交易日的所有股票
                        rs = bs.query_all_stock(day=f"{year}-12-31")
                        if rs.error_code != '0':
                            continue
                        
                        while rs.next() and rs.error_code == '0':
                            row = rs.get_row_data()
                            bs_code = row[0]  # sh.600000 / sz.000001 / bj.430001
                            symbol = bs_code.split('.')[1]  # 600000 / 000001 / 430001
                            # 统一使用 build_ts_code：自动识别沪深北三交易所
                            ts_code = build_ts_code(symbol)
                            
                            if symbol not in self._stocks:
                                # 查询股票基本信息
                                rs_detail = bs.query_stock_basic(code=bs_code)
                                if rs_detail.error_code == '0' and rs_detail.next():
                                    detail = rs_detail.get_row_data()
                                    list_date = pd.to_datetime(detail[6]) if detail[6] else pd.Timestamp(f"{year}-01-01")
                                    delist_date = pd.to_datetime(detail[7]) if detail[7] else None
                                    
                                    # 检测退市原因
                                    delist_reason = self._detect_delist_reason(detail)
                                    
                                    lifetime = StockLifetime(
                                        symbol=symbol,
                                        name=detail[1],
                                        ts_code=ts_code,
                                        list_date=list_date,
                                        delist_date=delist_date,
                                        delist_reason=delist_reason,
                                        board=self._detect_board(symbol),
                                    )
                                    
                                    self._stocks[symbol] = lifetime
                                    
                                    if delist_date:
                                        self._delisted.add(symbol)
                                        newly_delisted.append(symbol)
                                        
                                        # 固化最后交易状态
                                        self._finalize_delisted_stock(symbol, delist_date)
                    
                    # 检测代码变更（通过名称变化或特殊标记）
                    self._detect_code_changes()
                    
                    # 保存更新
                    if newly_delisted:
                        self._save_persisted_data()
                    
                    logger.info(f"Loaded {len(self._stocks)} stocks from Baostock ({len(self._delisted)} delisted)")
                    
                finally:
                    bs.logout()
            
        except Exception as e:
            logger.error(f"Failed to load from Baostock: {e}")
    
    def _detect_delist_reason(self, detail_row: List[str]) -> DelistReason:
        """检测退市原因"""
        # Baostock返回的字段：code, name, ipoDate, outDate, type, status, ...
        # 根据name或status判断
        name = detail_row[1] if len(detail_row) > 1 else ""
        status = detail_row[4] if len(detail_row) > 4 else ""
        
        if '吸收合并' in name or '合并' in name:
            return DelistReason.MERGER
        elif '收购' in name:
            return DelistReason.ACQUISITION
        elif '破产' in name or '清算' in name:
            return DelistReason.BANKRUPTCY
        elif '主动' in name or '自愿' in name:
            return DelistReason.VOLUNTARY
        elif '终止' in name or '强制' in name:
            return DelistReason.MANDATORY
        elif 'G' in name[:2] or '股改' in name:  # 股改导致的代码变更
            return DelistReason.CODE_CHANGE
        
        return DelistReason.UNKNOWN
    
    def _finalize_delisted_stock(self, symbol: str, delist_date: datetime):
        """固化退市股票的最后交易状态"""
        lifetime = self._stocks.get(symbol)
        if not lifetime:
            return
        
        # 尝试从数据库获取最后交易状态
        try:
            from scripts.data_engine import DataEngine
            engine = DataEngine()
            
            # 获取退市前最后5个交易日的数据
            end_date = delist_date.strftime('%Y-%m-%d')
            start_date = (delist_date - timedelta(days=30)).strftime('%Y-%m-%d')
            
            df = engine.query("""
                SELECT * FROM daily_bar_adjusted
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date DESC
                LIMIT 5
            """, (lifetime.ts_code, start_date, end_date))
            
            if not df.empty:
                last_row = df.iloc[0]
                lifetime.final_state = {
                    'last_trade_date': last_row['trade_date'].isoformat() if hasattr(last_row['trade_date'], 'isoformat') else str(last_row['trade_date']),
                    'last_close': float(last_row['close']),
                    'last_volume': int(last_row['volume']),
                    'total_traded_days': len(df),
                    'delist_price': float(last_row['close']),
                }
                logger.debug(f"Finalized {symbol}: last_price={lifetime.final_state['last_close']}")
                
        except Exception as e:
            logger.debug(f"Failed to finalize {symbol}: {e}")
    
    def _detect_code_changes(self):
        """检测代码变更（如股改、合并导致的代码变化）- O(n log n) 优化版"""
        # 筛选 CODE_CHANGE 类型的历史股票
        old_stocks = {
            s: l for s, l in self._stocks.items()
            if l.delist_reason == DelistReason.CODE_CHANGE and l.delist_date
        }
        if not old_stocks:
            return

        # 构建时间窗口索引：新股票按上市日期排序，便于范围查询
        # 只考虑退市后 60 天内重新上市的（放宽窗口）
        new_stocks = [
            (s, l)
            for s, l in self._stocks.items()
            if s not in old_stocks and l.list_date
        ]
        # 按上市日期排序，支持二分查找
        new_stocks.sort(key=lambda x: x[1].list_date)

        # 用字典做符号表：快速查找名称
        # 用列表维持有序（用于二分边界）
        name_map: Dict[str, List[str]] = {}  # 标准化名称 -> [symbol, ...]
        for s, l in new_stocks:
            key = self._normalize_name(l.name)
            name_map.setdefault(key, []).append(s)

        for old_sym, old_lifetime in old_stocks.items():
            if old_sym in self._code_change_map:
                continue  # 已有映射

            delist_date = old_lifetime.delist_date
            # 二分查找：退市日期之后 60 天内的新股票
            target_date = delist_date + timedelta(days=60)
            idx = bisect.bisect_right(new_stocks, ('', None), key=lambda x: x[1].list_date)
            # 找 [delist_date, target_date] 区间的股票
            candidates = [
                (s, l) for s, l in new_stocks[idx:]
                if old_lifetime.list_date <= l.list_date <= target_date
                if l.list_date >= delist_date
            ]

            # 名称相似度快速过滤
            old_key = self._normalize_name(old_lifetime.name)
            for new_sym, new_lifetime in candidates:
                new_key = self._normalize_name(new_lifetime.name)
                # 完全匹配或高相似度
                if old_key == new_key or self._name_similarity(old_lifetime.name, new_lifetime.name) > 0.5:
                    self._code_change_map[old_sym] = new_sym
                    old_lifetime.successor_symbol = new_sym
                    new_lifetime.predecessor_symbols.append(old_sym)
                    logger.info(f"Detected code change: {old_sym} -> {new_sym}")
                    break
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """计算名称相似度（简单实现）"""
        # 移除常见后缀
        suffixes = ['股份', '集团', '有限', '公司', 'ST', '*ST', '退市']
        n1 = name1
        n2 = name2
        for s in suffixes:
            n1 = n1.replace(s, '')
            n2 = n2.replace(s, '')
        
        # 计算Jaccard相似度
        set1 = set(n1)
        set2 = set(n2)
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    
    def _detect_board(self, symbol: str) -> str:
        """识别板块（委托到公共函数）"""
        from scripts.data_engine import detect_board
        return detect_board(symbol)
    
    def get_universe(
        self,
        date: datetime,
        include_delisted: bool = False,
        min_listing_days: int = 60,
    ) -> List[str]:
        """
        获取指定日期的有效股票池
        
        优先级：
        1. 如果有 DataEngine，使用 stock_basic_history PIT 查询（推荐）
        2. 否则回退到内存字典查询
        
        Parameters
        ----------
        date : datetime
            查询日期
        include_delisted : bool
            是否包含已退市股票（用于历史回测）
        min_listing_days : int
            最小上市天数过滤
        
        Returns
        -------
        List[str]
            股票代码列表
        """
        # 优先使用 PIT 查询（推荐路径）
        if self._data_engine is not None:
            date_str = date.strftime("%Y-%m-%d") if isinstance(date, datetime) else str(date)
            try:
                pit_stocks = self._data_engine.get_active_stocks(date_str)
                if pit_stocks:
                    logger.debug(f"PIT query for {date_str}: {len(pit_stocks)} stocks")
                    return pit_stocks
            except Exception as e:
                logger.warning(f"PIT query failed, falling back to memory: {e}")
        
        # 回退到内存字典查询
        result = []
        
        for symbol, lifetime in self._stocks.items():
            # 检查是否已上市
            if lifetime.list_date > date:
                continue
            
            # 检查是否已退市
            if lifetime.delist_date and lifetime.delist_date <= date:
                if not include_delisted:
                    continue
            
            # 检查上市天数
            days_listed = (date - lifetime.list_date).days
            if days_listed < min_listing_days:
                continue
            
            result.append(symbol)
        
        return result
    
    def is_tradable(self, symbol: str, date: datetime) -> Tuple[bool, str]:
        """
        检查股票在指定日期是否可交易
        
        Returns
        -------
        (is_tradable, reason)
        """
        if symbol not in self._stocks:
            return False, "股票不存在"
        
        lifetime = self._stocks[symbol]
        
        # 未上市
        if date < lifetime.list_date:
            return False, f"未上市（上市日期：{lifetime.list_date.date()}）"
        
        # 已退市
        if lifetime.delist_date and date >= lifetime.delist_date:
            return False, f"已退市（退市日期：{lifetime.delist_date.date()}）"
        
        # ST状态
        is_st, st_type = lifetime.get_st_status(date)
        if is_st:
            return False, f"{st_type}股票"
        
        return True, ""
    
    def get_tradable_status(self, 
                           symbol: str, 
                           date: datetime,
                           market_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        获取详细的可交易状态（用于回测数据导出）
        
        Returns
        -------
        {
            'is_tradable': bool,           # 是否可交易
            'can_buy': bool,               # 是否可买入
            'can_sell': bool,              # 是否可卖出
            'suspension_type': str,        # 停牌类型
            'reason': str,                 # 原因说明
            'limit_price': Optional[float] # 涨跌停价格（如适用）
        }
        """
        result = {
            'is_tradable': False,
            'can_buy': False,
            'can_sell': False,
            'suspension_type': SuspensionType.NORMAL.value,
            'reason': '',
            'limit_price': None
        }
        
        # 基本状态检查
        is_tradable, reason = self.is_tradable(symbol, date)
        
        if not is_tradable:
            result['reason'] = reason
            if '未上市' in reason:
                result['suspension_type'] = SuspensionType.NOT_YET_LISTED.value
            elif '退市' in reason:
                result['suspension_type'] = SuspensionType.DELISTED.value
            return result
        
        # 检查市场数据（停牌、涨跌停）
        if market_data:
            # 停牌检查
            if market_data.get('is_suspend', False):
                result['reason'] = '停牌'
                result['suspension_type'] = SuspensionType.SUSPEND.value
                return result
            
            # 涨跌停检查
            if market_data.get('limit_up', False):
                result['can_sell'] = True
                result['can_buy'] = False
                result['suspension_type'] = SuspensionType.LIMIT_UP.value
                result['reason'] = '涨停，无法买入'
                result['limit_price'] = market_data.get('high')
                return result
            
            if market_data.get('limit_down', False):
                result['can_buy'] = True
                result['can_sell'] = False
                result['suspension_type'] = SuspensionType.LIMIT_DOWN.value
                result['reason'] = '跌停，无法卖出'
                result['limit_price'] = market_data.get('low')
                return result
        
        # 正常可交易
        result['is_tradable'] = True
        result['can_buy'] = True
        result['can_sell'] = True
        result['reason'] = '正常交易'
        
        return result
    
    def get_code_mapping(self, old_symbol: str) -> Optional[str]:
        """
        获取代码变更映射
        
        用于处理股改、合并等导致的代码变化
        """
        # 直接映射
        if old_symbol in self._code_change_map:
            return self._code_change_map[old_symbol]
        
        # 检查是否是合并后的新代码
        for symbol, lifetime in self._stocks.items():
            if old_symbol in lifetime.predecessor_symbols:
                return symbol
        
        return None
    
    def resolve_historical_symbol(self, symbol: str, date: datetime) -> Optional[str]:
        """
        解析历史代码（处理代码变更后的追溯）
        
        例如：某股票股改前代码为000001，股改后变为000002
        查询股改前的数据时，需要映射到旧代码
        """
        # 如果当前代码在指定日期有效，直接返回
        if symbol in self._stocks:
            lifetime = self._stocks[symbol]
            if lifetime.is_active(date):
                return symbol
        
        # 查找该代码的前任
        for s, lifetime in self._stocks.items():
            if symbol in lifetime.predecessor_symbols:
                if lifetime.is_active(date):
                    return s
                # 递归查找更早的前任
                earlier = self.resolve_historical_symbol(s, date)
                if earlier:
                    return earlier
        
        return None
    
    def _is_st(self, symbol: str, date: datetime) -> bool:
        """检查指定日期是否为ST"""
        if symbol not in self._st_history:
            return False
        
        for start, end in self._st_history[symbol]:
            if start <= date and (end is None or date <= end):
                return True
        
        return False
    
    def get_delisting_warnings(
        self,
        holdings: List[str],
        date: datetime,
        warning_days: int = 30,
    ) -> List[Dict]:
        """
        获取退市预警
        
        Returns
        -------
        List[Dict]: [{'symbol': 'xxx', 'delist_date': datetime, 'days_remaining': int}, ...]
        """
        warnings = []
        
        for symbol in holdings:
            if symbol not in self._stocks:
                continue
            
            lifetime = self._stocks[symbol]
            if lifetime.delist_date is None:
                continue
            
            days = (lifetime.delist_date - date).days
            if 0 <= days <= warning_days:
                warnings.append({
                    'symbol': symbol,
                    'name': lifetime.name,
                    'delist_date': lifetime.delist_date,
                    'days_remaining': days,
                    'urgency': 'high' if days <= 10 else 'medium' if days <= 20 else 'low',
                })
        
        return sorted(warnings, key=lambda x: x['days_remaining'])
    
    def get_lifecycle(self, symbol: str) -> Optional[StockLifetime]:
        """获取股票生命周期信息"""
        return self._stocks.get(symbol)
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        return {
            'total_stocks': len(self._stocks),
            'delisted_stocks': len(self._delisted),
            'active_stocks': len(self._stocks) - len(self._delisted),
            'board_distribution': self._board_distribution(),
        }
    
    def _board_distribution(self) -> Dict[str, int]:
        """板块分布"""
        dist = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0}
        for lifetime in self._stocks.values():
            dist[lifetime.board] = dist.get(lifetime.board, 0) + 1
        return dist
