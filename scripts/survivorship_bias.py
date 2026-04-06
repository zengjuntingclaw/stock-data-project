"""幸存者偏差治理模块

核心问题：回测时只使用当前存活的股票，会高估策略表现（幸存者偏差）
解决方案：获取历史全量股票列表（含退市股），确保回测股票池的历史完整性
"""
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import pandas as pd
from loguru import logger


@dataclass
class StockLifetime:
    """股票生命周期"""
    symbol: str
    name: str
    list_date: datetime          # 上市日期
    delist_date: Optional[datetime] = None  # 退市日期（None表示未退市）
    board: str = '主板'          # 板块
    is_st: bool = False          # 是否ST
    st_start_date: Optional[datetime] = None
    st_end_date: Optional[datetime] = None


class SurvivorshipBiasHandler:
    """
    幸存者偏差处理器
    
    职责：
    1. 维护历史全量股票列表（含退市股）
    2. 提供任意历史时点的有效股票池
    3. 标记退市、ST等状态变化
    4. 确保回测时不会"预知未来"
    """
    
    def __init__(self):
        self._stocks: Dict[str, StockLifetime] = {}
        self._delisted: Set[str] = set()
        self._st_history: Dict[str, List[Tuple[datetime, Optional[datetime]]]] = {}
        
        logger.info("SurvivorshipBiasHandler initialized")
    
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
        """从Baostock加载历史全量股票（含退市）"""
        try:
            import baostock as bs
            
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"Baostock login failed: {lg.error_msg}")
                return
            
            for year in range(start_year, end_year + 1):
                # 查询每年最后一个交易日的所有股票
                rs = bs.query_all_stock(day=f"{year}-12-31")
                if rs.error_code != '0':
                    continue
                
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    symbol = row[0].split('.')[1]  # sh.600000 -> 600000
                    
                    if symbol not in self._stocks:
                        # 查询股票基本信息
                        rs_detail = bs.query_stock_basic(code=row[0])
                        if rs_detail.error_code == '0' and rs_detail.next():
                            detail = rs_detail.get_row_data()
                            self._stocks[symbol] = StockLifetime(
                                symbol=symbol,
                                name=detail[1],
                                list_date=pd.to_datetime(detail[6]) if detail[6] else pd.Timestamp(f"{year}-01-01"),
                                delist_date=pd.to_datetime(detail[7]) if detail[7] else None,
                                board=self._detect_board(symbol),
                            )
                            
                            if detail[7]:  # 有退市日期
                                self._delisted.add(symbol)
            
            bs.logout()
            logger.info(f"Loaded {len(self._stocks)} stocks from Baostock ({len(self._delisted)} delisted)")
            
        except Exception as e:
            logger.error(f"Failed to load from Baostock: {e}")
    
    def _detect_board(self, symbol: str) -> str:
        """识别板块"""
        import re
        if re.match(r'^68[0-9]{5}$', symbol):
            return '科创板'
        elif re.match(r'^30[0-9]{5}$', symbol):
            return '创业板'
        elif re.match(r'^8[0-9]{5}$|^4[0-9]{5}$', symbol):
            return '北交所'
        return '主板'
    
    def get_universe(
        self,
        date: datetime,
        include_delisted: bool = False,
        min_listing_days: int = 60,
    ) -> List[str]:
        """
        获取指定日期的有效股票池
        
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
        if self._is_st(symbol, date):
            return False, "ST股票"
        
        return True, ""
    
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
