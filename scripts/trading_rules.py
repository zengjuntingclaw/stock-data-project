"""A股交易规则配置中心 - 生产级回测核心配置

集中管理所有随时间变化的交易规则，确保回测的历史准确性。
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd


@dataclass(frozen=True)
class TradingRulePeriod:
    """规则生效时间段"""
    start_date: datetime
    end_date: Optional[datetime] = None
    
    def contains(self, date: datetime) -> bool:
        if date < self.start_date:
            return False
        if self.end_date and date > self.end_date:
            return False
        return True


class AShareTradingRules:
    """
    A股交易规则历史变迁管理
    
    关键时间节点：
    - 1990-12-19: 上交所成立，涨跌停±10%
    - 1992-05-21: 放开涨跌停限制
    - 1996-12-16: 恢复涨跌停±10%
    - 2020-08-24: 创业板注册制，涨跌停±20%
    - 2023-08-28: 印花税减半（1‰ → 0.5‰）
    """
    
    # 板块代码（英文避免编码问题）
    BOARD_MAIN = 'MAIN'
    BOARD_CHINEXT = 'CHINEXT'  # 创业板
    BOARD_STAR = 'STAR'        # 科创板
    BOARD_BSE = 'BSE'          # 北交所
    
    # ========== 涨跌停限制规则 ==========
    PRICE_LIMIT_RULES: Dict[str, List[Tuple[TradingRulePeriod, float]]] = {
        BOARD_MAIN: [
            (TradingRulePeriod(datetime(1990, 12, 19), datetime(1992, 5, 20)), 0.10),
            (TradingRulePeriod(datetime(1992, 5, 21), datetime(1996, 12, 15)), float('inf')),  # 无限制
            (TradingRulePeriod(datetime(1996, 12, 16)), 0.10),
        ],
        BOARD_CHINEXT: [
            (TradingRulePeriod(datetime(2009, 10, 30), datetime(2020, 8, 23)), 0.10),
            (TradingRulePeriod(datetime(2020, 8, 24)), 0.20),
        ],
        BOARD_STAR: [
            (TradingRulePeriod(datetime(2019, 7, 22)), 0.20),
        ],
        BOARD_BSE: [
            (TradingRulePeriod(datetime(2021, 11, 15)), 0.30),
        ],
    }
    
    # ========== 手数约束规则 ==========
    LOT_SIZE_RULES: Dict[str, List[Tuple[TradingRulePeriod, int, int]]] = {
        # (生效期, 最小手数, 手数基数)
        BOARD_MAIN: [(TradingRulePeriod(datetime(1990, 12, 19)), 100, 100)],
        BOARD_CHINEXT: [(TradingRulePeriod(datetime(2009, 10, 30)), 100, 100)],
        BOARD_STAR: [(TradingRulePeriod(datetime(2019, 7, 22)), 200, 1)],  # 200股起，1股递增
        BOARD_BSE: [(TradingRulePeriod(datetime(2021, 11, 15)), 100, 1)],
    }
    
    # ========== 印花税规则 ==========
    STAMP_TAX_RULES: List[Tuple[TradingRulePeriod, float]] = [
        (TradingRulePeriod(datetime(1990, 12, 19), datetime(1991, 10, 1)), 0.006),   # 6‰双边
        (TradingRulePeriod(datetime(1991, 10, 2), datetime(1997, 5, 9)), 0.003),     # 3‰双边
        (TradingRulePeriod(datetime(1997, 5, 10), datetime(1998, 6, 11)), 0.005),    # 5‰双边
        (TradingRulePeriod(datetime(1998, 6, 12), datetime(2001, 11, 15)), 0.004),   # 4‰双边
        (TradingRulePeriod(datetime(2001, 11, 16), datetime(2005, 1, 23)), 0.002),   # 2‰双边
        (TradingRulePeriod(datetime(2005, 1, 24), datetime(2007, 5, 29)), 0.001),    # 1‰双边
        (TradingRulePeriod(datetime(2007, 5, 30), datetime(2008, 4, 23)), 0.003),    # 3‰双边
        (TradingRulePeriod(datetime(2008, 4, 24), datetime(2023, 8, 27)), 0.001),    # 1‰卖方
        (TradingRulePeriod(datetime(2023, 8, 28)), 0.0005),                           # 0.5‰卖方
    ]
    
    @classmethod
    def get_board(cls, symbol: str) -> str:
        """根据股票代码识别板块（委托到 data_engine.detect_board 的映射）"""
        from scripts.data_engine import detect_board
        board_map = {
            '主板': cls.BOARD_MAIN,
            '创业板': cls.BOARD_CHINEXT,
            '科创板': cls.BOARD_STAR,
            '北交所': cls.BOARD_BSE,
        }
        return board_map.get(detect_board(symbol), cls.BOARD_MAIN)
    
    @classmethod
    def get_price_limit(cls, symbol: str, date: datetime, list_date: datetime = None) -> float:
        """获取指定日期股票的涨跌停限制
        
        Parameters
        ----------
        symbol : str
            6位股票代码
        date : datetime
            查询日期
        list_date : datetime, optional
            上市日期，用于判断新股首日特殊规则。
            如果不传，无法判断新股首日，使用通用规则。
            
        新股规则：
        - 主板核准制（2024年前上市）：首日涨跌停±10%，首日不设涨跌幅限制（仅限深市）
        - 注册制（创业板2020.8.24起、科创板2019.7.22起）：前5个交易日不设涨跌幅限制
        - 北交所：上市首日不设涨跌幅限制，之后±30%
        """
        board = cls.get_board(symbol)
        rules = cls.PRICE_LIMIT_RULES.get(board, cls.PRICE_LIMIT_RULES[cls.BOARD_MAIN])
        
        # 新股首日特殊处理
        if list_date is not None:
            days_since_list = (date - list_date).days
            
            # 注册制板块：前5个交易日不设涨跌幅限制
            if board in (cls.BOARD_STAR, cls.BOARD_CHINEXT):
                # 科创板：2019.7.22起注册制
                if board == cls.BOARD_STAR and date >= datetime(2019, 7, 22) and days_since_list < 5:
                    return float('inf')
                # 创业板：2020.8.24起注册制
                if board == cls.BOARD_CHINEXT and date >= datetime(2020, 8, 24) and days_since_list < 5:
                    return float('inf')
            
            # 北交所：首日不设涨跌幅限制
            if board == cls.BOARD_BSE and days_since_list < 1:
                return float('inf')
        
        for period, limit in rules:
            if period.contains(date):
                return limit
        return 0.10  # 默认10%
    
    @classmethod
    def get_lot_size(cls, symbol: str, date: datetime) -> Tuple[int, int]:
        """获取手数约束 (最小手数, 手数基数)"""
        board = cls.get_board(symbol)
        rules = cls.LOT_SIZE_RULES.get(board, cls.LOT_SIZE_RULES[cls.BOARD_MAIN])
        
        for period, min_lot, lot_base in rules:
            if period.contains(date):
                return min_lot, lot_base
        return 100, 100  # 默认100股
    
    @classmethod
    def get_stamp_tax_rate(cls, date: datetime) -> float:
        """获取指定日期的印花税率（仅卖方）"""
        for period, rate in cls.STAMP_TAX_RULES:
            if period.contains(date):
                return rate
        return 0.0005  # 默认最新税率
    
    @classmethod
    def round_shares(cls, symbol: str, date: datetime, target_shares: int) -> int:
        """根据手数约束规整股数"""
        min_lot, lot_base = cls.get_lot_size(symbol, date)
        
        if target_shares < min_lot:
            return 0  # 不足最小手数，无法交易
        
        # 科创板/北交所：200股起，之后1股递增
        if lot_base == 1:
            return max(min_lot, target_shares)
        
        # 主板/创业板：100股整数倍
        return (target_shares // lot_base) * lot_base


class TradingFilter:
    """
    交易过滤器 - 精确处理停牌与可交易状态
    
    在回测数据导出模块中增加is_tradable布尔值，
    如果当天停牌或一字涨跌停导致无法成交，该标志位应准确反映。
    """
    
    def __init__(self, trading_rules: AShareTradingRules = None):
        self.rules = trading_rules or AShareTradingRules()
    
    def check_tradable(self,
                      symbol: str,
                      date: datetime,
                      market_data: Dict[str, Any],
                      position: Optional[Dict] = None) -> Dict[str, Any]:
        """
        检查股票在指定日期的可交易状态
        
        Parameters
        ----------
        symbol : str
            股票代码
        date : datetime
            交易日期
        market_data : Dict
            市场数据，包含：
            - open, high, low, close: 价格
            - volume: 成交量
            - pre_close: 昨收
            - is_suspend: 是否停牌
        position : Dict, optional
            当前持仓信息（用于判断能否卖出）
            
        Returns
        -------
        Dict: {
            'is_tradable': bool,        # 是否可交易（综合判断）
            'can_buy': bool,            # 是否可买入
            'can_sell': bool,           # 是否可卖出
            'is_suspend': bool,         # 是否停牌
            'is_limit_up': bool,        # 是否涨停
            'is_limit_down': bool,      # 是否跌停
            'limit_up_price': Optional[float],   # 涨停价格
            'limit_down_price': Optional[float], # 跌停价格
            'reason': str,              # 不可交易原因
        }
        """
        result = {
            'is_tradable': False,
            'can_buy': False,
            'can_sell': False,
            'is_suspend': False,
            'is_limit_up': False,
            'is_limit_down': False,
            'limit_up_price': None,
            'limit_down_price': None,
            'reason': ''
        }
        
        # 1. 停牌检查
        if market_data.get('is_suspend', False) or market_data.get('volume', 0) == 0:
            result['is_suspend'] = True
            result['reason'] = '停牌'
            return result
        
        # 2. 计算涨跌停价格
        pre_close = market_data.get('pre_close', market_data.get('close', 0))
        if pre_close <= 0:
            result['reason'] = '无效前收盘价'
            return result
        
        limit_pct = self.rules.get_price_limit(symbol, date)
        
        # 新股首日或特殊情形无涨跌幅限制
        if limit_pct == float('inf'):
            result['is_tradable'] = True
            result['can_buy'] = True
            result['can_sell'] = True
            result['reason'] = '无涨跌幅限制'
            return result
        
        result['limit_up_price'] = round(pre_close * (1 + limit_pct), 2)
        result['limit_down_price'] = round(pre_close * (1 - limit_pct), 2)
        
        # 3. 涨跌停检查
        high = market_data.get('high', 0)
        low = market_data.get('low', 0)
        open_price = market_data.get('open', 0)
        
        # 涨停判断：最高价触及涨停价且最低价也接近涨停价（一字涨停）
        if high >= result['limit_up_price'] * 0.999:
            if low >= result['limit_up_price'] * 0.999:  # 一字涨停
                result['is_limit_up'] = True
                result['can_sell'] = True  # 可以卖出
                result['can_buy'] = False  # 无法买入
                result['reason'] = '一字涨停，无法买入'
            else:
                # 盘中触及涨停但打开，可以交易
                result['can_buy'] = True
                result['can_sell'] = True
        
        # 跌停判断
        elif low <= result['limit_down_price'] * 1.001:
            if high <= result['limit_down_price'] * 1.001:  # 一字跌停
                result['is_limit_down'] = True
                result['can_buy'] = True   # 可以买入
                result['can_sell'] = False # 无法卖出
                result['reason'] = '一字跌停，无法卖出'
            else:
                # 盘中触及跌停但打开，可以交易
                result['can_buy'] = True
                result['can_sell'] = True
        else:
            # 正常交易
            result['can_buy'] = True
            result['can_sell'] = True
        
        # 4. 综合判断
        result['is_tradable'] = result['can_buy'] or result['can_sell']
        
        if result['is_tradable'] and not result['reason']:
            result['reason'] = '正常交易'
        
        return result
    
    def filter_tradable_stocks(self,
                               symbols: List[str],
                               date: datetime,
                               market_data_dict: Dict[str, Dict]) -> List[str]:
        """
        批量过滤可交易股票
        
        Returns
        -------
        List[str]: 可交易股票列表
        """
        tradable = []
        for symbol in symbols:
            if symbol in market_data_dict:
                status = self.check_tradable(symbol, date, market_data_dict[symbol])
                if status['is_tradable']:
                    tradable.append(symbol)
        return tradable
    
    def get_execution_price(self,
                           symbol: str,
                           date: datetime,
                           market_data: Dict[str, Any],
                           side: str,  # 'buy' or 'sell'
                           order_type: str = 'market'  # 'market', 'limit'
                           ) -> Optional[float]:
        """
        获取执行价格
        
        考虑涨跌停限制：
        - 买入时如果涨停，无法成交
        - 卖出时如果跌停，无法成交
        """
        status = self.check_tradable(symbol, date, market_data)
        
        if side == 'buy' and not status['can_buy']:
            return None
        if side == 'sell' and not status['can_sell']:
            return None
        
        if order_type == 'market':
            # 市价单使用开盘价或VWAP
            return market_data.get('open')
        else:
            # 限价单需要检查价格是否在涨跌停范围内
            return market_data.get('close')


class TradingCalendar:
    """A股交易日历管理
    
    支持从2018年起的所有节假日。
    建议通过 DataEngine.sync_calendar() 从 AkShare 同步完整日历，
    或手动调用 add_holidays() 添加节假日。
    """
    
    # 2018年节假日
    HOLIDAYS_2018 = [
        '2018-01-01',  # 元旦
        '2018-02-15', '2018-02-16', '2018-02-17', '2018-02-18', '2018-02-19', '2018-02-20', '2018-02-21',  # 春节
        '2018-04-05', '2018-04-06', '2018-04-07',  # 清明
        '2018-04-29', '2018-04-30', '2018-05-01',  # 劳动节
        '2018-06-18',  # 端午
        '2018-09-24', '2018-09-25',  # 中秋
        '2018-10-01', '2018-10-02', '2018-10-03', '2018-10-04', '2018-10-05', '2018-10-06', '2018-10-07',  # 国庆
    ]
    
    # 2019年节假日
    HOLIDAYS_2019 = [
        '2019-01-01',  # 元旦
        '2019-02-04', '2019-02-05', '2019-02-06', '2019-02-07', '2019-02-08', '2019-02-09', '2019-02-10',  # 春节
        '2019-04-05',  # 清明
        '2019-05-01', '2019-05-02', '2019-05-03', '2019-05-04',  # 劳动节
        '2019-06-07',  # 端午
        '2019-09-13', '2019-09-14', '2019-09-15',  # 中秋
        '2019-10-01', '2019-10-02', '2019-10-03', '2019-10-04', '2019-10-05', '2019-10-06', '2019-10-07',  # 国庆
    ]
    
    # 2020年节假日
    HOLIDAYS_2020 = [
        '2020-01-01',  # 元旦
        '2020-01-24', '2020-01-25', '2020-01-26', '2020-01-27', '2020-01-28', '2020-01-29', '2020-01-30',  # 春节
        '2020-04-04', '2020-04-05', '2020-04-06',  # 清明
        '2020-05-01', '2020-05-02', '2020-05-03', '2020-05-04', '2020-05-05',  # 劳动节
        '2020-06-25', '2020-06-26', '2020-06-27',  # 端午
        '2020-10-01', '2020-10-02', '2020-10-03', '2020-10-04', '2020-10-05', '2020-10-06', '2020-10-07', '2020-10-08',  # 国庆+中秋
    ]
    
    # 2021年节假日
    HOLIDAYS_2021 = [
        '2021-01-01',  # 元旦
        '2021-02-11', '2021-02-12', '2021-02-13', '2021-02-14', '2021-02-15', '2021-02-16', '2021-02-17',  # 春节
        '2021-04-03', '2021-04-04', '2021-04-05',  # 清明
        '2021-05-01', '2021-05-02', '2021-05-03', '2021-05-04', '2021-05-05',  # 劳动节
        '2021-06-14',  # 端午
        '2021-09-20', '2021-09-21',  # 中秋
        '2021-10-01', '2021-10-02', '2021-10-03', '2021-10-04', '2021-10-05', '2021-10-06', '2021-10-07',  # 国庆
    ]
    
    # 2022年节假日
    HOLIDAYS_2022 = [
        '2022-01-01', '2022-01-02', '2022-01-03',  # 元旦
        '2022-01-31', '2022-02-01', '2022-02-02', '2022-02-03', '2022-02-04', '2022-02-05', '2022-02-06',  # 春节
        '2022-04-04', '2022-04-05',  # 清明
        '2022-04-30', '2022-05-01', '2022-05-02', '2022-05-03', '2022-05-04',  # 劳动节
        '2022-06-03', '2022-06-04', '2022-06-05',  # 端午
        '2022-09-10', '2022-09-11', '2022-09-12',  # 中秋
        '2022-10-01', '2022-10-02', '2022-10-03', '2022-10-04', '2022-10-05', '2022-10-06', '2022-10-07',  # 国庆
    ]
    
    # 2023年节假日
    HOLIDAYS_2023 = [
        '2023-01-01', '2023-01-02',  # 元旦
        '2023-01-21', '2023-01-22', '2023-01-23', '2023-01-24', '2023-01-25', '2023-01-26', '2023-01-27',  # 春节
        '2023-04-05',  # 清明
        '2023-04-29', '2023-04-30', '2023-05-01', '2023-05-02', '2023-05-03',  # 劳动节
        '2023-06-22', '2023-06-23', '2023-06-24',  # 端午
        '2023-09-29',  # 中秋
        '2023-10-01', '2023-10-02', '2023-10-03', '2023-10-04', '2023-10-05', '2023-10-06',  # 国庆
    ]
    
    # 2024年节假日
    HOLIDAYS_2024 = [
        '2024-01-01',  # 元旦
        '2024-02-09', '2024-02-10', '2024-02-11', '2024-02-12', '2024-02-13', '2024-02-14', '2024-02-15', '2024-02-16',  # 春节
        '2024-04-04', '2024-04-05', '2024-04-06',  # 清明
        '2024-05-01', '2024-05-02', '2024-05-03', '2024-05-04', '2024-05-05',  # 劳动节
        '2024-06-10',  # 端午
        '2024-09-15', '2024-09-16', '2024-09-17',  # 中秋
        '2024-10-01', '2024-10-02', '2024-10-03', '2024-10-04', '2024-10-05', '2024-10-06', '2024-10-07',  # 国庆
    ]
    
    # 2025年节假日
    HOLIDAYS_2025 = [
        '2025-01-01',  # 元旦
        '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31', '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04',  # 春节
        '2025-04-04', '2025-04-05', '2025-04-06',  # 清明
        '2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05',  # 劳动节
        '2025-05-31', '2025-06-01', '2025-06-02',  # 端午
        '2025-10-01', '2025-10-02', '2025-10-03', '2025-10-04', '2025-10-05', '2025-10-06', '2025-10-07', '2025-10-08',  # 国庆+中秋
    ]
    
    # 2026年节假日
    HOLIDAYS_2026 = [
        '2026-01-01', '2026-01-02',  # 元旦
        '2026-02-16', '2026-02-17', '2026-02-18', '2026-02-19', '2026-02-20', '2026-02-21', '2026-02-22',  # 春节
        '2026-04-05', '2026-04-06', '2026-04-07',  # 清明
        '2026-05-01', '2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05',  # 劳动节
        '2026-06-19', '2026-06-20', '2026-06-21',  # 端午
        '2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07', '2026-10-08',  # 国庆+中秋
    ]
    
    # 2027年节假日（预估，具体以国务院公告为准）
    HOLIDAYS_2027 = [
        '2027-01-01',  # 元旦
        '2027-02-06', '2027-02-07', '2027-02-08', '2027-02-09', '2027-02-10', '2027-02-11', '2027-02-12',  # 春节（预估）
        '2027-04-05',  # 清明
        '2027-05-01', '2027-05-02', '2027-05-03',  # 劳动节
        '2027-06-09', '2027-06-10', '2027-06-11',  # 端午
        '2027-10-01', '2027-10-02', '2027-10-03', '2027-10-04', '2027-10-05', '2027-10-06', '2027-10-07',  # 国庆
    ]
    
    def __init__(self):
        self._holidays = set()
        # 加载所有已知节假日（2018-2027）
        for year_holidays in [
            self.HOLIDAYS_2018, self.HOLIDAYS_2019, self.HOLIDAYS_2020,
            self.HOLIDAYS_2021, self.HOLIDAYS_2022, self.HOLIDAYS_2023,
            self.HOLIDAYS_2024, self.HOLIDAYS_2025, self.HOLIDAYS_2026,
            self.HOLIDAYS_2027,
        ]:
            self.add_holidays(year_holidays)
    
    def add_holidays(self, holidays):
        """添加节假日列表
        
        Parameters
        ----------
        holidays : list
            日期字符串列表，格式 'YYYY-MM-DD' 或 datetime 对象
        """
        for h in holidays:
            self._holidays.add(pd.Timestamp(h).normalize())
    
    def is_trading_day(self, date: datetime) -> bool:
        """判断是否为交易日"""
        if date.weekday() >= 5:  # 周末
            return False
        if pd.Timestamp(date).normalize() in self._holidays:
            return False
        return True
    
    def next_trading_day(self, date: datetime) -> datetime:
        """获取下一个交易日"""
        next_day = date + pd.Timedelta(days=1)
        while not self.is_trading_day(next_day):
            next_day += pd.Timedelta(days=1)
        return next_day
    
    def prev_trading_day(self, date: datetime) -> datetime:
        """获取上一个交易日"""
        prev_day = date - pd.Timedelta(days=1)
        while not self.is_trading_day(prev_day):
            prev_day -= pd.Timedelta(days=1)
        return prev_day
