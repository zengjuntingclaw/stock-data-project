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
        """根据股票代码识别板块"""
        import re
        # 注意：股票代码都是6位数字
        if re.match(r'^688[0-9]{3}$', symbol):  # 科创板 688xxx
            return cls.BOARD_STAR
        elif re.match(r'^30[0-9]{4}$', symbol):  # 创业板 300xxx-309xxx
            return cls.BOARD_CHINEXT
        elif re.match(r'^8[0-9]{5}$', symbol) or re.match(r'^4[0-9]{5}$', symbol):  # 北交所 8xxxxx/4xxxxx
            return cls.BOARD_BSE
        else:  # 主板 60xxxx, 00xxxx, 68xxxx(非688)
            return cls.BOARD_MAIN
    
    @classmethod
    def get_price_limit(cls, symbol: str, date: datetime) -> float:
        """获取指定日期股票的涨跌停限制"""
        board = cls.get_board(symbol)
        rules = cls.PRICE_LIMIT_RULES.get(board, cls.PRICE_LIMIT_RULES[cls.BOARD_MAIN])
        
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


class TradingCalendar:
    """A股交易日历管理
    
    支持从2018年起的所有节假日。
    建议通过 DataEngine.sync_calendar() 从 AkShare 同步完整日历，
    或手动调用 add_holidays() 添加节假日。
    """
    
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
    
    def __init__(self):
        self._holidays = set()
        # 加载已知的节假日
        self.add_holidays(self.HOLIDAYS_2024)
        self.add_holidays(self.HOLIDAYS_2025)
        self.add_holidays(self.HOLIDAYS_2026)
    
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
