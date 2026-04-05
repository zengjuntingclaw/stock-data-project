"""
ExecutionEngine v2.0 - A股真实交易规则引擎
===========================================
核心改进：
  1. T+1 交易限制（持仓时间戳管理）
  2. 多层次涨跌停板判定
  3. 除权除息自动调整
  4. 真实持仓成本计算
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Literal
from dataclasses import dataclass, field
from loguru import logger


# ──────────────────────────────────────────────────────────────
# 数据类定义
# ──────────────────────────────────────────────────────────────
@dataclass
class Position:
    """持仓记录（含时间戳）"""
    symbol: str
    shares: int                    # 总股数
    available_shares: int          # 可卖出股数（T+1后增加）
    cost_basis: float              # 成本价（复权后）
    buy_date: datetime             # 首次买入日期
    last_update: datetime          # 最后更新日期
    pending_shares: int = 0        # 待解冻股数（当日买入）
    adj_factor_at_buy: float = 1.0 # 买入时复权因子


@dataclass
class CorporateAction:
    """公司行动（除权除息）"""
    symbol: str
    ex_date: datetime
    action_type: Literal["dividend", "split", "bonus"]
    adj_ratio: float               # 复权比例变化
    cash_dividend: float = 0.0     # 现金分红（每股）


# ──────────────────────────────────────────────────────────────
# A股涨跌停判定
# ──────────────────────────────────────────────────────────────
class LimitBoardChecker:
    """
    A股多层次涨跌停板判定
    
    规则：
      - 主板（60, 00）：10%
      - 科创板（688）、创业板（30）：20%
      - 北交所（4, 8）：30%
      - ST/*ST：统一5%
      - 新股上市首日：特殊规则
    """
    
    # 涨跌停幅度配置
    LIMIT_CONFIG = {
        "main": 0.10,        # 主板
        "gem": 0.20,         # 创业板
        "star": 0.20,        # 科创板
        "bse": 0.30,         # 北交所
        "st": 0.05,          # ST股票
    }
    
    @staticmethod
    def get_board_type(symbol: str) -> str:
        """判断板块类型"""
        code = symbol.split(".")[0] if "." in symbol else symbol
        
        # ST判断（优先）
        # 注：需要配合股票名称判断，这里简化处理
        
        # 科创板（688xxx）
        if code.startswith("688"):
            return "star"
        # 创业板（30xxxx）
        elif code.startswith("30"):
            return "gem"
        # 北交所（4xxxxx, 8xxxxx）
        elif code.startswith("4") or code.startswith("8"):
            return "bse"
        # 主板（60xxxx, 00xxxx）
        elif code.startswith("6") or code.startswith("0"):
            return "main"
        else:
            return "main"  # 默认主板
    
    @staticmethod
    def get_limit_pct(symbol: str, name: str = "", is_st: bool = False) -> float:
        """
        获取涨跌停幅度
        
        Parameters
        ----------
        symbol : str
            股票代码
        name : str
            股票名称（用于判断ST）
        is_st : bool
            是否ST（从数据源获取）
        """
        # ST优先（统一5%）
        if is_st or "ST" in name or "*ST" in name:
            return LimitBoardChecker.LIMIT_CONFIG["st"]
        
        board = LimitBoardChecker.get_board_type(symbol)
        return LimitBoardChecker.LIMIT_CONFIG.get(board, 0.10)
    
    @staticmethod
    def check_limit(symbol: str, 
                    pct_chg: float, 
                    name: str = "",
                    is_st: bool = False,
                    list_date: Optional[datetime] = None,
                    trade_date: Optional[datetime] = None) -> Tuple[bool, bool]:
        """
        判断涨跌停
        
        Returns
        -------
        (is_limit_up, is_limit_down)
        """
        # 新股上市首日特殊处理
        if list_date and trade_date:
            days_listed = (trade_date.date() - list_date.date()).days
            if days_listed == 0:
                # 主板首日44%涨幅限制，无跌停限制
                board = LimitBoardChecker.get_board_type(symbol)
                if board in ["main"]:
                    return pct_chg >= 0.44, False
                # 科创板/创业板/北交所首日无涨跌停
                elif board in ["star", "gem", "bse"]:
                    return False, False
        
        limit_pct = LimitBoardChecker.get_limit_pct(symbol, name, is_st)
        
        # 允许0.1%误差（四舍五入）
        is_up = pct_chg >= (limit_pct - 0.001)
        is_down = pct_chg <= -(limit_pct - 0.001)
        
        return is_up, is_down
    
    @staticmethod
    def apply_to_dataframe(df: pd.DataFrame,
                           symbol_col: str = "symbol",
                           pct_chg_col: str = "pct_chg",
                           name_col: str = "name",
                           st_col: str = "is_st",
                           list_date_col: str = "list_date") -> pd.DataFrame:
        """
        向量化涨跌停判定
        
        输入DataFrame需包含：symbol, pct_chg, name（可选）, is_st（可选）
        """
        df = df.copy()
        
        # 批量判断板块
        df["_board"] = df[symbol_col].apply(LimitBoardChecker.get_board_type)
        
        # ST判断
        if st_col in df.columns:
            df["_is_st"] = df[st_col]
        elif name_col in df.columns:
            df["_is_st"] = df[name_col].str.contains("ST", na=False)
        else:
            df["_is_st"] = False
        
        # 涨跌停幅度
        df["_limit_pct"] = np.where(
            df["_is_st"],
            0.05,
            df["_board"].map({
                "main": 0.10,
                "gem": 0.20,
                "star": 0.20,
                "bse": 0.30
            }).fillna(0.10)
        )
        
        # 新股首日特殊处理（简化：跳过）
        
        # 判定涨跌停
        df["limit_up"] = df[pct_chg_col] >= (df["_limit_pct"] - 0.001)
        df["limit_down"] = df[pct_chg_col] <= -(df["_limit_pct"] - 0.001)
        
        # 清理临时列
        df.drop(columns=["_board", "_is_st", "_limit_pct"], inplace=True)
        
        return df


# ──────────────────────────────────────────────────────────────
# 交易执行引擎 v2
# ──────────────────────────────────────────────────────────────
class ExecutionEngineV2:
    """
    A股真实交易执行引擎
    
    核心特性：
      1. T+1 交易限制
      2. 除权除息自动调整
      3. 真实持仓管理
    """
    
    def __init__(self, 
                 cost_config: Optional[Dict] = None,
                 adv_limit: float = 0.1):
        """
        Parameters
        ----------
        cost_config : dict
            交易成本配置
        adv_limit : float
            成交量限制（占ADV的比例）
        """
        self.cost_config = cost_config or {
            "commission": 0.0003,     # 佣金 0.03%
            "min_commission": 5,       # 最低佣金 5元
            "stamp_tax": 0.001,        # 印花税 0.1%（仅卖出）
            "slippage": 0.001,         # 滑点 0.1%
        }
        self.adv_limit = adv_limit
        
        # 持仓管理
        self.positions: Dict[str, Position] = {}
        self.cash = 0.0
        
        # 交易记录
        self.trade_history: List[Dict] = []
        self.corporate_actions: List[CorporateAction] = []
    
    # ───────────────────────────────────────────────────────────
    # T+1 持仓管理
    # ───────────────────────────────────────────────────────────
    def update_positions_for_new_day(self, trade_date: datetime):
        """
        每日开盘前更新持仓：
          1. 将昨日买入的pending_shares转为available
          2. 处理除权除息
        """
        for symbol, pos in self.positions.items():
            if pos.pending_shares > 0:
                # T+1：昨日买入的今日可卖
                pos.available_shares += pos.pending_shares
                pos.pending_shares = 0
                logger.debug(f"{trade_date} | {symbol} 解冻 {pos.pending_shares} 股")
        
        # 处理除权除息
        self._process_corporate_actions(trade_date)
    
    def get_available_shares(self, symbol: str) -> int:
        """获取可卖出股数（T+1限制）"""
        if symbol not in self.positions:
            return 0
        return self.positions[symbol].available_shares
    
    # ───────────────────────────────────────────────────────────
    # 除权除息处理
    # ───────────────────────────────────────────────────────────
    def _process_corporate_actions(self, trade_date: datetime):
        """
        处理除权除息：
          - 送股/转增：股数增加，成本降低
          - 现金分红：现金增加，成本降低
        """
        for action in self.corporate_actions:
            if action.ex_date != trade_date:
                continue
            
            if action.symbol not in self.positions:
                continue
            
            pos = self.positions[action.symbol]
            
            if action.action_type in ["split", "bonus"]:
                # 送股/转增：股数×比例
                old_shares = pos.shares
                pos.shares = int(pos.shares * action.adj_ratio)
                pos.available_shares = int(pos.available_shares * action.adj_ratio)
                pos.pending_shares = int(pos.pending_shares * action.adj_ratio)
                # 成本÷比例
                pos.cost_basis = pos.cost_basis / action.adj_ratio
                
                logger.info(f"{trade_date} | {action.symbol} 除权："
                           f"{old_shares}→{pos.shares}股")
            
            elif action.action_type == "dividend":
                # 现金分红：现金增加
                dividend = pos.shares * action.cash_dividend
                self.cash += dividend
                # 成本降低（简化处理）
                pos.cost_basis -= action.cash_dividend
                
                logger.info(f"{trade_date} | {action.symbol} 分红："
                           f"+{dividend:.2f}元")
    
    def load_corporate_actions(self, df: pd.DataFrame):
        """
        从DataFrame加载公司行动
        
        需要列：symbol, ex_date, adj_factor_old, adj_factor_new, cash_dividend
        """
        for _, row in df.iterrows():
            # 计算复权比例变化
            adj_ratio = row.get("adj_factor_new", 1.0) / row.get("adj_factor_old", 1.0)
            
            action_type = "dividend" if row.get("cash_dividend", 0) > 0 else "split"
            
            action = CorporateAction(
                symbol=row["symbol"],
                ex_date=pd.Timestamp(row["ex_date"]),
                action_type=action_type,
                adj_ratio=adj_ratio,
                cash_dividend=row.get("cash_dividend", 0.0)
            )
            self.corporate_actions.append(action)
        
        logger.info(f"加载 {len(self.corporate_actions)} 条公司行动记录")
    
    # ───────────────────────────────────────────────────────────
    # 交易执行
    # ───────────────────────────────────────────────────────────
    def execute_trades(self,
                       target_weights: Dict[str, float],
                       market_data: pd.DataFrame,
                       trade_date: datetime) -> Dict:
        """
        执行交易
        
        Parameters
        ----------
        target_weights : dict
            目标权重 {symbol: weight}
        market_data : DataFrame
            当日行情（需包含open, high, low, close, limit_up, limit_down等）
        trade_date : datetime
            交易日期
            
        Returns
        -------
        dict: {
            "trades": List[Dict],
            "positions": Dict[str, Position],
            "cash": float
        }
        """
        # 开盘前更新持仓
        self.update_positions_for_new_day(trade_date)
        
        # 计算目标持仓
        portfolio_value = self.cash + sum(
            pos.shares * self._get_price(market_data, sym, "close")
            for sym, pos in self.positions.items()
        )
        
        target_shares = {
            sym: int(weight * portfolio_value / self._get_price(market_data, sym, "close") / 100) * 100
            for sym, weight in target_weights.items()
        }
        
        trades = []
        
        # 执行卖出（先卖后买）
        for sym, target in target_shares.items():
            current = self.positions.get(sym, Position(sym, 0, 0, 0, trade_date, trade_date))
            available = self.get_available_shares(sym)
            
            if target < available:
                # 卖出
                sell_shares = min(available - target, available)
                price = self._get_price(market_data, sym, "open")
                
                # 检查跌停
                if market_data.loc[market_data["symbol"] == sym, "limit_down"].iloc[0]:
                    logger.warning(f"{trade_date} | {sym} 跌停，无法卖出")
                    continue
                
                trade = self._execute_sell(sym, sell_shares, price, trade_date)
                trades.append(trade)
        
        # 执行买入
        for sym, target in target_shares.items():
            current = self.positions.get(sym, Position(sym, 0, 0, 0, trade_date, trade_date))
            
            if target > current.shares:
                # 买入
                buy_shares = target - current.shares
                price = self._get_price(market_data, sym, "open")
                
                # 检查涨停
                if market_data.loc[market_data["symbol"] == sym, "limit_up"].iloc[0]:
                    logger.warning(f"{trade_date} | {sym} 涨停，无法买入")
                    continue
                
                # 检查资金
                amount = buy_shares * price
                if amount > self.cash:
                    buy_shares = int(self.cash / price / 100) * 100
                
                if buy_shares > 0:
                    trade = self._execute_buy(sym, buy_shares, price, trade_date)
                    trades.append(trade)
        
        return {
            "trades": trades,
            "positions": self.positions,
            "cash": self.cash
        }
    
    def _execute_buy(self, symbol: str, shares: int, price: float, date: datetime) -> Dict:
        """执行买入"""
        amount = shares * price
        commission = max(amount * self.cost_config["commission"], self.cost_config["min_commission"])
        slippage = amount * self.cost_config["slippage"]
        total_cost = amount + commission + slippage
        
        if symbol in self.positions:
            pos = self.positions[symbol]
            # 加仓：更新成本
            total_shares = pos.shares + shares
            pos.cost_basis = (pos.cost_basis * pos.shares + total_cost) / total_shares
            pos.shares = total_shares
            pos.pending_shares += shares  # T+1限制
            pos.last_update = date
        else:
            pos = Position(
                symbol=symbol,
                shares=shares,
                available_shares=0,  # 今日不可卖
                cost_basis=total_cost / shares,
                buy_date=date,
                last_update=date,
                pending_shares=shares
            )
            self.positions[symbol] = pos
        
        self.cash -= total_cost
        
        trade = {
            "symbol": symbol,
            "side": "BUY",
            "shares": shares,
            "price": price,
            "amount": amount,
            "commission": commission,
            "slippage": slippage,
            "total_cost": total_cost,
            "date": date
        }
        self.trade_history.append(trade)
        
        logger.info(f"{date} | 买入 {symbol} {shares}股 @ {price:.2f}")
        return trade
    
    def _execute_sell(self, symbol: str, shares: int, price: float, date: datetime) -> Dict:
        """执行卖出"""
        amount = shares * price
        commission = max(amount * self.cost_config["commission"], self.cost_config["min_commission"])
        stamp_tax = amount * self.cost_config["stamp_tax"]
        slippage = amount * self.cost_config["slippage"]
        net_proceeds = amount - commission - stamp_tax - slippage
        
        pos = self.positions[symbol]
        pos.shares -= shares
        pos.available_shares -= shares
        pos.last_update = date
        
        if pos.shares == 0:
            del self.positions[symbol]
        
        self.cash += net_proceeds
        
        trade = {
            "symbol": symbol,
            "side": "SELL",
            "shares": shares,
            "price": price,
            "amount": amount,
            "commission": commission,
            "stamp_tax": stamp_tax,
            "slippage": slippage,
            "net_proceeds": net_proceeds,
            "date": date
        }
        self.trade_history.append(trade)
        
        logger.info(f"{date} | 卖出 {symbol} {shares}股 @ {price:.2f}")
        return trade
    
    def _get_price(self, market_data: pd.DataFrame, symbol: str, field: str) -> float:
        """获取价格"""
        row = market_data[market_data["symbol"] == symbol]
        if row.empty:
            return 0.0
        return row[field].iloc[0]


# ──────────────────────────────────────────────────────────────
# 兼容旧版接口
# ──────────────────────────────────────────────────────────────
class ExecutionEngine(ExecutionEngineV2):
    """向后兼容"""
    pass


# ──────────────────────────────────────────────────────────────
# 测试
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 测试涨跌停判定
    checker = LimitBoardChecker()
    
    test_cases = [
        ("600000", 0.10, "浦发银行", False),   # 主板涨停
        ("300001", 0.20, "特锐德", False),      # 创业板涨停
        ("688001", 0.20, "华兴源创", False),     # 科创板涨停
        ("430001", 0.30, "", False),            # 北交所涨停
        ("600000", 0.05, "ST某某", True),       # ST涨停
    ]
    
    print("=== 涨跌停判定测试 ===")
    for sym, pct, name, is_st in test_cases:
        is_up, is_down = checker.check_limit(sym, pct, name, is_st)
        limit_pct = checker.get_limit_pct(sym, name, is_st)
        print(f"{sym} ({name}): 涨幅{pct:.1%} → 涨停={is_up} (限制{limit_pct:.0%})")
