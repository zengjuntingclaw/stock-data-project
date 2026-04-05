"""
增强版绩效分析器
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np
import empyrical as ep
from loguru import logger

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
    yearly_metrics: Optional[Dict[int, Dict]] = None

class EnhancedPerformanceAnalyzer:
    def __init__(self, risk_free_rate: float = 0.03):
        self.rf = risk_free_rate
        logger.info(f"PerformanceAnalyzer: rf={risk_free_rate}")
    
    def calculate(self, returns: pd.Series, benchmark: Optional[pd.Series] = None,
                  turnover: Optional[pd.Series] = None) -> PerformanceMetrics:
        # 基础指标
        total_ret = (1 + returns).prod() - 1
        ann_ret = ep.annual_return(returns)
        ann_vol = ep.annual_volatility(returns)
        sharpe = ep.sharpe_ratio(returns, risk_free=self.rf)
        max_dd = ep.max_drawdown(returns)
        calmar = ep.calmar_ratio(returns)
        
        # 回撤天数
        dd_days = self._calc_dd_days(returns)
        
        # 相对基准指标
        ir = te = beta = alpha = np.nan
        if benchmark is not None:
            r, b = returns.align(benchmark, join='inner')
            te = (r - b).std() * np.sqrt(252)
            beta, alpha = self._calc_beta_alpha(r, b)
            ir = (r - b).mean() * 252 / te if te else np.nan
        
        return PerformanceMetrics(
            total_return=total_ret, annual_return=ann_ret, annual_volatility=ann_vol,
            sharpe_ratio=sharpe, max_drawdown=max_dd, calmar_ratio=calmar,
            information_ratio=ir, tracking_error=te, beta=beta, alpha=alpha,
            max_dd_days=dd_days, avg_turnover=turnover.mean() if turnover is not None else np.nan
        )
    
    def _calc_dd_days(self, returns: pd.Series) -> int:
        cum = (1 + returns).cumprod()
        running_max = cum.expanding().max()
        dd = (cum - running_max) / running_max
        
        max_days = in_dd = dd_start = 0
        for i, d in enumerate(dd):
            if d < 0 and not in_dd:
                in_dd, dd_start = True, i
            elif d == 0 and in_dd:
                max_days = max(max_days, i - dd_start)
                in_dd = False
        if in_dd:
            max_days = max(max_days, len(dd) - dd_start)
        return max_days
    
    def _calc_beta_alpha(self, returns: pd.Series, benchmark: pd.Series) -> Tuple[float, float]:
        X = np.column_stack([np.ones(len(benchmark)), benchmark])
        beta = np.linalg.lstsq(X, returns.values, rcond=None)[0]
        return beta[1], beta[0] * 252
    
    def report(self, m: PerformanceMetrics) -> str:
        return f"""
{'='*50}
绩效报告
{'='*50}
总收益: {m.total_return:>10.2%}  年化收益: {m.annual_return:>10.2%}
年化波动: {m.annual_volatility:>8.2%}  夏普比率: {m.sharpe_ratio:>10.2f}
最大回撤: {m.max_drawdown:>9.2%}  Calmar: {m.calmar_ratio:>12.2f}
信息比率: {m.information_ratio:>9.2f}  Tracking Error: {m.tracking_error:>7.2%}
Beta: {m.beta:>14.2f}  Alpha: {m.alpha:>13.2%}
最长回撤恢复: {m.max_dd_days:>5}天  平均换手率: {m.avg_turnover:>8.2%}
{'='*50}
"""

class BrinsonAttribution:
    def analyze(self, port_weights: pd.DataFrame, port_returns: pd.Series,
                bench_weights: pd.DataFrame, bench_returns: pd.Series) -> Dict:
        # 行业收益
        port_sector_ret = {}
        bench_sector_ret = {}
        
        for sector in port_weights.index:
            p_syms = port_weights.loc[sector][port_weights.loc[sector] > 0].index
            if len(p_syms) > 0:
                w = port_weights.loc[sector, p_syms] / port_weights.loc[sector, p_syms].sum()
                port_sector_ret[sector] = (port_returns[p_syms] * w).sum()
            
            b_syms = bench_weights.loc[sector][bench_weights.loc[sector] > 0].index
            if len(b_syms) > 0:
                w = bench_weights.loc[sector, b_syms] / bench_weights.loc[sector, b_syms].sum()
                bench_sector_ret[sector] = (bench_returns[b_syms] * w).sum()
        
        alloc = select = interact = 0
        for sector in set(port_weights.index) | set(bench_weights.index):
            wp = port_weights.loc[sector].sum() if sector in port_weights.index else 0
            wb = bench_weights.loc[sector].sum() if sector in bench_weights.index else 0
            rp = port_sector_ret.get(sector, 0)
            rb = bench_sector_ret.get(sector, 0)
            
            alloc += (wp - wb) * rb
            select += wb * (rp - rb)
            interact += (wp - wb) * (rp - rb)
        
        total = alloc + select + interact
        return {
            'total': total, 'allocation': alloc, 'selection': select, 'interaction': interact,
            'alloc_pct': alloc/total if total else 0, 'select_pct': select/total if total else 0
        }
