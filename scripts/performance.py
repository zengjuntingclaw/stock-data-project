"""绩效分析"""
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import empyrical as ep
from loguru import logger
from scripts.data_classes import PerformanceMetrics

class EnhancedPerformanceAnalyzer:
    def __init__(self, risk_free=0.03):
        self.rf = risk_free
    
    def calculate(self, returns: pd.Series, benchmark: Optional[pd.Series] = None, turnover: Optional[pd.Series] = None) -> PerformanceMetrics:
        ann_ret = ep.annual_return(returns)
        ann_vol = ep.annual_volatility(returns)
        ir = te = beta = alpha = np.nan
        if benchmark is not None:
            r, b = returns.align(benchmark, join='inner')
            te = (r - b).std() * np.sqrt(252)
            beta, alpha = self._beta_alpha(r, b)
            ir = (r - b).mean() * 252 / te if te else np.nan
        return PerformanceMetrics(
            total_return=(1 + returns).prod() - 1,
            annual_return=ann_ret,
            annual_volatility=ann_vol,
            sharpe_ratio=ep.sharpe_ratio(returns, self.rf),
            max_drawdown=ep.max_drawdown(returns),
            calmar_ratio=ep.calmar_ratio(returns),
            information_ratio=ir, tracking_error=te, beta=beta, alpha=alpha,
            max_dd_days=self._dd_days(returns),
            avg_turnover=turnover.mean() if turnover is not None else np.nan
        )
    
    def _dd_days(self, r: pd.Series) -> int:
        # 统一使用净值序列 (1+r).cumprod() 计算回撤，避免 r.cumprod() 基准不一致
        cum = (1 + r).cumprod()
        running_max = cum.expanding().max()
        dd = (cum - running_max) / running_max
        if dd.empty:
            return 0
        # 向量化计算：标记回撤区间
        is_dd = dd < 0
        # 计算回撤持续天数
        dd_groups = (~is_dd).cumsum()
        dd_days = is_dd.groupby(dd_groups).sum()
        return int(dd_days.max()) if len(dd_days) > 0 else 0
    
    def _beta_alpha(self, r: pd.Series, b: pd.Series) -> Tuple[float, float]:
        X = np.column_stack([np.ones(len(b)), b])
        beta = np.linalg.lstsq(X, r.values, rcond=None)[0]
        return beta[1], beta[0] * 252
    
    def report(self, m: PerformanceMetrics) -> str:
        return f"\n{'='*50}\n绩效报告\n{'='*50}\n总收益:{m.total_return:>8.2%} 年化收益:{m.annual_return:>8.2%}\n夏普:{m.sharpe_ratio:>10.2f} 最大回撤:{m.max_drawdown:>8.2%}\nIR:{m.information_ratio:>8.2f} TE:{m.tracking_error:>8.2%}\n{'='*50}\n"

class BrinsonAttribution:
    def analyze(self, pw: pd.DataFrame, pr: pd.Series, bw: pd.DataFrame, br: pd.Series) -> Dict:
        alloc = select = interact = 0
        for s in set(pw.index) | set(bw.index):
            wp, wb = pw.loc[s].sum() if s in pw.index else 0, bw.loc[s].sum() if s in bw.index else 0
            rp = pr[pw.loc[s][pw.loc[s] > 0].index].mean() if s in pw.index and len(pw.loc[s][pw.loc[s] > 0]) else 0
            rb = br[bw.loc[s][bw.loc[s] > 0].index].mean() if s in bw.index and len(bw.loc[s][bw.loc[s] > 0]) else 0
            alloc += (wp - wb) * rb; select += wb * (rp - rb); interact += (wp - wb) * (rp - rb)
        total = alloc + select + interact
        return {'total': total, 'allocation': alloc, 'selection': select, 'interaction': interact}
