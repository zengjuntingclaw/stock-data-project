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
    """
    Brinson 归因分析 - 分解主动收益来源

    模型将组合超额收益分解为三个部分：
    1. 资产配置效应（Allocation）：相对于基准的超配/低配带来的收益
    2. 选股效应（Selection）：在行业内部选股带来的收益
    3. 交互效应（Interaction）：配置与选股共同作用的部分

    Parameters
    ----------
    pw : pd.DataFrame
        组合权重，index=行业/sector, columns=资产
    pr : pd.Series
        组合收益率，index=资产
    bw : pd.DataFrame
        基准权重
    br : pd.Series
        基准收益率
    """

    def analyze(self, pw: pd.DataFrame, pr: pd.Series, bw: pd.DataFrame, br: pd.Series) -> Dict:
        alloc = select = interact = 0.0

        # 遍历所有资产（组合和基准的并集）
        all_assets = set(pw.index) | set(bw.index)

        for asset in all_assets:
            # 获取组合和基准的权重
            wp = pw.loc[asset].sum() if asset in pw.index else 0.0
            wb = bw.loc[asset].sum() if asset in bw.index else 0.0

            # 获取收益率（仅计算有权重且非零的资产）
            if asset in pw.index and wp > 0:
                active_mask = pw.loc[asset] > 0
                rp = pr[active_mask.index[active_mask]].mean()
            else:
                rp = 0.0

            if asset in bw.index and wb > 0:
                active_mask = bw.loc[asset] > 0
                rb = br[active_mask.index[active_mask]].mean()
            else:
                rb = 0.0

            # 计算三个归因效应
            weight_diff = wp - wb  # 组合 - 基准 权重差
            return_diff = rp - rb  # 组合 - 基准 收益差

            alloc += weight_diff * rb     # 资产配置效应
            select += wb * return_diff   # 选股效应
            interact += weight_diff * return_diff  # 交互效应

        total = alloc + select + interact
        return {
            'total': total,
            'allocation': alloc,
            'selection': select,
            'interaction': interact
        }
