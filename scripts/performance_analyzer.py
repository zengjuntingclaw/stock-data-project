"""
PerformanceAnalyzer - 统一绩效分析模块
=========================================
合并自：performance.py + enhanced_performance.py

核心功能：
  1. 基础绩效指标（收益、波动、夏普、回撤）
  2. 相对基准分析（IR、TE、Beta、Alpha）
  3. Brinson 归因分析
  4. 年度绩效分解
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from loguru import logger

try:
    import empyrical as ep
    HAS_EMPYRICAL = True
except ImportError:
    HAS_EMPYRICAL = False
    logger.warning("empyrical not installed. Run: pip install empyrical-reloaded")


# ──────────────────────────────────────────────────────────────
# 数据类
# ──────────────────────────────────────────────────────────────
@dataclass
class PerformanceMetrics:
    """绩效指标集"""
    total_return: float
    annual_return: float
    annual_volatility: float
    sharpe_ratio: float
    max_drawdown: float
    calmar_ratio: float
    information_ratio: float = np.nan
    tracking_error: float = np.nan
    beta: float = np.nan
    alpha: float = np.nan
    max_dd_days: int = 0
    avg_turnover: float = np.nan
    yearly_metrics: Optional[Dict[int, Dict]] = None
    
    def to_dict(self) -> Dict:
        return {
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "calmar_ratio": self.calmar_ratio,
            "information_ratio": self.information_ratio,
            "tracking_error": self.tracking_error,
            "beta": self.beta,
            "alpha": self.alpha,
            "max_dd_days": self.max_dd_days,
            "avg_turnover": self.avg_turnover
        }


@dataclass
class YearlyMetrics:
    """年度绩效"""
    year: int
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    turnover: float


# ──────────────────────────────────────────────────────────────
# 绩效分析器
# ──────────────────────────────────────────────────────────────
class PerformanceAnalyzer:
    """
    统一绩效分析器
    
    Example
    -------
    >>> analyzer = PerformanceAnalyzer(risk_free=0.03)
    >>> metrics = analyzer.calculate(returns, benchmark=benchmark_returns)
    >>> print(analyzer.report(metrics))
    """
    
    def __init__(self, risk_free_rate: float = 0.03):
        """
        Parameters
        ----------
        risk_free_rate : float
            无风险利率（年化）
        """
        self.rf = risk_free_rate
        logger.info(f"PerformanceAnalyzer initialized: rf={risk_free_rate:.2%}")
    
    def calculate(self,
                  returns: pd.Series,
                  benchmark: Optional[pd.Series] = None,
                  turnover: Optional[pd.Series] = None,
                  yearly: bool = True) -> PerformanceMetrics:
        """
        计算完整绩效指标
        
        Parameters
        ----------
        returns : Series
            策略日收益率
        benchmark : Series, optional
            基准日收益率
        turnover : Series, optional
            每日换手率
        yearly : bool
            是否计算年度分解
            
        Returns
        -------
        PerformanceMetrics
        """
        if not HAS_EMPYRICAL:
            return self._calculate_manual(returns, benchmark, turnover, yearly)
        
        # 基础指标
        total_ret = (1 + returns).prod() - 1
        ann_ret = ep.annual_return(returns)
        ann_vol = ep.annual_volatility(returns)
        sharpe = ep.sharpe_ratio(returns, risk_free=self.rf)
        max_dd = ep.max_drawdown(returns)
        calmar = ep.calmar_ratio(returns) if max_dd != 0 else np.nan
        
        # 回撤恢复天数
        dd_days = self._calc_dd_days(returns)
        
        # 相对基准指标
        ir = te = beta = alpha = np.nan
        if benchmark is not None:
            r, b = returns.align(benchmark, join="inner")
            if len(r) > 0:
                te = (r - b).std() * np.sqrt(252)
                beta, alpha = self._calc_beta_alpha(r, b)
                ir = (r - b).mean() * 252 / te if te else np.nan
        
        # 年度分解
        yearly_metrics = None
        if yearly:
            yearly_metrics = self._calc_yearly_metrics(returns, turnover)
        
        return PerformanceMetrics(
            total_return=total_ret,
            annual_return=ann_ret,
            annual_volatility=ann_vol,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            calmar_ratio=calmar,
            information_ratio=ir,
            tracking_error=te,
            beta=beta,
            alpha=alpha,
            max_dd_days=dd_days,
            avg_turnover=turnover.mean() if turnover is not None else np.nan,
            yearly_metrics=yearly_metrics
        )
    
    def _calculate_manual(self,
                          returns: pd.Series,
                          benchmark: Optional[pd.Series],
                          turnover: Optional[pd.Series],
                          yearly: bool) -> PerformanceMetrics:
        """手动计算（无 empyrical 时的降级方案）"""
        total_ret = (1 + returns).prod() - 1
        ann_ret = (1 + returns.mean()) ** 252 - 1
        ann_vol = returns.std() * np.sqrt(252)
        sharpe = (ann_ret - self.rf) / ann_vol if ann_vol else np.nan
        
        cum = (1 + returns).cumprod()
        running_max = cum.expanding().max()
        dd = (cum - running_max) / running_max
        max_dd = dd.min()
        
        calmar = ann_ret / abs(max_dd) if max_dd else np.nan
        dd_days = self._calc_dd_days(returns)
        
        ir = te = beta = alpha = np.nan
        if benchmark is not None:
            r, b = returns.align(benchmark, join="inner")
            if len(r) > 0:
                te = (r - b).std() * np.sqrt(252)
                beta, alpha = self._calc_beta_alpha(r, b)
                ir = (r - b).mean() * 252 / te if te else np.nan
        
        yearly_metrics = self._calc_yearly_metrics(returns, turnover) if yearly else None
        
        return PerformanceMetrics(
            total_return=total_ret, annual_return=ann_ret, annual_volatility=ann_vol,
            sharpe_ratio=sharpe, max_drawdown=max_dd, calmar_ratio=calmar,
            information_ratio=ir, tracking_error=te, beta=beta, alpha=alpha,
            max_dd_days=dd_days, avg_turnover=turnover.mean() if turnover else np.nan,
            yearly_metrics=yearly_metrics
        )
    
    def _calc_dd_days(self, returns: pd.Series) -> int:
        """计算最长回撤恢复天数"""
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
        """回归计算 Beta 和 Alpha"""
        X = np.column_stack([np.ones(len(benchmark)), benchmark])
        coeffs = np.linalg.lstsq(X, returns.values, rcond=None)[0]
        return coeffs[1], coeffs[0] * 252
    
    def _calc_yearly_metrics(self, 
                             returns: pd.Series,
                             turnover: Optional[pd.Series]) -> Dict[int, Dict]:
        """计算年度绩效分解"""
        yearly = {}
        for year, group in returns.groupby(returns.index.year):
            ann_ret = (1 + group).prod() - 1
            ann_vol = group.std() * np.sqrt(252)
            sharpe = (ann_ret - self.rf) / ann_vol if ann_vol else np.nan
            
            cum = (1 + group).cumprod()
            running_max = cum.expanding().max()
            dd = (cum - running_max) / running_max
            max_dd = dd.min()
            
            turn = turnover[group.index].mean() if turnover is not None else np.nan
            
            yearly[year] = {
                "return": ann_ret,
                "volatility": ann_vol,
                "sharpe": sharpe,
                "max_drawdown": max_dd,
                "turnover": turn
            }
        return yearly
    
    def report(self, m: PerformanceMetrics) -> str:
        """生成文本报告"""
        lines = [
            "=" * 60,
            "绩效分析报告",
            "=" * 60,
            f"总收益:     {m.total_return:>10.2%}    年化收益:   {m.annual_return:>10.2%}",
            f"年化波动:   {m.annual_volatility:>10.2%}    夏普比率:   {m.sharpe_ratio:>10.2f}",
            f"最大回撤:   {m.max_drawdown:>10.2%}    Calmar比率: {m.calmar_ratio:>10.2f}",
            f"信息比率:   {m.information_ratio:>10.2f}    跟踪误差:   {m.tracking_error:>10.2%}",
            f"Beta:       {m.beta:>10.2f}    Alpha:      {m.alpha:>10.2%}",
            f"最长回撤:   {m.max_dd_days:>10}天   平均换手:   {m.avg_turnover:>10.2%}",
            "=" * 60
        ]
        
        # 年度绩效
        if m.yearly_metrics:
            lines.append("\n年度绩效分解:")
            lines.append("-" * 60)
            lines.append(f"{'年份':>6}  {'收益':>10}  {'波动':>10}  {'夏普':>8}  {'回撤':>10}  {'换手':>8}")
            lines.append("-" * 60)
            for year, ym in sorted(m.yearly_metrics.items()):
                lines.append(
                    f"{year:>6}  {ym['return']:>10.2%}  {ym['volatility']:>10.2%}  "
                    f"{ym['sharpe']:>8.2f}  {ym['max_drawdown']:>10.2%}  {ym['turnover']:>8.2%}"
                )
            lines.append("=" * 60)
        
        return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# Brinson 归因分析
# ──────────────────────────────────────────────────────────────
class BrinsonAttribution:
    """
    Brinson 归因分析器
    
    将组合超额收益分解为：
      - 配置效应（Allocation）：行业权重偏离
      - 选股效应（Selection）：行业内选股能力
      - 交互效应（Interaction）：配置与选股的协同
    """
    
    def analyze(self,
                port_weights: pd.DataFrame,
                port_returns: pd.Series,
                bench_weights: pd.DataFrame,
                bench_returns: pd.Series) -> Dict:
        """
        执行归因分析
        
        Parameters
        ----------
        port_weights : DataFrame
            组合行业权重（index=行业，columns=股票代码）
        port_returns : Series
            股票收益率（index=股票代码）
        bench_weights : DataFrame
            基准行业权重（index=行业，columns=股票代码）
        bench_returns : Series
            基准成分股收益率（index=股票代码）
            
        Returns
        -------
        dict: {
            "total": 总超额收益,
            "allocation": 配置效应,
            "selection": 选股效应,
            "interaction": 交互效应,
            "alloc_pct": 配置贡献占比,
            "select_pct": 选股贡献占比
        }
        """
        # 计算行业收益率
        port_sector_ret = self._calc_sector_returns(port_weights, port_returns)
        bench_sector_ret = self._calc_sector_returns(bench_weights, bench_returns)
        
        # 归因分解
        alloc = select = interact = 0
        sectors = set(port_weights.index) | set(bench_weights.index)
        
        for sector in sectors:
            wp = port_weights.loc[sector].sum() if sector in port_weights.index else 0
            wb = bench_weights.loc[sector].sum() if sector in bench_weights.index else 0
            rp = port_sector_ret.get(sector, 0)
            rb = bench_sector_ret.get(sector, 0)
            
            alloc += (wp - wb) * rb
            select += wb * (rp - rb)
            interact += (wp - wb) * (rp - rb)
        
        total = alloc + select + interact
        
        return {
            "total": total,
            "allocation": alloc,
            "selection": select,
            "interaction": interact,
            "alloc_pct": alloc / total if total else 0,
            "select_pct": select / total if total else 0
        }
    
    def _calc_sector_returns(self, 
                              weights: pd.DataFrame,
                              returns: pd.Series) -> Dict[str, float]:
        """计算行业收益率（加权平均）"""
        sector_ret = {}
        for sector in weights.index:
            syms = weights.loc[sector][weights.loc[sector] > 0].index
            if len(syms) == 0:
                continue
            # 归一化权重
            w = weights.loc[sector, syms] / weights.loc[sector, syms].sum()
            # 行业收益率
            common = set(syms) & set(returns.index)
            if common:
                sector_ret[sector] = (returns[list(common)] * w[list(common)]).sum()
        return sector_ret


# ──────────────────────────────────────────────────────────────
# 向后兼容别名
# ──────────────────────────────────────────────────────────────
EnhancedPerformanceAnalyzer = PerformanceAnalyzer


# ──────────────────────────────────────────────────────────────
# 测试
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 模拟数据测试
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="B")
    returns = pd.Series(np.random.normal(0.001, 0.02, len(dates)), index=dates)
    benchmark = pd.Series(np.random.normal(0.0008, 0.015, len(dates)), index=dates)
    turnover = pd.Series(np.random.uniform(0.05, 0.15, len(dates)), index=dates)
    
    analyzer = PerformanceAnalyzer(risk_free_rate=0.03)
    metrics = analyzer.calculate(returns, benchmark=benchmark, turnover=turnover)
    print(analyzer.report(metrics))
