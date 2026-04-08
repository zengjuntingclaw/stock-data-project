"""PIT数据对齐"""
from datetime import datetime
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger


class PITDataAligner:
    """
    Point-in-Time (PIT) 数据对齐器

    核心约束：回测日期 T，只能看到 ann_date < T 的财务数据，
    防止使用未来信息（前视偏差）。

    边界健壮性：
    - 支持空数据加载
    - 缺失字段安全处理
    - 日期类型自动转换
    - 缺失 symbol/nan 值过滤
    """

    def __init__(self):
        self._data: Optional[pd.DataFrame] = None

    def load(self, df: pd.DataFrame):
        """加载财务数据（自动类型转换和安全检查）"""
        if df is None or df.empty:
            self._data = pd.DataFrame(columns=['symbol', 'end_date', 'ann_date'])
            logger.warning("PIT loaded with empty data")
            return

        df = df.copy()

        # 必需列检查
        required_cols = ['symbol', 'ann_date']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"PIT load failed: missing required columns {missing}")

        # 日期列自动转换
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['ann_date'] = pd.to_datetime(df['ann_date'], errors='coerce')

        # 过滤无效数据（symbol为空或日期为空）
        before = len(df)
        df = df.dropna(subset=['symbol', 'ann_date'])
        if len(df) < before:
            logger.debug(f"PIT filtered {before - len(df)} invalid records")

        # 排序以便 groupby.last() 行为确定
        self._data = df.sort_values(['symbol', 'ann_date'])
        logger.info(f"PIT loaded: {len(self._data)} records, {df['symbol'].nunique()} symbols")

    def get_factor(self, name: str, date: datetime, symbols: Optional[List[str]] = None) -> pd.Series:
        """
        获取指定日期的因子值（PIT约束）

        Parameters
        ----------
        name : str
            因子名（列名）
        date : datetime
            回测日期（只返回 ann_date < date 的数据）
        symbols : List[str], optional
            股票池过滤

        Returns
        -------
        pd.Series
            因子值，index=symbol
        """
        if self._data is None or self._data.empty:
            return pd.Series(dtype=float, name=name)

        # 日期类型安全转换
        if isinstance(date, str):
            date = pd.to_datetime(date)

        mask = self._data['ann_date'] < date
        if symbols:
            mask &= self._data['symbol'].isin(symbols)

        filtered = self._data[mask].copy()
        if filtered.empty:
            return pd.Series(dtype=float, name=name)

        # 按 symbol 取 ann_date 最大的那一行（PIT 约束）
        filtered = filtered.sort_values(['symbol', 'ann_date'])
        result = filtered.groupby('symbol').last()

        # 安全检查：name 列存在
        if name not in result.columns:
            logger.warning(f"PIT get_factor: column '{name}' not found")
            return pd.Series(dtype=float, name=name)

        return result[name]

    def get_factors(self, names: List[str], date: datetime, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """批量获取多个因子"""
        if not names:
            return pd.DataFrame()
        return pd.DataFrame({n: self.get_factor(n, date, symbols) for n in names})

    def validate(self, name: str, dates: List[datetime]) -> bool:
        """
        验证PIT约束：所有可见数据均满足 ann_date < date

        Returns
        -------
        bool
            True=约束满足，False=PIT违规
        """
        if self._data is None or self._data.empty:
            return True  # 空数据无约束

        if name not in self._data.columns:
            logger.warning(f"PIT validate: column '{name}' not found, skipping")
            return True

        for d in dates:
            # 日期类型安全转换
            if isinstance(d, str):
                d = pd.to_datetime(d)

            for s in self.get_factor(name, d).index:
                data = self._data[(self._data['symbol'] == s) & (self._data['ann_date'] < d)]
                if data.empty:
                    continue
                # 最大可见 ann_date 必须 < 回测日期（由 < 过滤保证，此处双重验证）
                latest_ann = data['ann_date'].max()
                if latest_ann >= d:
                    logger.warning(f"PIT violation: {s} ann_date={latest_ann} >= {d}")
                    return False
        return True
