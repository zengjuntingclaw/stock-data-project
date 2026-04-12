"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import DataRouter, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.tushare_source 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import DataRouter, _SourceAdapter
from typing import Optional
import pandas as pd

class TuShareAdapter(_SourceAdapter):
    """
    TuShare 适配器（向后兼容）
    注册到 DataRouter 中使用
    """
    name = "tushare"

    def __init__(self, token: str = None):
        self._token = token
        self._ts = None
        if token:
            self._import_tushare()

    def _import_tushare(self):
        try:
            import tushare as ts
            self._ts = ts
            if self._token:
                self._ts.set_token(self._token)
                self._ts = ts.pro_api(self._token)
        except ImportError:
            pass

    def is_available(self) -> bool:
        return self._ts is not None

    def fetch(self, symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
        if not self._ts:
            return None
        try:
            df = self._ts.daily(ts_code=symbol, start_date=start.replace("-", ""),
                                end_date=end.replace("-", ""))
            if df is not None and not df.empty:
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
            return df
        except Exception:
            return None

__all__ = ["TuShareAdapter"]
