"""
TuShareSource - TuShare 数据源
==============================
支持 TuShare Pro API（含 token），支持复权数据、停牌/退市处理。
"""
from __future__ import annotations

import os
import time
from typing import Optional

import pandas as pd
from loguru import logger

from scripts.data_fetcher import DataSource, FetchResult
from scripts.exchange_mapping import build_ts_code
from scripts.field_specs import standardize_df


class TuShareSource(DataSource):
    """
    TuShare Pro 数据源

    使用说明：
    1. pip install tushare
    2. 设置环境变量 TUSHARE_TOKEN，或直接传入 token

    优势：
    - 历史数据完整性高（含停牌/复权）
    - 支持涨跌停、退市等特殊状态
    - 接口稳定，频率限制宽松（Pro版）
    """

    name = "tushare"

    def __init__(self, token: Optional[str] = None):
        self._import_tushare()
        # Token 优先级：入参 > 环境变量
        self.token = token or os.environ.get("TUSHARE_TOKEN", "")
        if not self.token:
            raise ValueError(
                "TuShare token 未设置。请设置环境变量 TUSHARE_TOKEN "
                "或传入 token 参数。获取方式：https://tushare.pro/register"
            )
        self.pro = self._init_api()

    def _import_tushare(self):
        try:
            import tushare as ts

            self.ts = ts
        except ImportError:
            raise ImportError(
                "tushare not installed. Run: pip install tushare"
            )

    def _init_api(self):
        """初始化 TuShare Pro API"""
        self.ts.set_token(self.token)
        return self.ts.pro_api()

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def fetch_daily(
        self,
        symbol: str,
        start: str,
        end: str,
        adjust: str = "",
    ) -> FetchResult:
        """
        获取日线数据

        Args:
            symbol:  股票代码，如 '000001.SZ' 或纯数字 '000001'
            start:   开始日期 YYYY-MM-DD
            end:     结束日期 YYYY-MM-DD
            adjust:  复权方式 ''=不复权 'qfq'=前复权 'hfq'=后复权（默认不复权）
        """
        try:
            ts_code = self._normalize_code(symbol)
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
            )
            return self._process_result(df, "daily")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    def fetch_daily_adjusted(
        self,
        symbol: str,
        start: str,
        end: str,
        adjust: str = "qfq",
    ) -> FetchResult:
        """
        获取复权日线数据（复权因子来自 tushare.pro）
        """
        try:
            ts_code = self._normalize_code(symbol)
            fields = (
                "ts_code,trade_date,open,high,low,close,vol,amount,"
                "adjust_flag,turnover,volume_ratio,pct_chg"
            )
            df = self.pro.pro_bar(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                adj=adjust,
                freq="D",
            )
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
            return self._process_result(df, "pro_bar")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    def fetch_suspend(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> FetchResult:
        """
        获取停牌信息（用于识别停牌日）
        """
        try:
            ts_code = self._normalize_code(symbol)
            df = self.pro.suspend(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                trade_date="",
            )
            return self._process_result(df, "suspend")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    def fetch_limit_list(self, trade_date: str) -> FetchResult:
        """
        获取指定日期涨跌停股票列表（TuShare 涨停股数据）
        """
        try:
            df = self.pro.limit_list_d(
                trade_date=trade_date.replace("-", ""),
                # limit_type='' 表示获取全部
            )
            return self._process_result(df, "limit_list_d")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    def fetch_stock_basic(
        self,
        list_status: str = "L",
    ) -> FetchResult:
        """
        获取股票列表

        Args:
            list_status: L=上市 L=终止 P=暂停
        """
        try:
            df = self.pro.stock_basic(
                ts_code="",
                name="",
                exchange="",
                market="",
                list_status=list_status,
            )
            return self._process_result(df, "stock_basic")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    def fetch_stock_list(self) -> FetchResult:
        """兼容 DataFetcher 抽象：获取上市股票列表"""
        return self.fetch_stock_basic(list_status="L")

    def fetch_trade_cal(
        self,
        start: str,
        end: str,
        exchange: str = "SSE",
    ) -> FetchResult:
        """
        获取交易日历

        Args:
            exchange: SSE=上交所 SZSE=深交所 BSE=北交所
        """
        try:
            df = self.pro.trade_cal(
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
                is_open="1",
                exchange=exchange,
            )
            return self._process_result(df, "trade_cal")

        except Exception as e:
            return FetchResult(False, error=str(e), source=self.name)

    # ------------------------------------------------------------------
    # 私有方法
    # ------------------------------------------------------------------

    def _normalize_code(self, symbol: str) -> str:
        """统一代码格式：纯数字 → ts_code（如 '000001' → '000001.SZ'）"""
        return build_ts_code(symbol)

    def _process_result(
        self,
        df: Optional[pd.DataFrame],
        api_name: str,
    ) -> FetchResult:
        """处理 API 返回结果"""
        if df is None or df.empty:
            return FetchResult(False, error=f"{api_name} returned empty data", source=self.name)

        # 列名统一小写
        df.columns = [c.lower() for c in df.columns]

        # 标准化（字段名、类型、默认值）
        df = standardize_df(df)

        return FetchResult(True, data=df, source=self.name)
