"""数据验证模块

从 data_engine.py 拆分出的数据验证和交叉验证逻辑。
"""
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict
from datetime import datetime

from scripts.data_engine import detect_limit, DEFAULT_ADJ_TOLERANCE, DEFAULT_SAMPLE_RATIO


class DataValidator:
    """
    多源交叉验证器
    
    - 按 5% 比例随机抽样
    - 对比主数据源与备用源
    - 差异 > 0.5% 记录到 data_error.log
    """

    def __init__(self, error_log_path: str = "logs/data_error.log"):
        self.error_log = Path(error_log_path)
        self.error_log.parent.mkdir(parents=True, exist_ok=True)

    def validate(self, df: pd.DataFrame) -> Dict:
        """
        基础数据验证
        
        检查：价格 <= 0、涨跌幅异常(>20% 按板块区分)、成交量 < 0
        """
        issues = []
        if df.empty:
            return {"ok": True, "issues": [], "count": 0}

        # 价格 <= 0
        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                continue
            bad = df[df[col] <= 0]
            if len(bad) > 0:
                issues.append({
                    "type": "invalid_price",
                    "field": col,
                    "count": len(bad),
                    "sample": bad[["ts_code", "trade_date", col]].head(3).to_dict("records")
                })

        # 涨跌幅异常（按板块区分：主板>10%为异常，创业板/科创板>20%，北交所>30%）
        abnormal = pd.DataFrame()
        if "pct_chg" in df.columns and "ts_code" in df.columns:
            # 向量化计算每只股票的涨跌停阈值
            codes_6 = df["ts_code"].astype(str).str[:6]
            max_pct = codes_6.apply(detect_limit) * 100
            abnormal = df[df["pct_chg"].abs() > max_pct]
            if len(abnormal) > 0:
                issues.append({
                    "type": "abnormal_change",
                    "count": len(abnormal),
                    "sample": abnormal[["ts_code", "trade_date", "pct_chg"]].head(3).to_dict("records")
                })

        # 成交量 < 0
        if "volume" in df.columns:
            bad_vol = df[df["volume"] < 0]
            if len(bad_vol) > 0:
                issues.append({"type": "invalid_volume", "count": len(bad_vol)})

        return {"ok": len(issues) == 0, "issues": issues, "count": len(df)}

    def cross_validate(self,
                       primary: pd.DataFrame,
                       secondary: pd.DataFrame,
                       tolerance: float = DEFAULT_ADJ_TOLERANCE,
                       sample_ratio: float = DEFAULT_SAMPLE_RATIO) -> Dict:
        """
        多源交叉验证
        
        按 5% 比例随机抽样，比对收盘价，差异 > tolerance 记录到日志
        """
        if primary.empty or secondary.empty:
            return {"status": "skipped", "reason": "empty_data"}

        # 对齐
        key_cols = ["ts_code", "trade_date"]
        merged = primary.merge(
            secondary, on=key_cols, suffixes=("_p", "_s"), how="inner"
        )
        if len(merged) == 0:
            return {"status": "skipped", "reason": "no_overlap"}

        # 随机抽样 5%
        n_sample = max(1, int(len(merged) * sample_ratio))
        sample = merged.sample(n=min(n_sample, len(merged)), random_state=42)

        # 计算差异
        diff = (sample["close_p"] - sample["close_s"]).abs()
        diff_pct = diff / sample["close_p"].replace(0, np.nan)

        mismatches = sample[diff_pct > tolerance]

        result = {
            "status": "ok" if len(mismatches) == 0 else "mismatch",
            "total_checked": len(sample),
            "mismatch_count": len(mismatches),
            "mismatch_ratio": len(mismatches) / len(sample),
            "max_diff_pct": diff_pct.max(),
        }

        # 写日志
        if len(mismatches) > 0:
            self._log_errors(mismatches[["ts_code", "trade_date", "close_p", "close_s"]].assign(
                diff_pct=diff_pct[mismatches.index]
            ))

        return result

    def _log_errors(self, df: pd.DataFrame):
        with open(self.error_log, "a", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df.iterrows():
                f.write(
                    f"[{ts}] CROSS_VALIDATE_ERROR | "
                    f"ts_code={row['ts_code']} date={row['trade_date']} "
                    f"price_p={row['close_p']:.4f} price_s={row['close_s']:.4f} "
                    f"diff_pct={row['diff_pct']:.4f}\n"
                )
