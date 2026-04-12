"""数据验证模块

从 data_engine.py 拆分出的数据验证和交叉验证逻辑。
包含：基础校验、停牌/退市检测、缺失值自动修复、多源交叉验证。
"""
import numpy as np
import pandas as pd
from loguru import logger
from pathlib import Path
from typing import Dict, List, Optional, Callable
from datetime import datetime as _dt

def _get_now() -> _dt:
    """获取当前时间（支持单测 mock）。"""
    return _dt.now()

try:
    from scripts.data_engine import DEFAULT_ADJ_TOLERANCE, DEFAULT_SAMPLE_RATIO
except (ModuleNotFoundError, ImportError):
    from data_engine import DEFAULT_ADJ_TOLERANCE, DEFAULT_SAMPLE_RATIO


def _detect_limit(code: str) -> float:
    """根据股票代码返回涨跌停幅度（基础版，仅用于数据验证）
    
    注意：这是简化版，不含时间维度。
    完整涨跌停逻辑请使用 AShareTradingRules.get_price_limit()
    """
    c = str(code).zfill(6)
    if c.startswith("688"):
        return 0.20
    elif c.startswith("30"):
        return 0.20
    elif c.startswith("4") or c.startswith("8"):
        return 0.30
    else:
        return 0.10


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
            max_pct = codes_6.apply(_detect_limit) * 100
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
            ts = _get_now().strftime("%Y-%m-%d %H:%M:%S")
            for _, row in df.iterrows():
                f.write(
                    f"[{ts}] CROSS_VALIDATE_ERROR | "
                    f"ts_code={row['ts_code']} date={row['trade_date']} "
                    f"price_p={row['close_p']:.4f} price_s={row['close_s']:.4f} "
                    f"diff_pct={row['diff_pct']:.4f}\n"
                )


# =============================================================================
# 增强校验：停牌/退市检测、缺失值修复、详细报告
# =============================================================================

# 各板块涨跌停阈值（与 _detect_limit 保持一致）
_LIMIT_THRESHOLDS = {
    "688": 0.20,   # 科创板
    "30": 0.20,    # 创业板
    "4": 0.30,     # 北交所旧码段
    "8": 0.30,     # 北交所新码段
}


def detect_limit_threshold(code: str) -> float:
    """根据股票代码返回涨跌停阈值（基础版）"""
    c = str(code).zfill(6)
    for prefix, th in _LIMIT_THRESHOLDS.items():
        if c.startswith(prefix):
            return th
    return 0.10  # 主板


class EnhancedValidator:
    """
    增强数据验证器

    功能：
    - 缺失值检测与自动修复（前向填充）
    - 停牌/退市数据标记
    - 涨跌停数据校验
    - 字段完整性校验
    - 详细验证报告生成
    """

    def __init__(
        self,
        error_log_path: str = "logs/data_quality.log",
        suspended_codes: Optional[List[str]] = None,
    ):
        self.error_log = Path(error_log_path)
        self.error_log.parent.mkdir(parents=True, exist_ok=True)
        self.suspended_codes: set = set(suspended_codes or [])

    # -------------------------------------------------------------------------
    # 核心校验
    # -------------------------------------------------------------------------

    def validate(
        self,
        df: pd.DataFrame,
        ts_code: Optional[str] = None,
    ) -> Dict:
        """
        完整数据校验（返回详细报告）

        Returns:
            {
                "ok": bool,
                "total_rows": int,
                "issues": [
                    {"type": str, "severity": str, "detail": dict},
                    ...
                ],
                "suspended_dates": [str, ...],
                "zero_price_dates": [str, ...],
                "missing_fields": [str, ...],
            }
        """
        issues: List[Dict] = []
        if df.empty:
            return {"ok": True, "total_rows": 0, "issues": [], "suspended_dates": [], "zero_price_dates": [], "missing_fields": []}

        # 1. 字段完整性
        required = {"trade_date", "open", "high", "low", "close", "volume"}
        present = set(df.columns)
        missing_fields = list(required - present)
        if missing_fields:
            issues.append({
                "type": "missing_fields",
                "severity": "error",
                "detail": {"fields": missing_fields},
            })

        # 2. 缺失值
        null_counts = df.isnull().sum()
        nulls = null_counts[null_counts > 0]
        if not nulls.empty:
            issues.append({
                "type": "null_values",
                "severity": "warning",
                "detail": nulls.to_dict(),
            })

        # 3. 价格 <= 0
        price_cols = ["open", "high", "low", "close"]
        zero_price_dates: List[str] = []
        for col in price_cols:
            if col not in df.columns:
                continue
            bad = df[df[col] <= 0]
            if not bad.empty:
                dates = bad["trade_date"].astype(str).tolist()
                zero_price_dates.extend(dates)
                issues.append({
                    "type": "invalid_price",
                    "severity": "error",
                    "detail": {"field": col, "count": len(bad), "dates": dates[:5]},
                })

        # 4. 涨跌停异常（按板块区分）
        if "pct_chg" in df.columns and "ts_code" in df.columns:
            codes = df["ts_code"].astype(str).str[:6]
            thresholds = codes.apply(detect_limit_threshold) * 100
            abnormal_mask = df["pct_chg"].abs() > thresholds
            abnormal = df[abnormal_mask]
            if not abnormal.empty:
                issues.append({
                    "type": "abnormal_pct_chg",
                    "severity": "warning",
                    "detail": {
                        "count": len(abnormal),
                        "samples": abnormal[["ts_code", "trade_date", "pct_chg"]].head(5).to_dict("records"),
                    },
                })

        # 5. 停牌日检测（成交量=0 且 前一日收盘 = 当日收盘，视为停牌嫌疑）
        suspended_dates: List[str] = []
        if "volume" in df.columns and "close" in df.columns:
            df_sorted = df.sort_values("trade_date").copy()
            df_sorted["prev_close"] = df_sorted["close"].shift(1)
            # 停牌特征：成交量=0 或 close=0 且 前后收盘价相同
            suspended = df_sorted[
                ((df_sorted["volume"] == 0) | (df_sorted["close"] == 0)) &
                (df_sorted["close"] == df_sorted["prev_close"])
            ]
            if not suspended.empty:
                suspended_dates = suspended["trade_date"].astype(str).tolist()
                issues.append({
                    "type": "suspended_suspect",
                    "severity": "info",
                    "detail": {"count": len(suspended_dates), "dates": suspended_dates[:10]},
                })

        # 6. 成交额/成交量 极度异常（单日成交额 > 10亿 或 成交量 < 100）
        if "amount" in df.columns:
            huge = df[df["amount"] > 1e9]
            if not huge.empty:
                issues.append({
                    "type": "unusual_amount",
                    "severity": "warning",
                    "detail": {"count": len(huge), "dates": huge["trade_date"].astype(str).tolist()[:5]},
                })
        if "volume" in df.columns:
            tiny = df[df["volume"] < 100]
            if not tiny.empty:
                issues.append({
                    "type": "tiny_volume",
                    "severity": "info",
                    "detail": {"count": len(tiny), "dates": tiny["trade_date"].astype(str).tolist()[:5]},
                })

        return {
            "ok": all(i["severity"] != "error" for i in issues),
            "total_rows": len(df),
            "issues": issues,
            "suspended_dates": suspended_dates,
            "zero_price_dates": list(set(zero_price_dates)),
            "missing_fields": missing_fields,
        }

    def validate_and_fix(
        self,
        df: pd.DataFrame,
        forward_fill: bool = True,
        drop_duplicates: bool = True,
    ) -> pd.DataFrame:
        """
        校验并自动修复常见数据问题（返回修复后的 DataFrame）

        修复策略：
        - 缺失值：前向填充（close 缺失时用前一日 close）
        - 重复行：按 trade_date 去重，保留最后一条
        - 零价格：标记为 NaN（不填充，防止污染）
        """
        if df.empty:
            return df.copy()

        df = df.copy()

        # 去重
        if drop_duplicates and "trade_date" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["trade_date"], keep="last")
            if len(df) < before:
                logger.info(f"[EnhancedValidator] 去重 {before} → {len(df)} 行")

        # 按日期排序
        if "trade_date" in df.columns:
            df = df.sort_values("trade_date").reset_index(drop=True)

        # 前向填充价格类字段
        if forward_fill:
            price_cols = ["open", "high", "low", "close", "amount"]
            filled = df[price_cols].ffill()
            null_before = df[price_cols].isnull().sum().sum()
            df[price_cols] = filled
            null_after = df[price_cols].isnull().sum().sum()
            if null_after < null_before:
                logger.info(f"[EnhancedValidator] 前向填充缺失值 {null_before} → {null_after}")

        # 零价格 → NaN（不填，防止造假）
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col in df.columns:
                df.loc[df[col] <= 0, col] = np.nan

        return df

    def detect_suspended(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        标记停牌日（返回带 suspend_flag 列的 DataFrame）

        停牌判定逻辑（参考 TuShare suspend 表）：
        - 成交量 == 0 且 收盘价 == 前收盘
        """
        if df.empty or "trade_date" not in df.columns:
            return df

        df = df.sort_values("trade_date").copy()
        df["prev_close"] = df["close"].shift(1)
        df["prev_volume"] = df["volume"].shift(1)

        # 停牌：当日无成交 且 收盘价等于前收盘
        df["suspend_flag"] = (
            ((df["volume"] == 0) | (df["close"] == 0)) &
            (df["close"] == df["prev_close"])
        ).astype(int)

        # 清理临时列
        df = df.drop(columns=["prev_close", "prev_volume"], errors="ignore")
        return df

    def get_quality_score(self, validation_result: Dict) -> float:
        """
        计算数据质量评分（0~100）

        扣分项：error=严重 -30分/条，warning=-10分/条，info=-2分/条
        """
        score = 100.0
        for issue in validation_result.get("issues", []):
            sev = issue.get("severity", "info")
            delta = {"error": 30, "warning": 10, "info": 2}.get(sev, 5)
            score -= delta
        return max(0.0, score)

    def log_validation_report(self, result: Dict, ts_code: str = ""):
        """将验证报告追加到日志文件"""
        with open(self.error_log, "a", encoding="utf-8") as f:
            ts = _get_now().strftime("%Y-%m-%d %H:%M:%S")
            prefix = f"[{ts}] VALIDATION"
            if ts_code:
                prefix += f" ts_code={ts_code}"
            f.write(f"{prefix} score={self.get_quality_score(result):.1f}\n")
            for issue in result.get("issues", []):
                f.write(
                    f"  [{issue['severity'].upper():8s}] {issue['type']}: "
                    f"{issue['detail']}\n"
                )
            if result.get("suspended_dates"):
                f.write(f"  [INFO] suspended_suspect dates: {result['suspended_dates'][:10]}\n")

