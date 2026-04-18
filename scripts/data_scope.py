"""
data_scope - 数据范围约束模块
==============================
读取 data/data_scope.yaml，在数据获取入口处校验股票池和时间范围。
文件驱动，修改 YAML 即可调整约束，无需改代码。

使用示例：
    from scripts.data_scope import validate_scope, ScopeConfig

    # 校验单只股票+日期范围
    validate_scope(ts_code="600941.SH", start_date="2024-04-18", end_date="2026-04-17")

    # 读取当前配置
    cfg = ScopeConfig.load()
    print(cfg.allowed_stocks)
    print(cfg.date_range.start_date)
"""

from __future__ import annotations

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional


class ScopeViolationError(Exception):
    """数据范围违规异常"""
    pass


@dataclass
class DateRange:
    start_date: Optional[str] = None   # "YYYY-MM-DD" 或 None（无限制）
    end_date: Optional[str] = None     # "YYYY-MM-DD" 或 None（自动取今天）

    @classmethod
    def from_dict(cls, d: dict) -> "DateRange":
        return cls(
            start_date=d.get("start_date") or None,
            end_date=d.get("end_date") or None,
        )

    def effective_end_date(self) -> str:
        """返回实际结束日期，空则取今天"""
        if self.end_date:
            return self.end_date
        return date.today().isoformat()


@dataclass
class ScopeConfig:
    bypass_scope: bool = False
    allowed_stocks: List[str] = field(default_factory=list)
    date_range: DateRange = field(default_factory=DateRange)

    _instance: Optional["ScopeConfig"] = None
    _loaded: bool = False

    @classmethod
    def _config_path(cls) -> Path:
        base = Path(__file__).parent.parent
        return base / "data" / "data_scope.yaml"

    @classmethod
    def load(cls, force_reload: bool = False) -> "ScopeConfig":
        """加载配置文件（单例模式，force_reload=True 强制重新读取）"""
        if cls._loaded and not force_reload:
            return cls._instance

        path = cls._config_path()
        if not path.exists():
            # 文件不存在，返回无限制的默认配置
            cfg = cls(
                bypass_scope=False,
                allowed_stocks=[],
                date_range=DateRange(),
            )
            cls._instance = cfg
            cls._loaded = True
            return cfg

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        cfg = cls(
            bypass_scope=bool(raw.get("bypass_scope", False)),
            allowed_stocks=[str(s).strip() for s in raw.get("allowed_stocks", [])],
            date_range=DateRange.from_dict(raw.get("date_range", {})),
        )
        cls._instance = cfg
        cls._loaded = True
        return cfg

    @classmethod
    def reload(cls) -> "ScopeConfig":
        """强制重新加载配置（修改 YAML 后调用）"""
        return cls.load(force_reload=True)

    def is_stock_allowed(self, ts_code: str) -> bool:
        """检查股票是否在白名单中"""
        if not self.allowed_stocks:
            return True
        return ts_code in self.allowed_stocks

    def clamp_date_range(self, start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
        """
        将传入的日期范围裁剪到允许范围内。
        返回 (clamped_start, clamped_end)
        """
        eff_end = self.date_range.effective_end_date()

        # 确定起始日期
        if self.date_range.start_date:
            clamped_start = max(
                start_date or "1900-01-01",
                self.date_range.start_date
            )
        else:
            clamped_start = start_date or "1900-01-01"

        # 确定结束日期
        clamped_end = min(
            end_date or eff_end,
            eff_end
        )

        return clamped_start, clamped_end


def validate_scope(
    ts_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    raise_on_violation: bool = True,
) -> dict:
    """
    校验数据获取请求是否在允许范围内。

    参数：
        ts_code: 股票代码（如 "600941.SH"）
        start_date: 起始日期 "YYYY-MM-DD"
        end_date: 结束日期 "YYYY-MM-DD"
        raise_on_violation: True=违规抛异常，False=返回违规信息

    返回：
        dict {
            "allowed": bool,
            "violations": List[str],
            "clamped_start": str,
            "clamped_end": str,
            "message": str,
        }

    异常：
        ScopeViolationError — 当 raise_on_violation=True 且有违规时
    """
    cfg = ScopeConfig.load()

    violations: List[str] = []
    clamped_start = start_date
    clamped_end = end_date

    if cfg.bypass_scope:
        return {
            "allowed": True,
            "violations": [],
            "clamped_start": start_date,
            "clamped_end": end_date,
            "message": "bypass_scope=true，跳过范围检查",
        }

    # 1. 股票白名单检查
    if ts_code and cfg.allowed_stocks:
        if not cfg.is_stock_allowed(ts_code):
            violations.append(
                f"股票 {ts_code} 不在允许的股票池中。"
                f" 允许列表: {cfg.allowed_stocks}"
            )

    # 2. 日期范围检查
    eff_end = cfg.date_range.effective_end_date()

    if cfg.date_range.start_date and start_date:
        if start_date < cfg.date_range.start_date:
            violations.append(
                f"起始日期 {start_date} 早于允许最早日期 {cfg.date_range.start_date}。"
                f" 将自动裁剪到 {cfg.date_range.start_date}"
            )
            clamped_start = cfg.date_range.start_date

    if end_date:
        if end_date > eff_end:
            violations.append(
                f"结束日期 {end_date} 晚于允许最晚日期 {eff_end}。"
                f" 将自动裁剪到 {eff_end}"
            )
            clamped_end = eff_end
    else:
        clamped_end = eff_end

    allowed = len(violations) == 0

    result = {
        "allowed": allowed,
        "violations": violations,
        "clamped_start": clamped_start,
        "clamped_end": clamped_end,
        "message": "通过" if allowed else "; ".join(violations),
    }

    if not allowed and raise_on_violation:
        raise ScopeViolationError(result["message"])

    return result


def validate_batch(
    ts_codes: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    批量校验多个股票。返回一个汇总报告。
    """
    cfg = ScopeConfig.load()
    if cfg.bypass_scope:
        return {"allowed": True, "violations": [], "disallowed_stocks": [], "message": "bypass_scope=true"}

    disallowed = [c for c in ts_codes if not cfg.is_stock_allowed(c)]
    return {
        "allowed": len(disallowed) == 0,
        "disallowed_stocks": disallowed,
        "allowed_stocks": [c for c in ts_codes if c not in disallowed],
        "message": f"不允许的股票: {disallowed}" if disallowed else "全部通过",
    }


# =============================================================================
# 快速验证（供 pipeline_data_engine 调用）
# =============================================================================

def enforce_scope(ts_code: str, start_date: Optional[str], end_date: Optional[str]) -> tuple[str, str]:
    """
    强制校验并返回裁剪后的日期范围。
    违规时抛出 ScopeViolationError。
    相当于: validate_scope() + 提取 clamped_* 日期
    """
    result = validate_scope(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return result["clamped_start"], result["clamped_end"]
