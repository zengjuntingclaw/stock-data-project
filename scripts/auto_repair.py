"""
[DEPRECATED] 已合并到 scripts.pipeline_data_engine
===================================================
请迁移到:
    from scripts.pipeline_data_engine import QualityEngine, PipelineDataEngine

本文件仅用于向后兼容，将在 v4.0 完全移除。
"""
import warnings
warnings.warn(
    "scripts.auto_repair 已废弃，请使用 scripts.pipeline_data_engine",
    DeprecationWarning, stacklevel=2
)

from scripts.pipeline_data_engine import QualityEngine, PipelineDataEngine
from pathlib import Path
import json
from datetime import datetime

class AutoDataRepair:
    """
    向后兼容的自动数据修复器
    使用 QualityEngine 的修复逻辑
    """
    def __init__(self, engine=None):
        self._engine = engine or PipelineDataEngine()
        self._quality = self._engine.quality
        self._log_path = Path("logs/repair_log.json")
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    def repair(self, df, ts_code: str = None) -> dict:
        """执行自动修复"""
        result = self._quality.validate_and_repair(df, ts_code)
        self._log_repair(result, ts_code)
        return result

    def _log_repair(self, result: dict, ts_code: str):
        """记录修复日志"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "ts_code": ts_code,
            "result": result
        }
        try:
            existing = []
            if self._log_path.exists():
                existing = json.loads(self._log_path.read_text())
            existing.append(log_entry)
            # 保留最近 1000 条
            existing = existing[-1000:]
            self._log_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2))
        except Exception:
            pass

__all__ = ["AutoDataRepair"]
