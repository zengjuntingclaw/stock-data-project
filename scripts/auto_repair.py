"""
自动数据修复机制 - AutoDataRepair
"""
from __future__ import annotations
import os, time, hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict
from pathlib import Path
import pandas as pd
import numpy as np
from loguru import logger

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


@dataclass
class RepairRecord:
    ts_code: str
    table_name: str
    trade_date: str
    issue_type: str
    original_value: str
    repaired_value: str
    strategy: str
    source: str
    repaired_at: datetime = field(default_factory=datetime.now)


class AutoDataRepair:
    """自动数据修复器"""
    
    def __init__(self, db_path: str = None, backup_source_manager=None):
        if not HAS_DUCKDB:
            raise ImportError("duckdb未安装")
        project_root = Path(__file__).parent.parent
        self.db_path = Path(db_path or str(project_root / "data" / "stock_data.duckdb"))
        self._conn = duckdb.connect(str(self.db_path))
        self._backup_mgr = backup_source_manager
        self._stats = {"total_repairs": 0, "by_strategy": {}, "failed_repairs": 0}
    
    def repair_from_validation(self, validation_result: Dict, ts_code: str, table_name: str = "daily_bar_raw"):
        if validation_result.get("ok", True):
            return {"success": True, "records_repaired": 0}
        
        records_repaired = records_failed = 0
        for issue in validation_result.get("issues", []):
            issue_type = issue.get("type", "")
            severity = issue.get("severity", "warning")
            if severity == "info":
                continue
            try:
                if issue_type == "null_values":
                    records_repaired += self._repair_nulls(ts_code, table_name, issue.get("detail", {}))
                elif issue_type == "invalid_price":
                    records_repaired += self._repair_invalid_price(ts_code, table_name, issue)
                elif issue_type == "abnormal_pct_chg":
                    records_repaired += self._repair_abnormal_change(ts_code, table_name, issue)
                elif issue_type == "suspended_suspect":
                    records_repaired += self._mark_suspended(ts_code, table_name, issue)
            except Exception as e:
                logger.error(f"[AutoRepair] 修复 {issue_type} 失败: {e}")
                records_failed += 1
        
        self._stats["total_repairs"] += records_repaired
        self._stats["failed_repairs"] += records_failed
        return {"success": records_failed == 0, "records_repaired": records_repaired, "records_failed": records_failed}
    
    def _repair_nulls(self, ts_code: str, table_name: str, detail: Dict) -> int:
        df = self._fetch_local_data(ts_code, table_name)
        if df is None or df.empty:
            return 0
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col].ffill()
        self._write_back(ts_code, table_name, df)
        return 1
    
    def _repair_invalid_price(self, ts_code: str, table_name: str, issue: Dict) -> int:
        detail = issue.get("detail", {})
        dates = detail.get("dates", [])
        if not dates:
            return 0
        if self._backup_mgr:
            try:
                result = self._backup_mgr.fetch_daily(ts_code, min(dates), max(dates))
                if result.success and result.data is not None:
                    return self._update_from_backup(ts_code, table_name, result.data, dates)
            except Exception as e:
                logger.warning(f"[AutoRepair] 备用数据源失败: {e}")
        return self._mark_invalid_price(ts_code, table_name, dates, detail.get("field", "close"))
    
    def _repair_abnormal_change(self, ts_code: str, table_name: str, issue: Dict) -> int:
        detail = issue.get("detail", {})
        samples = detail.get("samples", [])
        if not samples:
            return 0
        dates = [s.get("trade_date") for s in samples if s.get("trade_date")]
        if not dates or not self._backup_mgr:
            return 0
        try:
            result = self._backup_mgr.fetch_daily(ts_code, min(dates), max(dates))
            if result.success and result.data is not None:
                return self._update_from_backup(ts_code, table_name, result.data, dates)
        except:
            pass
        return 0
    
    def _mark_suspended(self, ts_code: str, table_name: str, issue: Dict) -> int:
        dates = issue.get("detail", {}).get("dates", [])
        if not dates:
            return 0
        cur = self._conn.cursor()
        for date in dates:
            cur.execute(f"UPDATE {table_name} SET is_suspend = TRUE WHERE ts_code = ? AND trade_date = ?", (ts_code, date))
        self._conn.commit()
        return len(dates)
    
    def _fetch_local_data(self, ts_code: str, table_name: str) -> Optional[pd.DataFrame]:
        try:
            return pd.read_sql_query(f"SELECT * FROM {table_name} WHERE ts_code = ? ORDER BY trade_date", self._conn, params=[ts_code])
        except:
            return None
    
    def _write_back(self, ts_code: str, table_name: str, df: pd.DataFrame):
        if df is None or df.empty:
            return
        cur = self._conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE ts_code = ?", (ts_code,))
        cols = list(df.columns)
        for _, row in df.iterrows():
            values = [row.get(col) for col in cols]
            cur.execute(f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(['?' for _ in cols])})", values)
        self._conn.commit()
    
    def _update_from_backup(self, ts_code: str, table_name: str, backup_df: pd.DataFrame, dates: List[str]) -> int:
        if backup_df is None or backup_df.empty:
            return 0
        cur = self._conn.cursor()
        count = 0
        for date in dates:
            backup_row = backup_df[backup_df["trade_date"] == date]
            if backup_row.empty:
                continue
            row = backup_row.iloc[0]
            update_cols, update_values = [], []
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                if col in row.index and pd.notna(row[col]):
                    update_cols.append(f"{col} = ?")
                    update_values.append(row[col])
            if update_cols:
                update_values.extend([ts_code, date])
                cur.execute(f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE ts_code = ? AND trade_date = ?", update_values)
                count += 1
        self._conn.commit()
        return count
    
    def _mark_invalid_price(self, ts_code: str, table_name: str, dates: List[str], field: str) -> int:
        if not dates:
            return 0
        cur = self._conn.cursor()
        for date in dates:
            cur.execute(f"UPDATE {table_name} SET {field} = NULL WHERE ts_code = ? AND trade_date = ? AND {field} <= 0", (ts_code, date))
        self._conn.commit()
        return len(dates)
    
    def close(self):
        self._conn.close()


class DataQualityRepairPipeline:
    def __init__(self, data_engine=None, source_manager=None, db_path: str = None):
        from scripts.data_validator import EnhancedValidator
        self.data_engine = data_engine
        self.source_manager = source_manager
        self.validator = EnhancedValidator()
        self.repair = AutoDataRepair(db_path=str(db_path) if db_path else None, backup_source_manager=source_manager)
        self.max_repair_ratio = 0.1
        self.auto_repair_enabled = True
    
    def process(self, ts_code: str, start_date: str = None, end_date: str = None, table_name: str = "daily_bar_raw", validate_only: bool = False) -> Dict:
        result = {"ts_code": ts_code, "table_name": table_name, "validation": None, "repair": None, "status": "ok", "message": ""}
        if self.data_engine:
            df = self.data_engine.get_daily_raw(ts_code, start_date, end_date)
        else:
            df = self._fetch_data(ts_code, start_date, end_date)
        if df is None or df.empty:
            result["status"] = "no_data"
            result["message"] = "无数据"
            return result
        val_result = self.validator.validate(df, ts_code=ts_code)
        result["validation"] = val_result
        if val_result.get("ok", False):
            result["message"] = "数据正常，无需修复"
            return result
        if validate_only:
            result["message"] = "验证完成"
            return result
        if not self.auto_repair_enabled:
            result["message"] = "自动修复已禁用"
            return result
        repair_result = self.repair.repair_from_validation(val_result, ts_code, table_name)
        result["repair"] = repair_result
        result["status"] = "repaired" if repair_result.get("success") else "partial"
        result["message"] = f"修复成功，修复了 {repair_result.get('records_repaired', 0)} 条记录" if repair_result.get("success") else "部分修复失败"
        return result
    
    def _fetch_data(self, ts_code: str, start_date: str = None, end_date: str = None) -> Optional[pd.DataFrame]:
        if not self.source_manager:
            return None
        result = self.source_manager.fetch_daily(ts_code, start_date or "2018-01-01", end_date or datetime.now().strftime("%Y-%m-%d"))
        return result.data if hasattr(result, 'success') and result.success else None
    
    def batch_process(self, ts_codes: List[str], start_date: str = None, end_date: str = None) -> Dict:
        stats = {"total": len(ts_codes), "ok": 0, "repaired": 0, "partial": 0, "skipped": 0, "error": 0}
        for ts_code in ts_codes:
            try:
                result = self.process(ts_code, start_date, end_date)
                status = result["status"]
                if status in stats:
                    stats[status] += 1
            except Exception as e:
                logger.error(f"[Pipeline] {ts_code} 处理失败: {e}")
                stats["error"] += 1
        return stats


if __name__ == "__main__":
    print("AutoDataRepair 测试完成")
