"""
单元测试 - EnhancedDataEngine（统一入口版）
==========================================
测试 PipelineDataEngine（EnhancedDataEngine 别名）的核心功能。
这些测试替代了旧版测试，验证新架构的正确性。
"""
import pytest
import unittest
import unittest.mock as mock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))


# ──────────────────────────────────────────────────────────────────────────────
# Mock 数据
# ──────────────────────────────────────────────────────────────────────────────

def create_mock_daily_data(
    ts_code: str = "000001.SZ",
    start: str = "2024-01-01",
    end: str = "2024-01-10",
    has_issues: bool = False,
) -> pd.DataFrame:
    """创建模拟日线数据"""
    dates = pd.date_range(start, end, freq="B")
    data = {
        "ts_code": [ts_code] * len(dates),
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": np.random.uniform(10, 20, len(dates)),
        "high": np.random.uniform(10, 20, len(dates)),
        "low": np.random.uniform(10, 20, len(dates)),
        "close": np.random.uniform(10, 20, len(dates)),
        "volume": np.random.randint(1000000, 10000000, len(dates)),
        "amount": np.random.uniform(10000000, 100000000, len(dates)),
    }
    df = pd.DataFrame(data)
    if has_issues:
        df.loc[2, "close"] = 0
        df.loc[5, "open"] = np.nan
        df["pct_chg"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1) * 100
        df.loc[7, "pct_chg"] = 25.0
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Test: EnhancedDownloader 向后兼容
# ──────────────────────────────────────────────────────────────────────────────

class TestEnhancedDownloaderCompat(unittest.TestCase):
    """EnhancedDownloader 向后兼容接口测试"""

    def test_downloader_creates_engine(self):
        """EnhancedDownloader 内部委托 PipelineDataEngine"""
        from scripts.enhanced_data_engine import EnhancedDownloader
        downloader = EnhancedDownloader()
        self.assertIsNotNone(downloader._engine)

    def test_download_returns_record(self):
        """EnhancedDownloader.download 返回 DownloadRecord（网络可能失败，返回失败记录也正常）"""
        from scripts.enhanced_data_engine import EnhancedDownloader, DownloadRecord
        downloader = EnhancedDownloader()
        record = downloader.download("000001.SZ", "2024-01-01", "2024-01-05")
        self.assertIsInstance(record, DownloadRecord)
        self.assertEqual(record.ts_code, "000001.SZ")
        # success 可能为 True 或 False（取决于网络）
        self.assertIn("success", dir(record))


# ──────────────────────────────────────────────────────────────────────────────
# Test: EnhancedValidator 向后兼容
# ──────────────────────────────────────────────────────────────────────────────

class TestEnhancedValidatorCompat(unittest.TestCase):
    """EnhancedValidator 向后兼容接口测试"""

    def test_validator_creates_quality_engine(self):
        """EnhancedValidator 内部委托 QualityEngine"""
        from scripts.enhanced_data_engine import EnhancedValidator
        validator = EnhancedValidator()
        self.assertIsNotNone(validator._quality)

    def test_validate_complete_data(self):
        """完整数据通过校验"""
        from scripts.enhanced_data_engine import EnhancedValidator
        validator = EnhancedValidator()
        df = create_mock_daily_data(has_issues=False)
        result = validator.validate(df, "000001.SZ")
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("ok", False))

    def test_validate_invalid_data(self):
        """无效数据被检测"""
        from scripts.enhanced_data_engine import EnhancedValidator
        validator = EnhancedValidator()
        df = create_mock_daily_data(has_issues=True)
        result = validator.validate(df, "000001.SZ")
        self.assertFalse(result.get("ok", True))

    def test_validate_and_fix(self):
        """validate_and_fix 返回 DataFrame"""
        from scripts.enhanced_data_engine import EnhancedValidator
        validator = EnhancedValidator()
        df = create_mock_daily_data(has_issues=True)
        fixed = validator.validate_and_fix(df)
        self.assertIsInstance(fixed, pd.DataFrame)


# ──────────────────────────────────────────────────────────────────────────────
# Test: DownloadRecord 兼容性
# ──────────────────────────────────────────────────────────────────────────────

class TestDownloadRecordCompat(unittest.TestCase):
    """DownloadRecord 向后兼容测试"""

    def test_record_to_dict(self):
        """DownloadRecord.to_dict 正常工作"""
        from scripts.enhanced_data_engine import DownloadRecord
        record = DownloadRecord(
            ts_code="000001.SZ",
            start="2024-01-01",
            end="2024-01-10",
            record_count=100,
            success=True,
            error=""
        )
        data = record.to_dict()
        self.assertEqual(data["ts_code"], "000001.SZ")
        self.assertEqual(data["record_count"], 100)
        self.assertTrue(data["success"])

    def test_record_timestamp(self):
        """DownloadRecord 有 timestamp 属性"""
        from scripts.enhanced_data_engine import DownloadRecord
        record = DownloadRecord(
            ts_code="000001.SZ",
            start="2024-01-01",
            end="2024-01-10",
            record_count=50,
            success=True,
        )
        self.assertIsInstance(record.timestamp, datetime)


# ──────────────────────────────────────────────────────────────────────────────
# Test: PipelineDataEngine 核心 API
# ──────────────────────────────────────────────────────────────────────────────

class TestPipelineDataEngine(unittest.TestCase):
    """PipelineDataEngine 核心 API 测试"""

    def setUp(self):
        from scripts.pipeline_data_engine import PipelineDataEngine
        self.engine = PipelineDataEngine()

    def test_get_status(self):
        """get_status 返回系统状态"""
        status = self.engine.get_status()
        self.assertIsInstance(status, dict)

    def test_get_source_stats(self):
        """get_source_stats 返回数据源统计"""
        stats = self.engine.get_source_stats()
        self.assertIsInstance(stats, dict)

    def test_runtime_controller_via_engine(self):
        """引擎的 runtime 属性是 RuntimeController"""
        from scripts.pipeline_data_engine import RuntimeController
        self.assertIsInstance(self.engine.runtime, RuntimeController)

    def test_quality_engine_via_engine(self):
        """引擎内部有 quality 属性"""
        from scripts.pipeline_data_engine import QualityEngine
        self.assertIsInstance(self.engine.quality, QualityEngine)

    def test_storage_manager_via_engine(self):
        """引擎内部有 storage 属性"""
        from scripts.pipeline_data_engine import StorageManager
        self.assertIsInstance(self.engine.storage, StorageManager)


# ──────────────────────────────────────────────────────────────────────────────
# Test: DataRouter 多数据源
# ──────────────────────────────────────────────────────────────────────────────

class TestDataRouterMultiSource(unittest.TestCase):
    """多数据源切换测试"""

    def test_default_sources(self):
        """默认数据源已注册"""
        from scripts.pipeline_data_engine import DataRouter
        router = DataRouter()
        self.assertIn("akshare", router._sources)
        self.assertIn("baostock", router._sources)

    def test_source_health_status(self):
        """数据源健康状态可查询"""
        from scripts.pipeline_data_engine import DataRouter
        router = DataRouter()
        health = router.get_health_report()
        self.assertIsInstance(health, dict)


# ──────────────────────────────────────────────────────────────────────────────
# Test: QualityEngine 质量检测
# ──────────────────────────────────────────────────────────────────────────────

class TestQualityEngineIntegrity(unittest.TestCase):
    """数据完整性校验测试"""

    def test_detect_invalid_price(self):
        """检测价格为0的无效数据"""
        from scripts.pipeline_data_engine import QualityEngine
        engine = QualityEngine()
        df = create_mock_daily_data(has_issues=True)
        result = engine.validate(df, "000001.SZ")
        self.assertFalse(result.get("ok", True))

    def test_detect_missing_columns(self):
        """检测缺失必需列"""
        from scripts.pipeline_data_engine import QualityEngine
        engine = QualityEngine()
        df = pd.DataFrame({
            "trade_date": ["2024-01-01"] * 5,
            "close": [10, 11, 12, 13, 14],
        })
        result = engine.validate(df, "000001.SZ")
        self.assertFalse(result.get("ok", True))

    def test_detect_gaps_returns_list(self):
        """detect_gaps 返回列表"""
        from scripts.pipeline_data_engine import QualityEngine
        engine = QualityEngine()
        df = create_mock_daily_data()
        gaps = engine.detect_gaps(df, "000001.SZ")
        self.assertIsInstance(gaps, list)


# ──────────────────────────────────────────────────────────────────────────────
# Test: 断点续传（DownloadRecord）
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckpointCompat(unittest.TestCase):
    """断点续传相关 DownloadRecord 测试"""

    def test_checkpoint_record(self):
        """断点记录结构正确"""
        from scripts.enhanced_data_engine import DownloadRecord
        record = DownloadRecord(
            ts_code="000001.SZ",
            start="2024-01-01",
            end="2024-01-10",
            record_count=100,
            success=True,
        )
        self.assertEqual(record.ts_code, "000001.SZ")
        self.assertEqual(record.record_count, 100)


# ──────────────────────────────────────────────────────────────────────────────
# Test: create_pipeline_engine 工厂函数
# ──────────────────────────────────────────────────────────────────────────────

class TestFactoryFunction(unittest.TestCase):
    """create_pipeline_engine 工厂函数测试"""

    def test_create_pipeline_engine(self):
        """create_pipeline_engine 返回 PipelineDataEngine 实例"""
        from scripts.pipeline_data_engine import create_pipeline_engine, PipelineDataEngine
        engine = create_pipeline_engine()
        self.assertIsInstance(engine, PipelineDataEngine)


if __name__ == "__main__":
    unittest.main()
