"""
单元测试 - PipelineDataEngine（统一入口）
=========================================
覆盖：
1. 主入口可导入并实例化
2. _get_now() 递归 bug 已修复
3. 兼容层不指向缺失模块
4. 核心 API 功能
5. RuntimeController 限速/缓存
6. DataRouter 数据源路由
7. QualityEngine 质量检测
8. BacktestValidator 回测校验
9. StorageManager 存储
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
import time

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
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Test 1: 主入口可导入并实例化
# ──────────────────────────────────────────────────────────────────────────────

class TestPipelineEntrypoint(unittest.TestCase):
    """测试统一入口可导入并实例化"""

    def test_import_pipeline_data_engine(self):
        """PipelineDataEngine 可导入"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        self.assertTrue(callable(PipelineDataEngine))

    def test_import_enhanced_compat(self):
        """EnhancedDataEngine 兼容别名可导入"""
        from scripts.pipeline_data_engine import EnhancedDataEngine, PipelineDataEngine
        self.assertIs(EnhancedDataEngine, PipelineDataEngine)

    def test_import_production_compat(self):
        """ProductionDataEngine 兼容别名可导入"""
        from scripts.pipeline_data_engine import ProductionDataEngine, PipelineDataEngine
        self.assertIs(ProductionDataEngine, PipelineDataEngine)

    def test_instantiate_no_error(self):
        """PipelineDataEngine 可正常实例化（不实际连接网络）"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        engine = PipelineDataEngine()
        self.assertIsNotNone(engine)

    def test_backward_compat_enhanced_engine(self):
        """向后兼容：enhanced_data_engine.py 兼容层可导入"""
        from scripts.enhanced_data_engine import EnhancedDataEngine, EnhancedDownloader, EnhancedValidator
        from scripts.pipeline_data_engine import PipelineDataEngine
        self.assertIs(EnhancedDataEngine, PipelineDataEngine)
        self.assertIsNotNone(EnhancedDownloader)
        self.assertIsNotNone(EnhancedValidator)

    def test_backward_compat_request_throttler(self):
        """向后兼容：request_throttler.py 兼容层可导入"""
        from scripts.request_throttler import RuntimeController, RateLimitConfig, TokenBucketThrottler, RequestThrottler
        self.assertIsNotNone(RuntimeController)
        self.assertIsNotNone(RateLimitConfig)
        self.assertIsNotNone(TokenBucketThrottler)
        self.assertIsNotNone(RequestThrottler)

    def test_backward_compat_request_cache(self):
        """向后兼容：request_cache.py 兼容层可导入"""
        from scripts.request_cache import RequestCache, OfflinePlaybackMode
        self.assertIsNotNone(RequestCache)
        self.assertIsNotNone(OfflinePlaybackMode)


# ──────────────────────────────────────────────────────────────────────────────
# Test 2: _get_now() 递归 bug 已修复
# ──────────────────────────────────────────────────────────────────────────────

class TestGetNowFix(unittest.TestCase):
    """production_data_system.py 的 _get_now() 递归 bug 已修复"""

    def test_get_now_returns_datetime(self):
        """_get_now() 返回 datetime 对象（不再递归）"""
        from scripts.production_data_system import _get_now
        # 不抛 RecursionError 即通过
        result = _get_now()
        self.assertIsInstance(result, datetime)

    def test_production_data_system_imports(self):
        """ProductionDataSystem 可正常导入（依赖 _get_now 修复）"""
        from scripts.production_data_system import ProductionDataSystem
        self.assertTrue(callable(ProductionDataSystem))


# ──────────────────────────────────────────────────────────────────────────────
# Test 3: RuntimeController 限速/缓存
# ──────────────────────────────────────────────────────────────────────────────

class TestRuntimeController(unittest.TestCase):
    """RuntimeController 限速与缓存"""

    def test_register_source_no_error(self):
        """注册数据源不报错"""
        from scripts.pipeline_data_engine import RuntimeController, RateLimitConfig
        rc = RuntimeController()
        cfg = RateLimitConfig(requests_per_second=10.0, burst_size=5)
        rc.register_source("test_source", cfg)
        # 不报错即通过
        self.assertIn("test_source", rc._throttlers)

    def test_token_bucket_acquire_returns_bool(self):
        """TokenBucket acquire 返回 bool"""
        from scripts.pipeline_data_engine import RuntimeController, RateLimitConfig
        cfg = RateLimitConfig(requests_per_second=5.0, burst_size=3)
        rc = RuntimeController()
        rc.register_source("test_tb", cfg)
        result = rc.acquire("test_tb", timeout=0.1)
        self.assertIsInstance(result, bool)

    def test_cache_set_get(self):
        """RuntimeController 缓存 set/get"""
        from scripts.pipeline_data_engine import RuntimeController
        rc = RuntimeController()
        rc.set_cache("key1", {"data": 42}, ttl=60)
        val = rc.get_cache("key1")
        self.assertEqual(val, {"data": 42})

    def test_cache_get_nonexistent(self):
        """不存在的 key 返回 None"""
        from scripts.pipeline_data_engine import RuntimeController
        rc = RuntimeController()
        val = rc.get_cache("nonexistent_key")
        self.assertIsNone(val)


# ──────────────────────────────────────────────────────────────────────────────
# Test 4: DataRouter 路由
# ──────────────────────────────────────────────────────────────────────────────

class TestDataRouter(unittest.TestCase):
    """DataRouter 数据源路由"""

    def test_default_sources_registered(self):
        """默认数据源已注册（akshare, baostock）"""
        from scripts.pipeline_data_engine import DataRouter
        router = DataRouter()
        self.assertIn("akshare", router._sources)
        self.assertIn("baostock", router._sources)

    def test_register_multiple_sources(self):
        """注册多个数据源"""
        from scripts.pipeline_data_engine import DataRouter, RateLimitConfig
        router = DataRouter()
        # 默认已有 akshare/baostock，至少两个
        self.assertGreaterEqual(len(router._sources), 2)


# ──────────────────────────────────────────────────────────────────────────────
# Test 5: QualityEngine 质量检测
# ──────────────────────────────────────────────────────────────────────────────

class TestQualityEngine(unittest.TestCase):
    """QualityEngine 数据质量检测"""

    def setUp(self):
        from scripts.pipeline_data_engine import QualityEngine
        self.engine = QualityEngine()

    def test_validate_complete_data(self):
        """完整数据通过校验"""
        df = create_mock_daily_data(has_issues=False)
        result = self.engine.validate(df, "000001.SZ")
        self.assertIsInstance(result, dict)
        self.assertIn("ok", result)
        self.assertTrue(result.get("ok"))

    def test_validate_invalid_price(self):
        """价格为0的数据被检测"""
        df = create_mock_daily_data(has_issues=True)
        result = self.engine.validate(df, "000001.SZ")
        self.assertFalse(result.get("ok", True))

    def test_detect_gaps(self):
        """缺口检测返回列表"""
        df = create_mock_daily_data()
        gaps = self.engine.detect_gaps(df, "000001.SZ")
        self.assertIsInstance(gaps, list)

    def test_validate_and_repair_returns_dict(self):
        """validate_and_repair 返回字典"""
        df = create_mock_daily_data()
        result = self.engine.validate_and_repair(df)
        self.assertIsInstance(result, dict)


# ──────────────────────────────────────────────────────────────────────────────
# Test 6: StorageManager 存储
# ──────────────────────────────────────────────────────────────────────────────

class TestStorageManager(unittest.TestCase):
    """StorageManager 数据存储"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_storage.duckdb")

    def tearDown(self):
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_write_daily_writes_data(self):
        """write_daily 写入数据"""
        from scripts.pipeline_data_engine import StorageManager
        sm = StorageManager(self.db_path)
        df = create_mock_daily_data()
        written = sm.write_daily(df, "daily_bar_raw")
        self.assertGreater(written, 0)
        self.assertTrue(os.path.exists(self.db_path))

    def test_write_daily_idempotent(self):
        """write_daily 幂等（重复写入不报错）"""
        from scripts.pipeline_data_engine import StorageManager
        sm = StorageManager(self.db_path)
        df = create_mock_daily_data()
        sm.write_daily(df, "daily_bar_raw")
        sm.write_daily(df, "daily_bar_raw")  # 第二次不报错
        self.assertTrue(True)


# ──────────────────────────────────────────────────────────────────────────────
# Test 7: BacktestValidator 回测校验
# ──────────────────────────────────────────────────────────────────────────────

class TestBacktestValidator(unittest.TestCase):
    """BacktestValidator 回测一致性校验"""

    def setUp(self):
        from scripts.pipeline_data_engine import BacktestValidator
        self.validator = BacktestValidator()

    def test_validate_with_empty_data(self):
        """空数据校验不报错"""
        trades_df = pd.DataFrame(columns=["ts_code", "trade_date", "side", "price", "quantity"])
        positions_df = pd.DataFrame(columns=["ts_code", "quantity", "avg_cost"])
        result = self.validator.validate(trades=trades_df, positions=positions_df)
        self.assertIsInstance(result, dict)

    def test_validate_returns_dict(self):
        """validate_backtest 返回字典"""
        trades_df = pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "2024-01-02",
            "side": "BUY",
            "price": 10.0,
            "quantity": 100
        }])
        result = self.validator.validate(trades=trades_df, positions=pd.DataFrame())
        self.assertIsInstance(result, dict)


# ──────────────────────────────────────────────────────────────────────────────
# Test 8: 兼容层 RequestThrottler
# ──────────────────────────────────────────────────────────────────────────────

class TestCompatRequestThrottler(unittest.TestCase):
    """兼容层 RequestThrottler"""

    def test_request_throttler_register(self):
        """RequestThrottler.register_source 正常工作"""
        from scripts.request_throttler import RequestThrottler, RateLimitConfig
        rt = RequestThrottler()
        cfg = RateLimitConfig(requests_per_second=10.0, burst_size=5)
        rt.register_source("test", cfg)
        self.assertIn("test", rt._runtime._throttlers)

    def test_request_throttler_acquire(self):
        """RequestThrottler.acquire 返回 bool（默认超时）"""
        from scripts.request_throttler import RequestThrottler, RateLimitConfig
        rt = RequestThrottler()
        cfg = RateLimitConfig(requests_per_second=10.0, burst_size=5)
        rt.register_source("test2", cfg)
        result = rt.acquire("test2")
        self.assertIsInstance(result, bool)


# ──────────────────────────────────────────────────────────────────────────────
# Test 9: 兼容层 RequestCache
# ──────────────────────────────────────────────────────────────────────────────

class TestCompatRequestCache(unittest.TestCase):
    """兼容层 RequestCache"""

    def test_cache_get_set(self):
        """RequestCache get/set 正常"""
        from scripts.request_cache import RequestCache
        cache = RequestCache()
        cache.set("test_key", {"value": 123}, ttl=60)
        val = cache.get("test_key")
        self.assertEqual(val, {"value": 123})

    def test_offline_playback_record_fetch(self):
        """OfflinePlaybackMode record/fetch 正常"""
        from scripts.request_cache import OfflinePlaybackMode
        mode = OfflinePlaybackMode()
        test_data = pd.DataFrame({"a": [1, 2, 3]})
        mode.record("TEST", test_data, "2024-01-01", "2024-01-10")
        result = mode.fetch("TEST", "2024-01-01", "2024-01-10")
        self.assertIsNotNone(result)


# ──────────────────────────────────────────────────────────────────────────────
# Test 10: PipelineDataEngine 主引擎 API
# ──────────────────────────────────────────────────────────────────────────────

class TestPipelineDataEngineAPI(unittest.TestCase):
    """PipelineDataEngine 核心 API"""

    def test_get_status_returns_dict(self):
        """get_status 返回字典"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        engine = PipelineDataEngine()
        status = engine.get_status()
        self.assertIsInstance(status, dict)

    def test_get_source_stats_returns_dict(self):
        """get_source_stats 返回字典"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        engine = PipelineDataEngine()
        stats = engine.get_source_stats()
        self.assertIsInstance(stats, dict)

    def test_runtime_via_engine(self):
        """引擎内 RuntimeController 可访问"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        engine = PipelineDataEngine()
        self.assertIsNotNone(engine.runtime)


if __name__ == "__main__":
    unittest.main()
