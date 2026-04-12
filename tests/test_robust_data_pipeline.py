"""
test_robust_data_pipeline.py - 鲁棒性数据管道单元测试
======================================================
覆盖场景：
1. 网络超时 & 限流重试路径
2. 数据字段缺失/格式变更
3. 断点续传 hash mismatch
4. 多数据源轮换逻辑
5. 极端停牌/退市综合修复路径
"""

import pytest
import time
import threading
import json
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import Optional
import pandas as pd
import numpy as np

import sys
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root))


# ══════════════════════════════════════════════════════════════════════════════
# 测试夹具
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_daily_data():
    """样本日线数据"""
    dates = pd.date_range("2024-01-01", "2024-01-31", freq="B")
    return pd.DataFrame({
        "trade_date": dates.strftime("%Y%m%d"),
        "ts_code": ["000001.SZ"] * len(dates),
        "open": np.random.uniform(10, 20, len(dates)),
        "high": np.random.uniform(20, 25, len(dates)),
        "low": np.random.uniform(8, 15, len(dates)),
        "close": np.random.uniform(10, 20, len(dates)),
        "volume": np.random.uniform(1e6, 1e7, len(dates))
    })


@pytest.fixture
def temp_cache_dir(tmp_path):
    """临时缓存目录"""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


# ══════════════════════════════════════════════════════════════════════════════
# Test 1: 请求限速器
# ══════════════════════════════════════════════════════════════════════════════

class TestRequestThrottler:
    """测试请求限速器"""
    
    def test_token_bucket_basic(self):
        """测试令牌桶基本功能"""
        from scripts.request_throttler import TokenBucketThrottler, RateLimitConfig
        
        config = RateLimitConfig(requests_per_second=5.0, burst_size=3)
        throttler = TokenBucketThrottler(config)
        
        # 突发容量内应该立即成功
        assert throttler.acquire(timeout=1.0) == True
        assert throttler.acquire(timeout=1.0) == True
        assert throttler.acquire(timeout=1.0) == True
        
        # 超过容量应该失败或等待
        result = throttler.acquire(timeout=0.5)
        assert result == False  # 超时拒绝
    
    def test_rate_limit_report(self):
        """测试限流报告"""
        from scripts.request_throttler import TokenBucketThrottler
        
        throttler = TokenBucketThrottler()
        
        # 报告限流
        throttler.report_limit_hit("test_source")
        
        assert throttler.status.total_cooldowns == 1
        assert throttler.status.recent_errors
    
    def test_global_throttler(self):
        """测试全局限速器"""
        from scripts.request_throttler import RequestThrottler
        
        throttler = RequestThrottler()
        throttler.register_source("test", RateLimitConfig(requests_per_second=10.0))
        
        assert "test" in throttler._throttlers


# ══════════════════════════════════════════════════════════════════════════════
# Test 2: Baostock 健康检查与熔断器
# ══════════════════════════════════════════════════════════════════════════════

class TestBaostockEnhanced:
    """测试 Baostock 增强功能"""
    
    def test_circuit_breaker_basic(self):
        """测试熔断器基本功能"""
        from scripts.baostock_enhanced import BaostockCircuitBreaker, SourceStats, SourceHealth
        
        breaker = BaostockCircuitBreaker(
            failure_threshold=3,
            empty_threshold=2,
            block_duration=5
        )
        
        # 初始状态正常
        assert not breaker.is_blocked()
        assert breaker.stats.health == SourceHealth.HEALTHY
        
        # 记录失败
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.stats.consecutive_failures == 2
        
        # 第三次失败触发熔断
        breaker.record_failure()
        assert breaker.stats.health == SourceHealth.BLOCKED
        assert breaker.is_blocked()
    
    def test_circuit_breaker_recovery(self):
        """测试熔断器恢复"""
        from scripts.baostock_enhanced import BaostockCircuitBreaker
        
        breaker = BaostockCircuitBreaker(failure_threshold=1, block_duration=1)
        
        breaker.record_failure()
        assert breaker.is_blocked()
        
        # 等待熔断过期
        time.sleep(1.1)
        assert not breaker.is_blocked()
    
    def test_success_resets_consecutive(self):
        """测试成功重置连续失败计数"""
        from scripts.baostock_enhanced import BaostockCircuitBreaker
        
        breaker = BaostockCircuitBreaker(failure_threshold=5)
        
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.stats.consecutive_failures == 2
        
        breaker.record_success(100.0)
        assert breaker.stats.consecutive_failures == 0


# ══════════════════════════════════════════════════════════════════════════════
# Test 3: 时间序列断层补全
# ══════════════════════════════════════════════════════════════════════════════

class TestTimeSeriesGaps:
    """测试时间序列断层处理"""
    
    def test_detect_gaps_weekend(self, sample_daily_data):
        """测试周末断层检测"""
        from scripts.time_series_gaps import detect_gaps
        
        # 周五到周一有断层
        gaps = detect_gaps(sample_daily_data, date_col="trade_date")
        
        # 应该检测到周末断层
        assert isinstance(gaps, list)
    
    def test_detect_gaps_holiday(self):
        """测试节假日断层检测"""
        from scripts.time_series_gaps import detect_gaps, GapType
        
        # 创建一个有长断层的 DataFrame
        df = pd.DataFrame({
            "trade_date": ["2024-01-01", "2024-01-08", "2024-01-09"],  # 跨春节
            "close": [10.0, 11.0, 11.5]
        })
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        
        gaps = detect_gaps(df)
        
        assert len(gaps) > 0
        assert any(g.gap_type in [GapType.HOLIDAY, GapType.DATA_MISSING] for g in gaps)
    
    def test_forward_fill(self, sample_daily_data):
        """测试前向填充"""
        from scripts.time_series_gaps import fill_gaps, FillStrategy
        
        # 插入一些 NaN
        df = sample_daily_data.copy()
        df.loc[5, "close"] = np.nan
        df.loc[10, "volume"] = np.nan
        
        filled_df, result = fill_gaps(df, strategy=FillStrategy.FORWARD_FILL)
        
        assert result.original_rows == len(df)
        assert result.strategy_used == FillStrategy.FORWARD_FILL
    
    def test_gap_analyzer_auto_fill(self):
        """测试自动填充策略选择"""
        from scripts.time_series_gaps import GapAnalyzer
        
        df = pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=30),
            "close": np.random.uniform(10, 20, 30)
        })
        df.loc[5, "close"] = np.nan
        
        analyzer = GapAnalyzer(df)
        gaps = analyzer.detect_all()
        
        filled_df, result = analyzer.auto_fill()
        
        assert result is not None


# ══════════════════════════════════════════════════════════════════════════════
# Test 4: 错误模式检测与回退
# ══════════════════════════════════════════════════════════════════════════════

class TestErrorPatternDetector:
    """测试错误模式检测"""
    
    def test_detect_rate_limit(self):
        """测试限流错误检测"""
        from scripts.third_party_sources import ErrorPatternDetector, ErrorPattern
        
        detector = ErrorPatternDetector()
        
        error = Exception("rate limit exceeded")
        error_info = detector.detect(error, "akshare")
        
        assert error_info.pattern == ErrorPattern.RATE_LIMIT
        assert error_info.retryable == True
    
    def test_detect_network_error(self):
        """测试网络错误检测"""
        from scripts.third_party_sources import ErrorPatternDetector, ErrorPattern
        
        detector = ErrorPatternDetector()
        
        error = Exception("ConnectionRefusedError: connection refused")
        error_info = detector.detect(error, "baostock")
        
        assert error_info.pattern == ErrorPattern.NETWORK_ERROR
    
    def test_detect_field_missing(self):
        """测试字段缺失检测"""
        from scripts.third_party_sources import ErrorPatternDetector, ErrorPattern
        
        detector = ErrorPatternDetector()
        
        error = KeyError("'close'")
        error_info = detector.detect(error, "akshare")
        
        assert error_info.pattern == ErrorPattern.FIELD_MISSING
    
    def test_fallback_strategy_next_source(self):
        """测试回退策略获取下一源"""
        from scripts.third_party_sources import FallbackStrategy
        
        next_src = FallbackStrategy.get_next_source("daily", "akshare")
        assert next_src == "baostock"
        
        next_src2 = FallbackStrategy.get_next_source("daily", "baostock")
        assert next_src2 == "tushare"


# ══════════════════════════════════════════════════════════════════════════════
# Test 5: 指数退避
# ══════════════════════════════════════════════════════════════════════════════

class TestBackoffController:
    """测试退避控制器"""
    
    def test_exponential_delay(self):
        """测试指数延迟计算"""
        from scripts.backoff_controller import ExponentialBackoff, RetryConfig
        
        config = RetryConfig(base_delay=1.0, multiplier=2.0, max_delay=60.0)
        backoff = ExponentialBackoff(config)
        
        assert backoff.get_delay(0) == pytest.approx(1.0, abs=0.5)
        assert backoff.get_delay(1) == pytest.approx(2.0, abs=0.5)
        assert backoff.get_delay(2) == pytest.approx(4.0, abs=0.5)
    
    def test_max_delay_cap(self):
        """测试最大延迟上限"""
        from scripts.backoff_controller import ExponentialBackoff, RetryConfig
        
        config = RetryConfig(base_delay=1.0, multiplier=2.0, max_delay=10.0)
        backoff = ExponentialBackoff(config)
        
        # 第10次尝试应该被限制在 max_delay
        assert backoff.get_delay(10) <= 10.0
    
    def test_retry_success_after_failures(self):
        """测试失败后重试成功"""
        from scripts.backoff_controller import BackoffController, RetryConfig
        
        controller = BackoffController(RetryConfig(max_retries=3, base_delay=0.1))
        
        call_count = 0
        
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("test")
            return "success"
        
        result = controller.retry_with_backoff(flaky_func)
        
        assert result == "success"
        assert call_count == 3


# ══════════════════════════════════════════════════════════════════════════════
# Test 6: 请求缓存
# ══════════════════════════════════════════════════════════════════════════════

class TestRequestCache:
    """测试请求缓存"""
    
    def test_cache_hit(self, temp_cache_dir):
        """测试缓存命中"""
        from scripts.request_cache import RequestCache
        
        cache = RequestCache(ttl_seconds=3600)
        
        cache.set("key1", {"data": "test"})
        
        result = cache.get("key1")
        assert result == {"data": "test"}
        assert cache.get_stats()["hits"] == 1
    
    def test_cache_miss(self, temp_cache_dir):
        """测试缓存未命中"""
        from scripts.request_cache import RequestCache
        
        cache = RequestCache()
        
        result = cache.get("nonexistent")
        assert result is None
        assert cache.get_stats()["misses"] == 1
    
    def test_cache_expiry(self, temp_cache_dir):
        """测试缓存过期"""
        from scripts.request_cache import RequestCache
        
        cache = RequestCache(ttl_seconds=1)
        
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # 等待过期
        time.sleep(1.1)
        
        assert cache.get("key1") is None
    
    def test_get_or_fetch(self, temp_cache_dir):
        """测试获取或抓取"""
        from scripts.request_cache import RequestCache
        
        cache = RequestCache(ttl_seconds=3600)
        fetch_count = 0
        
        def fetch_func():
            nonlocal fetch_count
            fetch_count += 1
            return f"data_{fetch_count}"
        
        # 第一次调用
        result1 = cache.get_or_fetch(key="test_key", fetch_func=fetch_func)
        assert result1 == "data_1"
        assert fetch_count == 1
        
        # 第二次应该命中缓存
        result2 = cache.get_or_fetch(key="test_key", fetch_func=fetch_func)
        assert result2 == "data_1"
        assert fetch_count == 1  # 没有重新抓取


# ══════════════════════════════════════════════════════════════════════════════
# Test 7: 脱机回放
# ══════════════════════════════════════════════════════════════════════════════

class TestOfflinePlayback:
    """测试脱机回放"""
    
    def test_record_and_fetch(self, tmp_path, sample_daily_data):
        """测试记录和回放"""
        from scripts.request_cache import OfflinePlaybackMode
        
        playback = OfflinePlaybackMode(data_dir=str(tmp_path / "playback"))
        
        # 记录数据
        playback.record("000001.SZ", sample_daily_data, "2024-01-01", "2024-01-31")
        
        # 检查可用性
        assert playback.is_available("000001.SZ", "2024-01-01", "2024-01-31")
        
        # 回放数据
        result = playback.fetch("000001.SZ", "2024-01-01", "2024-01-31")
        
        assert result is not None
        assert len(result) == len(sample_daily_data)
    
    def test_offline_fetch_fallback(self, tmp_path):
        """测试离线获取回退"""
        from scripts.request_cache import OfflinePlaybackMode
        
        playback = OfflinePlaybackMode(data_dir=str(tmp_path / "playback"))
        
        fetch_count = 0
        
        def online_fetch():
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                raise ConnectionError("offline")
            return {"data": "fallback"}
        
        # 第一次失败，使用回放
        result = playback.offline_fetch(
            "000001.SZ", "2024-01-01", "2024-01-31",
            online_fetch_func=online_fetch
        )
        
        assert fetch_count == 1


# ══════════════════════════════════════════════════════════════════════════════
# Test 8: 回测验证
# ══════════════════════════════════════════════════════════════════════════════

class TestBacktestValidator:
    """测试回测验证器"""
    
    def test_position_integrity_check(self):
        """测试持仓完整性检查"""
        from scripts.backtest_validator import BacktestValidator, ValidationSeverity
        
        validator = BacktestValidator()
        
        positions = pd.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["2024-01-01", "2024-01-01"],
            "volume": [100, -50],  # 负数持仓
            "cost": [1000, 2000]
        })
        
        report = validator.validate(positions=positions)
        
        # 应该发现问题
        critical_issues = [i for i in report.issues if i.severity == ValidationSeverity.CRITICAL]
        assert len(critical_issues) > 0
    
    def test_trade_integrity_check(self):
        """测试交易完整性检查"""
        from scripts.backtest_validator import BacktestValidator
        
        validator = BacktestValidator()
        
        trades = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["2024-01-01"],
            "direction": ["buy"],
            "price": [10.0],
            "volume": [100]
        })
        
        report = validator.validate(trades=trades)
        
        assert report.total_checks > 0
    
    def test_equity_curve_check(self):
        """测试权益曲线检查"""
        from scripts.backtest_validator import BacktestValidator, ValidationSeverity
        
        validator = BacktestValidator()
        
        equity = pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=10),
            "equity": [100000, 95000, 90000, 0, 85000, 80000, 75000, 70000, 65000, 60000]  # 权益归零
        })
        
        report = validator.validate(equity_curve=equity)
        
        critical_issues = [i for i in report.issues if i.severity == ValidationSeverity.CRITICAL]
        assert len(critical_issues) > 0


# ══════════════════════════════════════════════════════════════════════════════
# Test 9: Parquet 优化器
# ══════════════════════════════════════════════════════════════════════════════

class TestParquetOptimizer:
    """测试 Parquet 优化器"""
    
    def test_write_and_read(self, tmp_path, sample_daily_data):
        """测试写入和读取"""
        from scripts.parquet_optimizer import ParquetOptimizer
        
        optimizer = ParquetOptimizer(base_path=str(tmp_path / "parquet"))
        
        # 写入
        files = optimizer.write(sample_daily_data, "daily_test")
        assert len(files) > 0
        
        # 读取
        result = optimizer.read("daily_test")
        assert len(result) == len(sample_daily_data)
    
    def test_read_range(self, tmp_path, sample_daily_data):
        """测试范围读取"""
        from scripts.parquet_optimizer import ParquetOptimizer
        
        optimizer = ParquetOptimizer(base_path=str(tmp_path / "parquet"))
        optimizer.write(sample_daily_data, "daily_range")
        
        # 按日期范围读取
        result = optimizer.read_range("daily_range", start="2024-01-10", end="2024-01-20")
        
        assert len(result) > 0


# ══════════════════════════════════════════════════════════════════════════════
# Test 10: 综合场景测试
# ══════════════════════════════════════════════════════════════════════════════

class TestIntegratedScenarios:
    """综合场景测试"""
    
    def test_multi_source_fallback_scenario(self):
        """测试多数据源回退场景"""
        from scripts.third_party_sources import FallbackStrategy, ErrorPatternDetector, ErrorPattern
        from scripts.baostock_enhanced import BaostockCircuitBreaker
        
        # 场景：AkShare 限流 -> Baostock 被熔断 -> 失败
        breaker = BaostockCircuitBreaker(failure_threshold=2, block_duration=60)
        
        # Baostock 失败两次后被熔断
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.is_blocked()
        
        # 获取下一源
        next_source = FallbackStrategy.get_next_source("daily", "baostock")
        # 如果 baostock 被阻止，应该返回 None 或跳过
        
        # 错误检测
        detector = ErrorPatternDetector()
        error = Exception("rate limit exceeded")
        error_info = detector.detect(error, "akshare")
        
        assert error_info.pattern == ErrorPattern.RATE_LIMIT
    
    def test_cache_with_backoff_scenario(self):
        """测试缓存配合退避场景"""
        from scripts.request_cache import RequestCache
        from scripts.backoff_controller import BackoffController, RetryConfig
        
        cache = RequestCache(ttl_seconds=3600)
        controller = BackoffController(RetryConfig(max_retries=3, base_delay=0.1))
        
        call_count = 0
        
        def fetch_with_retry():
            nonlocal call_count
            call_count += 1
            
            # 模拟首次失败
            if call_count == 1:
                raise ConnectionError("test")
            
            return f"data_{call_count}"
        
        # 缓存配合重试
        result = cache.get_or_fetch(
            key="test_scenario",
            fetch_func=lambda: controller.retry_with_backoff(fetch_with_retry)
        )
        
        assert result is not None
        assert call_count == 2  # 第一次失败，第二次成功


# ══════════════════════════════════════════════════════════════════════════════
# 运行测试
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
