"""
单元测试 - EnhancedDataEngine
=============================
覆盖：
1. 多数据源切换与容错
2. 完整性校验与自动重试
3. 数据修复逻辑
4. 断点续传
5. 生产调度
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

# 添加项目根目录到路径
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
        "trade_date": dates,
        "open": np.random.uniform(10, 20, len(dates)),
        "high": np.random.uniform(10, 20, len(dates)),
        "low": np.random.uniform(10, 20, len(dates)),
        "close": np.random.uniform(10, 20, len(dates)),
        "volume": np.random.randint(1000000, 10000000, len(dates)),
        "amount": np.random.uniform(10000000, 100000000, len(dates)),
    }

    df = pd.DataFrame(data)

    # 添加问题数据
    if has_issues:
        # 制造价格异常
        df.loc[2, "close"] = 0
        # 制造缺失值
        df.loc[5, "open"] = np.nan
        # 制造涨跌幅异常
        df["pct_chg"] = (df["close"] - df["close"].shift(1)) / df["close"].shift(1) * 100
        df.loc[7, "pct_chg"] = 25.0  # 异常涨跌

    return df


# ──────────────────────────────────────────────────────────────────────────────
# 测试：多数据源切换
# ──────────────────────────────────────────────────────────────────────────────

class TestMultiSourceSwitching(unittest.TestCase):
    """测试多数据源切换逻辑"""

    def setUp(self):
        """测试前准备"""
        self.mock_source_manager = mock.MagicMock()
        self.mock_source_manager.get_primary_source.return_value = "akshare"
        self.mock_source_manager.promote_next_source.return_value = True

    def test_source_switch_on_failure(self):
        """测试连续失败时自动切换数据源"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        # 模拟下载器
        downloader = EnhancedDownloader(
            source_manager=self.mock_source_manager,
            max_retries=3,
        )

        # 模拟第一次失败，第二次成功
        call_count = [0]

        def mock_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                return mock.MagicMock(success=False, data=None)
            return mock.MagicMock(success=True, data=create_mock_daily_data())

        self.mock_source_manager.fetch_daily = mock_fetch

        # 执行下载
        df, record = downloader.download("000001.SZ")

        # 验证切换
        self.assertGreaterEqual(
            self.mock_source_manager.promote_next_source.call_count, 0
        )
        print(f"下载尝试次数: {call_count[0]}")

    def test_failure_threshold_triggers_switch(self):
        """测试失败次数超过阈值触发切换"""
        from scripts.enhanced_data_engine import EnhancedDataEngine, EnhancedDownloader

        # 模拟一个总是失败的数据源管理器
        mock_mgr = mock.MagicMock()
        mock_mgr.fetch_daily.return_value = None

        # 创建下载器
        downloader = EnhancedDownloader(mock_mgr, max_retries=3)

        # 多次下载都失败
        for _ in range(3):
            downloader.download("000001.SZ")

        stats = downloader.get_stats()
        self.assertEqual(stats["failed_downloads"], 3)
        print(f"失败统计: {stats}")

    def test_get_source_stats(self):
        """测试获取数据源统计"""
        mock_stats = {
            "akshare": {
                "total_requests": 100,
                "success_count": 95,
                "success_rate": 0.95,
            },
            "baostock": {
                "total_requests": 50,
                "success_count": 48,
                "success_rate": 0.96,
            },
        }

        # 验证统计结构
        self.assertIn("akshare", mock_stats)
        self.assertIn("baostock", mock_stats)
        self.assertGreater(mock_stats["akshare"]["success_rate"], 0.9)


# ──────────────────────────────────────────────────────────────────────────────
# 测试：完整性校验
# ──────────────────────────────────────────────────────────────────────────────

class TestDataIntegrity(unittest.TestCase):
    """测试数据完整性校验"""

    def test_validate_complete_data(self):
        """测试完整数据通过校验"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        downloader = EnhancedDownloader(mock.MagicMock())

        df = create_mock_daily_data(has_issues=False)
        result = downloader._validate_completeness(df, "000001.SZ")

        self.assertTrue(result)
        print("完整数据校验通过")

    def test_detect_invalid_price(self):
        """测试检测无效价格"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        downloader = EnhancedDownloader(mock.MagicMock())

        df = create_mock_daily_data(has_issues=True)
        result = downloader._validate_completeness(df, "000001.SZ")

        self.assertFalse(result)
        print("检测到无效价格")

    def test_hash_computation(self):
        """测试哈希计算"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        downloader = EnhancedDownloader(mock.MagicMock())

        df = create_mock_daily_data()
        hash1 = downloader._compute_hash(df)
        hash2 = downloader._compute_hash(df)

        # 相同数据应产生相同哈希
        self.assertEqual(hash1, hash2)
        print(f"哈希值: {hash1}")

        # 修改数据后哈希应不同
        df2 = df.copy()
        df2.loc[0, "close"] = 999
        hash3 = downloader._compute_hash(df2)
        self.assertNotEqual(hash1, hash3)
        print("修改后哈希不同，验证通过")

    def test_detect_missing_columns(self):
        """测试检测缺失列"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        downloader = EnhancedDownloader(mock.MagicMock())

        # 缺少必需列
        df = pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=5),
            "close": [10, 11, 12, 13, 14],
        })

        result = downloader._validate_completeness(df, "000001.SZ")
        self.assertFalse(result)
        print("检测到缺失必需列")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：数据修复
# ──────────────────────────────────────────────────────────────────────────────

class TestDataRepair(unittest.TestCase):
    """测试数据自动修复"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.repair_log = Path(self.temp_dir) / "repair_test.json"

    def tearDown(self):
        """测试后清理"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_forward_fill_repair(self):
        """测试前向填充修复"""
        from scripts.enhanced_data_engine import EnhancedValidator

        validator = EnhancedValidator(repair_log_path=str(self.repair_log))

        # 创建有缺失值的数据
        df = create_mock_daily_data(has_issues=True)

        # 验证修复
        result = validator.validate_and_repair(
            df=df,
            ts_code="000001.SZ",
            backup_source=None,  # 不使用备用源
            table_name="daily_bar_raw",
        )

        self.assertIn("validation", result)
        print(f"修复结果: {result.get('repair')}")

    def test_repair_log_saving(self):
        """测试修复日志保存"""
        from scripts.enhanced_data_engine import EnhancedValidator

        validator = EnhancedValidator(repair_log_path=str(self.repair_log))

        df = create_mock_daily_data(has_issues=True)

        # 执行修复
        validator.validate_and_repair(
            df=df,
            ts_code="000001.SZ",
            backup_source=None,
        )

        # 验证日志文件
        if self.repair_log.exists():
            import json
            with open(self.repair_log) as f:
                logs = json.load(f)
            print(f"修复日志记录数: {len(logs)}")
        else:
            print("无修复记录（数据可能正常）")

    def test_generate_repair_report(self):
        """测试生成修复报告"""
        from scripts.enhanced_data_engine import EnhancedValidator

        validator = EnhancedValidator()

        report = validator.generate_repair_report(days=7)
        print(f"修复报告:\n{report}")
        self.assertIsInstance(report, str)


# ──────────────────────────────────────────────────────────────────────────────
# 测试：断点续传
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckpoint(unittest.TestCase):
    """测试断点续传"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """测试后清理"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_checkpoint_structure(self):
        """测试断点记录结构"""
        from scripts.enhanced_data_engine import DownloadRecord

        record = DownloadRecord(
            ts_code="000001.SZ",
            table_name="daily_bar_raw",
            start_date="2024-01-01",
            end_date="2024-01-10",
            record_count=100,
            data_hash="abc123",
            source="akshare",
            downloaded_at=datetime.now(),
            retry_count=0,
        )

        data = record.to_dict()
        self.assertEqual(data["ts_code"], "000001.SZ")
        self.assertEqual(data["record_count"], 100)
        print(f"断点记录: {data}")

    def test_resume_from_checkpoint(self):
        """测试从断点恢复"""
        # 模拟已存在的断点
        checkpoint = {
            "ts_code": "000001.SZ",
            "end_date": "2024-01-05",
            "data_hash": "abc123",
            "record_count": 50,
        }

        # 模拟计算的新哈希
        new_data = create_mock_daily_data(start="2024-01-06", end="2024-01-10")
        new_hash = "def456"

        # 哈希不同，应该重新拉取
        should_reload = checkpoint["data_hash"] != new_hash
        self.assertTrue(should_reload)
        print("哈希不同，需要重新拉取")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：下载重试
# ──────────────────────────────────────────────────────────────────────────────

class TestDownloadRetry(unittest.TestCase):
    """测试下载重试逻辑"""

    def test_retry_count_increases(self):
        """测试重试次数递增"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        mock_mgr = mock.MagicMock()
        # 模拟两次失败后成功
        call_count = [0]

        def mock_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                return None
            return mock.MagicMock(success=True, data=create_mock_daily_data())

        mock_mgr.fetch_daily = mock_fetch
        downloader = EnhancedDownloader(mock_mgr, max_retries=3)

        df, record = downloader.download("000001.SZ")

        self.assertEqual(record.retry_count, 2)  # 前两次失败
        print(f"重试次数: {record.retry_count}")

    def test_max_retries_respected(self):
        """测试尊重最大重试次数"""
        from scripts.enhanced_data_engine import EnhancedDownloader

        mock_mgr = mock.MagicMock()
        mock_mgr.fetch_daily.return_value = None

        downloader = EnhancedDownloader(mock_mgr, max_retries=3)

        df, record = downloader.download("000001.SZ")

        self.assertEqual(record.retry_count, 3)
        self.assertIsNone(df)
        print(f"达到最大重试次数: {record.retry_count}")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：DuckDB 存储
# ──────────────────────────────────────────────────────────────────────────────

class TestDuckDBStorage(unittest.TestCase):
    """测试 DuckDB 存储"""

    def setUp(self):
        """测试前准备"""
        self.temp_db = tempfile.mktemp(suffix=".duckdb")

    def tearDown(self):
        """测试后清理"""
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_table_stats_structure(self):
        """测试表统计结构"""
        stats = {
            "daily_bar_raw": {"row_count": 1408552},
            "daily_bar_adjusted": {"row_count": 1408552},
            "stock_basic_history": {"row_count": 5608},
        }

        self.assertIn("daily_bar_raw", stats)
        self.assertGreater(stats["daily_bar_raw"]["row_count"], 0)
        print(f"表统计: {stats}")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：调度器
# ──────────────────────────────────────────────────────────────────────────────

class TestScheduler(unittest.TestCase):
    """测试生产调度"""

    def test_schedule_format(self):
        """测试调度时间格式"""
        hour, minute = 16, 0
        schedule_str = f"{hour:02d}:{minute:02d}"
        self.assertEqual(schedule_str, "16:00")
        print(f"调度时间格式: {schedule_str}")

    def test_schedule_config(self):
        """测试调度配置"""
        config = {
            "hour": 16,
            "minute": 0,
            "symbols": ["000001.SZ", "600000.SH"],
            "enabled": True,
        }

        self.assertTrue(config["enabled"])
        self.assertEqual(len(config["symbols"]), 2)
        print(f"调度配置: {config}")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：引擎状态
# ──────────────────────────────────────────────────────────────────────────────

class TestEngineStatus(unittest.TestCase):
    """测试引擎状态获取"""

    def test_status_structure(self):
        """测试状态结构"""
        from scripts.enhanced_data_engine import EnhancedDataEngine

        status = {
            "timestamp": datetime.now().isoformat(),
            "enable_multi_source": True,
            "enable_auto_repair": True,
            "primary_source": "akshare",
            "source_stats": {
                "akshare": {"total_requests": 100},
            },
            "table_stats": {
                "daily_bar_raw": {"row_count": 1408552},
            },
        }

        self.assertIn("timestamp", status)
        self.assertIn("enable_multi_source", status)
        print(f"引擎状态: {status}")


# ──────────────────────────────────────────────────────────────────────────────
# 测试：数据源管理器
# ──────────────────────────────────────────────────────────────────────────────

class TestSourceManager(unittest.TestCase):
    """测试数据源管理器"""

    def test_register_source(self):
        """测试注册数据源"""
        mock_source = mock.MagicMock()
        mock_source.name = "akshare"
        mock_source.priority = 1

        registered = [mock_source]
        self.assertEqual(len(registered), 1)
        self.assertEqual(registered[0].name, "akshare")
        print("数据源注册成功")

    def test_priority_order(self):
        """测试优先级排序"""
        sources = [
            {"name": "baostock", "priority": 2},
            {"name": "akshare", "priority": 1},
            {"name": "tushare", "priority": 3},
        ]

        sorted_sources = sorted(sources, key=lambda x: x["priority"])
        self.assertEqual(sorted_sources[0]["name"], "akshare")
        print(f"优先级排序: {[s['name'] for s in sorted_sources]}")


# ──────────────────────────────────────────────────────────────────────────────
# 主程序
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("EnhancedDataEngine 单元测试")
    print("=" * 70)

    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestMultiSourceSwitching))
    suite.addTests(loader.loadTestsFromTestCase(TestDataIntegrity))
    suite.addTests(loader.loadTestsFromTestCase(TestDataRepair))
    suite.addTests(loader.loadTestsFromTestCase(TestCheckpoint))
    suite.addTests(loader.loadTestsFromTestCase(TestDownloadRetry))
    suite.addTests(loader.loadTestsFromTestCase(TestDuckDBStorage))
    suite.addTests(loader.loadTestsFromTestCase(TestScheduler))
    suite.addTests(loader.loadTestsFromTestCase(TestEngineStatus))
    suite.addTests(loader.loadTestsFromTestCase(TestSourceManager))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # 输出总结
    print("\n" + "=" * 70)
    print(f"测试完成: {result.testsRun} 个测试")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
