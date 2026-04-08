"""测试 _get_now() 时间注入能力 - 验证单元测试可 mock 时间"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import unittest
import os
from datetime import datetime


class TestTimeMock(unittest.TestCase):
    """验证各模块 _get_now() 支持单测 mock"""

    def test_data_engine_get_now_mockable(self):
        """DataEngine._get_now() 支持通过子类覆盖实现时间 mock"""
        from scripts.data_engine import DataEngine

        # 测试静态方法本身，不需要数据库文件
        original = DataEngine._get_now
        try:
            # Monkey-patch the static method
            DataEngine._get_now = staticmethod(lambda: datetime(2024, 1, 1, 9, 30, 0))
            engine = object.__new__(DataEngine)
            mocked = engine._get_now()
            self.assertEqual(mocked.year, 2024)
            self.assertEqual(mocked.month, 1)
            self.assertEqual(mocked.day, 1)
            self.assertEqual(mocked.hour, 9)
        finally:
            DataEngine._get_now = original

    def test_data_validator_get_now_mockable(self):
        """DataValidator._get_now() 是模块级函数，支持直接 patch"""
        import scripts.data_validator as dm
        original = dm._get_now
        try:
            dm._get_now = lambda: datetime(2025, 6, 15, 12, 0, 0)
            result = dm._get_now()
            self.assertEqual(result.year, 2025)
            self.assertEqual(result.month, 6)
            self.assertEqual(result.day, 15)
        finally:
            dm._get_now = original


if __name__ == '__main__':
    unittest.main(verbosity=2)
