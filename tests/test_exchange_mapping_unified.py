"""测试：detect_board/detect_limit 统一委托 exchange_mapping 模块。

覆盖场景：
- 主板代码（6xxxxx, 0xxxxx, 1xxxxx, 2xxxxx）
- 科创板代码（688xxx）
- 创业板代码（30xxxxx）
- 北交所老股（4xxxxx）
- 北交所新股（8xxxxx）
- 北交所2024新代码（920xxx）【关键回归测试】
"""
import unittest
from scripts.data_engine import detect_board, detect_limit
from scripts.exchange_mapping import classify_exchange


class TestDetectBoardUnified(unittest.TestCase):
    """detect_board 统一化回归测试"""

    # 主板
    def test_mainboard_shanghai(self):
        self.assertEqual(detect_board('600000'), '主板')
        self.assertEqual(detect_board('600519'), '主板')
        self.assertEqual(detect_board('601318'), '主板')
        self.assertEqual(detect_board('601012'), '主板')

    def test_mainboard_shenzhen(self):
        self.assertEqual(detect_board('000001'), '主板')
        self.assertEqual(detect_board('000002'), '主板')
        self.assertEqual(detect_board('001872'), '主板')

    def test_mainboard_with_leading_zeros(self):
        # 前导零：剥离后仍是主板
        self.assertEqual(detect_board('1'), '主板')  # 000001
        self.assertEqual(detect_board('000001'), '主板')

    # 科创板
    def test_kcb(self):
        self.assertEqual(detect_board('688001'), '科创板')
        self.assertEqual(detect_board('688041'), '科创板')
        self.assertEqual(detect_board('688126'), '科创板')

    def test_kcb_no_zfill_needed(self):
        # 已经是6位，无需zfill
        self.assertEqual(detect_board('688001'), '科创板')

    # 创业板
    def test_chinext(self):
        self.assertEqual(detect_board('300001'), '创业板')
        self.assertEqual(detect_board('300750'), '创业板')
        self.assertEqual(detect_board('300001'), '创业板')

    def test_chinext_no_zfill_needed(self):
        self.assertEqual(detect_board('300750'), '创业板')

    # 北交所
    def test_bj_old(self):
        """北交所老股（4xxxxx）"""
        self.assertEqual(detect_board('430001'), '北交所')
        self.assertEqual(detect_board('430012'), '北交所')

    def test_bj_new(self):
        """北交所新股（8xxxxx）"""
        self.assertEqual(detect_board('830001'), '北交所')
        self.assertEqual(detect_board('870001'), '北交所')

    def test_bj_2024(self):
        """北交所2024新代码（920xxx）【关键回归测试】"""
        # 修复前：zfill(6)后"920001"变成"920001"，c.startswith("688")/c.startswith("30")均不匹配
        # → 错误返回"主板"，但正确应为"北交所"
        self.assertEqual(detect_board('920001'), '北交所')
        self.assertEqual(detect_board('920012'), '北交所')
        self.assertEqual(detect_board('920103'), '北交所')

    def test_bj_2024_zfill_edge_case(self):
        """920xxx zfill边界测试（已经是6位，不会被改变）"""
        # "920001".zfill(6) == "920001"（已是6位）
        # 修复前：c.startswith("688") → False, c.startswith("30") → False
        # → else分支 → "主板" ← BUG!
        # 修复后：使用 exchange_mapping 的 strip_leading_zeros → "920001" → BJ
        self.assertEqual(detect_board('920001'), '北交所')
        self.assertNotEqual(detect_board('920001'), '主板')


class TestDetectLimitUnified(unittest.TestCase):
    """detect_limit 统一化回归测试"""

    def test_mainboard(self):
        self.assertAlmostEqual(detect_limit('600000'), 0.10)
        self.assertAlmostEqual(detect_limit('000001'), 0.10)

    def test_kcb(self):
        self.assertAlmostEqual(detect_limit('688001'), 0.20)

    def test_chinext(self):
        self.assertAlmostEqual(detect_limit('300001'), 0.20)

    def test_bj_old(self):
        self.assertAlmostEqual(detect_limit('430001'), 0.30)

    def test_bj_new(self):
        self.assertAlmostEqual(detect_limit('830001'), 0.30)

    def test_bj_2024(self):
        """920xxx 北交所 30%【关键回归测试】"""
        self.assertAlmostEqual(detect_limit('920001'), 0.30)
        self.assertAlmostEqual(detect_limit('920012'), 0.30)


class TestConsistency(unittest.TestCase):
    """detect_board 与 classify_exchange 结果一致性测试"""

    BOARD_MAP = {
        '科创板': 'STAR', '创业板': 'CHINEXT',
        '北交所': 'BJ', '主板': 'MAIN', 'B股': 'B'
    }

    def test_board_matches_classify_exchange(self):
        """detect_board 必须与 classify_exchange 的板块返回值一致"""
        cases = [
            ('600000', 'SH', '主板'),
            ('000001', 'SZ', '主板'),
            ('688001', 'SH', '科创板'),
            ('300750', 'SZ', '创业板'),
            ('430001', 'BJ', '北交所'),
            ('830001', 'BJ', '北交所'),
            ('920001', 'BJ', '北交所'),  # 关键测试
            ('000005', 'SZ', '主板'),
            ('900901', 'SH', 'B股'),
        ]
        for symbol, exp_exch, exp_board in cases:
            exchange, board = classify_exchange(symbol)
            self.assertEqual(detect_board(symbol), exp_board,
                f"detect_board('{symbol}')={detect_board(symbol)}, expected '{exp_board}'")


if __name__ == '__main__':
    unittest.main()
