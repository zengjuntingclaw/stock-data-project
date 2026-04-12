"""
test_data_caliber_v3.py - 数据口径修复测试（v3.2 关键验证）

覆盖 Task 1-4 的核心修复：
  1. ts_code 统一：全路径使用 build_ts_code()
  2. get_active_stocks()：PIT 历史可交易股票池
  3. 指数 out_date 更新：禁止覆盖重建
  4. 日线双层写入：raw vs adjusted 分离

运行：python -m unittest tests.test_data_caliber_v3
"""
import unittest
import tempfile
import os
from pathlib import Path

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

from scripts.exchange_mapping import (
    build_ts_code, build_bs_code, classify_exchange
)


# ─────────────────────────────────────────────────────────────────
# Task 1: ts_code / bs_code 全路径统一
# ─────────────────────────────────────────────────────────────────
class TestBuildTsCodeCoverage(unittest.TestCase):
    """build_ts_code 覆盖测试（4类代码 + 前导零）"""

    def test_sh_mainboard(self):
        """主板沪市 6xxxxx → .SH"""
        self.assertEqual(build_ts_code('600000'), '600000.SH')
        self.assertEqual(build_ts_code('601318'), '601318.SH')
        self.assertEqual(build_ts_code('600519'), '600519.SH')

    def test_sz_mainboard(self):
        """主板深市 0xxxxx / 1xxxxx / 2xxxxx → .SZ"""
        self.assertEqual(build_ts_code('000001'), '000001.SZ')
        self.assertEqual(build_ts_code('001872'), '001872.SZ')
        self.assertEqual(build_ts_code('200002'), '200002.SZ')  # B股

    def test_sz_chinext(self):
        """创业板 30xxxxx → .SZ"""
        self.assertEqual(build_ts_code('300001'), '300001.SZ')
        self.assertEqual(build_ts_code('300750'), '300750.SZ')

    def test_kcb(self):
        """科创板 688xxx → .SH（不是 SZ！）"""
        self.assertEqual(build_ts_code('688001'), '688001.SH')
        self.assertEqual(build_ts_code('688126'), '688126.SH')

    def test_bj_old(self):
        """北交所老股 4xxxxx → .BJ"""
        self.assertEqual(build_ts_code('430001'), '430001.BJ')
        self.assertEqual(build_ts_code('430012'), '430012.BJ')

    def test_bj_new(self):
        """北交所新股 8xxxxx → .BJ"""
        self.assertEqual(build_ts_code('830001'), '830001.BJ')
        self.assertEqual(build_ts_code('870001'), '870001.BJ')

    def test_bj_2024(self):
        """北交所2024新代码 920xxx → .BJ（关键！920不是SH）"""
        self.assertEqual(build_ts_code('920001'), '920001.BJ')
        self.assertEqual(build_ts_code('920012'), '920012.BJ')
        self.assertEqual(build_ts_code('920103'), '920103.BJ')

    def test_leading_zero_sh(self):
        """前导零后 6 位：'1' → '000001'，0 开头 → SZ（深市主板）"""
        self.assertEqual(build_ts_code('1'), '000001.SZ')

    def test_leading_zero_sz(self):
        """6位完整代码 '000008'（前导零 + 末位8）→ 深市主板 SZ
        注意：_is_bj_code 对 6 位输入走正则精确路径，'000008' 不匹配
        4xxxxx/8xxxxx/920xxx，因此正确返回 SZ 而非 BJ。"""
        self.assertEqual(build_ts_code('000008'), '000008.SZ')

    def test_leading_zero_bj_old(self):
        """前导零北交所老股：'430001' → '430001.BJ'"""
        self.assertEqual(build_ts_code('430001'), '430001.BJ')

    def test_leading_zero_bj_2024(self):
        """前导零北交所2024：'920001' → '920001.BJ'"""
        self.assertEqual(build_ts_code('920001'), '920001.BJ')


class TestBuildBsCodeCoverage(unittest.TestCase):
    """build_bs_code（Baostock格式）覆盖测试"""

    def test_sh_baostock(self):
        """沪市 → sh.xxxxxx"""
        self.assertEqual(build_bs_code('600000'), 'sh.600000')
        self.assertEqual(build_bs_code('688001'), 'sh.688001')

    def test_sz_baostock(self):
        """深市 → sz.xxxxxx"""
        self.assertEqual(build_bs_code('000001'), 'sz.000001')
        self.assertEqual(build_bs_code('300750'), 'sz.300750')

    def test_bj_baostock(self):
        """北交所 → bj.xxxxxx（关键！920/4/8开头）"""
        self.assertEqual(build_bs_code('920001'), 'bj.920001')
        self.assertEqual(build_bs_code('430001'), 'bj.430001')
        self.assertEqual(build_bs_code('830001'), 'bj.830001')


class TestNoHardcodedExchangeLogic(unittest.TestCase):
    """
    验证 data_engine.py 中不存在硬编码交易所判断逻辑。

    扫描 data_engine.py，确认：
    1. 不存在 "sh.{sym6}" 或 "sz.{sym6}" 的手写字符串拼接
    2. 不存在 startswith(("6","5","9","688")) 这样的手写分支
    """

    @classmethod
    def setUpClass(cls):
        de_path = Path(__file__).parent.parent / 'scripts' / 'data_engine.py'
        with open(de_path, encoding='utf-8') as f:
            cls.source = f.read()

    def test_no_manual_sh_prefix(self):
        """确认不存在手动拼接 sh. 前缀"""
        # 允许：'sh.' 在字符串字面量中（注释/docstring）
        # 禁止：f"sh.{sym6}" 或 "sh." + sym6 这样的代码
        import re
        # 匹配 f-string 或字符串拼接中的 sh. 引用
        pattern = re.compile(r'''(?:f["']sh\.\{|["']sh\.\s*\+|\+\s*["']sh\.)''')
        matches = pattern.findall(self.source)
        self.assertEqual(
            len(matches), 0,
            f"发现硬编码 sh. 前缀拼接：{matches[:3]}"
        )

    def test_no_manual_sz_prefix(self):
        """确认不存在手动拼接 sz. 前缀"""
        import re
        pattern = re.compile(r'''(?:f["']sz\.\{|["']sz\.\s*\+|\+\s*["']sz\.)''')
        matches = pattern.findall(self.source)
        self.assertEqual(
            len(matches), 0,
            f"发现硬编码 sz. 前缀拼接：{matches[:3]}"
        )

    def test_no_startswith_tuple_9(self):
        """确认不存在 startswith(('6','5','9','688')) 这样的手写分支"""
        import re
        # 匹配 startswith 调用中含 '9' 的情况
        pattern = re.compile(r"\.startswith\s*\(\s*\(.*'9'.*\)|\.startswith\s*\(\s*\(.*'688'.*\)", re.DOTALL)
        matches = pattern.findall(self.source)
        self.assertEqual(
            len(matches), 0,
            f"发现手写 startswith 含 '9'/'688' 分支：{matches[:2]}"
        )

    def test_uses_build_bs_code(self):
        """确认 baostock 相关函数调用了 build_bs_code"""
        import re
        # 只要有一处调用 build_bs_code 即可
        self.assertIn(
            'build_bs_code', self.source,
            "data_engine.py 应调用 exchange_mapping.build_bs_code()"
        )


# ─────────────────────────────────────────────────────────────────
# Task 2: get_active_stocks() PIT 查询
# ─────────────────────────────────────────────────────────────────
@unittest.skipUnless(HAS_DUCKDB, "requires duckdb")
class TestGetActiveStocks(unittest.TestCase):
    """get_active_stocks(trade_date) PIT 查询测试"""

    def setUp(self):
        from scripts.data_engine import DataEngine
        self.db_path = tempfile.mktemp(suffix='.duckdb')
        self.engine = DataEngine(db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _seed_stocks(self):
        """写入测试股票数据（通过 engine.execute 避免多连接冲突）

        eff_date 必须 <= 各测试查询日期，否则 PIT 查询（eff_date <= trade_date）
        会过滤掉所有记录。这里将 eff_date 设为与 list_date 相同，模拟上市当天
        即建立历史快照的场景。
        """
        test_data = [
            # ts_code, symbol, name, exchange, list_date, delist_date, is_delisted, eff_date
            ('000001.SZ', '000001', '平安银行', 'SZ', '1991-04-03', None, False, '1991-04-03'),
            ('600000.SH', '600000', '浦发银行', 'SH', '1999-11-10', None, False, '1999-11-10'),
            ('000002.SZ', '000002', '万科A', 'SZ', '1991-01-29', '2024-06-28', True, '1991-01-29'),
        ]
        for row in test_data:
            self.engine.execute("""
                INSERT INTO stock_basic_history
                (ts_code, symbol, name, exchange, list_date, delist_date, is_delisted, eff_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)

    def test_active_before_1990(self):
        """1990年：所有股票都未上市，返回空"""
        self._seed_stocks()
        result = self.engine.get_active_stocks('1990-01-01')
        self.assertEqual(result, [])

    def test_active_in_2000(self):
        """2000年：3只股票均已上市（万科退市日2024年）"""
        self._seed_stocks()
        result = self.engine.get_active_stocks('2000-01-01')
        self.assertEqual(set(result), {'000001.SZ', '600000.SH', '000002.SZ'})

    def test_active_after_wanke_delist(self):
        """2024-07-01：万科已退市（2024-06-28），不在可交易池"""
        self._seed_stocks()
        result = self.engine.get_active_stocks('2024-07-01')
        self.assertNotIn('000002.SZ', result,
            "退市股票不应出现在可交易池")
        self.assertEqual(set(result), {'000001.SZ', '600000.SH'})

    def test_returns_list_of_strings(self):
        """返回值必须是 List[str]（ts_code格式）"""
        self._seed_stocks()
        result = self.engine.get_active_stocks('2020-01-01')
        self.assertIsInstance(result, list)
        for item in result:
            self.assertIsInstance(item, str)
            self.assertIn('.', item)  # ts_code 格式


# ─────────────────────────────────────────────────────────────────
# Task 3: 指数成分股 out_date 更新
# ─────────────────────────────────────────────────────────────────
@unittest.skipUnless(HAS_DUCKDB, "requires duckdb")
class TestIndexConstituentsOutDate(unittest.TestCase):
    """index_constituents_history out_date 更新测试"""

    def setUp(self):
        from scripts.data_engine import DataEngine
        self.db_path = tempfile.mktemp(suffix='.duckdb')
        self.engine = DataEngine(db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _seed_constituents(self, date, stocks):
        """手动写入成分股快照（通过 engine.execute 避免多连接冲突）"""
        for s in stocks:
            self.engine.execute("""
                INSERT INTO index_constituents_history
                (index_code, ts_code, in_date, out_date, source)
                VALUES (?, ?, ?, NULL, 'test')
            """, ('000300.SH', s, date))

    def _get_all_ich(self):
        """读取全表"""
        return self.engine.query(
            "SELECT * FROM index_constituents_history ORDER BY ts_code"
        )

    def test_no_delete_on_resync(self):
        """验证：重新同步时绝对不能 DELETE 全表"""
        import re
        # 读取 sync_index_constituents 源码
        de_path = Path(__file__).parent.parent / 'scripts' / 'data_engine.py'
        with open(de_path, encoding='utf-8') as f:
            source = f.read()
        # 找 sync_index_constituents 方法的源码
        start = source.find('def sync_index_constituents(')
        end = source.find('\n    def ', start + 1)
        method_src = source[start:end]
        # 确认没有 DELETE 语句
        self.assertNotIn(
            'DELETE FROM index_constituents', method_src,
            "sync_index_constituents 不得使用 DELETE FROM index_constituents"
        )
        # 确认有 INSERT OR IGNORE
        self.assertIn(
            'INSERT OR IGNORE', method_src,
            "sync_index_constituents 应使用 INSERT OR IGNORE"
        )

    def test_out_date_update_pattern_exists(self):
        """验证：out_date 更新逻辑存在"""
        de_path = Path(__file__).parent.parent / 'scripts' / 'data_engine.py'
        with open(de_path, encoding='utf-8') as f:
            source = f.read()
        start = source.find('def sync_index_constituents(')
        end = source.find('\n    def ', start + 1)
        method_src = source[start:end]
        self.assertIn(
            'out_date', method_src,
            "sync_index_constituents 应更新 out_date"
        )

    def test_get_universe_pit_query(self):
        """验证：get_universe_at_date 支持 PIT 查询（in_date/out_date）"""
        # 写入：000001 在 2023-01-01 加入，2024-06-01 退出
        self.engine.execute("""
            INSERT INTO index_constituents_history
            (index_code, ts_code, in_date, out_date, source)
            VALUES ('000300.SH', '000001.SZ', '2023-01-01', '2024-06-01', 'test')
        """)

        # 查询 2023-06-01：000001 应在指数内
        df_2023 = self.engine.get_universe_at_date('000300.SH', '2023-06-01')
        self.assertIn('000001.SZ', df_2023,
            "2023年000001在指数内时应被返回")

        # 查询 2024-07-01：000001 已退出，不应返回
        df_2024 = self.engine.get_universe_at_date('000300.SH', '2024-07-01')
        self.assertNotIn('000001.SZ', df_2024,
            "退出后000001不应再被返回")


# ─────────────────────────────────────────────────────────────────
# Task 4: 日线双层写入验证
# ─────────────────────────────────────────────────────────────────
@unittest.skipUnless(HAS_DUCKDB, "requires duckdb")
class TestDailyBarDualLayer(unittest.TestCase):
    """daily_bar_raw vs daily_bar_adjusted 双层分离验证"""

    def setUp(self):
        from scripts.data_engine import DataEngine
        self.db_path = tempfile.mktemp(suffix='.duckdb')
        self.engine = DataEngine(db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_raw_and_adjusted_both_written(self):
        """验证：save_quotes 同时写入 raw 和 adjusted 两表"""
        import pandas as pd
        # 构造含 raw_*/adj_* 双字段的测试数据
        raw_open, adj_open = 10.0, 12.0   # 复权因子 1.2
        raw_close, adj_close = 11.0, 13.2

        test_df = pd.DataFrame([{
            'ts_code': '000001.SZ',
            'trade_date': pd.Timestamp('2024-01-02'),
            'raw_open': raw_open, 'raw_high': 11.5, 'raw_low': 9.8, 'raw_close': raw_close,
            'open': adj_open, 'high': 13.8, 'low': 11.76, 'close': adj_close,
            'pre_close': 9.9, 'volume': 1000000, 'amount': 11000000,
            'pct_chg': 10.0, 'turnover': 1.5,
            'adj_factor': 1.2,
            'is_suspend': False, 'limit_up': False, 'limit_down': False,
            'data_source': 'test',
        }])

        self.engine.save_quotes(test_df)

        # 读取 raw 表：close 应为原始价
        raw = self.engine.query("SELECT * FROM daily_bar_raw WHERE ts_code='000001.SZ'")
        self.assertEqual(len(raw), 1, "raw 表应有1条记录")
        self.assertAlmostEqual(raw.iloc[0]['close'], raw_close, places=4,
            msg="raw 表 close 应为原始价")

        # 读取 adjusted 表：close 应为复权价
        adj = self.engine.query("SELECT * FROM daily_bar_adjusted WHERE ts_code='000001.SZ'")
        self.assertEqual(len(adj), 1, "adjusted 表应有1条记录")
        self.assertAlmostEqual(adj.iloc[0]['close'], adj_close, places=4,
            msg="adjusted 表 close 应为复权价")

        # raw close ≠ adjusted close（验证分离）
        self.assertNotAlmostEqual(
            raw.iloc[0]['close'], adj.iloc[0]['close'], places=3,
            msg="raw 和 adjusted 的 close 应不同（否则没有真正分离）"
        )

    def test_adj_factor_preserved(self):
        """验证：adj_factor 保留在 adjusted 表中；raw 表 adj_factor = 1.0（原始价）"""
        import pandas as pd
        test_df = pd.DataFrame([{
            'ts_code': '000001.SZ',
            'trade_date': pd.Timestamp('2024-01-03'),
            'raw_open': 10.0, 'raw_high': 11.0, 'raw_low': 9.5, 'raw_close': 10.5,
            'open': 12.0, 'high': 13.2, 'low': 11.4, 'close': 12.6,
            'pre_close': 9.9, 'volume': 1000000, 'amount': 12000000,
            'pct_chg': 9.5, 'turnover': 1.4,
            'adj_factor': 1.2,
            'is_suspend': False, 'limit_up': False, 'limit_down': False,
            'data_source': 'test',
        }])

        self.engine.save_quotes(test_df)

        raw = self.engine.query("SELECT adj_factor FROM daily_bar_raw WHERE ts_code='000001.SZ'")
        adj = self.engine.query("SELECT adj_factor FROM daily_bar_adjusted WHERE ts_code='000001.SZ'")

        # raw 表存原始价，adj_factor = 1.0（raw层不做复权）
        self.assertAlmostEqual(raw.iloc[0]['adj_factor'], 1.0, places=4,
            msg="raw 表 adj_factor 应为 1.0（不复权）")
        # adjusted 表保留真实复权因子
        self.assertAlmostEqual(adj.iloc[0]['adj_factor'], 1.2, places=4,
            msg="adjusted 表 adj_factor 应为 1.2")

    def test_upsert_idempotent(self):
        """验证：重复写入同一数据（UPSERT）不产生重复行"""
        import pandas as pd
        test_df = pd.DataFrame([{
            'ts_code': '000001.SZ',
            'trade_date': pd.Timestamp('2024-01-04'),
            'raw_open': 10.0, 'raw_high': 11.0, 'raw_low': 9.5, 'raw_close': 10.5,
            'open': 12.0, 'high': 13.2, 'low': 11.4, 'close': 12.6,
            'pre_close': 9.9, 'volume': 1000000, 'amount': 12000000,
            'pct_chg': 9.5, 'turnover': 1.4,
            'adj_factor': 1.2,
            'is_suspend': False, 'limit_up': False, 'limit_down': False,
            'data_source': 'test',
        }])

        # 写入两次
        self.engine.save_quotes(test_df)
        self.engine.save_quotes(test_df)

        raw = self.engine.query("SELECT COUNT(*) as cnt FROM daily_bar_raw WHERE ts_code='000001.SZ'")
        adj = self.engine.query("SELECT COUNT(*) as cnt FROM daily_bar_adjusted WHERE ts_code='000001.SZ'")

        self.assertEqual(raw.iloc[0]['cnt'], 1, "raw 表不应有重复行")
        self.assertEqual(adj.iloc[0]['cnt'], 1, "adjusted 表不应有重复行")


if __name__ == '__main__':
    unittest.main()
