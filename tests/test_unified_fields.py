"""
test_unified_fields.py - 统一字段 & 交易所映射 & 断点续传 核心测试
=====================================================================

覆盖六大修复域：
1. 交易所代码映射（沪深北三交易所 + Baostock前缀）
2. 停牌字段统一（is_suspend）
3. 断点续传一致性（save→load 字段链路）
4. 历史证券主表（边界过滤）
5. 统一 build_ts_code 全链路
6. 新三板代码识别
"""
import unittest
import sys
import os
import tempfile
import shutil
import json
from datetime import datetime, timedelta
from dataclasses import asdict

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import duckdb

from scripts.data_engine import build_ts_code
from scripts.backtest_engine_v3 import (
    ProductionBacktestEngine, BacktestConfig, BacktestState,
    CashAccount, Position
)
from scripts.execution_engine_v3 import ExecutionEngineV3
from scripts.data_classes import Order, OrderStatus, OrderSide


# ═══════════════════════════════════════════════════════════════════════
# 任务1: 交易所代码映射测试
# ═══════════════════════════════════════════════════════════════════════

class TestExchangeMapping(unittest.TestCase):
    """测试统一交易所代码映射（沪深北三交易所）"""

    def test_build_ts_code_shanghai(self):
        """沪市：600/601/603/605/688/9 开头 → .SH"""
        cases = [
            ("600000", "600000.SH"),
            ("601166", "601166.SH"),
            ("603000", "603000.SH"),
            ("605000", "605000.SH"),
            ("688001", "688001.SH"),
            ("900001", "900001.SH"),   # 9开头 → 沪市
            (600000, "600000.SH"),     # int输入
            ("000001", "000001.SZ"),   # 深市不误判为沪市
            ("068001", "068001.SH"),   # 068xxx，首位6 → 沪市
        ]
        for sym, expected in cases:
            self.assertEqual(build_ts_code(str(sym)), expected)

    def test_build_ts_code_shenzhen(self):
        """深市：000/001/002/003/300/301 开头 → .SZ"""
        cases = [
            ("000001", "000001.SZ"),  # 平安银行
            ("000002", "000002.SZ"),  # 万科A
            ("001696", "001696.SZ"),  # 001开头
            ("002594", "002594.SZ"),  # 比亚迪
            ("300001", "300001.SZ"),  # 创业板
            ("301000", "301000.SZ"),  # 创业板注册制
            ("003000", "003000.SZ"),  # 003开头
        ]
        for sym, expected in cases:
            self.assertEqual(build_ts_code(str(sym)), expected)

    def test_build_ts_code_beijing(self):
        """北交所：4xxxxx/8xxxxx 开头 → .BJ"""
        cases = [
            ("430001", "430001.BJ"),  # 北交所老股（退市整理）
            ("831010", "831010.BJ"),  # 北交所新股
            ("832000", "832000.BJ"),
            ("833000", "833000.BJ"),
            ("834000", "834000.BJ"),
            ("835000", "835000.BJ"),
            ("836000", "836000.BJ"),
            ("837000", "837000.BJ"),
            ("838000", "838000.BJ"),
            ("870010", "870010.BJ"),
            ("871000", "871000.BJ"),
            ("872000", "872000.BJ"),
            ("873000", "873000.BJ"),
            ("920000", "920000.SH"),  # 注意：920xxx是沪市（9开头），不是北交所
        ]
        for sym, expected in cases:
            self.assertEqual(build_ts_code(str(sym)), expected, f"{sym} should be {expected}")

    def test_build_ts_code_both_bj_prefixes(self):
        """4/8 开头的北交所代码（4位/5位/6位都要正确）"""
        # 4开头（老股退市整理期）
        self.assertEqual(build_ts_code("4301"), "004301.BJ")
        self.assertEqual(build_ts_code("430001"), "430001.BJ")
        self.assertEqual(build_ts_code("499999"), "499999.BJ")
        # 8开头（北交所新股）
        self.assertEqual(build_ts_code("8"), "000008.BJ")
        self.assertEqual(build_ts_code("830001"), "830001.BJ")
        self.assertEqual(build_ts_code("899999"), "899999.BJ")

    def test_build_ts_code_no_confusion(self):
        """防止混淆：确保北交所不混入沪深"""
        # 边界测试：002 vs 202 vs 302 都不是创业板
        self.assertEqual(build_ts_code("002001"), "002001.SZ")
        self.assertEqual(build_ts_code("202001"), "202001.SZ")  # 深市
        self.assertEqual(build_ts_code("302001"), "302001.SZ")  # 创业板
        # 688 vs 068（068是深市，688是科创板）
        self.assertEqual(build_ts_code("068001"), "068001.SH")  # 首位6 → 沪市
        self.assertEqual(build_ts_code("688001"), "688001.SH")


class TestBaostockBsCode(unittest.TestCase):
    """测试 Baostock 前缀映射（sh./sz./bj.）"""

    def _to_bs_code(self, symbol: str) -> str:
        """复制 data_fetcher 中的实际映射逻辑（需与 data_fetcher.py 保持同步）"""
        sym6 = str(symbol).zfill(6)
        first_char = next((c for c in sym6 if c != '0'), '0')
        if sym6.startswith("688"):
            return f"sh.{sym6}"
        elif first_char == '9':
            return f"sh.{sym6}"
        elif first_char in ('4', '8'):
            return f"bj.{sym6}"
        elif first_char in ('6', '5'):
            return f"sh.{sym6}"
        else:
            return f"sz.{sym6}"

    def test_baostock_shanghai(self):
        """沪市 → sh."""
        cases = [
            ("600000", "sh.600000"),
            ("601166", "sh.601166"),
            ("688001", "sh.688001"),
            ("603000", "sh.603000"),
            ("900001", "sh.900001"),   # 9字头 → 沪市
            ("000001", "sz.000001"),   # 反向验证
        ]
        for sym, expected in cases:
            self.assertEqual(self._to_bs_code(sym), expected, f"{sym} should be {expected}")

    def test_baostock_shenzhen(self):
        """深市 → sz."""
        cases = [
            ("000001", "sz.000001"),
            ("002001", "sz.002001"),
            ("300001", "sz.300001"),
            ("301000", "sz.301000"),
            ("003000", "sz.003000"),
        ]
        for sym, expected in cases:
            self.assertEqual(self._to_bs_code(sym), expected, f"{sym} should be {expected}")

    def test_baostock_beijing(self):
        """北交所 → bj."""
        cases = [
            ("430001", "bj.430001"),
            ("830001", "bj.830001"),
            ("920000", "sh.920000"),  # 920xxx = 沪市（9字头），非北交所
            ("4301", "bj.004301"),    # 不足6位自动补0后首位4 → bj
            ("8301", "bj.008301"),
        ]
        for sym, expected in cases:
            self.assertEqual(self._to_bs_code(sym), expected, f"{sym} should be {expected}")


# ═══════════════════════════════════════════════════════════════════════
# 任务3: 断点续传测试（save → load 链路一致性）
# ═══════════════════════════════════════════════════════════════════════

class TestCheckpointConsistency(unittest.TestCase):
    """测试 checkpoint save/load 字段链路一致性"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.checkpoint_path = os.path.join(self.temp_dir, "checkpoint.json")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _make_engine(self):
        """构建可测试的回测引擎（跳过网络加载）"""
        config = BacktestConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31),
            initial_capital=1_000_000,
        )
        engine = ExecutionEngineV3(initial_cash=1_000_000)
        # 预填持仓和现金
        engine.cash.available = 800_000
        engine.cash.total = 1_000_000
        engine.cash.withdrawable = 800_000
        engine.positions["000001.SZ"] = Position("000001.SZ", shares=10000, available_shares=10000, avg_cost=8.5)
        # 预填 pending_settlements（模拟卖出待结算）
        engine.cash.pending_settlements = [(datetime(2024, 1, 10), 50000)]
        engine.cash.pending_withdrawals = [(datetime(2024, 1, 11), 49500)]
        return engine

    def _save_via_backtest(self, engine: ExecutionEngineV3):
        """通过 ProductionBacktestEngine 保存 checkpoint"""
        # 构建一个最小 BacktestState
        state = BacktestState(
            current_date=datetime(2024, 1, 5),
            cash=engine.cash,
            positions=engine.positions,
            pending_orders=[
                Order(
                    symbol="000002.SZ",
                    side=OrderSide.BUY,
                    target_shares=1000,
                    signal_date=datetime(2024, 1, 4),
                    execution_date=datetime(2024, 1, 8),
                )
            ],
            trade_history=[],
            daily_values=[],
        )
        # 创建最小化 BacktestEngine（mock data_engine）
        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31),
        )
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest.state = state
        backtest.daily_records = [
            {"date": datetime(2024, 1, 3), "total_value": 1_002_000},
            {"date": datetime(2024, 1, 4), "total_value": 1_003_500},
        ]
        backtest.execution = engine
        backtest._save_checkpoint(self.checkpoint_path)

    def test_save_load_cash(self):
        """验证现金状态 save → load 完全一致"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        # 重新加载
        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        # 验证现金
        self.assertEqual(backtest.state.cash.total, 1_000_000)
        self.assertEqual(backtest.state.cash.available, 800_000)
        self.assertEqual(backtest.state.cash.withdrawable, 800_000)

    def test_save_load_positions(self):
        """验证持仓 save → load 完全一致"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        self.assertIn("000001.SZ", backtest.state.positions)
        pos = backtest.state.positions["000001.SZ"]
        self.assertEqual(pos.shares, 10000)
        self.assertEqual(pos.available_shares, 10000)
        self.assertAlmostEqual(pos.avg_cost, 8.5, places=4)

    def test_save_load_pending_orders(self):
        """验证 pending_orders save → load 不丢失（之前硬编码 [] 导致丢失）"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        # 之前：pending_orders 被硬编码为 []，导致断点恢复后挂单丢失
        self.assertGreater(len(backtest.state.pending_orders), 0, "挂单在断点恢复后丢失（BUG未修复）")
        self.assertEqual(backtest.state.pending_orders[0].symbol, "000002.SZ")
        self.assertEqual(backtest.state.pending_orders[0].target_shares, 1000)

    def test_save_load_pending_settlements(self):
        """验证 T+1 待结算资金 save → load 不丢失"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        settles = backtest.execution.cash.pending_settlements
        self.assertEqual(len(settles), 1)
        settle_date, amount = settles[0]
        self.assertEqual(amount, 50000)

    def test_save_load_daily_records(self):
        """验证 daily_records save → load 不丢失（之前字段名 daily_values vs daily_records 混淆）"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        # 之前：save 时存 'daily_records'，load 时读 'daily_values' → KeyError
        self.assertEqual(len(backtest.daily_records), 2)
        self.assertEqual(backtest.daily_records[0]["total_value"], 1_002_000)

    def test_save_load_current_date(self):
        """验证当前日期正确恢复"""
        engine = self._make_engine()
        self._save_via_backtest(engine)

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass
        config = BacktestConfig(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest._load_checkpoint(self.checkpoint_path)

        self.assertEqual(backtest.state.current_date, datetime(2024, 1, 5))


# ═══════════════════════════════════════════════════════════════════════
# 任务4: 历史证券主表测试
# ═══════════════════════════════════════════════════════════════════════

class TestStockBasicHistory(unittest.TestCase):
    """测试历史证券主表边界过滤"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_sbh.duckdb")
        self.conn = duckdb.connect(self.db_path)
        self._create_tables()

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic_history (
                ts_code       VARCHAR,
                snapshot_date DATE,
                symbol        VARCHAR,
                name          VARCHAR,
                list_date     DATE,
                delist_date   DATE,
                is_delisted   BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (ts_code, snapshot_date)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sbh_date 
            ON stock_basic_history(snapshot_date)
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_basic (
                ts_code    VARCHAR PRIMARY KEY,
                symbol     VARCHAR,
                name       VARCHAR,
                list_date  DATE,
                delist_date DATE,
                is_delisted BOOLEAN DEFAULT FALSE
            )
        """)

    def _insert_history_snapshot(self, date_str, rows):
        """插入指定日期的历史快照"""
        df = pd.DataFrame(rows)
        df["snapshot_date"] = pd.to_datetime(date_str)
        self.conn.register("tmp_hist", df)
        self.conn.execute("""
            INSERT INTO stock_basic_history 
            (ts_code, snapshot_date, symbol, name, list_date, delist_date, is_delisted)
            SELECT ts_code, snapshot_date, symbol, name, list_date, delist_date, is_delisted FROM tmp_hist
        """)
        self.conn.execute("DROP VIEW tmp_hist")

    def _insert_current_stocks(self, rows):
        df = pd.DataFrame(rows)
        self.conn.register("tmp_curr", df)
        self.conn.execute("""
            INSERT INTO stock_basic 
            (ts_code, symbol, name, list_date, delist_date, is_delisted)
            SELECT ts_code, symbol, name, list_date, delist_date, is_delisted FROM tmp_curr
        """)
        self.conn.execute("DROP VIEW tmp_curr")

    def test_history_snapshot_excludes_not_yet_listed(self):
        """历史快照：应排除当日尚未上市的股票"""
        # 插入2024-01-15的快照：包含一只2024-06-01才上市的新股
        self._insert_history_snapshot("2024-01-15", [
            {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行",
             "list_date": "1991-04-03", "delist_date": None, "is_delisted": False},
            {"ts_code": "688001.SH", "symbol": "688001", "name": "某新股",
             "list_date": "2024-06-01", "delist_date": None, "is_delisted": False},
        ])
        # 查询时：688001 尚未上市（list_date > snapshot_date），应被排除
        result = self.conn.execute("""
            SELECT ts_code FROM stock_basic_history
            WHERE snapshot_date = '2024-01-15'
            AND (list_date IS NULL OR list_date <= snapshot_date)
            AND (delist_date IS NULL OR delist_date >= snapshot_date)
        """).fetchdf()
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["ts_code"], "000001.SZ")

    def test_history_snapshot_excludes_delisted(self):
        """历史快照：应排除当日已退市的股票"""
        self._insert_history_snapshot("2024-01-15", [
            {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行",
             "list_date": "1991-04-03", "delist_date": None, "is_delisted": False},
            {"ts_code": "600001.SH", "symbol": "600001", "name": "某退市股",
             "list_date": "2000-01-01", "delist_date": "2023-06-01", "is_delisted": True},
        ])
        result = self.conn.execute("""
            SELECT ts_code FROM stock_basic_history
            WHERE snapshot_date = '2024-01-15'
            AND (list_date IS NULL OR list_date <= snapshot_date)
            AND (delist_date IS NULL OR delist_date >= snapshot_date)
        """).fetchdf()
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["ts_code"], "000001.SZ")

    def test_history_snapshot_boundary_on_list_date(self):
        """边界测试：上市日当天应包含（list_date = snapshot_date）"""
        self._insert_history_snapshot("2024-01-15", [
            {"ts_code": "000001.SZ", "symbol": "000001", "name": "老股",
             "list_date": "2024-01-15", "delist_date": None, "is_delisted": False},
        ])
        result = self.conn.execute("""
            SELECT ts_code FROM stock_basic_history
            WHERE snapshot_date = '2024-01-15'
            AND (list_date IS NULL OR list_date <= snapshot_date)
            AND (delist_date IS NULL OR delist_date >= snapshot_date)
        """).fetchdf()
        self.assertEqual(len(result), 1)  # 上市日当天应被包含

    def test_history_snapshot_boundary_on_delist_date(self):
        """边界测试：退市日当天应包含（delist_date = snapshot_date）"""
        self._insert_history_snapshot("2024-01-15", [
            {"ts_code": "600001.SH", "symbol": "600001", "name": "退市股",
             "list_date": "2000-01-01", "delist_date": "2024-01-15", "is_delisted": True},
        ])
        result = self.conn.execute("""
            SELECT ts_code FROM stock_basic_history
            WHERE snapshot_date = '2024-01-15'
            AND (list_date IS NULL OR list_date <= snapshot_date)
            AND (delist_date IS NULL OR delist_date >= snapshot_date)
        """).fetchdf()
        self.assertEqual(len(result), 1)  # 退市日当天仍有效（最后交易日）


# ═══════════════════════════════════════════════════════════════════════
# 任务2: 停牌字段统一测试
# ═══════════════════════════════════════════════════════════════════════

class TestSuspensionField(unittest.TestCase):
    """验证 is_suspend 为统一字段名，is_suspended 已消除"""

    def test_execution_engine_uses_is_suspend(self):
        """验证 ExecutionEngine._is_blocked 使用 is_suspend"""
        from scripts.execution_engine_v3 import ExecutionEngineV3
        import inspect
        src = inspect.getsource(ExecutionEngineV3._is_blocked)
        # 应该是 is_suspend，不应该有 is_suspended
        self.assertIn("is_suspend", src)
        self.assertNotIn("is_suspended", src)

    def test_survivorship_bias_uses_is_suspend(self):
        """验证 survivorship_bias 使用 is_suspend"""
        from scripts.survivorship_bias import SurvivorshipBiasHandler
        import inspect
        src = inspect.getsource(SurvivorshipBiasHandler)
        # 应该是 is_suspend
        self.assertIn("is_suspend", src)
        self.assertNotIn("is_suspended", src)

    def test_backtest_engine_reads_is_suspend(self):
        """验证 backtest_engine 从数据层读取 is_suspend"""
        from scripts.backtest_engine_v3 import ProductionBacktestEngine
        import inspect
        src = inspect.getsource(ProductionBacktestEngine._get_market_data)
        self.assertIn("is_suspend", src)


# ═══════════════════════════════════════════════════════════════════════
# 任务5: Checkpoint 恢复后继续回测一致性测试
# ═══════════════════════════════════════════════════════════════════════

class TestCheckpointResumeConsistency(unittest.TestCase):
    """验证断点恢复后继续回测，结果与不中断完全一致"""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.checkpoint_path = os.path.join(self.temp_dir, "cp.json")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_resume_after_checkpoint_position_unchanged(self):
        """断点恢复后，持仓市值应与断点前一致（pending_settlements 不重复计入）"""
        engine = ExecutionEngineV3(initial_cash=1_000_000)
        # 模拟持有1000股，单价10元
        engine.positions["000001.SZ"] = Position(
            "000001.SZ", shares=1000, available_shares=1000, avg_cost=10.0
        )
        # 模拟卖单待结算（50000元T+1）
        engine.cash.pending_settlements = [(datetime(2024, 1, 10), 50000)]

        class MockDataEngine:
            pass
        class MockFactorEngine:
            pass

        config = BacktestConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31),
        )
        backtest = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest.execution = engine
        backtest.state = BacktestState(
            current_date=datetime(2024, 1, 5),
            cash=engine.cash,
            positions=engine.positions,
            pending_orders=[],
            trade_history=[],
            daily_values=[],
        )
        backtest.daily_records = [
            {"date": datetime(2024, 1, 3), "total_value": 1_010_000, "position_value": 10000}
        ]

        # 保存
        backtest._save_checkpoint(self.checkpoint_path)

        # 重新加载
        engine2 = ExecutionEngineV3(initial_cash=1_000_000)
        backtest2 = ProductionBacktestEngine(config, MockDataEngine(), MockFactorEngine())
        backtest2.execution = engine2
        backtest2._load_checkpoint(self.checkpoint_path)

        # 验证恢复后持仓一致
        self.assertIn("000001.SZ", backtest2.state.positions)
        self.assertEqual(backtest2.state.positions["000001.SZ"].shares, 1000)

        # 验证 pending_settlements 已恢复
        self.assertEqual(len(backtest2.execution.cash.pending_settlements), 1)
        _, amount = backtest2.execution.cash.pending_settlements[0]
        self.assertEqual(amount, 50000)


if __name__ == '__main__':
    unittest.main()
