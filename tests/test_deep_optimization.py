"""
深度优化整合测试
=================
测试7大任务的修复效果：
1. 交易所代码映射（含920xxx北交所）
2. 字段名统一（is_suspend等）
3. Checkpoint状态一致性
4. 历史证券主表
5. 数据完整性
6. 代码质量
7. 回测可用性
"""

import sys
import os
import unittest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np

# 导入新模块
from scripts.exchange_mapping import (
    classify_exchange, build_ts_code, build_bs_code, 
    get_price_limit_pct, get_lot_size, round_shares
)
from scripts.field_specs import (
    FieldNames, normalize_column_names, ensure_columns,
    standardize_df, get_required_market_data_fields
)
from scripts.security_master import (
    HistoricalSecurityMaster, StockLifetime, ListingStatus, DelistReason
)
from scripts.checkpoint_manager import (
    CheckpointManager, BacktestState, CashState, PositionState,
    OrderState, TradeState
)


# ============================================================================
# Task 1: 交易所代码映射测试
# ============================================================================

class TestExchangeMapping(unittest.TestCase):
    """任务1: 交易所代码映射测试"""
    
    def test_shanghai_main(self):
        """沪市主板: 600/601/603/605"""
        self.assertEqual(classify_exchange('600000'), ('SH', '主板'))
        self.assertEqual(classify_exchange('601318'), ('SH', '主板'))
        self.assertEqual(classify_exchange('603288'), ('SH', '主板'))
        self.assertEqual(classify_exchange('605117'), ('SH', '主板'))
    
    def test_shanghai_star(self):
        """科创板: 688xxx"""
        self.assertEqual(classify_exchange('688001'), ('SH', '科创板'))
        self.assertEqual(classify_exchange('688981'), ('SH', '科创板'))
    
    def test_shenzhen_main(self):
        """深市主板: 000/001/002"""
        self.assertEqual(classify_exchange('000001'), ('SZ', '主板'))
        self.assertEqual(classify_exchange('001289'), ('SZ', '主板'))
        self.assertEqual(classify_exchange('002415'), ('SZ', '主板'))
    
    def test_shenzhen_chinext(self):
        """创业板: 300/301"""
        self.assertEqual(classify_exchange('300001'), ('SZ', '创业板'))
        self.assertEqual(classify_exchange('301269'), ('SZ', '创业板'))
    
    def test_beijing_exchange_old(self):
        """北交所老股: 43xxxx"""
        self.assertEqual(classify_exchange('430001'), ('BJ', '北交所'))
        self.assertEqual(classify_exchange('430047'), ('BJ', '北交所'))
    
    def test_beijing_exchange_new(self):
        """北交所新股: 83xxxx/87xxxx"""
        self.assertEqual(classify_exchange('830001'), ('BJ', '北交所'))
        self.assertEqual(classify_exchange('832735'), ('BJ', '北交所'))
        self.assertEqual(classify_exchange('873001'), ('BJ', '北交所'))
        self.assertEqual(classify_exchange('873122'), ('BJ', '北交所'))
    
    def test_beijing_exchange_2024(self):
        """北交所2024新代码: 920xxx（关键测试！）"""
        # 这是最重要的测试：920xxx必须识别为北交所，不能误判为沪市
        self.assertEqual(classify_exchange('920001'), ('BJ', '北交所'))
        self.assertEqual(classify_exchange('920999'), ('BJ', '北交所'))
        
        # 验证ts_code和bs_code
        self.assertEqual(build_ts_code('920001'), '920001.BJ')
        self.assertEqual(build_bs_code('920001'), 'bj.920001')
    
    def test_ts_code_construction(self):
        """ts_code构造测试"""
        self.assertEqual(build_ts_code('600000'), '600000.SH')
        self.assertEqual(build_ts_code('000001'), '000001.SZ')
        self.assertEqual(build_ts_code('688001'), '688001.SH')
        self.assertEqual(build_ts_code('300001'), '300001.SZ')
        self.assertEqual(build_ts_code('920001'), '920001.BJ')
    
    def test_baostock_code(self):
        """Baostock代码格式"""
        self.assertEqual(build_bs_code('600000'), 'sh.600000')
        self.assertEqual(build_bs_code('000001'), 'sz.000001')
        self.assertEqual(build_bs_code('920001'), 'bj.920001')
    
    def test_price_limit(self):
        """涨跌停幅度"""
        self.assertEqual(get_price_limit_pct('600000'), 0.10)  # 主板10%
        self.assertEqual(get_price_limit_pct('688001'), 0.20)  # 科创板20%
        self.assertEqual(get_price_limit_pct('300001'), 0.20)  # 创业板20%
        self.assertEqual(get_price_limit_pct('920001'), 0.30)  # 北交所30%
    
    def test_lot_size(self):
        """交易单位"""
        self.assertEqual(get_lot_size('600000'), (100, 100))   # 主板100股起
        self.assertEqual(get_lot_size('688001'), (200, 1))     # 科创板200股起
        self.assertEqual(get_lot_size('920001'), (100, 1))     # 北交所100股起
    
    def test_share_rounding(self):
        """股数规整"""
        self.assertEqual(round_shares('600000', 550), 500)     # 主板规整到100
        self.assertEqual(round_shares('688001', 150), 0)       # 科创板不足200
        self.assertEqual(round_shares('688001', 250), 250)     # 科创板200起1股递增
        # 北交所100股起，1股递增，150股应该规整为150（满足最小100，按1股递增）
        self.assertEqual(round_shares('920001', 150), 150)     # 北交所100起1股递增


# ============================================================================
# Task 2: 字段名统一测试
# ============================================================================

class TestFieldNormalization(unittest.TestCase):
    """任务2: 字段名统一测试"""
    
    def test_suspend_field_unification(self):
        """停牌字段统一为is_suspend"""
        # 测试各种旧字段名映射到is_suspend
        test_cases = [
            {'is_suspended': True},
            {'suspended': False},
            {'suspend': True},
        ]
        
        for old_data in test_cases:
            df = pd.DataFrame([old_data])
            df_norm = normalize_column_names(df)
            self.assertIn(FieldNames.IS_SUSPEND, df_norm.columns)
    
    def test_date_field_unification(self):
        """日期字段统一为trade_date"""
        df = pd.DataFrame({'date': ['2024-01-01'], 'dt': ['2024-01-02']})
        df_norm = normalize_column_names(df)
        self.assertIn(FieldNames.TRADE_DATE, df_norm.columns)
    
    def test_symbol_field_unification(self):
        """代码字段统一为symbol"""
        df = pd.DataFrame({'code': ['000001'], 'stock_code': ['000002']})
        df_norm = normalize_column_names(df)
        self.assertIn(FieldNames.SYMBOL, df_norm.columns)
    
    def test_required_fields_ensure(self):
        """必需字段补全"""
        df = pd.DataFrame({
            FieldNames.SYMBOL: ['000001'],
            FieldNames.TRADE_DATE: ['2024-01-01'],
        })
        
        required = get_required_market_data_fields()
        df_full = ensure_columns(df, required)
        
        # 验证所有必需字段都存在
        for field in required:
            self.assertIn(field, df_full.columns)
        
        # 验证默认值
        self.assertFalse(df_full[FieldNames.IS_SUSPEND].iloc[0])
        self.assertEqual(df_full[FieldNames.VOLUME].iloc[0], 0)
    
    def test_full_standardization(self):
        """一站式标准化"""
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['000001'],
            'open': [10.0],
            'high': [10.5],
            'low': [9.8],
            'close': [10.2],
            'volume': [100000],
        })
        
        df_std = standardize_df(df)
        
        # 验证列名标准化
        self.assertIn(FieldNames.TRADE_DATE, df_std.columns)
        self.assertIn(FieldNames.SYMBOL, df_std.columns)
        self.assertIn(FieldNames.IS_SUSPEND, df_std.columns)
        
        # 验证类型
        self.assertIsInstance(df_std[FieldNames.IS_SUSPEND].iloc[0], (bool, np.bool_))


# ============================================================================
# Task 3: Checkpoint状态一致性测试
# ============================================================================

class TestCheckpointConsistency(unittest.TestCase):
    """任务3: Checkpoint状态一致性测试"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.manager = CheckpointManager(self.temp_dir)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_cash_state_serialization(self):
        """资金状态序列化/反序列化"""
        cash = CashState(
            total=1000000.0,
            available=500000.0,
            withdrawable=300000.0,
            frozen=200000.0,
            pending_settlements=[('2024-01-16', 100000.0), ('2024-01-17', 50000.0)],
            pending_withdrawals=[('2024-01-17', 80000.0)],
        )
        
        cash_dict = cash.to_dict()
        cash2 = CashState.from_dict(cash_dict)
        
        self.assertEqual(cash2.total, cash.total)
        self.assertEqual(cash2.available, cash.available)
        self.assertEqual(len(cash2.pending_settlements), 2)
        self.assertEqual(cash2.pending_settlements[0][0], '2024-01-16')
    
    def test_full_state_roundtrip(self):
        """完整状态往返测试"""
        state = BacktestState(
            current_date='2024-01-15T00:00:00',
            cash=CashState(
                total=1000000.0,
                available=500000.0,
                pending_settlements=[('2024-01-16', 100000.0)],
            ),
            positions={
                '000001': PositionState('000001', 1000, 500, 10.0),
            },
            pending_orders=[
                OrderState('000002', 'BUY', 500, 0, 'PENDING', '2024-01-14', '2024-01-15'),
            ],
            trade_history=[
                TradeState('T001', '000001', 'BUY', 1000, 10.0, '2024-01-10', 5.0, 0.0, 1.0),
            ],
            daily_records=[{'date': '2024-01-10', 'nav': 1000000}],
        )
        
        # 序列化/反序列化
        state_dict = state.to_dict()
        state2 = BacktestState.from_dict(state_dict)
        
        # 验证所有字段一致
        self.assertEqual(state2.current_date, state.current_date)
        self.assertEqual(state2.cash.total, state.cash.total)
        self.assertEqual(state2.positions['000001'].shares, 1000)
        self.assertEqual(len(state2.pending_orders), 1)
        self.assertEqual(len(state2.trade_history), 1)
    
    def test_checkpoint_save_load(self):
        """Checkpoint保存/加载一致性"""
        state = BacktestState(
            current_date='2024-01-15T00:00:00',
            cash=CashState(
                total=1000000.0,
                available=500000.0,
                pending_settlements=[('2024-01-16', 100000.0)],
                pending_withdrawals=[('2024-01-17', 50000.0)],
            ),
            positions={
                '000001': PositionState('000001', 1000, 500, 10.0),
                '600000': PositionState('600000', 2000, 2000, 15.0),
            },
            pending_orders=[],
            trade_history=[],
            daily_records=[],
        )
        
        # 保存
        path = self.manager.save(state, "test")
        
        # 加载
        loaded = self.manager.load(path)
        
        # 验证T+1/T+2状态完全一致
        self.assertEqual(loaded.cash.total, 1000000.0)
        self.assertEqual(loaded.cash.available, 500000.0)
        self.assertEqual(len(loaded.cash.pending_settlements), 1)
        self.assertEqual(len(loaded.cash.pending_withdrawals), 1)
        self.assertEqual(loaded.positions['000001'].available_shares, 500)
    
    def test_pickle_json_equivalence(self):
        """Pickle和JSON格式等价性"""
        state = BacktestState(
            current_date='2024-01-15T00:00:00',
            cash=CashState(total=1000000.0, available=500000.0),
            positions={'000001': PositionState('000001', 1000, 500, 10.0)},
            pending_orders=[],
            trade_history=[],
            daily_records=[],
        )
        
        # 保存（会同时生成json和pkl）
        json_path = self.manager.save(state, "equivalence_test")
        pkl_path = json_path.replace('.json', '.pkl')
        
        # 分别加载
        from_json = self.manager.load(json_path)
        from_pkl = self.manager.load(pkl_path)
        
        # 验证等价
        self.assertEqual(from_json.cash.total, from_pkl.cash.total)
        self.assertEqual(from_json.positions['000001'].shares, from_pkl.positions['000001'].shares)


# ============================================================================
# Task 4: 历史证券主表测试
# ============================================================================

class TestHistoricalSecurityMaster(unittest.TestCase):
    """任务4: 历史证券主表测试"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.master = HistoricalSecurityMaster(data_dir=self.temp_dir)
        
        # 添加测试股票
        test_stocks = [
            StockLifetime(
                symbol='000001', name='平安银行', ts_code='000001.SZ',
                exchange='SZ', board='主板',
                list_date=datetime(1991, 4, 3), delist_date=None,
            ),
            StockLifetime(
                symbol='600000', name='浦发银行', ts_code='600000.SH',
                exchange='SH', board='主板',
                list_date=datetime(1999, 11, 10), delist_date=None,
            ),
            StockLifetime(
                symbol='688001', name='华兴源创', ts_code='688001.SH',
                exchange='SH', board='科创板',
                list_date=datetime(2019, 7, 22), delist_date=None,
            ),
            StockLifetime(
                symbol='920001', name='北交所新股', ts_code='920001.BJ',
                exchange='BJ', board='北交所',
                list_date=datetime(2024, 1, 1), delist_date=None,
            ),
            StockLifetime(
                symbol='000002', name='退市股票', ts_code='000002.SZ',
                exchange='SZ', board='主板',
                list_date=datetime(2000, 1, 1), delist_date=datetime(2020, 6, 1),
                delist_reason=DelistReason.MANDATORY_FINANCIAL,
            ),
        ]
        
        for stock in test_stocks:
            self.master.add_stock(stock)
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_universe_on_date(self):
        """指定日期股票池查询"""
        # 2024年：不含已退市的000002
        universe = self.master.get_universe_on_date('2024-06-01', include_delisted=False)
        self.assertEqual(len(universe), 4)
        
        # 包含退市股
        universe_all = self.master.get_universe_on_date('2024-06-01', include_delisted=True)
        self.assertEqual(len(universe_all), 5)
    
    def test_tradable_filtering(self):
        """可交易股票筛选"""
        universe = self.master.get_universe_on_date('2024-06-01')
        tradable = universe[universe['is_tradable']]
        
        # 920001北交所应该可交易
        self.assertIn('920001', tradable['symbol'].tolist())
        # 000002已退市不可交易
        self.assertNotIn('000002', tradable['symbol'].tolist())
    
    def test_historical_universe_reconstruction(self):
        """历史股票池重建"""
        # 2010年：000001(1991上市)、600000(1999上市)、000002(2000上市)已上市
        # 688001(2019上市)、920001(2024上市)未上市
        universe_2010 = self.master.get_universe_on_date('2010-01-01')
        tradable_2010 = universe_2010[universe_2010['is_tradable']]
        # 3只已上市可交易（不含688001和920001）
        self.assertEqual(len(tradable_2010), 3)
        
        # 2020年：688001已上市，000002未退市
        universe_2020 = self.master.get_universe_on_date('2020-01-01')
        self.assertIn('688001', universe_2020['symbol'].tolist())
        self.assertIn('000002', universe_2020['symbol'].tolist())
    
    def test_stock_status_check(self):
        """个股状态检查"""
        # 退市前可交易
        status_before = self.master.check_stock_status('000002', '2019-01-01')
        self.assertTrue(status_before['is_tradable'])
        
        # 退市后不可交易
        status_after = self.master.check_stock_status('000002', '2021-01-01')
        self.assertFalse(status_after['is_tradable'])
        self.assertIn('Delisted', status_after['not_tradable_reason'])
    
    def test_beijing_exchange_2024_in_universe(self):
        """北交所2024代码在股票池中"""
        universe = self.master.get_universe_on_date('2024-06-01')
        stock_920 = universe[universe['symbol'] == '920001']
        
        self.assertEqual(len(stock_920), 1)
        self.assertEqual(stock_920.iloc[0]['exchange'], 'BJ')
        self.assertTrue(stock_920.iloc[0]['is_tradable'])


# ============================================================================
# Task 5: 数据完整性测试
# ============================================================================

class TestDataCompleteness(unittest.TestCase):
    """任务5: 数据完整性测试"""
    
    def test_required_fields_coverage(self):
        """必需字段覆盖"""
        required = get_required_market_data_fields()
        
        # 验证包含核心字段
        self.assertIn(FieldNames.SYMBOL, required)
        self.assertIn(FieldNames.TRADE_DATE, required)
        self.assertIn(FieldNames.OPEN, required)
        self.assertIn(FieldNames.HIGH, required)
        self.assertIn(FieldNames.LOW, required)
        self.assertIn(FieldNames.CLOSE, required)
        self.assertIn(FieldNames.VOLUME, required)
        self.assertIn(FieldNames.IS_SUSPEND, required)
    
    def test_field_types_defined(self):
        """字段类型定义"""
        from scripts.field_specs import FIELD_TYPES
        
        # 关键字段有类型定义
        self.assertEqual(FIELD_TYPES[FieldNames.IS_SUSPEND], 'bool')
        self.assertEqual(FIELD_TYPES[FieldNames.VOLUME], 'int')
        self.assertEqual(FIELD_TYPES[FieldNames.CLOSE], 'float')
    
    def test_data_standardization_preserves_values(self):
        """数据标准化保持值不变"""
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['000001'],
            'open': [10.0],
            'close': [10.5],
            'is_suspended': [True],
        })
        
        df_std = standardize_df(df)
        
        # 值应保持不变
        self.assertEqual(df_std[FieldNames.OPEN].iloc[0], 10.0)
        self.assertEqual(df_std[FieldNames.CLOSE].iloc[0], 10.5)
        self.assertEqual(df_std[FieldNames.IS_SUSPEND].iloc[0], True)


# ============================================================================
# Task 6 & 7: 集成测试
# ============================================================================

class TestIntegration(unittest.TestCase):
    """集成测试：验证各模块协同工作"""
    
    def test_end_to_end_workflow(self):
        """端到端工作流测试"""
        # 1. 创建证券主表
        master = HistoricalSecurityMaster(data_dir=None)
        master.add_stock(StockLifetime(
            symbol='920001', name='测试北交所', ts_code='920001.BJ',
            exchange='BJ', board='北交所',
            list_date=datetime(2024, 1, 1), delist_date=None,
        ))
        
        # 2. 获取股票池
        universe = master.get_universe_on_date('2024-06-01')
        self.assertEqual(universe.iloc[0]['exchange'], 'BJ')
        
        # 3. 构造标准化数据
        df = pd.DataFrame({
            'date': ['2024-06-01'],
            'code': ['920001'],
            'open': [10.0], 'high': [10.5], 'low': [9.8], 'close': [10.2],
            'volume': [100000],
        })
        df_std = standardize_df(df)
        
        # 4. 验证交易所映射正确
        exchange, board = classify_exchange(df_std[FieldNames.SYMBOL].iloc[0])
        self.assertEqual(exchange, 'BJ')
        
        # 5. 创建checkpoint状态
        state = BacktestState(
            current_date='2024-06-01T00:00:00',
            cash=CashState(total=1000000.0, available=1000000.0),
            positions={'920001': PositionState('920001', 1000, 1000, 10.0)},
            pending_orders=[],
            trade_history=[],
            daily_records=[],
        )
        
        # 6. 验证状态序列化
        state_dict = state.to_dict()
        self.assertEqual(state_dict['positions']['920001']['symbol'], '920001')


# ============================================================================
# 主程序
# ============================================================================

if __name__ == "__main__":
    # 运行所有测试
    unittest.main(verbosity=2)
