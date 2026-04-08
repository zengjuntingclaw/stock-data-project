"""
数据一致性检查脚本
====================
用于验证A股历史交易数据的完整性和一致性。

检查项：
1. 交易所代码映射正确性
2. 字段命名一致性
3. 数据完整性（空值、缺失日期、重复记录）
4. 时间序列连续性
5. 价格合理性
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from loguru import logger

from scripts.exchange_mapping import classify_exchange, build_ts_code
from scripts.field_specs import FieldNames, get_required_market_data_fields


@dataclass
class DataQualityReport:
    """数据质量报告"""
    total_stocks: int = 0
    total_records: int = 0
    error_count: int = 0
    warning_count: int = 0
    
    # 详细问题列表
    errors: List[Dict] = None
    warnings: List[Dict] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    def add_error(self, category: str, symbol: str, message: str, details: Dict = None):
        """添加错误记录"""
        self.errors.append({
            'level': 'ERROR',
            'category': category,
            'symbol': symbol,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        })
        self.error_count += 1
    
    def add_warning(self, category: str, symbol: str, message: str, details: Dict = None):
        """添加警告记录"""
        self.warnings.append({
            'level': 'WARNING',
            'category': category,
            'symbol': symbol,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        })
        self.warning_count += 1
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'summary': {
                'total_stocks': self.total_stocks,
                'total_records': self.total_records,
                'error_count': self.error_count,
                'warning_count': self.warning_count,
                'pass_rate': f"{(1 - self.error_count / max(self.total_records, 1)):.2%}"
            },
            'errors': self.errors,
            'warnings': self.warnings
        }
    
    def print_report(self):
        """打印报告"""
        print("\n" + "=" * 80)
        print("数据质量检查报告")
        print("=" * 80)
        print(f"检查股票数: {self.total_stocks}")
        print(f"检查记录数: {self.total_records}")
        print(f"错误数: {self.error_count}")
        print(f"警告数: {self.warning_count}")
        print(f"通过率: {(1 - self.error_count / max(self.total_records, 1)):.2%}")
        
        if self.errors:
            print("\n错误详情:")
            print("-" * 80)
            for i, err in enumerate(self.errors[:20], 1):  # 只显示前20个
                print(f"{i}. [{err['category']}] {err['symbol']}: {err['message']}")
            if len(self.errors) > 20:
                print(f"... 还有 {len(self.errors) - 20} 个错误")
        
        if self.warnings:
            print("\n警告详情:")
            print("-" * 80)
            for i, warn in enumerate(self.warnings[:10], 1):  # 只显示前10个
                print(f"{i}. [{warn['category']}] {warn['symbol']}: {warn['message']}")
            if len(self.warnings) > 10:
                print(f"... 还有 {len(self.warnings) - 10} 个警告")
        
        print("=" * 80)


class DataConsistencyChecker:
    """数据一致性检查器"""
    
    def __init__(self):
        self.report = DataQualityReport()
    
    def check_exchange_mapping(self, df: pd.DataFrame) -> bool:
        """
        检查交易所代码映射正确性
        
        Parameters
        ----------
        df : pd.DataFrame
            包含symbol和ts_code的数据框
        
        Returns
        -------
        bool : 是否通过检查
        """
        passed = True
        
        for idx, row in df.iterrows():
            symbol = str(row.get(FieldNames.SYMBOL, '')).zfill(6)
            ts_code = row.get(FieldNames.TS_CODE, '')
            
            # 检查symbol是否能正确映射到ts_code
            expected_ts_code = build_ts_code(symbol)
            
            if ts_code != expected_ts_code:
                self.report.add_error(
                    'EXCHANGE_MAPPING',
                    symbol,
                    f"ts_code不匹配: 实际={ts_code}, 期望={expected_ts_code}",
                    {'actual': ts_code, 'expected': expected_ts_code}
                )
                passed = False
            
            # 检查北交所代码是否正确识别
            exchange, board = classify_exchange(symbol)
            if symbol.startswith('920') and exchange != 'BJ':
                self.report.add_error(
                    'EXCHANGE_MAPPING',
                    symbol,
                    f"920xxx代码未识别为北交所: {exchange}",
                    {'exchange': exchange, 'board': board}
                )
                passed = False
        
        return passed
    
    def check_field_consistency(self, df: pd.DataFrame) -> bool:
        """
        检查字段命名一致性
        
        Parameters
        ----------
        df : pd.DataFrame
            数据框
        
        Returns
        -------
        bool : 是否通过检查
        """
        passed = True
        required_fields = get_required_market_data_fields()
        
        # 检查必需字段
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            self.report.add_error(
                'FIELD_CONSISTENCY',
                'ALL',
                f"缺少必需字段: {missing_fields}"
            )
            passed = False
        
        # 检查旧字段名（警告级别）
        old_field_map = {
            'date': FieldNames.TRADE_DATE,
            'is_suspended': FieldNames.IS_SUSPEND,
            'code': FieldNames.SYMBOL,
        }
        
        for old_name, new_name in old_field_map.items():
            if old_name in df.columns and new_name not in df.columns:
                self.report.add_warning(
                    'FIELD_CONSISTENCY',
                    'ALL',
                    f"使用旧字段名 '{old_name}'，建议更新为 '{new_name}'"
                )
        
        return passed
    
    def check_data_completeness(self, df: pd.DataFrame) -> bool:
        """
        检查数据完整性
        
        Parameters
        ----------
        df : pd.DataFrame
            数据框
        
        Returns
        -------
        bool : 是否通过检查
        """
        passed = True
        
        # 检查空数据
        if df.empty:
            self.report.add_error('DATA_COMPLETENESS', 'ALL', "数据框为空")
            return False
        
        # 检查关键字段空值
        critical_fields = [FieldNames.SYMBOL, FieldNames.TRADE_DATE, 
                          FieldNames.OPEN, FieldNames.HIGH, FieldNames.LOW, FieldNames.CLOSE]
        
        for field in critical_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    self.report.add_error(
                        'DATA_COMPLETENESS',
                        'ALL',
                        f"字段 '{field}' 有 {null_count} 个空值"
                    )
                    passed = False
        
        # 检查重复记录
        if FieldNames.SYMBOL in df.columns and FieldNames.TRADE_DATE in df.columns:
            duplicates = df.duplicated(subset=[FieldNames.SYMBOL, FieldNames.TRADE_DATE]).sum()
            if duplicates > 0:
                self.report.add_error(
                    'DATA_COMPLETENESS',
                    'ALL',
                    f"发现 {duplicates} 条重复记录(symbol+date)"
                )
                passed = False
        
        return passed
    
    def check_price_validity(self, df: pd.DataFrame) -> bool:
        """
        检查价格合理性
        
        Parameters
        ----------
        df : pd.DataFrame
            数据框
        
        Returns
        -------
        bool : 是否通过检查
        """
        passed = True
        
        price_fields = [FieldNames.OPEN, FieldNames.HIGH, FieldNames.LOW, FieldNames.CLOSE]
        
        for field in price_fields:
            if field not in df.columns:
                continue
            
            # 检查负价格
            negative_prices = (df[field] < 0).sum()
            if negative_prices > 0:
                self.report.add_error(
                    'PRICE_VALIDITY',
                    'ALL',
                    f"字段 '{field}' 有 {negative_prices} 个负价格"
                )
                passed = False
            
            # 检查零价格（警告）
            zero_prices = (df[field] == 0).sum()
            if zero_prices > 0:
                self.report.add_warning(
                    'PRICE_VALIDITY',
                    'ALL',
                    f"字段 '{field}' 有 {zero_prices} 个零价格（可能是停牌）"
                )
        
        # 检查OHLC逻辑关系
        if all(f in df.columns for f in price_fields):
            invalid_ohlc = ((df[FieldNames.HIGH] < df[FieldNames.LOW]) |
                           (df[FieldNames.HIGH] < df[FieldNames.OPEN]) |
                           (df[FieldNames.HIGH] < df[FieldNames.CLOSE]) |
                           (df[FieldNames.LOW] > df[FieldNames.OPEN]) |
                           (df[FieldNames.LOW] > df[FieldNames.CLOSE])).sum()
            
            if invalid_ohlc > 0:
                self.report.add_error(
                    'PRICE_VALIDITY',
                    'ALL',
                    f"发现 {invalid_ohlc} 条OHLC逻辑错误记录"
                )
                passed = False
        
        return passed
    
    def check_date_continuity(self, df: pd.DataFrame, symbol: str) -> bool:
        """
        检查日期连续性
        
        Parameters
        ----------
        df : pd.DataFrame
            单只股票的数据框
        symbol : str
            股票代码
        
        Returns
        -------
        bool : 是否通过检查
        """
        passed = True
        
        if df.empty or FieldNames.TRADE_DATE not in df.columns:
            return passed
        
        # 排序
        df_sorted = df.sort_values(FieldNames.TRADE_DATE)
        dates = pd.to_datetime(df_sorted[FieldNames.TRADE_DATE])
        
        # 检查日期范围
        if len(dates) >= 2:
            date_range = (dates.max() - dates.min()).days
            trading_days = len(dates)
            
            # 如果数据跨度超过1年但交易日少于100天，警告
            if date_range > 365 and trading_days < 100:
                self.report.add_warning(
                    'DATE_CONTINUITY',
                    symbol,
                    f"数据跨度{date_range}天但只有{trading_days}个交易日"
                )
        
        return passed
    
    def run_all_checks(self, df: pd.DataFrame) -> DataQualityReport:
        """
        运行所有检查
        
        Parameters
        ----------
        df : pd.DataFrame
            待检查的数据框
        
        Returns
        -------
        DataQualityReport : 检查报告
        """
        self.report.total_records = len(df)
        
        if FieldNames.SYMBOL in df.columns:
            self.report.total_stocks = df[FieldNames.SYMBOL].nunique()
        
        logger.info(f"开始数据一致性检查: {self.report.total_stocks}只股票, {self.report.total_records}条记录")
        
        # 运行各项检查
        checks = [
            ('交易所映射', self.check_exchange_mapping),
            ('字段一致性', self.check_field_consistency),
            ('数据完整性', self.check_data_completeness),
            ('价格合理性', self.check_price_validity),
        ]
        
        for name, check_func in checks:
            try:
                result = check_func(df)
                logger.info(f"检查 '{name}': {'通过' if result else '未通过'}")
            except Exception as e:
                logger.error(f"检查 '{name}' 出错: {e}")
                self.report.add_error('CHECK_ERROR', 'ALL', f"{name}检查失败: {e}")
        
        # 按股票检查日期连续性
        if FieldNames.SYMBOL in df.columns:
            for symbol in df[FieldNames.SYMBOL].unique():
                symbol_df = df[df[FieldNames.SYMBOL] == symbol]
                self.check_date_continuity(symbol_df, symbol)
        
        logger.info(f"检查完成: {self.report.error_count}个错误, {self.report.warning_count}个警告")
        
        return self.report


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='A股历史数据一致性检查')
    parser.add_argument('--data-path', type=str, help='数据文件路径(CSV/Parquet)')
    parser.add_argument('--output', type=str, default='data_quality_report.json', 
                       help='报告输出路径')
    args = parser.parse_args()
    
    # 创建示例数据进行测试
    if not args.data_path:
        print("创建示例数据进行测试...")
        test_data = pd.DataFrame({
            FieldNames.SYMBOL: ['000001', '000001', '600000', '600000', '920001'],
            FieldNames.TS_CODE: ['000001.SZ', '000001.SZ', '600000.SH', '600000.SH', '920001.BJ'],
            FieldNames.TRADE_DATE: pd.date_range('2024-01-01', periods=5),
            FieldNames.OPEN: [10.0, 10.2, 15.0, 15.2, 5.0],
            FieldNames.HIGH: [10.5, 10.6, 15.5, 15.6, 5.2],
            FieldNames.LOW: [9.8, 10.0, 14.8, 15.0, 4.9],
            FieldNames.CLOSE: [10.2, 10.4, 15.2, 15.4, 5.1],
            FieldNames.VOLUME: [100000, 120000, 80000, 90000, 50000],
            FieldNames.AMOUNT: [1000000, 1250000, 1200000, 1380000, 255000],
            FieldNames.IS_SUSPEND: [False, False, False, False, False],
        })
        
        checker = DataConsistencyChecker()
        report = checker.run_all_checks(test_data)
        report.print_report()
        
        # 保存报告
        import json
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
        print(f"\n报告已保存到: {args.output}")
    else:
        # 加载实际数据
        print(f"加载数据: {args.data_path}")
        if args.data_path.endswith('.parquet'):
            df = pd.read_parquet(args.data_path)
        else:
            df = pd.read_csv(args.data_path)
        
        checker = DataConsistencyChecker()
        report = checker.run_all_checks(df)
        report.print_report()
        
        # 保存报告
        import json
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
        print(f"\n报告已保存到: {args.output}")


if __name__ == "__main__":
    main()
