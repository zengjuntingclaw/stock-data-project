#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ground Truth 数据生成脚本
========================

交互式生成已确认正确的验证数据，用于：
1. 数据 QA 回归测试
2. 回测引擎验证
3. 复权因子正确性验证

Usage:
    python generate_ground_truth.py --interactive
    python generate_ground_truth.py --from-source akshare --date 2024-01-15
    python generate_ground_truth.py --verify-existing
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 添加项目路径
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.field_specs import FieldNames

# ============================================================================
# Ground Truth 配置
# ============================================================================

# 验证股票池（覆盖各板块）
VALIDATION_STOCKS = {
    '大盘': ['000001.SZ', '600000.SH'],      # 平安银行、浦发银行
    '小盘': ['002415.SZ', '300059.SZ'],       # 海康威视、东方财富
    '创业板': ['300750.SZ', '300014.SZ'],      # 宁德时代、亿纬锂能
    '科创板': ['688981.SH', '688111.SH'],     # 中芯国际、金山办公
    '北交所': ['832566.BJ', '430047.BJ'],     # 梓撞科技、诺思兰德
}

# 关键验证日期
KEY_DATES = {
    'normal': '2024-01-15',           # 正常交易日
    'ex_dividend': '2024-06-20',      # 除权除息日
    'limit': '2024-03-15',            # 涨跌停日
    'suspend': '2024-09-10',          # 停牌日
    'ipo': '2023-01-03',              # 上市首日（部分股票）
}

# 容差配置
TOLERANCE = {
    'price': 0.005,      # 价格 ±0.5%
    'adj_factor': 0.01,  # 复权因子 ±0.01
    'volume': 0.01,      # 成交量 ±1%
}


# ============================================================================
# Schema 定义
# ============================================================================

QUOTES_SCHEMA = pa.schema([
    ('ts_code', pa.string()),
    ('symbol', pa.string()),
    ('trade_date', pa.date32()),
    ('open', pa.float64()),
    ('high', pa.float64()),
    ('low', pa.float64()),
    ('close', pa.float64()),
    ('volume', pa.int64()),
    ('amount', pa.float64()),
    ('adj_factor', pa.float64()),
    ('source', pa.string()),
    ('verified_at', pa.date32()),
    ('verified_by', pa.string()),
    ('notes', pa.string()),
])

CORP_ACTIONS_SCHEMA = pa.schema([
    ('ts_code', pa.string()),
    ('ann_date', pa.date32()),
    ('ex_date', pa.date32()),
    ('div_type', pa.string()),
    ('div_amount', pa.float64()),
    ('split_ratio', pa.float64()),
    ('adj_factor_before', pa.float64()),
    ('adj_factor_after', pa.float64()),
    ('source', pa.string()),
    ('verified_at', pa.date32()),
    ('verified_by', pa.string()),
    ('notes', pa.string()),
])


# ============================================================================
# 数据生成器
# ============================================================================

class GroundTruthGenerator:
    """Ground Truth 数据生成器"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def fetch_from_akshare(self, ts_code: str, trade_date: str) -> Optional[Dict]:
        """从 AkShare 获取原始数据"""
        try:
            import akshare as ak
            
            symbol = ts_code.split('.')[0]
            
            # 尝试获取日线数据
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period='daily',
                start_date=trade_date.replace('-', ''),
                end_date=trade_date.replace('-', ''),
                adjust=''  # 不复权
            )
            
            if df.empty:
                return None
            
            row = df.iloc[0]
            return {
                'ts_code': ts_code,
                'symbol': symbol,
                'trade_date': pd.to_datetime(trade_date).date(),
                'open': float(row['开盘']),
                'high': float(row['最高']),
                'low': float(row['最低']),
                'close': float(row['收盘']),
                'volume': int(row['成交量']),
                'amount': float(row['成交额']),
                'adj_factor': 1.0,  # 需要手动核实
                'source': 'akshare',
                'verified_at': datetime.now().date(),
                'verified_by': 'script',
                'notes': '',
            }
            
        except Exception as e:
            print(f"  [ERROR] AkShare fetch failed for {ts_code}: {e}")
            return None
    
    def fetch_from_baostock(self, ts_code: str, trade_date: str) -> Optional[Dict]:
        """从 Baostock 获取原始数据（备选）"""
        try:
            import baostock as bs
            
            # 登录
            lg = bs.login()
            
            # 转换代码格式
            bs_code = f"sh.{ts_code.split('.')[0]}" if 'SH' in ts_code else f"sz.{ts_code.split('.')[0]}"
            
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date=trade_date,
                end_date=trade_date,
                frequency="d",
                adjustflag="3"  # 不复权
            )
            
            bs.logout()
            
            if rs.error_code != '0' or rs.next() is False:
                return None
            
            row = rs.get_row_data()
            return {
                'ts_code': ts_code,
                'symbol': ts_code.split('.')[0],
                'trade_date': pd.to_datetime(row[0]).date(),
                'open': float(row[1]) if row[1] else None,
                'high': float(row[2]) if row[2] else None,
                'low': float(row[3]) if row[3] else None,
                'close': float(row[4]) if row[4] else None,
                'volume': int(float(row[5])) if row[5] else 0,
                'amount': float(row[6]) if row[6] else 0,
                'adj_factor': 1.0,
                'source': 'baostock',
                'verified_at': datetime.now().date(),
                'verified_by': 'script',
                'notes': '',
            }
            
        except Exception as e:
            print(f"  [ERROR] Baostock fetch failed for {ts_code}: {e}")
            return None
    
    def interactive_verify(self, data: Dict) -> Dict:
        """交互式验证数据"""
        print(f"\n{'='*60}")
        print(f"股票: {data['ts_code']} ({data['symbol']})")
        print(f"日期: {data['trade_date']}")
        print(f"{'='*60}")
        print(f"  开盘: {data['open']}")
        print(f"  最高: {data['high']}")
        print(f"  最低: {data['low']}")
        print(f"  收盘: {data['close']}")
        print(f"  成交量: {data['volume']:,}")
        print(f"  复权因子: {data['adj_factor']}")
        print(f"  来源: {data['source']}")
        print(f"{'='*60}")
        
        response = input("确认数据正确？(y/n/s=跳过/e=编辑): ").strip().lower()
        
        if response == 'y':
            data['verified_by'] = 'manual'
            return data
        elif response == 'n':
            return None
        elif response == 's':
            return None
        elif response == 'e':
            # 编辑模式
            for key in ['open', 'high', 'low', 'close', 'volume', 'adj_factor']:
                new_val = input(f"  {key} [{data[key]}]: ").strip()
                if new_val:
                    data[key] = float(new_val) if key != 'volume' else int(new_val)
            data['verified_by'] = 'manual'
            return data
        else:
            return None
    
    def generate_quotes(self, stocks: Optional[List[str]] = None, 
                        dates: Optional[List[str]] = None,
                        interactive: bool = False) -> pd.DataFrame:
        """生成行情验证数据"""
        
        if stocks is None:
            stocks = [code for codes in VALIDATION_STOCKS.values() for code in codes]
        if dates is None:
            dates = list(KEY_DATES.values())
        
        records = []
        
        for ts_code in stocks:
            for trade_date in dates:
                print(f"\nFetching {ts_code} @ {trade_date}...")
                
                # 尝试 AkShare
                data = self.fetch_from_akshare(ts_code, trade_date)
                
                # 如果失败，尝试 Baostock
                if data is None:
                    print(f"  [WARN] AkShare failed, trying Baostock...")
                    data = self.fetch_from_baostock(ts_code, trade_date)
                
                if data is None:
                    print(f"  [SKIP] No data available")
                    continue
                
                # 交互式验证
                if interactive:
                    data = self.interactive_verify(data)
                    if data is None:
                        continue
                
                records.append(data)
        
        if not records:
            print("\n[WARN] No records generated")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        print(f"\n[INFO] Generated {len(df)} ground truth records")
        
        return df
    
    def save_quotes(self, df: pd.DataFrame):
        """保存行情验证数据"""
        if df.empty:
            return
        
        # 确保类型正确
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        df['verified_at'] = pd.to_datetime(df['verified_at']).dt.date
        
        output_path = self.output_dir / 'quotes.parquet'
        
        # 读取已有数据（如果存在）
        if output_path.exists():
            existing = pd.read_parquet(output_path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
        
        # 转换为 PyArrow Table 并保存
        table = pa.Table.from_pandas(df, schema=QUOTES_SCHEMA)
        pq.write_table(table, output_path)
        
        print(f"[INFO] Saved {len(df)} records to {output_path}")


# ============================================================================
# 主程序
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='生成 Ground Truth 验证数据')
    parser.add_argument('--interactive', action='store_true', 
                       help='交互式验证模式')
    parser.add_argument('--from-source', choices=['akshare', 'baostock'],
                       default='akshare', help='数据来源')
    parser.add_argument('--stocks', nargs='+', help='指定股票代码')
    parser.add_argument('--dates', nargs='+', help='指定日期')
    parser.add_argument('--verify-existing', action='store_true',
                       help='验证已有数据')
    
    args = parser.parse_args()
    
    output_dir = PROJECT_ROOT / 'validation' / 'ground_truth'
    generator = GroundTruthGenerator(output_dir)
    
    if args.verify_existing:
        # 验证已有数据
        print("Verifying existing ground truth data...")
        quotes_path = output_dir / 'quotes.parquet'
        if quotes_path.exists():
            df = pd.read_parquet(quotes_path)
            print(f"  Found {len(df)} existing records")
            print(f"  Stock count: {df['ts_code'].nunique()}")
            print(f"  Date range: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
        else:
            print("  No existing ground truth data found")
        return
    
    # 生成新数据
    df = generator.generate_quotes(
        stocks=args.stocks,
        dates=args.dates,
        interactive=args.interactive
    )
    
    if not df.empty:
        generator.save_quotes(df)


if __name__ == "__main__":
    main()
