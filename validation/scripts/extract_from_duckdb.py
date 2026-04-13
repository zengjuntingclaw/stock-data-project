#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从本地 DuckDB 提取 Ground Truth 数据
绕过网络问题，直接使用已有数据
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

PROJECT_ROOT = Path(r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
DB_PATH = PROJECT_ROOT / 'data' / 'stock_data.duckdb'
OUTPUT_DIR = PROJECT_ROOT / 'validation' / 'ground_truth'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 验证股票池
VALIDATION_STOCKS = [
    ('大盘', '000001.SZ', '平安银行'),
    ('大盘', '600000.SH', '浦发银行'),
    ('小盘', '002415.SZ', '海康威视'),
    ('小盘', '300059.SZ', '东方财富'),
    ('创业板', '300750.SZ', '宁德时代'),
    ('创业板', '300014.SZ', '亿纬锂能'),
    ('科创板', '688981.SH', '中芯国际'),
    ('科创板', '688111.SH', '金山办公'),
]

# 关键日期
KEY_DATES = {
    'normal': '2024-01-15',       # 正常交易日
    'ex_dividend': '2024-06-20',  # 除权除息日
    'limit': '2024-03-15',        # 涨跌停日
    'suspend': '2024-09-10',      # 停牌日
    'ipo': '2023-01-03',          # 年初交易日
}


def main():
    print(f'从本地 DuckDB 提取 Ground Truth 数据')
    print(f'DB: {DB_PATH}')
    print('=' * 60)
    
    if not DB_PATH.exists():
        print(f'[ERROR] 数据库不存在: {DB_PATH}')
        sys.exit(1)
    
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    # 查看表结构
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f'可用表: {[t[0] for t in tables]}')
    
    # 检查 daily_bar_raw 表是否存在
    raw_exists = any(t[0] == 'daily_bar_raw' for t in tables)
    adj_exists = any(t[0] == 'daily_bar_adjusted' for t in tables)
    print(f'daily_bar_raw: {raw_exists}, daily_bar_adjusted: {adj_exists}')
    
    if not raw_exists:
        print('[ERROR] daily_bar_raw 表不存在')
        conn.close()
        sys.exit(1)
    
    # 查看总行数
    count = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
    print(f'daily_bar_raw 总行数: {count:,}')
    
    # 提取验证数据
    records = []
    
    for cat, ts_code, name in VALIDATION_STOCKS:
        print(f'\n[{cat}] {ts_code} ({name})')
        
        for date_name, date_str in KEY_DATES.items():
            try:
                # 从 daily_bar_raw 提取
                row = conn.execute(f"""
                    SELECT ts_code, symbol, trade_date, 
                           open, high, low, close, volume, amount,
                           adj_factor
                    FROM daily_bar_raw 
                    WHERE ts_code = '{ts_code}' 
                    AND trade_date = '{date_str}'
                """).fetchone()
                
                if row is None:
                    print(f'  {date_name} ({date_str}): 无数据')
                    continue
                
                # 从 daily_bar_adjusted 提取复权价
                adj_row = None
                if adj_exists:
                    adj_row = conn.execute(f"""
                        SELECT open, high, low, close
                        FROM daily_bar_adjusted 
                        WHERE ts_code = '{ts_code}' 
                        AND trade_date = '{date_str}'
                    """).fetchone()
                
                record = {
                    'ts_code': row[0],
                    'symbol': row[1],
                    'trade_date': row[2],
                    'open_raw': row[3],
                    'high_raw': row[4],
                    'low_raw': row[5],
                    'close_raw': row[6],
                    'volume': row[7],
                    'amount': row[8],
                    'adj_factor': row[9],
                    'open_adj': adj_row[0] if adj_row else None,
                    'high_adj': adj_row[1] if adj_row else None,
                    'low_adj': adj_row[2] if adj_row else None,
                    'close_adj': adj_row[3] if adj_row else None,
                    'category': cat,
                    'name': name,
                    'date_type': date_name,
                    'source': 'duckdb_local',
                    'verified_at': datetime.now().strftime('%Y-%m-%d'),
                    'verified_by': 'script_duckdb',
                    'notes': '',
                }
                
                records.append(record)
                print(f'  {date_name} ({date_str}): close_raw={row[6]:.2f}, adj_factor={row[9]:.6f}, close_adj={adj_row[3] if adj_row else "N/A"}')
                
            except Exception as e:
                print(f'  {date_name} ({date_str}): ERROR - {e}')
    
    conn.close()
    
    if not records:
        print('\n[ERROR] 未提取到任何数据')
        sys.exit(1)
    
    # 保存
    df = pd.DataFrame(records)
    output_path = OUTPUT_DIR / 'quotes.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    
    print(f'\n{"=" * 60}')
    print(f'保存到: {output_path}')
    print(f'总记录: {len(df)}')
    print(f'股票数: {df["ts_code"].nunique()}')
    print(f'日期数: {df["trade_date"].nunique()}')
    
    # 打印摘要
    print(f'\n数据摘要:')
    for _, row in df.iterrows():
        print(f'  {row["ts_code"]:12s} {row["date_type"]:12s} close={row["close_raw"]:8.2f} adj={row["adj_factor"]:.6f}')


if __name__ == '__main__':
    main()
