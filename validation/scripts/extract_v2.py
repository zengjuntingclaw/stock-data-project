#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从本地 DuckDB 提取 Ground Truth 数据（修正版）
使用数据库中实际存在的股票，覆盖5个板块
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

# 根据实际数据库调整验证股票池
VALIDATION_STOCKS = [
    ('大盘(沪)', '600000.SH', '浦发银行'),
    ('大盘(深)', '000001.SZ', '平安银行'),
    ('小盘',     '002002.SZ', '苏宁环球'),  # 002开头代表
    ('小盘',     '002013.SZ', '中航机电'),
    ('创业板',   '300023.SZ', '宝莱特'),
    ('创业板',   '300028.SZ', '金亚科技'),
    ('科创板',   '688000.SH', '华兴源创'),
    ('科创板',   '688001.SH', '华兴源创2'),
    ('中盘',     '600519.SH', '贵州茅台'),   # 高价值标的
    ('中盘',     '000858.SZ', '五粮液'),
]

# 关键日期（微调确保覆盖不同市场状态）
KEY_DATES = {
    'normal': '2024-01-15',       # 正常交易日
    'ex_dividend': '2024-06-20',  # 除权除息日附近
    'volatile': '2024-03-15',     # 波动日
    'trend': '2024-09-10',        # 趋势日
    'year_start': '2023-01-03',   # 年初交易日
}


def main():
    print('Ground Truth 数据提取（DuckDB 本地）')
    print('=' * 60)
    
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    # 先查看总行数
    count = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
    print(f'daily_bar_raw: {count:,} 行')
    
    # 提取
    records = []
    
    for cat, ts_code, name in VALIDATION_STOCKS:
        print(f'\n[{cat}] {ts_code} ({name})')
        
        for date_name, date_str in KEY_DATES.items():
            try:
                row = conn.execute(f"""
                    SELECT ts_code, symbol, trade_date, 
                           open, high, low, close, volume, amount, adj_factor
                    FROM daily_bar_raw 
                    WHERE ts_code = '{ts_code}' 
                    AND trade_date = '{date_str}'
                """).fetchone()
                
                if row is None:
                    print(f'  {date_name}: SKIP (no data)')
                    continue
                
                # 验证 adj_factor 自洽：close_raw * adj_factor ≈ close_adj
                adj_row = conn.execute(f"""
                    SELECT close
                    FROM daily_bar_adjusted 
                    WHERE ts_code = '{ts_code}' 
                    AND trade_date = '{date_str}'
                """).fetchone()
                
                close_adj = adj_row[0] if adj_row else None
                expected_adj = row[6] * row[9] if close_adj else None
                adj_match = abs(close_adj - expected_adj) < 0.01 if close_adj and expected_adj else None
                
                record = {
                    'ts_code': row[0],
                    'symbol': row[1],
                    'trade_date': str(row[2]),
                    'open': row[3],
                    'high': row[4],
                    'low': row[5],
                    'close': row[6],
                    'volume': int(row[7]) if row[7] else 0,
                    'amount': row[8],
                    'adj_factor': row[9],
                    'close_adj': close_adj,
                    'adj_selfcheck': adj_match,
                    'category': cat,
                    'name': name,
                    'date_type': date_name,
                    'source': 'duckdb_local',
                    'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'notes': '',
                }
                
                records.append(record)
                chk = 'OK' if adj_match else 'MISMATCH' if adj_match is not None else 'N/A'
                print(f'  {date_name}: close={row[6]:8.2f} adj_factor={row[9]:.6f} adj_close={close_adj} selfcheck={chk}')
                
            except Exception as e:
                print(f'  {date_name}: ERROR - {e}')
    
    conn.close()
    
    if not records:
        print('\n[ERROR] No records extracted')
        sys.exit(1)
    
    # 保存
    df = pd.DataFrame(records)
    output_path = OUTPUT_DIR / 'quotes.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    
    print(f'\n{"=" * 60}')
    print(f'Saved: {output_path}')
    print(f'Records: {len(df)}')
    print(f'Stocks: {df["ts_code"].nunique()}')
    print(f'Dates: {df["trade_date"].nunique()}')
    
    # 自洽性统计
    if 'adj_selfcheck' in df.columns:
        ok_count = df['adj_selfcheck'].sum()
        total = df['adj_selfcheck'].notna().sum()
        print(f'adj_factor selfcheck: {ok_count}/{total} OK')
    
    # 打印完整表
    print(f'\n{"=" * 60}')
    print('Full Data Table:')
    print(df[['ts_code', 'name', 'date_type', 'close', 'adj_factor', 'close_adj', 'adj_selfcheck']].to_string(index=False))


if __name__ == '__main__':
    main()
