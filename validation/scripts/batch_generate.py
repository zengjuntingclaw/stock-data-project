#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量生成 Ground Truth 数据
从 AkShare 抓取 10 只股票 × 5 个关键日期的原始行情数据
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import akshare as ak

# 验证股票池
VALIDATION_STOCKS = {
    '大盘': ['000001', '600000'],
    '小盘': ['002415', '300059'],
    '创业板': ['300750', '300014'],
    '科创板': ['688981', '688111'],
    '北交所': ['832566', '430047'],
}

# ts_code 映射
TS_CODE_MAP = {
    '000001': '000001.SZ', '600000': '600000.SH',
    '002415': '002415.SZ', '300059': '300059.SZ',
    '300750': '300750.SZ', '300014': '300014.SZ',
    '688981': '688981.SH', '688111': '688111.SH',
    '832566': '832566.BJ', '430047': '430047.BJ',
}

# 关键日期
KEY_DATES = {
    'normal': '20240115',
    'ex_dividend': '20240620',
    'limit': '20240315',
    'suspend': '20240910',
    'ipo': '20230103',
}

# AkShare 列名映射
COL_MAP = {
    '日期': 'trade_date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount',
    '涨跌幅': 'pct_chg',
}

OUTPUT_DIR = Path(__file__).parent.parent / 'ground_truth'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_one(symbol: str, date_str: str) -> dict | None:
    """从 AkShare 获取单只股票单日数据"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol, period='daily',
            start_date=date_str, end_date=date_str,
            adjust=''  # 不复权
        )
        if df.empty:
            return None
        
        row = df.iloc[0]
        
        # 获取复权因子（用前复权价反推）
        df_qfq = ak.stock_zh_a_hist(
            symbol=symbol, period='daily',
            start_date=date_str, end_date=date_str,
            adjust='hfq'  # 后复权
        )
        
        adj_factor = 1.0
        if not df_qfq.empty:
            # adj_factor = 后复权价 / 原始价
            hfq_close = df_qfq.iloc[0]['收盘']
            raw_close = row['收盘']
            if raw_close and raw_close > 0:
                adj_factor = round(hfq_close / raw_close, 6)
        
        return {
            'ts_code': TS_CODE_MAP.get(symbol, f'{symbol}.SZ'),
            'symbol': symbol,
            'trade_date': row['日期'],
            'open': float(row['开盘']),
            'high': float(row['最高']),
            'low': float(row['最低']),
            'close': float(row['收盘']),
            'volume': int(row['成交量']),
            'amount': float(row['成交额']),
            'adj_factor': adj_factor,
            'pct_chg': float(row.get('涨跌幅', 0)),
            'source': 'akshare',
            'verified_at': datetime.now().strftime('%Y-%m-%d'),
            'verified_by': 'script',
            'notes': '',
        }
        
    except Exception as e:
        print(f'  [ERROR] {symbol} @ {date_str}: {e}')
        return None


def main():
    all_stocks = [(cat, sym) for cat, syms in VALIDATION_STOCKS.items() for sym in syms]
    all_dates = list(KEY_DATES.items())
    
    records = []
    total = len(all_stocks) * len(all_dates)
    count = 0
    success = 0
    fail = 0
    empty = 0
    
    print(f'Ground Truth 生成: {len(all_stocks)} 只股票 × {len(all_dates)} 个日期 = {total} 条')
    print('=' * 60)
    
    for cat, symbol in all_stocks:
        ts_code = TS_CODE_MAP.get(symbol, f'{symbol}.SZ')
        print(f'\n[{cat}] {ts_code}')
        
        for date_name, date_str in all_dates:
            count += 1
            print(f'  [{count}/{total}] {date_name} ({date_str})...', end=' ')
            
            data = fetch_one(symbol, date_str)
            
            if data is None:
                empty += 1
                print('EMPTY (该日无数据)')
                continue
            
            records.append(data)
            success += 1
            print(f'OK (close={data["close"]}, adj={data["adj_factor"]})')
    
    print(f'\n{"=" * 60}')
    print(f'完成: 成功 {success}, 无数据 {empty}, 失败 {fail}')
    
    if not records:
        print('[ERROR] 未生成任何记录')
        sys.exit(1)
    
    # 保存为 Parquet
    df = pd.DataFrame(records)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    
    output_path = OUTPUT_DIR / 'quotes.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)
    
    print(f'\n[SAVED] {output_path}')
    print(f'  Records: {len(df)}')
    print(f'  Stocks: {df["ts_code"].nunique()}')
    print(f'  Dates: {df["trade_date"].nunique()}')
    
    # 打印摘要
    print(f'\n{"=" * 60}')
    print('数据摘要:')
    print(df.groupby('ts_code').agg(
        dates=('trade_date', 'count'),
        close_range=('close', lambda x: f'{x.min():.2f}~{x.max():.2f}')
    ).to_string())


if __name__ == '__main__':
    main()
