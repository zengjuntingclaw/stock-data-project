"""
数据字段规范 - 全链路统一命名
==============================
本模块定义数据层、回测层、输出层的统一字段名规范。

设计原则：
1. 单一真相源：所有字段名在此定义，禁止硬编码
2. 向后兼容：支持旧字段名自动映射到新字段名
3. 类型安全：每个字段都有明确的类型定义
4. 文档内嵌：字段含义直接在代码中说明
"""

from typing import Dict, List, Set, Any, Callable
from dataclasses import dataclass
from enum import Enum


# ============================================================================
# 核心字段名定义（权威来源）
# ============================================================================

class FieldNames:
    """标准字段名常量类"""
    
    # === 股票标识 ===
    SYMBOL = "symbol"           # 6位数字代码（如 '000001'）
    TS_CODE = "ts_code"         # 带后缀代码（如 '000001.SZ'）
    NAME = "name"               # 股票名称
    EXCHANGE = "exchange"       # 交易所代码（SH/SZ/BJ）
    BOARD = "board"             # 板块（主板/科创板/创业板/北交所）
    
    # === 日期字段 ===
    TRADE_DATE = "trade_date"   # 交易日期（标准名称）
    LIST_DATE = "list_date"     # 上市日期
    DELIST_DATE = "delist_date" # 退市日期
    ANN_DATE = "ann_date"       # 公告日期（财务数据）
    
    # === 价格字段 ===
    OPEN = "open"               # 开盘价
    HIGH = "high"               # 最高价
    LOW = "low"                 # 最低价
    CLOSE = "close"             # 收盘价
    PRE_CLOSE = "pre_close"     # 昨收价（注意：不是prev_close）
    VWAP = "vwap"               # 成交均价
    
    # === 成交量额 ===
    VOLUME = "volume"           # 成交量（股）
    AMOUNT = "amount"           # 成交额（元）
    TURNOVER = "turnover"       # 换手率
    AVG_VOLUME_20 = "avg_volume_20"  # 20日均量
    
    # === 复权因子 ===
    ADJ_FACTOR = "adj_factor"   # 复权因子
    ADJ_CLOSE = "adj_close"     # 后复权收盘价
    
    # === 涨跌停 ===
    LIMIT_UP = "limit_up"       # 涨停价
    LIMIT_DOWN = "limit_down"   # 跌停价
    IS_LIMIT_UP = "is_limit_up"     # 是否涨停
    IS_LIMIT_DOWN = "is_limit_down" # 是否跌停
    
    # === 交易状态（关键：统一使用is_suspend） ===
    IS_SUSPEND = "is_suspend"   # 是否停牌（标准名称，禁止用is_suspended）
    IS_ST = "is_st"             # 是否ST
    IS_DELISTED = "is_delisted" # 是否已退市
    IS_TRADABLE = "is_tradable" # 是否可交易（综合判断）
    
    # === 财务指标 ===
    PE_TTM = "pe_ttm"           # 滚动市盈率
    PB = "pb"                   # 市净率
    PS_TTM = "ps_ttm"           # 滚动市销率
    PCF_TTM = "pcf_ttm"         # 滚动市现率
    TOTAL_MV = "total_mv"       # 总市值
    CIRC_MV = "circ_mv"         # 流通市值
    
    # === 财务数据 ===
    REVENUE = "revenue"         # 营业收入
    NET_PROFIT = "net_profit"   # 净利润
    ROE = "roe"                 # 净资产收益率
    ROA = "roa"                 # 总资产收益率
    GROSS_MARGIN = "gross_margin"   # 毛利率
    NET_MARGIN = "net_margin"       # 净利率
    EPS = "eps"                 # 每股收益
    BPS = "bps"                 # 每股净资产
    
    # === 衍生指标 ===
    PCT_CHG = "pct_chg"         # 涨跌幅
    CHANGE = "change"           # 涨跌额
    AMPLITUDE = "amplitude"     # 振幅


# ============================================================================
# 字段别名映射（向后兼容）
# ============================================================================

# 旧字段名 -> 新字段名
FIELD_ALIASES: Dict[str, str] = {
    # 日期字段
    "date": FieldNames.TRADE_DATE,
    "dt": FieldNames.TRADE_DATE,
    "trade_dt": FieldNames.TRADE_DATE,
    "trading_date": FieldNames.TRADE_DATE,
    
    # 停牌字段（关键：统一映射到is_suspend）
    "is_suspended": FieldNames.IS_SUSPEND,
    "suspended": FieldNames.IS_SUSPEND,
    "suspend": FieldNames.IS_SUSPEND,
    "is_trading": FieldNames.IS_SUSPEND,  # 逻辑取反，需要特殊处理
    
    # 价格字段
    "prev_close": FieldNames.PRE_CLOSE,
    "previous_close": FieldNames.PRE_CLOSE,
    "last_close": FieldNames.PRE_CLOSE,
    
    # 成交量额
    "vol": FieldNames.VOLUME,
    "turnover_vol": FieldNames.VOLUME,
    "turnover_amount": FieldNames.AMOUNT,
    "turnover_rate": FieldNames.TURNOVER,
    
    # 标识字段
    "code": FieldNames.SYMBOL,
    "stock_code": FieldNames.SYMBOL,
    "stock_symbol": FieldNames.SYMBOL,
}


# ============================================================================
# 字段类型定义
# ============================================================================

FIELD_TYPES: Dict[str, str] = {
    # 字符串
    FieldNames.SYMBOL: "str",
    FieldNames.TS_CODE: "str",
    FieldNames.NAME: "str",
    FieldNames.EXCHANGE: "str",
    FieldNames.BOARD: "str",
    
    # 日期
    FieldNames.TRADE_DATE: "date",
    FieldNames.LIST_DATE: "date",
    FieldNames.DELIST_DATE: "date",
    FieldNames.ANN_DATE: "date",
    
    # 数值（价格）
    FieldNames.OPEN: "float",
    FieldNames.HIGH: "float",
    FieldNames.LOW: "float",
    FieldNames.CLOSE: "float",
    FieldNames.PRE_CLOSE: "float",
    FieldNames.VWAP: "float",
    FieldNames.LIMIT_UP: "float",
    FieldNames.LIMIT_DOWN: "float",
    
    # 数值（量额）
    FieldNames.VOLUME: "int",
    FieldNames.AMOUNT: "float",
    FieldNames.TURNOVER: "float",
    FieldNames.AVG_VOLUME_20: "float",
    
    # 数值（因子）
    FieldNames.ADJ_FACTOR: "float",
    FieldNames.ADJ_CLOSE: "float",
    
    # 布尔
    FieldNames.IS_SUSPEND: "bool",
    FieldNames.IS_ST: "bool",
    FieldNames.IS_DELISTED: "bool",
    FieldNames.IS_TRADABLE: "bool",
    FieldNames.IS_LIMIT_UP: "bool",
    FieldNames.IS_LIMIT_DOWN: "bool",
    
    # 数值（财务）
    FieldNames.PE_TTM: "float",
    FieldNames.PB: "float",
    FieldNames.PS_TTM: "float",
    FieldNames.PCF_TTM: "float",
    FieldNames.TOTAL_MV: "float",
    FieldNames.CIRC_MV: "float",
    FieldNames.REVENUE: "float",
    FieldNames.NET_PROFIT: "float",
    FieldNames.ROE: "float",
    FieldNames.ROA: "float",
    FieldNames.GROSS_MARGIN: "float",
    FieldNames.NET_MARGIN: "float",
    FieldNames.EPS: "float",
    FieldNames.BPS: "float",
    
    # 数值（衍生）
    FieldNames.PCT_CHG: "float",
    FieldNames.CHANGE: "float",
    FieldNames.AMPLITUDE: "float",
}


# ============================================================================
# 核心函数
# ============================================================================

def normalize_column_names(df, inplace=False, strict=False):
    """
    标准化DataFrame列名（旧字段名映射到新字段名）
    
    Parameters
    ----------
    df : pd.DataFrame
        输入数据框
    inplace : bool
        是否原地修改
    strict : bool
        是否严格模式（遇到未知字段报错）
    
    Returns
    -------
    pd.DataFrame : 标准化后的数据框
    
    Examples
    --------
    >>> df = pd.DataFrame({'date': ['2024-01-01'], 'is_suspended': [True]})
    >>> df = normalize_column_names(df)
    >>> list(df.columns)
    ['trade_date', 'is_suspend']
    """
    import pandas as pd
    
    if not inplace:
        df = df.copy()
    
    # 列名映射
    rename_map = {}
    unknown_cols = []
    
    for col in df.columns:
        if col in FIELD_ALIASES:
            new_name = FIELD_ALIASES[col]
            # 特殊处理：is_trading 需要逻辑取反
            if col == "is_trading":
                df[col] = ~df[col].astype(bool)
            rename_map[col] = new_name
        elif col not in FIELD_TYPES and col not in vars(FieldNames).values():
            unknown_cols.append(col)
    
    if rename_map:
        df = df.rename(columns=rename_map)
    
    if unknown_cols and strict:
        raise ValueError(f"Unknown columns: {unknown_cols}")
    
    return df


def ensure_columns(df, required: List[str], fill_value=None):
    """
    确保DataFrame包含必需的列，缺失则填充默认值
    
    Parameters
    ----------
    df : pd.DataFrame
        输入数据框
    required : List[str]
        必需字段列表
    fill_value : any
        缺失字段的填充值（None时根据类型自动推断）
    
    Returns
    -------
    pd.DataFrame : 补全后的数据框
    """
    import pandas as pd
    import numpy as np
    
    df = df.copy()
    
    for col in required:
        if col not in df.columns:
            # 根据字段类型推断默认值
            if fill_value is not None:
                default = fill_value
            elif col in FIELD_TYPES:
                type_str = FIELD_TYPES[col]
                if type_str == "bool":
                    default = False
                elif type_str == "int":
                    default = 0
                elif type_str == "float":
                    default = np.nan
                elif type_str == "date":
                    default = pd.NaT
                else:
                    default = None
            else:
                default = None
            
            df[col] = default
    
    return df


def validate_columns(df, required: List[str], raise_error: bool = True) -> bool:
    """
    验证DataFrame是否包含必需的列
    
    Parameters
    ----------
    df : pd.DataFrame
        输入数据框
    required : List[str]
        必需字段列表
    raise_error : bool
        缺失时是否抛出异常
    
    Returns
    -------
    bool : 验证是否通过
    """
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        if raise_error:
            raise ValueError(f"Missing required columns: {missing}")
        return False
    
    return True


def get_required_market_data_fields() -> List[str]:
    """
    获取回测必需的市场数据字段
    
    Returns
    -------
    List[str] : 必需字段列表
    """
    return [
        FieldNames.SYMBOL,
        FieldNames.TRADE_DATE,
        FieldNames.OPEN,
        FieldNames.HIGH,
        FieldNames.LOW,
        FieldNames.CLOSE,
        FieldNames.PRE_CLOSE,
        FieldNames.VOLUME,
        FieldNames.AMOUNT,
        FieldNames.IS_SUSPEND,
    ]


def get_optional_market_data_fields() -> List[str]:
    """
    获取可选的市场数据字段（增强回测精度）
    
    Returns
    -------
    List[str] : 可选字段列表
    """
    return [
        FieldNames.LIMIT_UP,
        FieldNames.LIMIT_DOWN,
        FieldNames.IS_LIMIT_UP,
        FieldNames.IS_LIMIT_DOWN,
        FieldNames.IS_ST,
        FieldNames.ADJ_FACTOR,
        FieldNames.TURNOVER,
        FieldNames.AVG_VOLUME_20,
    ]


# ============================================================================
# 快捷函数
# ============================================================================

def standardize_df(df, required_fields: List[str] = None, 
                   optional_fields: List[str] = None) -> Any:
    """
    一站式DataFrame标准化：列名映射 + 必需字段补全 + 类型转换
    
    Parameters
    ----------
    df : pd.DataFrame
        输入数据框
    required_fields : List[str], optional
        必需字段（默认使用get_required_market_data_fields）
    optional_fields : List[str], optional
        可选字段（默认使用get_optional_market_data_fields）
    
    Returns
    -------
    pd.DataFrame : 标准化后的数据框
    """
    import pandas as pd
    
    if required_fields is None:
        required_fields = get_required_market_data_fields()
    if optional_fields is None:
        optional_fields = get_optional_market_data_fields()
    
    # 1. 列名标准化
    df = normalize_column_names(df)
    
    # 2. 必需字段补全
    df = ensure_columns(df, required_fields)
    
    # 3. 可选字段补全（如果有的话）
    df = ensure_columns(df, optional_fields)
    
    # 4. 类型转换
    for col in df.columns:
        if col in FIELD_TYPES:
            type_str = FIELD_TYPES[col]
            try:
                if type_str == "bool":
                    df[col] = df[col].astype(bool)
                elif type_str == "int":
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                elif type_str == "float":
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif type_str == "date":
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                pass  # 转换失败保持原样
    
    return df


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    
    print("Field Specs Test:")
    print("=" * 60)
    
    # 测试1: 列名标准化
    print("\n1. Column Normalization Test:")
    df = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'code': ['000001', '000002'],
        'is_suspended': [False, True],
        'prev_close': [10.0, 20.0],
        'vol': [10000, 20000],
    })
    print(f"   Original columns: {list(df.columns)}")
    df_norm = normalize_column_names(df)
    print(f"   Normalized columns: {list(df_norm.columns)}")
    assert 'trade_date' in df_norm.columns
    assert 'is_suspend' in df_norm.columns
    assert 'pre_close' in df_norm.columns
    print("   [OK] Pass")
    
    # 测试2: 字段补全
    print("\n2. Field Ensure Test:")
    df2 = pd.DataFrame({
        'symbol': ['000001'],
        'trade_date': ['2024-01-01'],
        'open': [10.0],
        'close': [10.5],
    })
    df2_full = ensure_columns(df2, get_required_market_data_fields())
    print(f"   Columns after ensure: {len(df2_full.columns)}")
    assert 'is_suspend' in df2_full.columns
    assert 'volume' in df2_full.columns
    print("   [OK] Pass")
    
    # 测试3: 一站式标准化
    print("\n3. Standardize DF Test:")
    df3 = pd.DataFrame({
        'date': ['2024-01-01'],
        'code': ['000001'],
        'open': [10.0],
        'high': [10.5],
        'low': [9.8],
        'close': [10.2],
        'volume': [100000],
    })
    df3_std = standardize_df(df3)
    print(f"   Output columns: {sorted(df3_std.columns)}")
    assert df3_std['is_suspend'].iloc[0] == False  # 默认填充False
    print("   [OK] Pass")
    
    print("\n" + "=" * 60)
    print("All tests passed!")
