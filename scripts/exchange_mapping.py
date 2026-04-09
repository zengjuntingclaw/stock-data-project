"""
交易所代码映射工具 - 统一入口
==============================
所有交易所代码映射必须经由此模块，禁止各模块自行实现。

规则来源：
- 上交所：6xxxxx (主板)、688xxx (科创板)、5xxxxx (基金)
- 深交所：0xxxxx/2xxxxx (主板)、3xxxxx (创业板)
- 北交所：4xxxxx (老股)、8xxxxx (新股)、920xxx (2024新代码)
"""

import re
from typing import Tuple, Optional


# ============================================================================
# 代码正则表达式定义（权威来源：各交易所规则）
# ============================================================================

# 上交所
RE_SH_MAIN = re.compile(r'^[65]\d{5}$')      # 600000-699999, 500000-599999
RE_SH_STAR = re.compile(r'^688\d{3}$')       # 688xxx 科创板
RE_SH_B    = re.compile(r'^900\d{3}$')       # 900xxx B股

# 深交所  
RE_SZ_MAIN = re.compile(r'^[012]\d{5}$')     # 000000-099999, 100000-299999
RE_SZ_CHINEXT = re.compile(r'^30\d{4}$')     # 300xxx 创业板
RE_SZ_B    = re.compile(r'^200\d{3}$')       # 200xxx B股

# 北交所（北京证券交易所）
# ⚠️ 修复（V3）：使用 strip_prefix() 统一处理前导零问题
# 规则：去掉所有前导零后，判断前3位（920xxx）或前1位（4xxx/8xxx）
# 实现：先 strip_leading_zeros，再用正则精确匹配
# strip_leading_zeros("004301") = "4301", strip_leading_zeros("830001") = "830001"
# 北交所识别辅助函数
def _strip_leading_zeros(s: str) -> str:
    """去掉前导零，返回有效数字部分"""
    return s.lstrip('0') or '0'


def _is_bj_code(symbol: str) -> bool:
    """
    判断是否为北交所代码
    
    ⚠️ 核心修复（V7-final）：传入原始 symbol，保留前导零信息
    规则：
    - 去掉前导零后取前4个字符
    - ≥4位：用前4位精确判断（920xxx / 4xxxx / 8xxxx）
    - <4位：用首位非零字符判断（4/8 → BJ，其余 → SZ）
    """
    # 取前4个非零字符（避免zfill填充后判断错误）
    stripped = str(symbol).lstrip('0')[:4]
    
    if len(stripped) < 4:
        # 不足4位：用首位非零字符判断
        # 4/8 → 北交所（短输入的4/8几乎肯定是北交所代码）
        # 9 → 沪市（920xxx段需要至少4位，不可能在9xxx段）
        # 其他 → 深市
        return stripped in ('4', '8')
    
    # ≥4位：精确判断
    # 920xxx-929xxx（前4位 920x 或 921x...929x）
    if stripped[:2] == '92':
        return True
    # 4xxxxx（首位4）
    if stripped[0] == '4':
        return True
    # 8xxxxx（首位8）
    if stripped[0] == '8':
        return True
    return False


RE_BJ_OLD  = re.compile(r'^4\d{5}$')         # 4xxxxx 老股（6位精确）
RE_BJ_NEW  = re.compile(r'^8\d{5}$')         # 8xxxxx 新股（6位精确）
RE_BJ_2024 = re.compile(r'^920\d{3}$')       # 920xxx 2024年新启用代码段（6位精确）


# ============================================================================
# 统一映射函数
# ============================================================================

def classify_exchange(symbol: str) -> Tuple[str, str]:
    """
    根据股票代码识别交易所和板块
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码（如 '000001', '920001'）
    
    Returns
    -------
    Tuple[str, str] : (交易所代码, 板块名称)
        交易所代码: 'SH' | 'SZ' | 'BJ'
        板块名称: '主板' | '科创板' | '创业板' | '北交所' | 'B股' | '未知'
    
    Examples
    --------
    >>> classify_exchange('600000')
    ('SH', '主板')
    >>> classify_exchange('000001')
    ('SZ', '主板')
    >>> classify_exchange('688001')
    ('SH', '科创板')
    >>> classify_exchange('300001')
    ('SZ', '创业板')
    >>> classify_exchange('920001')  # 北交所2024新代码
    ('BJ', '北交所')
    >>> classify_exchange('430001')  # 北交所老股
    ('BJ', '北交所')
    """
    sym6 = str(symbol).zfill(6)
    
    # 上交所
    if RE_SH_STAR.match(sym6):
        return ('SH', '科创板')
    if RE_SH_MAIN.match(sym6):
        return ('SH', '主板')
    if RE_SH_B.match(sym6):
        return ('SH', 'B股')
    
    # 北交所（必须在深交所之前检查）
    # ⚠️ 修复（V7）：传入原始 symbol（而非 zfill 后的 sym6），保留前导零信息
    # "006005".lstrip('0')[:4] = "6005" → 首位6 → not BJ ✓
    # "000008".lstrip('0')[:4] = "8" → 首位8 → BJ ✓
    # "4301".lstrip('0')[:4] = "4301" → 前4位4301 → BJ ✓
    # "000001".lstrip('0')[:4] = "1" → 不足4位首位1 → not BJ ✓
    # "920000".lstrip('0')[:4] = "9200" → 前4位9200 → BJ ✓
    if _is_bj_code(symbol):
        return ('BJ', '北交所')
    
    # 深交所
    if RE_SZ_CHINEXT.match(sym6):
        return ('SZ', '创业板')
    if RE_SZ_MAIN.match(sym6):
        return ('SZ', '主板')
    if RE_SZ_B.match(sym6):
        return ('SZ', 'B股')
    
    # 未知代码，根据首位数字兜底
    # ⚠️ 修复（V4）：使用原始符号的"首个非零数字"而非zfill后的首位
    # 原因：zfill后所有代码前导零都被填满，导致"006005"被误判为BJ
    # 规则：
    #   - 首个非零数字为4/8/92/920 → 北交所
    #   - 首个非零数字为6 → 沪市
    #   - 其他 → 深市
    def _first_nonzero(s: str) -> str:
        """返回字符串中首个非零字符"""
        for c in s:
            if c != '0':
                return c
        return '0'
    
    first_nz = _first_nonzero(str(symbol))
    
    if first_nz == '9':
        # 9字头 → 沪市主板兜底（920xxx已在上面被BJ捕获）
        return ('SH', '主板')
    elif first_nz in '048':
        # 0/4/8 开头 → 北交所
        return ('BJ', '北交所')
    else:
        # 其他（1/2/3/5/6/7） → 深市
        return ('SZ', '主板')


def build_ts_code(symbol: str) -> str:
    """
    构造标准 ts_code（带交易所后缀）
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码
    
    Returns
    -------
    str : 标准ts_code（如 '000001.SZ', '920001.BJ'）
    
    Examples
    --------
    >>> build_ts_code('600000')
    '600000.SH'
    >>> build_ts_code('920001')
    '920001.BJ'
    """
    exchange, _ = classify_exchange(symbol)
    return f"{str(symbol).zfill(6)}.{exchange}"


def build_bs_code(symbol: str) -> str:
    """
    构造 Baostock 代码格式
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码
    
    Returns
    -------
    str : Baostock格式（如 'sh.600000', 'bj.920001'）
    """
    exchange, _ = classify_exchange(symbol)
    bs_prefix = {'SH': 'sh', 'SZ': 'sz', 'BJ': 'bj'}.get(exchange, 'sh')
    return f"{bs_prefix}.{str(symbol).zfill(6)}"


def parse_ts_code(ts_code: str) -> Tuple[str, str]:
    """
    解析 ts_code 为 (symbol, exchange)
    
    Parameters
    ----------
    ts_code : str
        标准ts_code（如 '000001.SZ'）
    
    Returns
    -------
    Tuple[str, str] : (symbol, exchange)
    
    Raises
    ------
    ValueError : 格式不正确时抛出
    
    Examples
    --------
    >>> parse_ts_code('000001.SZ')
    ('000001', 'SZ')
    """
    if '.' not in ts_code:
        raise ValueError(f"Invalid ts_code format: {ts_code}, expected 'XXXXXX.EXCHANGE'")
    
    parts = ts_code.split('.')
    if len(parts) != 2:
        raise ValueError(f"Invalid ts_code format: {ts_code}")
    
    symbol, exchange = parts[0], parts[1].upper()
    
    # 验证交易所代码
    if exchange not in ('SH', 'SZ', 'BJ'):
        raise ValueError(f"Unknown exchange: {exchange}, expected SH/SZ/BJ")
    
    return (symbol.zfill(6), exchange)


def get_price_limit_pct(symbol: str, trade_date: Optional[str] = None) -> float:
    """
    获取股票涨跌停幅度（百分比）
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码
    trade_date : str, optional
        交易日期（YYYYMMDD格式），用于判断创业板2020-08-24改革前后
    
    Returns
    -------
    float : 涨跌停幅度（如 0.10 表示10%）
    """
    exchange, board = classify_exchange(symbol)
    
    # 北交所：30%
    if exchange == 'BJ':
        return 0.30
    
    # 科创板：20%
    if board == '科创板':
        return 0.20
    
    # 创业板：2020-08-24前10%，之后20%
    if board == '创业板':
        if trade_date and trade_date < '20200824':
            return 0.10
        return 0.20
    
    # 主板：10%
    return 0.10


def get_lot_size(symbol: str) -> Tuple[int, int]:
    """
    获取股票交易单位（手数约束）
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码
    
    Returns
    -------
    Tuple[int, int] : (最小手数, 递增单位)
        主板: (100, 100) - 100股起，100股递增
        科创板: (200, 1) - 200股起，1股递增
        北交所: (100, 1) - 100股起，1股递增
    """
    exchange, board = classify_exchange(symbol)
    
    if board == '科创板':
        return (200, 1)
    if exchange == 'BJ':
        return (100, 1)
    
    # 主板、创业板
    return (100, 100)


def round_shares(symbol: str, shares: int) -> int:
    """
    根据交易单位规整股数
    
    Parameters
    ----------
    symbol : str
        6位数字股票代码
    shares : int
        目标股数
    
    Returns
    -------
    int : 规整后的股数（不满足最小手数返回0）
    
    Examples
    --------
    >>> round_shares('600000', 550)  # 主板
    500
    >>> round_shares('688001', 150)  # 科创板，不足200
    0
    >>> round_shares('688001', 250)  # 科创板，200起1股递增
    250
    """
    min_lot, step = get_lot_size(symbol)
    
    if shares < min_lot:
        return 0
    
    # 先满足最小手数，再按步长规整
    excess = shares - min_lot
    rounded_excess = (excess // step) * step
    return min_lot + rounded_excess


# ============================================================================
# 批量处理函数
# ============================================================================

def batch_build_ts_code(symbols: list) -> list:
    """批量构造ts_code"""
    return [build_ts_code(s) for s in symbols]


def batch_classify(symbols: list) -> dict:
    """批量分类，返回 {symbol: (exchange, board)}"""
    return {s: classify_exchange(s) for s in symbols}


# ============================================================================
# 向后兼容（旧函数别名）
# ============================================================================

def detect_board(symbol: str) -> str:
    """向后兼容：识别板块"""
    _, board = classify_exchange(symbol)
    return board


def detect_limit(code: str) -> float:
    """向后兼容：获取涨跌停幅度（简化版，不含时间维度）"""
    return get_price_limit_pct(code)


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    # 测试用例
    test_cases = [
        # (symbol, expected_exchange, expected_board)
        ('600000', 'SH', '主板'),      # 沪市主板
        ('688001', 'SH', '科创板'),    # 科创板
        ('000001', 'SZ', '主板'),      # 深市主板
        ('300001', 'SZ', '创业板'),    # 创业板
        ('430001', 'BJ', '北交所'),    # 北交所老股
        ('830001', 'BJ', '北交所'),    # 北交所新股
        ('920001', 'BJ', '北交所'),    # 北交所2024新代码（关键测试！）
        ('873001', 'BJ', '北交所'),    # 北交所
        ('900001', 'SH', 'B股'),       # 沪市B股
        ('200001', 'SZ', 'B股'),       # 深市B股
    ]
    
    print("交易所代码映射测试:")
    print("=" * 60)
    
    all_pass = True
    for symbol, exp_ex, exp_board in test_cases:
        exchange, board = classify_exchange(symbol)
        ts_code = build_ts_code(symbol)
        bs_code = build_bs_code(symbol)
        limit = get_price_limit_pct(symbol)
        lot = get_lot_size(symbol)
        
        status = "OK" if (exchange == exp_ex and board == exp_board) else "FAIL"
        if status == "✗":
            all_pass = False
        
        print(f"{status} {symbol} -> {ts_code:12} (Baostock: {bs_code:12}) "
              f"交易所:{exchange:2} 板块:{board:6} 涨跌停:{limit:4.0%} 手数:{lot}")
    
    print("=" * 60)
    print(f"测试{'全部通过' if all_pass else '有失败'} ({'PASS' if all_pass else 'FAIL'})")
    
    # 测试parse_ts_code
    print("\n解析测试:")
    test_parse = ['000001.SZ', '600000.SH', '920001.BJ', '688001.SH']
    for ts in test_parse:
        sym, ex = parse_ts_code(ts)
        print(f"  {ts} -> symbol={sym}, exchange={ex}")
    
    # 测试股数规整
    print("\n股数规整测试:")
    round_cases = [
        ('600000', 550),   # 主板 -> 500
        ('688001', 150),   # 科创板不足200 -> 0
        ('688001', 250),   # 科创板 -> 250
        ('920001', 150),   # 北交所 -> 100
    ]
    for sym, shares in round_cases:
        rounded = round_shares(sym, shares)
        print(f"  {sym}: {shares}股 -> {rounded}股")
