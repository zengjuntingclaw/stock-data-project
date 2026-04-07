"""A股多因子回测框架 v3.0 - 回测入口

用法:
    python run_backtest.py                                    # 交互式提示
    python run_backtest.py --start 2022-01-01 --end 2024-12-31 --capital 1e7
    python run_backtest.py --start 2023-01-01 --rebalance ME  # 月末调仓
    python run_backtest.py --strategy my_strategy.py           # 自定义策略文件
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Callable

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from loguru import logger

# ── 日志配置 ────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="{time:HH:mm:ss} | {level: <8}| {message}",
    level="INFO",
    colorize=False,
)
logger.add(
    project_root / "logs" / "v3_{time}.log",
    rotation="10 MB",
    retention="7 days",
    level="DEBUG",
)

# ── 组件导入 ────────────────────────────────────────────────────
from scripts.data_engine import DataEngine
from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig


# ── 版本 ────────────────────────────────────────────────────────
__version__ = "3.0"


# ── 内置策略 ────────────────────────────────────────────────────
def equal_weight_top50(factor_data, date) -> Dict[str, float]:
    """等权买入因子排名前50只（示例策略，可替换）"""
    if factor_data is None or factor_data.empty:
        return {}
    n = min(50, len(factor_data))
    return {sym: 1.0 / n for sym in factor_data.index[:n]}


# ── 策略加载器 ──────────────────────────────────────────────────
def load_strategy(path: str) -> Callable:
    """从文件加载策略函数，支持两种格式：

    1. 策略模块（strategy_file.py）:
       def my_strategy(factor_data, date) -> Dict[str, float]:
           return {...}

    2. pickle 文件（strategy_file.pkl）:
       序列化的 Callable 对象
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"策略文件不存在: {path}")

    if p.suffix == ".py":
        import importlib.util
        spec = importlib.util.spec_from_file_location("_strategy", p)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_strategy"] = mod
        spec.loader.exec_module(mod)
        if not hasattr(mod, "my_strategy") and not hasattr(mod, "strategy"):
            raise AttributeError(
                f"策略文件 {path} 必须定义 my_strategy(factor_data, date) 或 strategy(factor_data, date)"
            )
        return getattr(mod, "my_strategy", None) or getattr(mod, "strategy")
    elif p.suffix == ".pkl":
        import pickle
        with open(p, "rb") as f:
            return pickle.load(f)
    else:
        raise ValueError(f"不支持的策略文件格式: {p.suffix} (仅支持 .py 和 .pkl)")


# ── 框架验证 ────────────────────────────────────────────────────
def _verify_framework() -> bool:
    """Verify core components (no external data required)"""
    from scripts.trading_rules import AShareTradingRules, TradingCalendar
    from scripts.execution_engine_v3 import ExecutionEngineV3

    rules_ok = AShareTradingRules.get_board("600000") == AShareTradingRules.BOARD_MAIN
    cal = TradingCalendar()
    cal_ok = not cal.is_trading_day(datetime(2024, 1, 1))  # 元旦

    engine = ExecutionEngineV3(initial_cash=1e6)
    exec_ok = engine.cash.available == 1e6

    return rules_ok and cal_ok and exec_ok


# ── 主入口 ───────────────────────────────────────────────────────
def run_backtest(
    start: str = "2022-01-01",
    end: str = "2024-12-31",
    capital: float = 1e7,
    rebalance: str = "ME",
    strategy_file: str = None,
    strategy_func: Callable = None,
    universe_size: int = 3000,
    output: str = None,
    verbose: bool = False,
) -> bool:
    """运行回测（可直接调用或通过 CLI）"""
    # 策略
    if strategy_func is not None:
        strategy = strategy_func
        strategy_name = strategy.__name__
    elif strategy_file:
        strategy = load_strategy(strategy_file)
        strategy_name = Path(strategy_file).stem
    else:
        strategy = equal_weight_top50
        strategy_name = "equal_weight_top50"

    logger.info(f"{'='*50}\nA-share backtest v{__version__} 启动\n{'='*50}")
    logger.info(f"Period: {start} ~ {end}  |  Capital: {capital/1e7:.1f}千万  |  Rebalance: {rebalance}")
    logger.info(f"Strategy: {strategy_name}")

    # 框架验证
    if not _verify_framework():
        logger.error("Framework verification FAILED，请检查 imports")
        return False

    # 初始化
    data_engine = DataEngine()

    config = BacktestConfig(
        start_date=datetime.strptime(start, "%Y-%m-%d"),
        end_date=datetime.strptime(end, "%Y-%m-%d"),
        initial_capital=capital,
        rebalance_freq=rebalance,
    )

    engine = ProductionBacktestEngine(
        config=config,
        data_engine=data_engine,
        factor_engine=None,
    )

    # 运行
    result = engine.run(strategy)

    if "error" in result:
        logger.error(f"Backtest failed: {result['error']}")
        return False

    perf = result.get("performance", {})
    trades = result.get("trades", 0)
    logger.info(f"Total return: {perf.get('total_return', 0):.2%}")
    logger.info(f"Sharpe ratio: {perf.get('sharpe_ratio', 0):.2f}")
    logger.info(f"Max drawdown: {perf.get('max_drawdown', 0):.2%}")
    logger.info(f"Annual return: {perf.get('annual_return', 0):.2%}")
    logger.info(f"Trade count: {trades}")
    logger.info("Backtest done!")

    # 输出报告
    if output:
        _save_report(result, perf, strategy_name, start, end, output)
        logger.info(f"Report saved: {output}")

    return True


def _save_report(result, perf, strategy_name, start, end, output_path: str):
    """保存回测摘要为 JSON"""
    import json
    from pathlib import Path

    report = {
        "version": __version__,
        "strategy": strategy_name,
        "period": {"start": start, "end": end},
        "performance": {k: float(v) if v is not None else None for k, v in perf.items()},
        "trades": result.get("trades", 0),
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


# ── CLI ─────────────────────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run_backtest.py",
        description="A股多因子回测框架 v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_backtest.py --start 2023-01-01 --end 2024-12-31
  python run_backtest.py -s 2023-01-01 -e 2024-12-31 -c 5e6 -r D
  python run_backtest.py --strategy my_strategy.py --output _output/report.json
  python run_backtest.py --verify-only   # 仅验证框架，不跑回测
        """,
    )
    p.add_argument("-s", "--start", default="2022-01-01",
                   help="回测开始日期 (YYYY-MM-DD)  [默认: 2022-01-01]")
    p.add_argument("-e", "--end", default="2024-12-31",
                   help="回测结束日期 (YYYY-MM-DD)  [默认: 2024-12-31]")
    p.add_argument("-c", "--capital", type=float, default=1e7,
                   help="初始资金（元）  [默认: 10000000]")
    p.add_argument("-r", "--rebalance", choices=["D", "W-FRI", "ME"], default="ME",
                   help="调仓频率: D=每日, W-FRI=每周五, ME=每月末  [默认: ME]")
    p.add_argument("--strategy", metavar="FILE",
                   help="策略文件路径 (.py 或 .pkl)")
    p.add_argument("--universe-size", type=int, default=3000,
                   help="股票池大小  [默认: 3000]")
    p.add_argument("--output", "-o", metavar="PATH",
                   help="回测报告输出路径 (.json)")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="详细输出")
    p.add_argument("--verify-only", action="store_true",
                   help="仅验证框架组件，不运行回测")
    p.add_argument("--version", action="version", version=f"%(prog)s v{__version__}")
    return p


def _ensure_dirs():
    """创建必要目录"""
    (project_root / "logs").mkdir(exist_ok=True)
    (project_root / "_output").mkdir(exist_ok=True)


if __name__ == "__main__":
    _ensure_dirs()

    parser = _build_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.remove()
        logger.add(sys.stdout, level="DEBUG")

    if args.verify_only:
        ok = _verify_framework()
        if ok:
            logger.info("Framework components verified PASS")
            sys.exit(0)
        else:
            logger.error("框架组件验证失败")
            sys.exit(1)

    success = run_backtest(
        start=args.start,
        end=args.end,
        capital=args.capital,
        rebalance=args.rebalance,
        strategy_file=args.strategy,
        universe_size=args.universe_size,
        output=args.output,
        verbose=args.verbose,
    )
    sys.exit(0 if success else 1)
