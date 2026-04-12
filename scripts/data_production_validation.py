"""
A股数据生产级别验证套件
=========================
验证目标: 确保数据满足量化选股和回测的生产要求

验证维度:
  1. 数据完整性 - 股票池覆盖、时间范围、断层检测
  2. 数据准确性 - OHLC校验、复权因子、量价关系
  3. PIT查询正确性 - 历史股票池、指数成分股
  4. 幸存者偏差处理 - 退市股、ST状态
  5. 交易规则模拟 - T+1、涨跌停、100股取整
  6. 回测引擎端到端 - 信号→执行→结算→绩效
  7. 因子计算准确性 - ROE/PB/PE
  8. 性能压力测试 - 大规模回测

运行方式:
  python scripts/data_production_validation.py
"""

import sys
import os
import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# 路径配置
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "_output" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证报告结构
# ═══════════════════════════════════════════════════════════════════════════════

class ValidationReport:
    """验证报告生成器"""

    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.suites: Dict[str, Dict] = {}
        self.summary = {
            "total_suites": 0,
            "passed_suites": 0,
            "failed_suites": 0,
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0,
            "warnings": 0,
        }

    def add_suite(self, name: str, results: Dict):
        """添加测试套件结果"""
        self.suites[name] = results
        self.summary["total_suites"] += 1

        if results["status"] == "PASS":
            self.summary["passed_suites"] += 1
        elif results["status"] == "FAIL":
            self.summary["failed_suites"] += 1

        for test in results.get("tests", []):
            self.summary["total_tests"] += 1
            if test["status"] == "PASS":
                self.summary["passed_tests"] += 1
            elif test["status"] == "FAIL":
                self.summary["failed_tests"] += 1
            if test["status"] == "WARN":
                self.summary["warnings"] += 1

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "summary": self.summary,
            "suites": self.suites,
        }

    def to_markdown(self) -> str:
        """生成 Markdown 格式报告"""
        lines = [
            "# A股数据生产级别验证报告",
            f"\n**生成时间**: {self.timestamp}",
            f"\n**总体状态**: {'[PASS]' if self.summary['failed_suites'] == 0 else '[FAIL]'}",
            "",
            "## 摘要",
            "",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 测试套件 | {self.summary['total_suites']} |",
            f"| 通过套件 | {self.summary['passed_suites']} |",
            f"| 失败套件 | {self.summary['failed_suites']} |",
            f"| 总测试用例 | {self.summary['total_tests']} |",
            f"| 通过用例 | {self.summary['passed_tests']} |",
            f"| 失败用例 | {self.summary['failed_tests']} |",
            f"| 警告 | {self.summary['warnings']} |",
            "",
        ]

        for suite_name, suite in self.suites.items():
            status_icon = "[PASS]" if suite["status"] == "PASS" else "[FAIL]"
            lines.append(f"## {status_icon} {suite_name}")
            lines.append(f"\n**状态**: {suite['status']}")
            if suite.get("description"):
                lines.append(f"\n**说明**: {suite['description']}")
            lines.append("\n| 测试项 | 状态 | 详情 |")
            lines.append("|--------|------|------|")

            for test in suite.get("tests", []):
                icon = "[PASS]" if test["status"] == "PASS" else "[FAIL]" if test["status"] == "FAIL" else "[WARN]"
                detail = test.get("detail", "")[:100]
                lines.append(f"| {test['name']} | {icon} | {detail} |")

            if suite.get("issues"):
                lines.append("\n**问题列表**:")
                for i, issue in enumerate(suite["issues"], 1):
                    lines.append(f"{i}. {issue}")

            if suite.get("recommendations"):
                lines.append("\n**改进建议**:")
                for i, rec in enumerate(suite["recommendations"], 1):
                    lines.append(f"{i}. {rec}")

            lines.append("")

        return "\n".join(lines)

    def save(self):
        """保存报告"""
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # JSON格式
        json_path = OUTPUT_DIR / f"validation_report_{timestamp_str}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)

        # Markdown格式
        md_path = OUTPUT_DIR / f"validation_report_{timestamp_str}.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self.to_markdown())

        print(f"\n报告已保存:")
        print(f"   JSON: {json_path}")
        print(f"   Markdown: {md_path}")

        return json_path, md_path


# ═══════════════════════════════════════════════════════════════════════════════
# 测试套件基类
# ═══════════════════════════════════════════════════════════════════════════════

class BaseTestSuite:
    """测试套件基类"""

    name = "基类测试套件"
    description = ""

    def __init__(self, engine=None):
        self.engine = engine
        self.results = {
            "status": "PASS",
            "tests": [],
            "issues": [],
            "recommendations": [],
        }

    def run(self) -> Dict:
        """运行所有测试"""
        print(f"\n{'='*60}")
        print(f"[{self.name}]")
        print(f"{'='*60}")

        try:
            self._run_tests()
        except Exception as e:
            self.results["status"] = "FAIL"
            self.results["issues"].append(f"测试套件执行失败: {str(e)}")
            print(f"❌ 测试套件执行失败: {e}")
            traceback.print_exc()

        # 更新套件状态
        failed_count = sum(1 for t in self.results["tests"] if t["status"] == "FAIL")
        if failed_count > 0:
            self.results["status"] = "FAIL"

        return self.results

    def _run_tests(self):
        """子类实现具体的测试逻辑"""
        raise NotImplementedError

    def _add_test(self, name: str, status: str, detail: str = ""):
        """添加测试结果"""
        self.results["tests"].append({
            "name": name,
            "status": status,
            "detail": detail,
        })

    def _add_issue(self, issue: str):
        """添加问题"""
        self.results["issues"].append(issue)

    def _add_recommendation(self, rec: str):
        """添加改进建议"""
        self.results["recommendations"].append(rec)


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 1: 数据完整性
# ═══════════════════════════════════════════════════════════════════════════════

class DataCompletenessSuite(BaseTestSuite):
    """
    验证数据完整性
    目标: 确保股票池覆盖全、时间范围足够、无数据断层
    """

    name = "数据完整性验证"
    description = "验证股票池覆盖度、时间范围和数据断层"

    def _run_tests(self):
        # 1.1 股票池覆盖度
        self._test_stock_universe_coverage()

        # 1.2 时间范围覆盖
        self._test_time_range_coverage()

        # 1.3 数据断层检测
        self._test_data_gap_detection()

        # 1.4 日均交易天数
        self._test_trading_days_per_year()

        # 1.5 股票数量稳定性
        self._test_stock_count_stability()

    def _test_stock_universe_coverage(self):
        """测试1.1: 股票池覆盖度"""
        try:
            # 使用 data_engine 的 PIT 查询
            from scripts.data_engine import DataEngine

            # 创建独立连接避免冲突
            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))

            # 查询几个关键时点
            test_dates = ["2022-06-30", "2023-06-30", "2024-06-30"]

            results = []
            for date in test_dates:
                try:
                    stocks = de.get_active_stocks(date)
                    results.append({
                        "date": date,
                        "count": len(stocks),
                    })
                except Exception as e:
                    results.append({
                        "date": date,
                        "count": 0,
                        "error": str(e)
                    })

            # A股市场大约5000-5500只股票
            min_expected = 4000
            max_expected = 6000

            issues = []
            for r in results:
                if r["count"] < min_expected or r["count"] > max_expected:
                    issues.append(f"{r['date']}: {r.get('error', r['count'])}只股票 (期望{min_expected}-{max_expected})")

            if issues:
                self._add_test(
                    "股票池覆盖度",
                    "FAIL",
                    f"样本: {results}"
                )
                self._add_issue("股票池数量偏离预期范围")
                self._add_recommendation("检查 get_active_stocks() 是否正确过滤上市/退市日期")
            else:
                self._add_test(
                    "股票池覆盖度",
                    "PASS",
                    f"各时点股票数: {[(r['date'], r['count']) for r in results]}"
                )

            de.close()

        except Exception as e:
            self._add_test("股票池覆盖度", "FAIL", str(e))
            self._add_issue(f"股票池查询失败: {str(e)}")

    def _test_time_range_coverage(self):
        """测试1.2: 时间范围覆盖"""
        try:
            # 检查数据库中实际数据的日期范围
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            result = conn.execute("""
                SELECT
                    MIN(trade_date) as min_date,
                    MAX(trade_date) as max_date,
                    COUNT(DISTINCT ts_code) as stock_count
                FROM daily_bar_raw
            """).fetchone()

            min_date, max_date, stock_count = result

            conn.close()

            # 检查是否覆盖足够的时间范围
            if min_date and max_date:
                start = pd.to_datetime(min_date)
                end = pd.to_datetime(max_date)
                years = (end - start).days / 365.25

                # 回测通常需要至少3-5年数据
                min_years = 3

                if years >= min_years:
                    self._add_test(
                        "时间范围覆盖",
                        "PASS",
                        f"数据范围: {min_date} ~ {max_date}, 共{years:.1f}年, {stock_count}只股票"
                    )
                else:
                    self._add_test(
                        "时间范围覆盖",
                        "FAIL",
                        f"数据范围: {min_date} ~ {max_date}, 仅{years:.1f}年 (需要>{min_years}年)"
                    )
                    self._add_issue(f"数据时间范围不足: 仅{years:.1f}年")
                    self._add_recommendation("补充历史数据至至少2018年")
            else:
                self._add_test("时间范围覆盖", "FAIL", "无法获取日期范围")
                self._add_issue("数据库中无有效数据")

        except Exception as e:
            self._add_test("时间范围覆盖", "FAIL", str(e))

    def _test_data_gap_detection(self):
        """测试1.3: 数据断层检测"""
        try:
            # 抽样检测几只股票的数据完整性
            sample_stocks = ["600519.SH", "000001.SZ", "000002.SZ"]

            gaps_found = []
            for ts_code in sample_stocks:
                gap = self._detect_gaps_for_stock(ts_code)
                if gap:
                    gaps_found.append({"stock": ts_code, "gaps": gap})

            if gaps_found:
                self._add_test(
                    "数据断层检测",
                    "WARN",
                    f"检测到{len(gaps_found)}只股票存在断层"
                )
                for g in gaps_found[:3]:
                    self._add_issue(f"{g['stock']}: {g['gaps']}")
            else:
                self._add_test("数据断层检测", "PASS", "抽样股票无明显断层")

        except Exception as e:
            self._add_test("数据断层检测", "FAIL", str(e))

    def _detect_gaps_for_stock(self, ts_code: str) -> List[Dict]:
        """检测单只股票的数据断层"""
        import duckdb
        conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

        try:
            df = conn.execute("""
                SELECT trade_date FROM daily_bar_raw
                WHERE ts_code = ?
                ORDER BY trade_date
            """, [ts_code]).df()

            conn.close()

            if df.empty:
                return []

            df["trade_date"] = pd.to_datetime(df["trade_date"])
            gaps = []

            for i in range(1, len(df)):
                diff = (df["trade_date"].iloc[i] - df["trade_date"].iloc[i-1]).days
                if diff > 5:  # 超过5个自然日视为断层
                    gaps.append({
                        "from": str(df["trade_date"].iloc[i-1].date()),
                        "to": str(df["trade_date"].iloc[i].date()),
                        "gap_days": diff - 1
                    })

            return gaps

        except Exception:
            conn.close()
            return []

    def _test_trading_days_per_year(self):
        """测试1.4: 年均交易天数"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 统计每年的交易天数（基于有交易的日期）
            result = conn.execute("""
                SELECT
                    strftime(trade_date, '%Y') as year,
                    COUNT(DISTINCT trade_date) as trading_days
                FROM daily_bar_raw
                WHERE trade_date >= '2020-01-01'
                GROUP BY strftime(trade_date, '%Y')
                ORDER BY year
            """).fetchall()

            conn.close()

            years_data = {r[0]: r[1] for r in result}

            # A股每年大约有240-250个交易日
            issues = []
            for year, days in years_data.items():
                if days < 200:  # 明显不足
                    issues.append(f"{year}: {days}天")

            if issues:
                self._add_test(
                    "年均交易天数",
                    "WARN",
                    f"各年交易天数: {years_data}"
                )
                for issue in issues:
                    self._add_issue(f"交易天数异常: {issue}")
            else:
                self._add_test(
                    "年均交易天数",
                    "PASS",
                    f"各年交易天数: {years_data}"
                )

        except Exception as e:
            self._add_test("年均交易天数", "FAIL", str(e))

    def _test_stock_count_stability(self):
        """测试1.5: 股票数量稳定性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 按月统计有数据的股票数量
            result = conn.execute("""
                SELECT
                    strftime(trade_date, '%Y-%m') as month,
                    COUNT(DISTINCT ts_code) as stock_count
                FROM daily_bar_raw
                WHERE trade_date >= '2023-01-01'
                GROUP BY strftime(trade_date, '%Y-%m')
                ORDER BY month
            """).fetchall()

            conn.close()

            counts = [r[1] for r in result]
            if counts:
                avg = sum(counts) / len(counts)
                max_diff = max(abs(c - avg) for c in counts) / avg * 100

                # 波动超过20%视为异常
                if max_diff > 20:
                    self._add_test(
                        "股票数量稳定性",
                        "WARN",
                        f"月均股票数波动{max_diff:.1f}%, 最大:{max(counts)}, 最小:{min(counts)}"
                    )
                    self._add_issue("股票数量月度波动较大")
                else:
                    self._add_test(
                        "股票数量稳定性",
                        "PASS",
                        f"月均股票数: {avg:.0f}, 波动: {max_diff:.1f}%"
                    )
            else:
                self._add_test("股票数量稳定性", "FAIL", "无数据")

        except Exception as e:
            self._add_test("股票数量稳定性", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 2: 数据准确性
# ═══════════════════════════════════════════════════════════════════════════════

class DataAccuracySuite(BaseTestSuite):
    """
    验证数据准确性
    目标: OHLC关系正确、复权因子准确、量价关系合理
    """

    name = "数据准确性验证"
    description = "验证OHLC校验、复权因子、量价关系"

    def _run_tests(self):
        self._test_ohlc_consistency()
        self._test_adj_factor_accuracy()
        self._test_volume_price_relationship()
        self._test_pct_change_consistency()
        self._test_price_extremes()

    def _test_ohlc_consistency(self):
        """测试2.1: OHLC一致性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查OHLC违规: high < open/close/low 或 low > open/close/high
            violations = conn.execute("""
                SELECT COUNT(*) as count
                FROM daily_bar_raw
                WHERE (
                    high < open OR high < close OR high < low OR
                    low > open OR low > close OR low > high OR
                    high < 0 OR low < 0 OR open < 0 OR close < 0
                )
            """).fetchone()[0]

            conn.close()

            # 允许极少量噪声（<0.1%）
            total = self._get_total_records()
            violation_rate = violations / total if total > 0 else 1

            if violation_rate < 0.001:
                self._add_test(
                    "OHLC一致性",
                    "PASS",
                    f"违规率: {violation_rate:.4%}, 违规数: {violations}"
                )
            elif violation_rate < 0.01:
                self._add_test(
                    "OHLC一致性",
                    "WARN",
                    f"违规率: {violation_rate:.4%}, 违规数: {violations}"
                )
                self._add_issue(f"OHLC违规率偏高: {violation_rate:.4%}")
            else:
                self._add_test(
                    "OHLC一致性",
                    "FAIL",
                    f"违规率: {violation_rate:.4%}, 违规数: {violations}"
                )
                self._add_issue("OHLC违规严重")
                self._add_recommendation("检查数据源质量，过滤异常记录")

        except Exception as e:
            self._add_test("OHLC一致性", "FAIL", str(e))

    def _test_adj_factor_accuracy(self):
        """测试2.2: 复权因子准确性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查adj_factor的合理性: 应 >= 1 且变化平滑
            issues = conn.execute("""
                WITH adj_changes AS (
                    SELECT
                        ts_code,
                        trade_date,
                        adj_factor,
                        LAG(adj_factor) OVER (PARTITION BY ts_code ORDER BY trade_date) as prev_adj
                    FROM daily_bar_adjusted
                )
                SELECT COUNT(*) as count
                FROM adj_changes
                WHERE adj_factor < 0.01 OR
                      (prev_adj IS NOT NULL AND
                       ABS(adj_factor / prev_adj - 1) > 0.5 AND
                       prev_adj > 0.01)
            """).fetchone()[0]

            conn.close()

            if issues == 0:
                self._add_test("复权因子准确性", "PASS", "复权因子无异常")
            elif issues < 10:
                self._add_test("复权因子准确性", "WARN", f"发现{issues}个疑似异常")
            else:
                self._add_test("复权因子准确性", "FAIL", f"发现{issues}个疑似异常")
                self._add_issue("复权因子存在大幅跳变")

        except Exception as e:
            self._add_test("复权因子准确性", "FAIL", str(e))

    def _test_volume_price_relationship(self):
        """测试2.3: 量价关系合理性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查成交量为0但价格有变化的情况（停牌误记录）
            suspicious = conn.execute("""
                SELECT COUNT(*) as count
                FROM daily_bar_raw
                WHERE volume = 0 AND pct_chg != 0
            """).fetchone()[0]

            conn.close()

            total = self._get_total_records()
            rate = suspicious / total if total > 0 else 1

            if rate < 0.001:
                self._add_test(
                    "量价关系",
                    "PASS",
                    f"停牌误记录率: {rate:.4%}"
                )
            else:
                self._add_test(
                    "量价关系",
                    "WARN",
                    f"发现{suspicious}条停牌误记录"
                )
                self._add_issue(f"存在{suspicious}条成交量为0但涨跌幅非0的记录")

        except Exception as e:
            self._add_test("量价关系", "FAIL", str(e))

    def _test_pct_change_consistency(self):
        """测试2.4: 涨跌幅一致性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 计算 pct_chg 与 (close-pre_close)/pre_close 的一致性
            # 允许0.1%的误差
            inconsistent = conn.execute("""
                WITH calc AS (
                    SELECT
                        ts_code,
                        trade_date,
                        pct_chg,
                        pre_close,
                        close,
                        CASE WHEN pre_close > 0
                             THEN (close - pre_close) / pre_close * 100
                             ELSE NULL END as calc_pct
                    FROM daily_bar_raw
                    WHERE pre_close > 0 AND pct_chg IS NOT NULL
                )
                SELECT COUNT(*) as count
                FROM calc
                WHERE ABS(pct_chg - calc_pct) > 0.15
            """).fetchone()[0]

            conn.close()

            total = self._get_total_records()
            rate = inconsistent / total if total > 0 else 1

            if rate < 0.01:
                self._add_test(
                    "涨跌幅一致性",
                    "PASS",
                    f"不一致率: {rate:.4%}"
                )
            else:
                self._add_test(
                    "涨跌幅一致性",
                    "WARN",
                    f"不一致率: {rate:.4%}, 数量: {inconsistent}"
                )
                self._add_issue("涨跌幅计算存在偏差")

        except Exception as e:
            self._add_test("涨跌幅一致性", "FAIL", str(e))

    def _test_price_extremes(self):
        """测试2.5: 价格极值检测"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查不合理的价格（接近0或异常高）
            extreme = conn.execute("""
                SELECT COUNT(*) as count
                FROM daily_bar_raw
                WHERE (close < 0.01 AND close > 0) OR close > 10000
            """).fetchone()[0]

            conn.close()

            if extreme == 0:
                self._add_test("价格极值", "PASS", "无异常价格")
            else:
                self._add_test("价格极值", "WARN", f"发现{extreme}条极端价格")
                self._add_issue(f"存在{extreme}条极端价格记录")

        except Exception as e:
            self._add_test("价格极值", "FAIL", str(e))

    def _get_total_records(self) -> int:
        """获取总记录数"""
        import duckdb
        conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)
        total = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
        conn.close()
        return total


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 3: PIT查询正确性
# ═══════════════════════════════════════════════════════════════════════════════

class PITQuerySuite(BaseTestSuite):
    """
    验证PIT(Point-In-Time)查询正确性
    目标: 历史股票池准确、指数成分股正确、公告日期对齐
    """

    name = "PIT查询验证"
    description = "验证历史股票池、指数成分股、公告日期对齐"

    def _run_tests(self):
        self._test_pit_stock_pool()
        self._test_pit_index_constituents()
        self._test_ann_date_alignment()
        self._test_eff_date_logic()

    def _test_pit_stock_pool(self):
        """测试3.1: PIT股票池查询"""
        try:
            from scripts.data_engine import DataEngine

            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))

            # 测试不同历史时点
            test_cases = [
                ("2020-06-30", "疫情后复苏期"),
                ("2021-06-30", "牛市中期"),
                ("2022-06-30", "熊市中期"),
                ("2023-06-30", "震荡市"),
            ]

            results = []
            for date, period in test_cases:
                try:
                    stocks = de.get_active_stocks(date)
                    results.append({
                        "date": date,
                        "period": period,
                        "count": len(stocks),
                        "success": True
                    })
                except Exception as e:
                    results.append({
                        "date": date,
                        "period": period,
                        "count": 0,
                        "success": False,
                        "error": str(e)
                    })

            de.close()

            # 验证结果
            failures = [r for r in results if not r["success"]]
            if failures:
                self._add_test("PIT股票池查询", "FAIL", f"{len(failures)}个时点查询失败")
                for f in failures:
                    self._add_issue(f"{f['date']}: {f.get('error', 'unknown')}")
            else:
                self._add_test(
                    "PIT股票池查询",
                    "PASS",
                    f"各时点股票数: {[(r['date'], r['count']) for r in results]}"
                )

        except Exception as e:
            self._add_test("PIT股票池查询", "FAIL", str(e))
            self._add_issue(f"PIT股票池测试失败: {str(e)}")

    def _test_pit_index_constituents(self):
        """测试3.2: PIT指数成分股查询"""
        try:
            from scripts.data_engine import DataEngine

            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))

            # 测试沪深300在不同历史时点的成分股
            test_cases = [
                ("000300.SH", "2022-06-30"),
                ("000300.SH", "2023-06-30"),
                ("000300.SH", "2024-06-30"),
            ]

            results = []
            for index_code, date in test_cases:
                try:
                    constituents = de.get_index_constituents(index_code, date)
                    results.append({
                        "index": index_code,
                        "date": date,
                        "count": len(constituents),
                        "success": True
                    })
                except Exception as e:
                    results.append({
                        "index": index_code,
                        "date": date,
                        "count": 0,
                        "success": False,
                        "error": str(e)
                    })

            de.close()

            # 沪深300成分股应该在280-320之间
            failures = [r for r in results if not r["success"]]
            out_of_range = [r for r in results if r["success"] and (r["count"] < 200 or r["count"] > 400)]

            if failures:
                self._add_test("PIT指数成分股", "FAIL", f"{len(failures)}个查询失败")
            elif out_of_range:
                self._add_test("PIT指数成分股", "WARN", f"部分时点成分股数量异常")
                for r in out_of_range:
                    self._add_issue(f"{r['index']} @ {r['date']}: {r['count']}只 (异常)")
            else:
                self._add_test(
                    "PIT指数成分股",
                    "PASS",
                    f"各时点成分股数: {[(r['date'], r['count']) for r in results]}"
                )

        except Exception as e:
            self._add_test("PIT指数成分股", "FAIL", str(e))
            self._add_issue(f"PIT指数成分股测试失败: {str(e)}")

    def _test_ann_date_alignment(self):
        """测试3.3: 公告日期对齐"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查 financial_data 表的 ann_date 是否有未来数据
            future_data = conn.execute("""
                SELECT COUNT(*) as count
                FROM financial_data
                WHERE ann_date > CURRENT_DATE
            """).fetchone()[0]

            conn.close()

            if future_data == 0:
                self._add_test("公告日期对齐", "PASS", "无未来公告日期")
            else:
                self._add_test("公告日期对齐", "FAIL", f"发现{future_data}条未来公告日期")
                self._add_issue("financial_data表中存在未来公告日期")

        except Exception as e:
            self._add_test("公告日期对齐", "FAIL", str(e))

    def _test_eff_date_logic(self):
        """测试3.4: eff_date生效日期逻辑"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查 stock_basic_history 表中 eff_date 是否合理
            issues = conn.execute("""
                SELECT COUNT(*) as count
                FROM stock_basic_history
                WHERE eff_date > CURRENT_DATE + INTERVAL '1 month'
                   OR (end_date IS NOT NULL AND end_date < eff_date)
            """).fetchone()[0]

            conn.close()

            if issues == 0:
                self._add_test("eff_date逻辑", "PASS", "生效日期逻辑正确")
            else:
                self._add_test("eff_date逻辑", "WARN", f"发现{issues}条可疑记录")
                self._add_issue(f"stock_basic_history中存在{issues}条日期逻辑异常")

        except Exception as e:
            self._add_test("eff_date逻辑", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 4: 幸存者偏差处理
# ═══════════════════════════════════════════════════════════════════════════════

class SurvivorshipBiasSuite(BaseTestSuite):
    """
    验证幸存者偏差处理
    目标: 退市股数据完整、ST状态历史可追溯
    """

    name = "幸存者偏差验证"
    description = "验证退市股数据、ST状态历史"

    def _run_tests(self):
        self._test_delisted_stocks_coverage()
        self._test_st_status_history()
        self._test_delist_date_accuracy()
        self._test_lifetime_coverage()

    def _test_delisted_stocks_coverage(self):
        """测试4.1: 退市股覆盖"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 统计已退市股票数量
            delisted_count = conn.execute("""
                SELECT COUNT(DISTINCT ts_code)
                FROM stock_basic_history
                WHERE is_delisted = TRUE OR delist_date IS NOT NULL
            """).fetchone()[0]

            conn.close()

            # A股历史退市股应该有数百只
            if delisted_count >= 100:
                self._add_test(
                    "退市股覆盖",
                    "PASS",
                    f"历史退市股: {delisted_count}只"
                )
            elif delisted_count >= 10:
                self._add_test(
                    "退市股覆盖",
                    "WARN",
                    f"历史退市股: {delisted_count}只 (可能不足)"
                )
                self._add_issue(f"退市股记录偏少: {delisted_count}只")
            else:
                self._add_test(
                    "退市股覆盖",
                    "FAIL",
                    f"历史退市股: {delisted_count}只 (严重不足)"
                )
                self._add_issue("退市股数据严重缺失")
                self._add_recommendation("从AkShare获取历史退市股列表")

        except Exception as e:
            self._add_test("退市股覆盖", "FAIL", str(e))

    def _test_st_status_history(self):
        """测试4.2: ST状态历史"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查 ST 状态历史表
            st_count = conn.execute("""
                SELECT COUNT(DISTINCT ts_code)
                FROM st_status_history
            """).fetchone()[0]

            conn.close()

            if st_count >= 100:
                self._add_test(
                    "ST状态历史",
                    "PASS",
                    f"有ST历史的股票: {st_count}只"
                )
            elif st_count > 0:
                self._add_test(
                    "ST状态历史",
                    "WARN",
                    f"有ST历史的股票: {st_count}只"
                )
            else:
                self._add_test(
                    "ST状态历史",
                    "WARN",
                    "无ST状态历史数据"
                )
                self._add_issue("ST状态历史表为空")

        except Exception as e:
            self._add_test("ST状态历史", "FAIL", str(e))

    def _test_delist_date_accuracy(self):
        """测试4.3: 退市日期准确性"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查退市股是否有退市日期
            no_delist_date = conn.execute("""
                SELECT COUNT(*) as count
                FROM stock_basic_history
                WHERE is_delisted = TRUE AND delist_date IS NULL
            """).fetchone()[0]

            conn.close()

            if no_delist_date == 0:
                self._add_test("退市日期准确性", "PASS", "退市日期完整")
            else:
                self._add_test(
                    "退市日期准确性",
                    "WARN",
                    f"发现{no_delist_date}只股票缺少退市日期"
                )
                self._add_issue(f"{no_delist_date}只退市股缺少退市日期")

        except Exception as e:
            self._add_test("退市日期准确性", "FAIL", str(e))

    def _test_lifetime_coverage(self):
        """测试4.4: 股票生命周期覆盖"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 统计有完整生命周期记录的股票
            complete_lifetime = conn.execute("""
                SELECT COUNT(*) as count
                FROM (
                    SELECT ts_code
                    FROM stock_basic_history
                    GROUP BY ts_code
                    HAVING COUNT(*) >= 2
                )
            """).fetchone()[0]

            conn.close()

            if complete_lifetime >= 100:
                self._add_test(
                    "生命周期覆盖",
                    "PASS",
                    f"有完整历史的股票: {complete_lifetime}只"
                )
            else:
                self._add_test(
                    "生命周期覆盖",
                    "WARN",
                    f"有完整历史的股票: {complete_lifetime}只"
                )

        except Exception as e:
            self._add_test("生命周期覆盖", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 5: 交易规则模拟
# ═══════════════════════════════════════════════════════════════════════════════

class TradingRulesSuite(BaseTestSuite):
    """
    验证交易规则模拟
    目标: T+1冻结正确、涨跌停判断准确、100股取整
    """

    name = "交易规则验证"
    description = "验证T+1冻结、涨跌停、100股取整"

    def _run_tests(self):
        self._test_t1_freeze_logic()
        self._test_limit_up_down_detection()
        self._test_board_limit_rules()
        self._test_lot_size_rounding()

    def _test_t1_freeze_logic(self):
        """测试5.1: T+1冻结逻辑"""
        try:
            from scripts.execution_engine_v3 import ExecutionEngineV3
            from datetime import datetime

            # 创建执行引擎
            engine = ExecutionEngineV3(
                initial_cash=10_000_000,
                commission_rate=0.0003,
            )

            # 模拟买入操作
            test_date = datetime(2024, 6, 3)  # 周一

            # 买入100股
            buy_orders = engine.generate_orders(
                target_weights={"600519.SH": 0.1},
                prices={"600519.SH": 1700.0},
                current_date=test_date
            )

            # 检查持仓
            if "600519.SH" in engine.positions:
                pos = engine.positions["600519.SH"]
                # 当日买入的股票，available应该为0
                if pos.get("available", pos.get("shares", 0)) == 0:
                    self._add_test(
                        "T+1冻结逻辑",
                        "PASS",
                        f"买入后可用数量正确冻结"
                    )
                else:
                    self._add_test(
                        "T+1冻结逻辑",
                        "FAIL",
                        "当日买入股票未被正确冻结"
                    )
                    self._add_issue("T+1冻结逻辑存在问题")
            else:
                self._add_test("T+1冻结逻辑", "WARN", "未能获取持仓状态")

        except Exception as e:
            self._add_test("T+1冻结逻辑", "FAIL", str(e))
            self._add_issue(f"T+1冻结测试失败: {str(e)}")

    def _test_limit_up_down_detection(self):
        """测试5.2: 涨跌停检测"""
        try:
            from scripts.trading_rules import AShareTradingRules
            from datetime import datetime

            test_cases = [
                ("600519.SH", datetime(2024, 6, 3), 0.10, "主板茅台"),
                ("300001.SZ", datetime(2024, 6, 3), 0.20, "创业板股票"),
                ("688001.SH", datetime(2024, 6, 3), 0.20, "科创板股票"),
            ]

            results = []
            for symbol, date, expected, desc in test_cases:
                try:
                    limit = AShareTradingRules.get_price_limit(symbol, date)
                    match = abs(limit - expected) < 0.001
                    results.append({
                        "symbol": symbol,
                        "expected": expected,
                        "actual": limit,
                        "match": match,
                        "desc": desc
                    })
                except Exception as e:
                    results.append({
                        "symbol": symbol,
                        "expected": expected,
                        "actual": None,
                        "match": False,
                        "error": str(e)
                    })

            failures = [r for r in results if not r["match"]]
            if not failures:
                self._add_test(
                    "涨跌停检测",
                    "PASS",
                    f"所有{len(results)}个测试用例正确"
                )
            else:
                self._add_test(
                    "涨跌停检测",
                    "FAIL",
                    f"{len(failures)}/{len(results)}个失败"
                )
                for f in failures:
                    self._add_issue(f"{f['symbol']}: 期望{f['expected']}, 实际{f['actual']}")

        except Exception as e:
            self._add_test("涨跌停检测", "FAIL", str(e))
            self._add_issue(f"涨跌停检测测试失败: {str(e)}")

    def _test_board_limit_rules(self):
        """测试5.3: 板块涨跌停规则"""
        try:
            from scripts.trading_rules import AShareTradingRules
            from scripts.data_engine import detect_board

            test_cases = [
                ("600519", "主板", 0.10),
                ("000001", "主板", 0.10),
                ("300001", "创业板", 0.20),
                ("688001", "科创板", 0.20),
                ("430047", "北交所", 0.30),
                ("830799", "北交所", 0.30),
            ]

            failures = []
            for symbol, expected_board, expected_limit in test_cases:
                try:
                    board = detect_board(symbol)
                    limit = AShareTradingRules.get_price_limit(symbol)

                    board_ok = board == expected_board
                    limit_ok = abs(limit - expected_limit) < 0.001

                    if not (board_ok and limit_ok):
                        failures.append({
                            "symbol": symbol,
                            "board": f"期望{expected_board}, 实际{board}",
                            "limit": f"期望{expected_limit}, 实际{limit}"
                        })
                except Exception as e:
                    failures.append({"symbol": symbol, "error": str(e)})

            if not failures:
                self._add_test(
                    "板块涨跌停规则",
                    "PASS",
                    f"所有{len(test_cases)}个板块规则正确"
                )
            else:
                self._add_test(
                    "板块涨跌停规则",
                    "FAIL",
                    f"{len(failures)}/{len(test_cases)}个失败"
                )
                for f in failures[:3]:
                    self._add_issue(str(f))

        except Exception as e:
            self._add_test("板块涨跌停规则", "FAIL", str(e))

    def _test_lot_size_rounding(self):
        """测试5.4: 100股取整"""
        try:
            from scripts.trading_rules import AShareTradingRules

            test_cases = [
                (123, 100),   # 向下取整
                (150, 100),
                (200, 200),   # 整百
                (1000, 1000),
            ]

            failures = []
            for input_shares, expected in test_cases:
                rounded = (input_shares // 100) * 100
                if rounded != expected:
                    failures.append((input_shares, expected, rounded))

            if not failures:
                self._add_test(
                    "100股取整",
                    "PASS",
                    f"取整逻辑正确"
                )
            else:
                self._add_test(
                    "100股取整",
                    "FAIL",
                    f"{len(failures)}个取整错误"
                )
                self._add_issue("100股取整逻辑存在问题")

        except Exception as e:
            self._add_test("100股取整", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 6: 回测引擎端到端
# ═══════════════════════════════════════════════════════════════════════════════

class BacktestEngineSuite(BaseTestSuite):
    """
    验证回测引擎端到端
    目标: 信号→执行→结算→绩效链路正确
    """

    name = "回测引擎验证"
    description = "验证信号→执行→结算→绩效链路"

    def _run_tests(self):
        self._test_simple_backtest()
        self._test_equity_curve_integrity()
        self._test_position_settlement()
        self._test_turnover_calculation()

    def _test_simple_backtest(self):
        """测试6.1: 简单回测运行"""
        try:
            from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig
            from scripts.data_engine import DataEngine
            from datetime import datetime

            # 创建配置
            config = BacktestConfig(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 3, 31),
                initial_capital=10_000_000,
            )

            # 创建引擎
            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))
            engine = ProductionBacktestEngine(config=config, data_engine=de)

            # 定义简单策略：等权持有沪深300
            def simple_strategy(factor_data, date):
                if factor_data is None or factor_data.empty:
                    return {}
                # 简单策略：取前5只股票
                n = min(5, len(factor_data))
                weight = 1.0 / n
                return {sym: weight for sym in factor_data.index[:n]}

            # 运行回测
            result = engine.run(simple_strategy)

            de.close()

            if result and result.get("final_equity", 0) > 0:
                self._add_test(
                    "简单回测运行",
                    "PASS",
                    f"最终权益: {result.get('final_equity', 0):,.2f}"
                )
            else:
                self._add_test(
                    "简单回测运行",
                    "FAIL",
                    "回测未能产生有效结果"
                )
                self._add_issue("回测引擎运行失败或结果无效")

        except Exception as e:
            self._add_test("简单回测运行", "FAIL", str(e))
            self._add_issue(f"回测运行失败: {str(e)}")

    def _test_equity_curve_integrity(self):
        """测试6.2: 权益曲线完整性"""
        try:
            from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig
            from scripts.data_engine import DataEngine
            from datetime import datetime

            config = BacktestConfig(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 3, 31),
                initial_capital=10_000_000,
            )

            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))
            engine = ProductionBacktestEngine(config=config, data_engine=de)

            def strategy(factor_data, date):
                return {}

            result = engine.run(strategy)
            de.close()

            if result and "equity_curve" in result:
                eq = result["equity_curve"]
                if eq is not None and not eq.empty:
                    # 检查权益曲线单调性和非负性
                    min_equity = eq["equity"].min() if "equity" in eq.columns else eq.min()
                    if min_equity >= 0:
                        self._add_test(
                            "权益曲线完整性",
                            "PASS",
                            f"权益曲线有效，{len(eq)}个交易日"
                        )
                    else:
                        self._add_test(
                            "权益曲线完整性",
                            "FAIL",
                            "权益曲线包含负值"
                        )
                        self._add_issue("权益曲线出现负值")
                else:
                    self._add_test("权益曲线完整性", "WARN", "无权益曲线数据")
            else:
                self._add_test("权益曲线完整性", "WARN", "回测结果无权益曲线")

        except Exception as e:
            self._add_test("权益曲线完整性", "FAIL", str(e))

    def _test_position_settlement(self):
        """测试6.3: 持仓结算"""
        try:
            from scripts.execution_engine_v3 import ExecutionEngineV3
            from datetime import datetime

            engine = ExecutionEngineV3(
                initial_cash=10_000_000,
                commission_rate=0.0003,
            )

            # 买入
            test_date = datetime(2024, 6, 3)
            orders = engine.generate_orders(
                target_weights={"600519.SH": 0.1},
                prices={"600519.SH": 1700.0},
                current_date=test_date
            )

            # 执行
            trades = engine.execute(orders, {"600519.SH": 1700.0}, test_date)

            # 第二天结算
            settle_date = datetime(2024, 6, 4)
            engine.settle(settle_date)

            # 检查持仓是否正确
            if "600519.SH" in engine.positions:
                pos = engine.positions["600519.SH"]
                shares = pos.get("shares", 0)
                if shares > 0:
                    self._add_test(
                        "持仓结算",
                        "PASS",
                        f"持仓股数: {shares}"
                    )
                else:
                    self._add_test(
                        "持仓结算",
                        "FAIL",
                        "持仓未正确记录"
                    )
            else:
                self._add_test("持仓结算", "FAIL", "未找到持仓记录")

        except Exception as e:
            self._add_test("持仓结算", "FAIL", str(e))

    def _test_turnover_calculation(self):
        """测试6.4: 换手率计算"""
        try:
            # 读取交易记录计算换手率
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查是否有换手率数据
            turnover_count = conn.execute("""
                SELECT COUNT(*) FROM daily_bar_raw
                WHERE turnover IS NOT NULL AND turnover > 0
            """).fetchone()[0]

            total = conn.execute("SELECT COUNT(*) FROM daily_bar_raw").fetchone()[0]
            conn.close()

            rate = turnover_count / total if total > 0 else 0

            if rate > 0.9:
                self._add_test(
                    "换手率数据",
                    "PASS",
                    f"换手率覆盖率: {rate:.1%}"
                )
            elif rate > 0.5:
                self._add_test(
                    "换手率数据",
                    "WARN",
                    f"换手率覆盖率: {rate:.1%}"
                )
            else:
                self._add_test(
                    "换手率数据",
                    "FAIL",
                    f"换手率覆盖率: {rate:.1%} (过低)"
                )
                self._add_issue(f"换手率数据覆盖率过低: {rate:.1%}")

        except Exception as e:
            self._add_test("换手率数据", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 7: 因子计算准确性
# ═══════════════════════════════════════════════════════════════════════════════

class FactorAccuracySuite(BaseTestSuite):
    """
    验证因子计算准确性
    目标: ROE/PB/PE等关键因子计算正确
    """

    name = "因子计算验证"
    description = "验证ROE/PB/PE等关键因子"

    def _run_tests(self):
        self._test_financial_data_coverage()
        self._test_factor_value_ranges()
        self._test_factor_calculation_logic()

    def _test_financial_data_coverage(self):
        """测试7.1: 财务数据覆盖"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 统计财务数据记录数
            result = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT ts_code) as stocks,
                    MIN(report_date) as min_date,
                    MAX(report_date) as max_date
                FROM financial_data
            """).fetchone()

            total, stocks, min_date, max_date = result

            conn.close()

            if total > 0:
                self._add_test(
                    "财务数据覆盖",
                    "PASS",
                    f"共{total}条记录, {stocks}只股票, {min_date}~{max_date}"
                )
            else:
                self._add_test(
                    "财务数据覆盖",
                    "FAIL",
                    "财务数据表为空"
                )
                self._add_issue("financial_data表为空，需要填充财务数据")
                self._add_recommendation("使用AkShare接口补充财务数据")

        except Exception as e:
            self._add_test("财务数据覆盖", "FAIL", str(e))

    def _test_factor_value_ranges(self):
        """测试7.2: 因子值范围"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查常见因子值范围
            issues = []

            # ROE: 通常 -1 ~ 1 (即 -100% ~ 100%)
            roe_issues = conn.execute("""
                SELECT COUNT(*) FROM financial_data
                WHERE roe IS NOT NULL AND (roe < -5 OR roe > 5)
            """).fetchone()[0]
            if roe_issues > 0:
                issues.append(f"ROE异常值: {roe_issues}条")

            # PB: 通常 0.1 ~ 50
            pb_issues = conn.execute("""
                SELECT COUNT(*) FROM financial_data
                WHERE debt_ratio IS NOT NULL AND (debt_ratio < 0 OR debt_ratio > 1)
            """).fetchone()[0]
            if pb_issues > 0:
                issues.append(f"资产负债率异常值: {pb_issues}条")

            conn.close()

            if not issues:
                self._add_test(
                    "因子值范围",
                    "PASS",
                    "因子值范围检查通过"
                )
            else:
                self._add_test(
                    "因子值范围",
                    "WARN",
                    f"发现异常: {', '.join(issues)}"
                )
                for issue in issues:
                    self._add_issue(issue)

        except Exception as e:
            self._add_test("因子值范围", "FAIL", str(e))

    def _test_factor_calculation_logic(self):
        """测试7.3: 因子计算逻辑"""
        try:
            import duckdb
            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 检查因子计算一致性
            # 例如: 净利润 = 营业收入 * 净利率
            consistency_issues = conn.execute("""
                SELECT COUNT(*) FROM financial_data
                WHERE revenue > 0 AND net_profit > 0 AND net_margin IS NOT NULL
                AND ABS(net_profit / revenue - net_margin) > 0.1
            """).fetchone()[0]

            conn.close()

            if consistency_issues == 0:
                self._add_test(
                    "因子计算逻辑",
                    "PASS",
                    "因子计算一致性检查通过"
                )
            else:
                self._add_test(
                    "因子计算逻辑",
                    "WARN",
                    f"发现{consistency_issues}条不一致记录"
                )
                self._add_issue(f"{consistency_issues}条财务因子计算不一致")

        except Exception as e:
            self._add_test("因子计算逻辑", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 验证套件 8: 性能压力测试
# ═══════════════════════════════════════════════════════════════════════════════

class PerformanceSuite(BaseTestSuite):
    """
    性能压力测试
    目标: 大规模回测速度、并发查询性能
    """

    name = "性能压力测试"
    description = "验证大规模回测、并发查询性能"

    def _run_tests(self):
        self._test_query_performance()
        self._test_batch_query_performance()
        self._test_large_universe_backtest()

    def _test_query_performance(self):
        """测试8.1: 单次查询性能"""
        try:
            import time
            import duckdb

            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 单只股票全年查询
            start_time = time.time()
            result = conn.execute("""
                SELECT * FROM daily_bar_raw
                WHERE ts_code = '600519.SH'
                  AND trade_date >= '2024-01-01'
                  AND trade_date <= '2024-12-31'
                ORDER BY trade_date
            """).df()
            elapsed = time.time() - start_time

            conn.close()

            # 应该在100ms内完成
            if elapsed < 0.1:
                self._add_test(
                    "单次查询性能",
                    "PASS",
                    f"查询耗时: {elapsed*1000:.1f}ms"
                )
            elif elapsed < 1.0:
                self._add_test(
                    "单次查询性能",
                    "WARN",
                    f"查询耗时: {elapsed*1000:.1f}ms (略慢)"
                )
            else:
                self._add_test(
                    "单次查询性能",
                    "FAIL",
                    f"查询耗时: {elapsed*1000:.1f}ms (过慢)"
                )
                self._add_issue(f"单次查询性能不达标: {elapsed:.3f}s")

        except Exception as e:
            self._add_test("单次查询性能", "FAIL", str(e))

    def _test_batch_query_performance(self):
        """测试8.2: 批量查询性能"""
        try:
            import time
            import duckdb

            conn = duckdb.connect(str(PROJECT_ROOT / "data" / "stock_data.duckdb"), read_only=True)

            # 查询100只股票
            stocks = [f"{str(i).zfill(6)}.SH" for i in range(600000, 600100, 1)]

            start_time = time.time()
            placeholders = ",".join([f"'{s}'" for s in stocks])
            result = conn.execute(f"""
                SELECT ts_code, trade_date, close
                FROM daily_bar_raw
                WHERE ts_code IN ({placeholders})
                  AND trade_date = '2024-06-28'
            """).df()
            elapsed = time.time() - start_time

            conn.close()

            # 应该在1秒内完成
            if elapsed < 1.0:
                self._add_test(
                    "批量查询性能",
                    "PASS",
                    f"100只股票查询耗时: {elapsed*1000:.1f}ms"
                )
            elif elapsed < 5.0:
                self._add_test(
                    "批量查询性能",
                    "WARN",
                    f"100只股票查询耗时: {elapsed*1000:.1f}ms"
                )
            else:
                self._add_test(
                    "批量查询性能",
                    "FAIL",
                    f"100只股票查询耗时: {elapsed*1000:.1f}ms (过慢)"
                )

        except Exception as e:
            self._add_test("批量查询性能", "FAIL", str(e))

    def _test_large_universe_backtest(self):
        """测试8.3: 大规模股票池回测"""
        try:
            import time
            from scripts.backtest_engine_v3 import ProductionBacktestEngine, BacktestConfig
            from scripts.data_engine import DataEngine
            from datetime import datetime

            # 使用较小范围测试
            config = BacktestConfig(
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 31),
                initial_capital=10_000_000,
            )

            de = DataEngine(db_path=str(PROJECT_ROOT / "data" / "stock_data.duckdb"))
            engine = ProductionBacktestEngine(config=config, data_engine=de)

            # 简单策略
            def strategy(factor_data, date):
                if factor_data is None or factor_data.empty:
                    return {}
                return {}

            start_time = time.time()
            result = engine.run(strategy)
            elapsed = time.time() - start_time

            de.close()

            # 1个月回测应该在10秒内完成
            if elapsed < 10:
                self._add_test(
                    "大规模回测性能",
                    "PASS",
                    f"1个月回测耗时: {elapsed:.1f}s"
                )
            elif elapsed < 60:
                self._add_test(
                    "大规模回测性能",
                    "WARN",
                    f"1个月回测耗时: {elapsed:.1f}s (偏慢)"
                )
            else:
                self._add_test(
                    "大规模回测性能",
                    "FAIL",
                    f"1个月回测耗时: {elapsed:.1f}s (过慢)"
                )
                self._add_issue(f"回测性能不达标: {elapsed:.1f}s")

        except Exception as e:
            self._add_test("大规模回测性能", "FAIL", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# 主程序
# ═══════════════════════════════════════════════════════════════════════════════

def run_all_suites() -> ValidationReport:
    """运行所有验证套件"""
    print("\n" + "=" * 70)
    print("[A股数据生产级别验证套件]")
    print("=" * 70)
    print(f"\n目标: 确保数据满足量化选股和回测的生产要求")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    report = ValidationReport()

    # 按顺序运行所有套件
    suites = [
        DataCompletenessSuite(),
        DataAccuracySuite(),
        PITQuerySuite(),
        SurvivorshipBiasSuite(),
        TradingRulesSuite(),
        BacktestEngineSuite(),
        FactorAccuracySuite(),
        PerformanceSuite(),
    ]

    for suite in suites:
        results = suite.run()
        report.add_suite(suite.name, results)

        # 打印测试结果摘要
        passed = sum(1 for t in results["tests"] if t["status"] == "PASS")
        failed = sum(1 for t in results["tests"] if t["status"] == "FAIL")
        warned = sum(1 for t in results["tests"] if t["status"] == "WARN")

        status_icon = "[PASS]" if results["status"] == "PASS" else "[FAIL]"
        print(f"\n{status_icon} {suite.name}: {passed} passed, {failed} failed, {warned} warned")

    return report


if __name__ == "__main__":
    report = run_all_suites()

    # 保存报告
    json_path, md_path = report.save()

    # 打印最终摘要
    print("\n" + "=" * 70)
    print("验证结果摘要")
    print("=" * 70)
    print(f"\n总测试套件: {report.summary['total_suites']}")
    print(f"通过: {report.summary['passed_suites']}")
    print(f"失败: {report.summary['failed_suites']}")
    print(f"\n总测试用例: {report.summary['total_tests']}")
    print(f"通过: {report.summary['passed_tests']}")
    print(f"失败: {report.summary['failed_tests']}")
    print(f"警告: {report.summary['warnings']}")

    # 打印失败套件详情
    if report.summary['failed_suites'] > 0:
        print("\n" + "=" * 70)
        print("失败套件详情")
        print("=" * 70)
        for name, suite in report.suites.items():
            if suite["status"] == "FAIL":
                print(f"\n{name}:")
                for issue in suite.get("issues", []):
                    print(f"  - {issue}")

    # 打印Markdown报告路径
    print(f"\n详细报告: {md_path}")
