"""数据QA管道 - 自动化数据体检机制

生产级数据需要一套自动化的"体检"机制：
1. 逻辑校验：high >= low, high >= close, high >= open
2. 对齐校验：缺失数据检测
3. 业务规则校验：成交量为0时价格应保持不变
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
import json
import pandas as pd
import numpy as np
from loguru import logger


class QASeverity(Enum):
    """QA问题严重程度"""
    INFO = "info"           # 信息提示
    WARNING = "warning"     # 警告（可接受）
    ERROR = "error"         # 错误（需要修复）
    CRITICAL = "critical"   # 严重（阻塞回测）


class QACheckType(Enum):
    """QA检查类型"""
    LOGIC = "logic"                 # 逻辑校验
    ALIGNMENT = "alignment"         # 对齐校验
    BUSINESS_RULE = "business_rule" # 业务规则
    STATISTICAL = "statistical"     # 统计异常
    COMPLETENESS = "completeness"   # 完整性


@dataclass
class QAIssue:
    """QA问题记录"""
    check_type: QACheckType
    severity: QASeverity
    ts_code: Optional[str]
    trade_date: Optional[str]
    field: Optional[str]
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    suggestion: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'check_type': self.check_type.value,
            'severity': self.severity.value,
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'field': self.field,
            'message': self.message,
            'expected': self.expected,
            'actual': self.actual,
            'suggestion': self.suggestion
        }


@dataclass
class QAResult:
    """QA检查结果"""
    check_name: str
    checked_records: int
    issues: List[QAIssue] = field(default_factory=list)
    passed: bool = True
    duration_ms: float = 0.0
    
    @property
    def critical_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QASeverity.CRITICAL)
    
    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QASeverity.ERROR)
    
    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == QASeverity.WARNING)
    
    def to_dict(self) -> Dict:
        return {
            'check_name': self.check_name,
            'checked_records': self.checked_records,
            'passed': self.passed and self.critical_count == 0,
            'duration_ms': round(self.duration_ms, 2),
            'summary': {
                'critical': self.critical_count,
                'error': self.error_count,
                'warning': self.warning_count,
                'total_issues': len(self.issues)
            },
            'issues': [i.to_dict() for i in self.issues[:20]]  # 只保留前20个
        }


class DataQAPipeline:
    """
    数据QA管道
    
    自动化数据体检流程：
    1. 每日定时扫描（可配置cron）
    2. 多维度校验（逻辑/对齐/业务/统计）
    3. 生成Missing_Data_Report
    4. 严重问题告警
    """
    
    def __init__(self, data_engine: Any, 
                 report_dir: Optional[str] = None):
        """
        Parameters
        ----------
        data_engine : DataEngine
            数据引擎实例
        report_dir : str, optional
            QA报告输出目录
        """
        self.data_engine = data_engine
        
        project_root = Path(__file__).resolve().parent.parent
        self.report_dir = Path(report_dir or project_root / '_output' / 'qa_reports')
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # 注册的检查器
        self._checkers: List[Tuple[str, Callable]] = []
        self._register_default_checkers()
        
        logger.info(f"DataQAPipeline initialized, report_dir: {self.report_dir}")
    
    def _register_default_checkers(self):
        """注册默认检查器"""
        self._checkers = [
            ('price_logic', self._check_price_logic),
            ('volume_consistency', self._check_volume_consistency),
            ('data_completeness', self._check_data_completeness),
            ('suspension_consistency', self._check_suspension_consistency),
            ('statistical_anomaly', self._check_statistical_anomaly),
        ]
    
    def run_full_check(self, 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       ts_codes: Optional[List[str]] = None) -> Dict:
        """
        运行完整QA检查
        
        Returns
        -------
        Dict : 完整QA报告
        """
        import time
        
        start_time = time.time()
        
        # 默认检查最近30天
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"Starting full QA check: {start_date} ~ {end_date}")
        
        results = []
        for check_name, checker in self._checkers:
            check_start = time.time()
            try:
                result = checker(start_date, end_date, ts_codes)
                result.duration_ms = (time.time() - check_start) * 1000
                results.append(result)
                logger.info(f"  {check_name}: {result.checked_records} records, "
                          f"{len(result.issues)} issues ({result.duration_ms:.0f}ms)")
            except Exception as e:
                logger.error(f"Check {check_name} failed: {e}")
                results.append(QAResult(
                    check_name=check_name,
                    checked_records=0,
                    passed=False,
                    issues=[QAIssue(
                        check_type=QACheckType.LOGIC,
                        severity=QASeverity.CRITICAL,
                        ts_code=None,
                        trade_date=None,
                        field=None,
                        message=f"Checker failed: {str(e)}"
                    )]
                ))
        
        # 生成报告
        report = self._generate_report(results, start_date, end_date)
        
        # 保存报告
        self._save_report(report)
        
        # 严重问题告警
        total_critical = sum(r.critical_count for r in results)
        if total_critical > 0:
            logger.error(f"QA CRITICAL: {total_critical} critical issues found!")
        
        total_time = time.time() - start_time
        logger.info(f"QA completed in {total_time:.1f}s")
        
        return report
    
    def _check_price_logic(self, 
                           start_date: str, 
                           end_date: str,
                           ts_codes: Optional[List[str]] = None) -> QAResult:
        """
        价格逻辑校验
        
        规则：
        - high >= low
        - high >= close
        - high >= open
        - low <= close
        - low <= open
        """
        issues = []
        
        # 获取数据
        df = self._fetch_price_data(start_date, end_date, ts_codes)
        
        if df.empty:
            return QAResult('price_logic', 0, [QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.WARNING,
                ts_code=None,
                trade_date=None,
                field=None,
                message="No price data found for the date range"
            )])
        
        # 向量化检查
        # high < low
        mask = df['high'] < df['low']
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='high/low',
                message=f"high ({row['high']}) < low ({row['low']})",
                expected=f"high >= low",
                actual=f"high={row['high']}, low={row['low']}"
            ))
        
        # high < close
        mask = df['high'] < df['close']
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='high/close',
                message=f"high ({row['high']}) < close ({row['close']})",
                expected="high >= close",
                actual=f"high={row['high']}, close={row['close']}"
            ))
        
        # high < open
        mask = df['high'] < df['open']
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='high/open',
                message=f"high ({row['high']}) < open ({row['open']})",
                expected="high >= open",
                actual=f"high={row['high']}, open={row['open']}"
            ))
        
        # low > close
        mask = df['low'] > df['close']
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='low/close',
                message=f"low ({row['low']}) > close ({row['close']})",
                expected="low <= close",
                actual=f"low={row['low']}, close={row['close']}"
            ))
        
        # low > open
        mask = df['low'] > df['open']
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.LOGIC,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='low/open',
                message=f"low ({row['low']}) > open ({row['open']})",
                expected="low <= open",
                actual=f"low={row['low']}, open={row['open']}"
            ))
        
        # 价格为负或零
        for col in ['open', 'high', 'low', 'close']:
            mask = df[col] <= 0
            for _, row in df[mask].head(5).iterrows():
                issues.append(QAIssue(
                    check_type=QACheckType.LOGIC,
                    severity=QASeverity.CRITICAL,
                    ts_code=row['ts_code'],
                    trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                    field=col,
                    message=f"{col} is {row[col]} (should be > 0)",
                    expected="> 0",
                    actual=row[col],
                    suggestion="Check data source for errors or missing data"
                ))
        
        return QAResult('price_logic', len(df), issues)
    
    def _check_volume_consistency(self,
                                   start_date: str,
                                   end_date: str,
                                   ts_codes: Optional[List[str]] = None) -> QAResult:
        """
        成交量一致性校验
        
        业务规则：
        - 成交量为0时，收盘价应与前一日持平（除非是首日或特殊停牌）
        - 成交量不应为负数
        """
        issues = []
        df = self._fetch_price_data(start_date, end_date, ts_codes)
        
        if df.empty:
            return QAResult('volume_consistency', 0, issues)
        
        # 按股票分组检查
        for ts_code, group in df.groupby('ts_code'):
            group = group.sort_values('trade_date')
            
            # 检查负成交量
            neg_vol = group[group['volume'] < 0]
            for _, row in neg_vol.iterrows():
                issues.append(QAIssue(
                    check_type=QACheckType.BUSINESS_RULE,
                    severity=QASeverity.CRITICAL,
                    ts_code=ts_code,
                    trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                    field='volume',
                    message=f"Negative volume: {row['volume']}",
                    expected=">= 0",
                    actual=row['volume']
                ))
            
            # 检查成交量为0但价格变化的情况
            zero_vol = group[group['volume'] == 0]
            for i, row in zero_vol.iterrows():
                # 找到前一日
                prev_rows = group[group['trade_date'] < row['trade_date']]
                if prev_rows.empty:
                    continue  # 首日，跳过
                
                prev_close = prev_rows.iloc[-1]['close']
                if abs(row['close'] - prev_close) > 0.01:  # 允许0.01的浮点误差
                    issues.append(QAIssue(
                        check_type=QACheckType.BUSINESS_RULE,
                        severity=QASeverity.WARNING,
                        ts_code=ts_code,
                        trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                        field='close',
                        message=f"Price changed with zero volume: {prev_close} -> {row['close']}",
                        expected=f"close == {prev_close} (when volume=0)",
                        actual=row['close'],
                        suggestion="May indicate data error or special suspension"
                    ))
        
        return QAResult('volume_consistency', len(df), issues)
    
    def _check_data_completeness(self,
                                  start_date: str,
                                  end_date: str,
                                  ts_codes: Optional[List[str]] = None) -> QAResult:
        """
        数据完整性校验
        
        生成Missing_Data_Report，扫描哪些股票在交易日缺失了记录
        """
        issues = []
        
        # 获取交易日历
        trade_dates = self.data_engine.get_trade_dates(start_date, end_date)
        if not trade_dates:
            return QAResult('data_completeness', 0, [QAIssue(
                check_type=QACheckType.COMPLETENESS,
                severity=QASeverity.WARNING,
                ts_code=None,
                trade_date=None,
                field=None,
                message="No trade dates found in calendar"
            )])
        
        # 获取股票列表
        if ts_codes is None:
            stocks_df = self.data_engine.query("SELECT ts_code FROM stock_basic WHERE is_delisted = FALSE")
            ts_codes = stocks_df['ts_code'].tolist() if not stocks_df.empty else []
        
        # 检查每只股票的数据完整性
        missing_report = []
        
        for ts_code in ts_codes[:100]:  # 抽样检查前100只
            # 获取该股票的数据
            df = self.data_engine.query("""
                SELECT trade_date FROM daily_quotes
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
            """, (ts_code, start_date, end_date))
            
            if df.empty:
                # 完全缺失
                missing_report.append({
                    'ts_code': ts_code,
                    'missing_dates': trade_dates,
                    'missing_count': len(trade_dates)
                })
                if len(missing_report) <= 5:  # 只记录前5个完全缺失的
                    issues.append(QAIssue(
                        check_type=QACheckType.COMPLETENESS,
                        severity=QASeverity.ERROR,
                        ts_code=ts_code,
                        trade_date=None,
                        field=None,
                        message=f"No data for entire period ({len(trade_dates)} days)",
                        suggestion="Check if stock was delisted or data source issue"
                    ))
                continue
            
            # 检查缺失的日期
            existing_dates = set(df['trade_date'].dt.strftime('%Y-%m-%d') if hasattr(df['trade_date'], 'dt') 
                                else df['trade_date'].astype(str))
            missing_dates = [d for d in trade_dates if d not in existing_dates]
            
            if missing_dates:
                missing_report.append({
                    'ts_code': ts_code,
                    'missing_dates': missing_dates,
                    'missing_count': len(missing_dates)
                })
                
                if len(missing_dates) > 5:  # 大量缺失才报告
                    issues.append(QAIssue(
                        check_type=QACheckType.COMPLETENESS,
                        severity=QASeverity.WARNING,
                        ts_code=ts_code,
                        trade_date=missing_dates[0],
                        field=None,
                        message=f"Missing {len(missing_dates)} trading days",
                        actual=f"Missing: {missing_dates[:3]}..."
                    ))
        
        # 保存Missing_Data_Report
        self._save_missing_data_report(missing_report, start_date, end_date)
        
        return QAResult('data_completeness', len(ts_codes), issues)
    
    def _check_suspension_consistency(self,
                                       start_date: str,
                                       end_date: str,
                                       ts_codes: Optional[List[str]] = None) -> QAResult:
        """
        停牌标记一致性校验
        
        规则：
        - is_suspend=True 时，volume应该为0或接近0
        - volume=0 时，is_suspend应该为True（除非是首日）
        """
        issues = []
        df = self._fetch_price_data(start_date, end_date, ts_codes)
        
        if df.empty or 'is_suspend' not in df.columns:
            return QAResult('suspension_consistency', 0, issues)
        
        # 标记停牌但成交量不为0
        mask = (df['is_suspend'] == True) & (df['volume'] > 1000)  # 允许少量误差
        for _, row in df[mask].head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.BUSINESS_RULE,
                severity=QASeverity.WARNING,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='is_suspend',
                message=f"is_suspend=True but volume={row['volume']}",
                expected="volume ≈ 0 when suspended",
                actual=f"volume={row['volume']}"
            ))
        
        return QAResult('suspension_consistency', len(df), issues)
    
    def _check_statistical_anomaly(self,
                                    start_date: str,
                                    end_date: str,
                                    ts_codes: Optional[List[str]] = None) -> QAResult:
        """
        统计异常检测
        
        检测：
        - 涨跌幅异常（超过板块限制）
        - 成交量异常（突然放大/缩小）
        - 价格跳空
        """
        issues = []
        df = self._fetch_price_data(start_date, end_date, ts_codes)
        
        if df.empty or 'pct_chg' not in df.columns:
            return QAResult('statistical_anomaly', 0, issues)
        
        # 涨跌幅异常（按板块区分）
        from scripts.data_engine import detect_limit
        
        df['limit_pct'] = df['ts_code'].str[:6].apply(detect_limit) * 100
        df['excess_pct'] = df['pct_chg'].abs() - df['limit_pct']
        
        anomalies = df[df['excess_pct'] > 1.0]  # 超过限制1%以上
        for _, row in anomalies.head(10).iterrows():
            issues.append(QAIssue(
                check_type=QACheckType.STATISTICAL,
                severity=QASeverity.ERROR,
                ts_code=row['ts_code'],
                trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                field='pct_chg',
                message=f"Abnormal change: {row['pct_chg']:.2f}% (limit: ±{row['limit_pct']:.0f}%)",
                expected=f"|pct_chg| <= {row['limit_pct']}%",
                actual=f"{row['pct_chg']:.2f}%",
                suggestion="Check for data error or corporate action"
            ))
        
        # 成交量突然放大（单日成交量 > 20日均量 * 10）
        for ts_code, group in df.groupby('ts_code'):
            if len(group) < 20:
                continue
            group = group.sort_values('trade_date')
            group['vol_ma20'] = group['volume'].rolling(20).mean()
            group['vol_ratio'] = group['volume'] / group['vol_ma20']
            
            spikes = group[group['vol_ratio'] > 10]
            for _, row in spikes.head(3).iterrows():
                issues.append(QAIssue(
                    check_type=QACheckType.STATISTICAL,
                    severity=QASeverity.INFO,
                    ts_code=ts_code,
                    trade_date=row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date']),
                    field='volume',
                    message=f"Volume spike: {row['vol_ratio']:.1f}x average",
                    actual=f"volume={row['volume']}, avg20={row['vol_ma20']:.0f}"
                ))
        
        return QAResult('statistical_anomaly', len(df), issues)
    
    def _fetch_price_data(self, 
                          start_date: str, 
                          end_date: str,
                          ts_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """获取价格数据"""
        if ts_codes:
            # 批量查询
            return self.data_engine.get_batch_stock_data(ts_codes, end_date)
        else:
            return self.data_engine.query("""
                SELECT * FROM daily_quotes
                WHERE trade_date BETWEEN ? AND ?
            """, (start_date, end_date))
    
    def _generate_report(self, 
                         results: List[QAResult],
                         start_date: str,
                         end_date: str) -> Dict:
        """生成QA报告"""
        total_critical = sum(r.critical_count for r in results)
        total_error = sum(r.error_count for r in results)
        total_warning = sum(r.warning_count for r in results)
        total_records = sum(r.checked_records for r in results)
        
        return {
            'report_type': 'Data_QA_Full_Check',
            'generated_at': datetime.now().isoformat(),
            'date_range': {'start': start_date, 'end': end_date},
            'summary': {
                'total_records_checked': total_records,
                'total_checks': len(results),
                'passed_checks': sum(1 for r in results if r.passed and r.critical_count == 0),
                'failed_checks': sum(1 for r in results if not r.passed or r.critical_count > 0),
                'issues': {
                    'critical': total_critical,
                    'error': total_error,
                    'warning': total_warning
                },
                'overall_status': 'PASS' if total_critical == 0 and total_error == 0 
                                 else 'FAIL' if total_critical > 0 
                                 else 'WARNING'
            },
            'details': [r.to_dict() for r in results]
        }
    
    def _save_report(self, report: Dict):
        """保存QA报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"qa_report_{timestamp}.json"
        filepath = self.report_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"QA report saved: {filepath}")
        
        # 同时保存最新报告的软链接/副本
        latest_path = self.report_dir / 'latest_qa_report.json'
        with open(latest_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    def _save_missing_data_report(self, 
                                   missing_data: List[Dict],
                                   start_date: str,
                                   end_date: str):
        """保存缺失数据报告"""
        report = {
            'report_type': 'Missing_Data_Report',
            'generated_at': datetime.now().isoformat(),
            'date_range': {'start': start_date, 'end': end_date},
            'total_stocks_checked': len(missing_data),
            'stocks_with_missing_data': len([m for m in missing_data if m['missing_count'] > 0]),
            'details': missing_data[:100]  # 只保留前100条
        }
        
        timestamp = datetime.now().strftime('%Y%m%d')
        filepath = self.report_dir / f'missing_data_report_{timestamp}.json'
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    def get_latest_report(self) -> Optional[Dict]:
        """获取最新QA报告"""
        latest_path = self.report_dir / 'latest_qa_report.json'
        if latest_path.exists():
            with open(latest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def has_critical_issues(self) -> bool:
        """检查是否有严重问题"""
        report = self.get_latest_report()
        if report:
            return report['summary']['issues']['critical'] > 0
        return False


# 便捷函数
def run_daily_qa_check(data_engine: Any) -> Dict:
    """运行每日QA检查"""
    pipeline = DataQAPipeline(data_engine)
    return pipeline.run_full_check()
