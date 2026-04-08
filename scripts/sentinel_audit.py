"""QClaw-Sentinel 自动化代码审查工具

使用方法：
    python sentinel_audit.py <file_path>
    
示例：
    python sentinel_audit.py scripts/execution_engine_v3.py
"""
import sys
import ast
import re
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass
from enum import Enum


from datetime import datetime as _dt


def _get_now():
    """获取当前时间（支持单测 mock）。"""
    return _dt.now()


class Severity(Enum):
    CRITICAL = "🚨"
    HIGH = "❌"
    MEDIUM = "⚠️"
    LOW = "💡"


@dataclass
class AuditIssue:
    line: int
    severity: Severity
    category: str
    message: str
    suggestion: str


class StaticCodeAuditor:
    """静态代码审计器"""
    
    PATTERNS = {
        # (pattern, severity, category, message, suggestion)
        (r'\.shift\s*\(\s*-\s*\d+\s*\)', 
         Severity.CRITICAL, "未来函数",
         "使用负数shift获取未来数据",
         "改为使用正数shift获取历史数据：.shift(1)"),
        
        (r'for\s+\w+\s+in\s+range\s*\(\s*len\s*\(\s*\w+\s*\)\s*\)',
         Severity.HIGH, "性能",
         "使用range(len(df))遍历DataFrame",
         "改为向量化操作或使用df.itertuples()"),
        
        (r'\.dropna\s*\(\s*\)\s*$',
         Severity.HIGH, "数据完整性",
         "直接删除所有NA值",
         "指定subset和how参数，避免误删有效数据"),
        
        (r'except\s*:\s*$',
         Severity.CRITICAL, "异常处理",
         "裸except捕获所有异常",
         "指定具体异常类型：except ValueError:"),
        
        (r'eval\s*\(',
         Severity.CRITICAL, "安全",
         "使用eval执行动态代码",
         "避免使用eval，改用ast.literal_eval或更安全的方式"),
        
        (r'\.apply\s*\(\s*lambda',
         Severity.MEDIUM, "性能",
         "使用apply(lambda)",
         "检查是否可替换为向量化操作，速度提升10-100倍"),
        
        (r'pd\.concat\s*\(\s*\[\s*\w+\s+for\s+\w+\s+in',
         Severity.MEDIUM, "性能",
         "循环中动态concat",
         "预分配list后一次性concat：pd.concat(list_of_dfs)"),
        
        (r'\.copy\s*\(\s*\).*\.copy\s*\(\s*\)',
         Severity.LOW, "内存",
         "连续多次copy",
         "考虑使用view或inplace操作减少内存占用"),
        
        (r'datetime\s*\.\s*now\s*\(\s*\)',
         Severity.MEDIUM, "可复现性",
         "使用_get_now()获取当前时间",
         "回测应使用固定时间或传入的时间参数，确保可复现"),
    }
    
    @classmethod
    def audit(cls, code: str) -> List[AuditIssue]:
        """审计代码"""
        issues = []
        lines = code.split('\n')
        
        for i, line in enumerate(lines, 1):
            for pattern, severity, category, msg, suggestion in cls.PATTERNS:
                if re.search(pattern, line):
                    issues.append(AuditIssue(
                        line=i,
                        severity=severity,
                        category=category,
                        message=msg,
                        suggestion=suggestion
                    ))
        
        return issues


class QuantSpecificAuditor:
    """量化特定审计器"""
    
    @staticmethod
    def check_lookahead_bias(code: str) -> List[AuditIssue]:
        """检查未来函数"""
        issues = []
        
        # 检查是否在信号生成中使用当日收盘价
        if 'def generate_signal' in code or 'def get_factor' in code:
            if re.search(r'\bclose\b(?!.*shift)', code):
                issues.append(AuditIssue(
                    line=0,
                    severity=Severity.CRITICAL,
                    category="未来函数",
                    message="信号生成函数中直接使用close价格，可能存在未来函数",
                    suggestion="使用shift(1)获取昨日收盘价：df['close'].shift(1)"
                ))
        
        return issues
    
    @staticmethod
    def check_survivorship_bias(code: str) -> List[AuditIssue]:
        """检查幸存者偏差"""
        issues = []
        
        # 检查是否使用了固定的股票列表
        if re.search(r'symbols\s*=\s*\[.*\d{6}', code):
            if 'delisted' not in code.lower() and 'survivorship' not in code.lower():
                issues.append(AuditIssue(
                    line=0,
                    severity=Severity.CRITICAL,
                    category="幸存者偏差",
                    message="使用固定股票列表，未处理退市股票",
                    suggestion="使用SurvivorshipBiasHandler获取历史全量股票"
                ))
        
        return issues
    
    @staticmethod
    def check_price_adjustment(code: str) -> List[AuditIssue]:
        """检查复权处理"""
        issues = []
        
        # 检查是否混用复权方式
        if 'qfq' in code and 'hfq' in code:
            issues.append(AuditIssue(
                line=0,
                severity=Severity.HIGH,
                category="数据一致性",
                message="混用前复权(qfq)和后复权(hfq)",
                suggestion="统一使用一种复权方式，建议后复权(hfq)避免未来函数"
            ))
        
        return issues


class SentinelReporter:
    """审查报告生成器"""
    
    @staticmethod
    def generate_report(issues: List[AuditIssue], file_path: str) -> str:
        """生成审查报告"""
        if not issues:
            return f"[PASS] {file_path}: 未发现明显问题"
        
        # 按严重程度分组
        critical = [i for i in issues if i.severity == Severity.CRITICAL]
        high = [i for i in issues if i.severity == Severity.HIGH]
        medium = [i for i in issues if i.severity == Severity.MEDIUM]
        low = [i for i in issues if i.severity == Severity.LOW]
        
        report = []
        report.append(f"\n{'='*60}")
        report.append(f"QClaw-Sentinel 审查报告: {file_path}")
        report.append(f"{'='*60}")
        report.append(f"总计: {len(issues)} 个问题")
        report.append(f"  [CRIT] 致命: {len(critical)}")
        report.append(f"  [HIGH] 严重: {len(high)}")
        report.append(f"  [WARN] 警告: {len(medium)}")
        report.append(f"  [INFO] 建议: {len(low)}")
        
        if critical:
            report.append(f"\n{'='*60}")
            report.append("[CRITICAL] 致命错误 (Critical Errors)")
            report.append(f"{'='*60}")
            for issue in critical:
                report.append(f"  Line{issue.line}: [{issue.category}] {issue.message}")
                report.append(f"    Suggest: {issue.suggestion}")
        
        if high:
            report.append(f"\n{'='*60}")
            report.append("[HIGH] 严重问题 (High Priority)")
            report.append(f"{'='*60}")
            for issue in high:
                report.append(f"  Line{issue.line}: [{issue.category}] {issue.message}")
                report.append(f"    Suggest: {issue.suggestion}")
        
        if medium:
            report.append(f"\n{'='*60}")
            report.append("[WARNING] 警告 (Warnings)")
            report.append(f"{'='*60}")
            for issue in medium:
                report.append(f"  Line{issue.line}: [{issue.category}] {issue.message}")
        
        report.append(f"\n{'='*60}")
        return '\n'.join(report)


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python sentinel_audit.py <file_path>")
        print("示例: python sentinel_audit.py scripts/execution_engine_v3.py")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"错误: 文件不存在 {file_path}")
        sys.exit(1)
    
    # 读取代码
    with open(file_path, 'r', encoding='utf-8') as f:
        code = f.read()
    
    # 执行审计
    all_issues = []
    all_issues.extend(StaticCodeAuditor.audit(code))
    all_issues.extend(QuantSpecificAuditor.check_lookahead_bias(code))
    all_issues.extend(QuantSpecificAuditor.check_survivorship_bias(code))
    all_issues.extend(QuantSpecificAuditor.check_price_adjustment(code))
    
    # 生成报告
    report = SentinelReporter.generate_report(all_issues, file_path)
    print(report)
    
    # 返回码：有致命错误则返回1
    has_critical = any(i.severity == Severity.CRITICAL for i in all_issues)
    sys.exit(1 if has_critical else 0)


if __name__ == "__main__":
    main()
