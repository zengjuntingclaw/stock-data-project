"""测试运行脚本

用法：
    python run_tests.py              # 运行所有测试
    python run_tests.py -v           # 详细输出
    python run_tests.py test_core    # 只运行核心测试
    python run_tests.py test_production  # 只运行生产级测试
"""
import sys
import subprocess
from pathlib import Path


def run_tests(pattern=None, verbose=False):
    """运行测试"""
    test_dir = Path(__file__).parent / "tests"
    
    cmd = ["python", "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    
    if pattern:
        cmd.append(f"tests/{pattern}.py")
    else:
        cmd.append("tests/")
    
    # 添加覆盖率报告（如果安装了pytest-cov）
    # cmd.extend(["--cov=scripts", "--cov-report=term-missing"])
    
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    return result.returncode


def main():
    """主函数"""
    args = sys.argv[1:]
    
    verbose = "-v" in args or "--verbose" in args
    
    # 查找测试模式
    pattern = None
    for arg in args:
        if not arg.startswith("-"):
            pattern = arg
            break
    
    return_code = run_tests(pattern, verbose)
    sys.exit(return_code)


if __name__ == "__main__":
    main()
