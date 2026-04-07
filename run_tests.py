"""Test runner for A股多因子回测框架 v3.0

Usage:
    python run_tests.py              # Run all tests
    python run_tests.py -v           # Verbose
    python run_tests.py test_core    # Core tests only
    python run_tests.py test_production
    python run_tests.py --cov        # With coverage report
"""
from __future__ import annotations

import sys
import subprocess
from pathlib import Path


def run_tests(pattern: str = None, verbose: bool = False, coverage: bool = False) -> int:
    """Run pytest via sys.executable (portable across venv/CI/Windows)."""
    project_root = Path(__file__).parent
    cmd = [sys.executable, "-m", "pytest"]

    if verbose:
        cmd.append("-v")

    if coverage:
        cmd.extend(["--cov=scripts", "--cov-report=term-missing"])

    if pattern:
        cmd.append(f"tests/{pattern}.py")
    else:
        cmd.append("tests/")

    # Show Python version and pytest version for reproducibility
    print(f"Python: {sys.executable}")
    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)

    return subprocess.run(cmd, cwd=project_root).returncode


def main() -> None:
    """CLI entry point."""
    args = sys.argv[1:]
    verbose = "-v" in args or "--verbose" in args
    coverage = "--cov" in args
    args = [a for a in args if a not in ("-v", "--verbose", "--cov")]

    pattern = args[0] if args else None
    code = run_tests(pattern=pattern, verbose=verbose, coverage=coverage)
    sys.exit(code)


if __name__ == "__main__":
    main()
