"""测试配置和 fixtures

为所有测试模块提供共享配置和工具函数
"""
import sys
from pathlib import Path
import os

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 测试数据目录
TEST_DATA_DIR = Path(__file__).parent / "test_data"
TEST_DATA_DIR.mkdir(exist_ok=True)

# 禁用日志输出（测试时太吵）
os.environ["LOGURU_LEVEL"] = "WARNING"
