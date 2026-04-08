"""
conftest.py - pytest/unittest 路径配置

确保项目 scripts/ 目录优先级高于全局 scripts/ 目录
（防止 C:\Users\zengj\.qclaw\workspace\scripts 干扰导入）
"""
import sys
import os

_project_root = os.path.join(os.path.dirname(__file__), '..')
_scripts_path = os.path.join(_project_root, 'scripts')

# 将项目 scripts 目录插入 sys.path 最前
if _scripts_path not in sys.path:
    sys.path.insert(0, _scripts_path)

# 同时确保项目根目录在路径中
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
