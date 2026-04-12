import sys, inspect
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import importlib.util
spec = importlib.util.spec_from_file_location(
    "data_consistency_checker",
    r"C:\Users\zengj\.qclaw\workspace\stock_data_project\scripts\data_consistency_checker.py"
)
checker_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker_mod)
DataConsistencyChecker = checker_mod.DataConsistencyChecker
print("Methods:")
for name, method in inspect.getmembers(DataConsistencyChecker, predicate=inspect.isfunction):
    if not name.startswith('_'):
        print(f"  {name}")
        sig = inspect.signature(method)
        print(f"    {sig}")
