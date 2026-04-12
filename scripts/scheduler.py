"""
Scheduler - 任务调度层
=====================
支持：
- Airflow DAG（生产环境）
- 本地简易调度（开发/测试）

Airflow 使用方式：
    1. 将 AIRFLOW_HOME 指向项目根目录
    2. 把 dags/ 目录链接到 $AIRFLOW_HOME/dags/
    3. airflow webserver -p 8080 & airflow scheduler

本地运行（无需 Airflow）：
    python -m scripts.scheduler local --task daily_download
"""
from __future__ import annotations

import os
import sys
import argparse
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Optional

# 确保项目根目录在路径中
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger

# =============================================================================
# 业务逻辑导入
# =============================================================================

from scripts.data_fetcher import DataFetcher
from scripts.data_validator import EnhancedValidator
from scripts.data_store import DataStore, ParquetStorage


# =============================================================================
# Airflow DAG 定义（适用于 Airflow 2.x+）
# =============================================================================

# 以下代码仅在被 Airflow 加载时执行，不影响本地运行
try:
    from airflow import DAG
    from airflow.decorators import task, dag
    from airflow.operators.python import PythonOperator
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False


# ----------------------------------------
# 默认参数
# ----------------------------------------

DEFAULT_DAG_ARGS = {
    "owner": "quant",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ----------------------------------------
# 任务函数（PythonCallable）
# ----------------------------------------

def _daily_download_task(**context):
    """
    每日全量股票数据下载
    由 Airflow 的 PythonOperator 调用
    """
    execution_date = context.get("ds", date.today().isoformat())

    logger.info(f"[Scheduler] 开始下载任务，执行日期: {execution_date}")

    fetcher = DataFetcher(primary="tushare")
    store = DataStore(db_path="data/stock_data.duckdb")
    validator = EnhancedValidator()

    # 1. 获取股票列表
    result = fetcher.fetch_stock_list()
    if not result.success:
        logger.error(f"[Scheduler] 获取股票列表失败: {result.error}")
        raise RuntimeError(f"获取股票列表失败: {result.error}")

    df_stocks = result.data
    ts_codes = (
        df_stocks["ts_code"].dropna().unique().tolist()
        if "ts_code" in df_stocks.columns
        else []
    )
    logger.info(f"[Scheduler] 共 {len(ts_codes)} 只股票待下载")

    # 2. 下载近30天数据（增量）
    end = execution_date
    start = (datetime.fromisoformat(execution_date) - timedelta(days=30)).strftime("%Y-%m-%d")

    success, failed = 0, []
    for ts_code in ts_codes[:100]:  # 先处理100只，完整跑批可去掉 [:100]
        try:
            r = fetcher.fetch_daily(ts_code, start, end)
            if not r.success:
                failed.append(ts_code)
                continue

            df = r.data
            if df is None or df.empty:
                continue

            # 校验
            vresult = validator.validate(df, ts_code=ts_code)
            if not vresult["ok"]:
                validator.log_validation_report(vresult, ts_code)

            df_fixed = validator.validate_and_fix(df)

            # 写入 DuckDB（raw）
            store.execute(
                "INSERT INTO daily_bar_raw (trade_date, ts_code, open, high, low, close, volume, amount) "
                "SELECT trade_date, ts_code, open, high, low, close, volume, amount FROM df_fixed",
            )
            success += 1

        except Exception as e:
            logger.warning(f"[Scheduler] {ts_code} 下载失败: {e}")
            failed.append(ts_code)

    logger.info(
        f"[Scheduler] 下载完成，成功 {success}，失败 {len(failed)}"
        + (f"，失败列表: {failed[:10]}" if failed else "")
    )

    if failed:
        raise RuntimeError(f"部分股票下载失败: {failed[:10]}")


def _daily_validate_task(**context):
    """每日数据质量校验"""
    logger.info("[Scheduler] 开始数据质量校验")
    # TODO: 与 DataEngine 集成，对比 DuckDB 与 Parquet 数据一致性
    logger.info("[Scheduler] 校验完成")


# ----------------------------------------
# DAG 实例（按需取消注释启用）
# ----------------------------------------

# # 每日增量下载 DAG（交易日 16:05 执行，覆盖当天数据）
# with DAG(
#     dag_id="stock_data_daily_download",
#     default_args=DEFAULT_DAG_ARGS,
#     start_date=datetime(2024, 1, 1),
#     schedule_interval="5 16 * * 1-5",  # 周一至周五 16:05
#     catchup=False,
#     tags=["stock_data", "daily"],
# ) as dag_daily:
#     download = PythonOperator(
#         task_id="download_daily_data",
#         python_callable=_daily_download_task,
#     )
#     validate = PythonOperator(
#         task_id="validate_daily_data",
#         python_callable=_daily_validate_task,
#     )
#     download >> validate  # 依赖关系


# # 全量回填 DAG（按需手动触发）
# with DAG(
#     dag_id="stock_data_full_backfill",
#     default_args=DEFAULT_DAG_ARGS,
#     start_date=datetime(2020, 1, 1),
#     schedule_interval=None,  # 手动触发
#     catchup=True,
#     tags=["stock_data", "backfill"],
# ) as dag_backfill:
#     backfill = PythonOperator(
#         task_id="backfill_full_data",
#         python_callable=_daily_download_task,
#     )


# =============================================================================
# 本地调度（无需 Airflow）
# =============================================================================

def run_local(task_name: str, **kwargs):
    """本地运行指定任务（用于开发/测试）"""
    logger.info(f"[LocalScheduler] 任务: {task_name}")

    if task_name == "daily_download":
        context = {"ds": date.today().isoformat()}
        _daily_download_task(**context)
    elif task_name == "validate":
        _daily_validate_task(**{"ds": date.today().isoformat()})
    else:
        logger.error(f"未知任务: {task_name}")


# =============================================================================
# CLI 入口
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="stock_data_project 调度器")
    parser.add_argument("mode", choices=["local", "airflow"], help="运行模式")
    parser.add_argument("--task", default="daily_download", help="任务名（local 模式）")
    args = parser.parse_args()

    if args.mode == "local":
        run_local(args.task)
    elif args.mode == "airflow":
        if not HAS_AIRFLOW:
            print("ERROR: Airflow 未安装。请运行: pip install apache-airflow")
            sys.exit(1)
        print("INFO: 请使用 'airflow dags list' 查看可用 DAG，"
              "'airflow webserver -p 8080' 启动 Web UI")
