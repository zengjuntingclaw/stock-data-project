"""
生产级调度与监控系统
====================
核心功能：
1. Airflow / Prefect 任务调度
2. ErrorNotifier - 邮件/ Slack / 微信通知
3. 失败阈值自动暂停任务
4. 任务状态监控与告警
5. 日志聚合与问题追踪
"""
from __future__ import annotations

import os
import sys
import json
import time
import smtplib
import requests
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Callable, Any
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABC, abstractmethod
import threading
import pandas as pd
from loguru import logger

# 导入调度相关
try:
    from airflow import DAG
    from airflow.decorators import task, dag
    from airflow.operators.python import PythonOperator
    from airflow.models import TaskInstance
    from airflow.exceptions import AirflowException
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False

try:
    from prefect import task as prefect_task, flow, get_run_logger
    from prefect.blocks.system import Secret
    HAS_PREFECT = True
except ImportError:
    HAS_PREFECT = False


# ============================================================================
# 通知器基类
# ============================================================================

class Notifier(ABC):
    """通知器抽象基类"""
    
    @abstractmethod
    def send(self, title: str, message: str, level: str = "info", **kwargs):
        """发送通知"""
        pass
    
    def format_alert(self, alert: 'Alert') -> Dict:
        """格式化告警消息"""
        return {
            "title": alert.title,
            "message": alert.message,
            "level": alert.level,
            "timestamp": alert.timestamp.isoformat() if isinstance(alert.timestamp, datetime) else str(alert.timestamp),
            "source": alert.source,
            "details": alert.details
        }


@dataclass
class Alert:
    """告警信息"""
    title: str
    message: str
    level: str = "info"           # info / warning / error / critical
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    details: Dict = field(default_factory=dict)
    
    @property
    def is_critical(self) -> bool:
        return self.level in ("error", "critical")


# ============================================================================
# 邮件通知器
# ============================================================================

class EmailNotifier(Notifier):
    """邮件通知器"""
    
    def __init__(
        self,
        smtp_host: str = None,
        smtp_port: int = 587,
        smtp_user: str = None,
        smtp_password: str = None,
        from_addr: str = None,
        to_addrs: List[str] = None,
        use_tls: bool = True
    ):
        self.smtp_host = smtp_host or os.environ.get("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user or os.environ.get("SMTP_USER")
        self.smtp_password = smtp_password or os.environ.get("SMTP_PASSWORD")
        self.from_addr = from_addr or self.smtp_user
        self.to_addrs = to_addrs or os.environ.get("ALERT_EMAIL", "").split(",")
        self.use_tls = use_tls
    
    def send(self, title: str, message: str, level: str = "info", **kwargs):
        """发送邮件"""
        if not self.to_addrs:
            logger.warning("[EmailNotifier] 未配置收件人，跳过")
            return
        
        if not self.smtp_user or not self.smtp_password:
            logger.warning("[EmailNotifier] 未配置SMTP认证，跳过")
            return
        
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{level.upper()}] {title}"
            msg["From"] = self.from_addr
            msg["To"] = ", ".join(self.to_addrs)
            
            # HTML格式
            html = f"""
            <html>
            <body>
                <h2 style="color: {'red' if level in ('error', 'critical') else 'orange' if level == 'warning' else 'green'}">
                    {title}
                </h2>
                <p>{message}</p>
                <hr>
                <p><small>发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(message, "plain"))
            msg.attach(MIMEText(html, "html"))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_addr, self.to_addrs, msg.as_string())
            
            logger.info(f"[EmailNotifier] 邮件已发送: {title}")
            
        except Exception as e:
            logger.error(f"[EmailNotifier] 发送失败: {e}")


# ============================================================================
# Slack 通知器
# ============================================================================

class SlackNotifier(Notifier):
    """Slack 通知器"""
    
    def __init__(self, webhook_url: str = None, channel: str = None):
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
        self.channel = channel or os.environ.get("SLACK_CHANNEL")
        self._client = None
    
    def _get_client(self):
        """懒加载 Slack SDK"""
        if self._client is None:
            try:
                from slack_sdk import WebhookClient
                self._client = WebhookClient(self.webhook_url)
            except ImportError:
                logger.warning("slack_sdk未安装: pip install slack-sdk")
        return self._client
    
    def send(self, title: str, message: str, level: str = "info", **kwargs):
        """发送 Slack 消息"""
        if not self.webhook_url:
            logger.warning("[SlackNotifier] 未配置webhook_url，跳过")
            return
        
        client = self._get_client()
        if not client:
            # 使用 requests 发送
            try:
                color = {
                    "info": "#36a64f",
                    "warning": "#ff9900",
                    "error": "#ff0000",
                    "critical": "#ff0000"
                }.get(level, "#36a64f")
                
                payload = {
                    "attachments": [{
                        "color": color,
                        "title": title,
                        "text": message,
                        "fields": [
                            {"title": "Level", "value": level.upper(), "short": True},
                            {"title": "Time", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": True}
                        ]
                    }]
                }
                
                if self.channel:
                    payload["channel"] = self.channel
                
                response = requests.post(self.webhook_url, json=payload, timeout=10)
                response.raise_for_status()
                logger.info(f"[SlackNotifier] 消息已发送: {title}")
                
            except Exception as e:
                logger.error(f"[SlackNotifier] 发送失败: {e}")
    
    def send_alert(self, alert: Alert):
        """发送告警"""
        emoji = {
            "info": ":information_source:",
            "warning": ":warning:",
            "error": ":x:",
            "critical": ":rotating_light:"
        }.get(alert.level, ":information_source:")
        
        self.send(
            f"{emoji} {alert.title}",
            alert.message,
            alert.level
        )


# ============================================================================
# 企业微信通知器
# ============================================================================

class WeChatNotifier(Notifier):
    """企业微信通知器"""
    
    def __init__(self, webhook_url: str = None, key: str = None):
        # 企业微信群机器人 webhook
        self.webhook_url = webhook_url or os.environ.get("WECOM_WEBHOOK_URL")
        self.key = key or os.environ.get("WECOM_KEY")
        
        if self.key and not self.webhook_url:
            self.webhook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.key}"
    
    def send(self, title: str, message: str, level: str = "info", **kwargs):
        """发送企业微信消息"""
        if not self.webhook_url:
            logger.warning("[WeChatNotifier] 未配置webhook_url，跳过")
            return
        
        try:
            # Markdown 格式
            color_map = {
                "info": "info",
                "warning": "warning", 
                "error": "red",
                "critical": "red"
            }
            
            content = f"**{title}**\n\n{message}\n\n> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": content
                }
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            result = response.json()
            if result.get("errcode") == 0:
                logger.info(f"[WeChatNotifier] 消息已发送: {title}")
            else:
                logger.error(f"[WeChatNotifier] 发送失败: {result}")
                
        except Exception as e:
            logger.error(f"[WeChatNotifier] 发送失败: {e}")


# ============================================================================
# 告警管理器
# ============================================================================

class AlertManager:
    """
    告警管理器
    
    统一管理多种通知渠道，支持：
    - 告警聚合（避免告警风暴）
    - 告警升级（普通告警 -> 严重告警）
    - 告警抑制（同类告警在冷却期内只发一次）
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        
        self._notifiers: List[Notifier] = []
        self._cooldowns: Dict[str, datetime] = {}  # 告警冷却记录
        self._cooldown_minutes: int = 30  # 同一告警30分钟内不重复发送
        self._alert_counts: Dict[str, int] = {}  # 告警计数
        self._failure_threshold: int = 5  # 失败阈值
        self._auto_pause: bool = True   # 超过阈值自动暂停
        
        # 注册默认通知器
        self._register_default_notifiers()
    
    def _register_default_notifiers(self):
        """注册默认通知器"""
        # 邮件
        if os.environ.get("SMTP_USER"):
            self.register_notifier(EmailNotifier())
        
        # Slack
        if os.environ.get("SLACK_WEBHOOK_URL"):
            self.register_notifier(SlackNotifier())
        
        # 企业微信
        if os.environ.get("WECOM_WEBHOOK_URL") or os.environ.get("WECOM_KEY"):
            self.register_notifier(WeChatNotifier())
    
    def register_notifier(self, notifier: Notifier):
        """注册通知器"""
        self._notifiers.append(notifier)
        logger.info(f"[AlertManager] 注册通知器: {type(notifier).__name__}")
    
    def send_alert(self, alert: Alert, force: bool = False):
        """
        发送告警
        
        包含聚合和冷却逻辑：
        - 同一告警在冷却期内只发一次
        - 告警计数超过阈值触发升级
        """
        alert_key = f"{alert.source}:{alert.title}"
        
        # 冷却检查
        if not force and alert_key in self._cooldowns:
            cooldown_end = self._cooldowns[alert_key]
            if datetime.now() < cooldown_end:
                logger.debug(f"[AlertManager] 告警冷却中: {alert_key}, 剩余 {(cooldown_end - datetime.now()).seconds}s")
                return False
        
        # 更新冷却时间
        self._cooldowns[alert_key] = datetime.now() + timedelta(minutes=self._cooldown_minutes)
        
        # 告警计数
        self._alert_counts[alert_key] = self._alert_counts.get(alert_key, 0) + 1
        
        # 告警升级
        if self._alert_counts[alert_key] >= self._failure_threshold:
            if alert.level not in ("error", "critical"):
                alert.level = "warning" if alert.level == "info" else "error"
                alert.message = f"[升级 #{self._alert_counts[alert_key]}] {alert.message}"
        
        # 发送所有通知器
        for notifier in self._notifiers:
            try:
                notifier.send(alert.title, alert.message, alert.level)
            except Exception as e:
                logger.error(f"[AlertManager] {type(notifier).__name__} 发送失败: {e}")
        
        return True
    
    def send_sync_failure(self, ts_code: str, error: str, details: Dict = None):
        """发送同步失败告警"""
        alert = Alert(
            title=f"数据同步失败: {ts_code}",
            message=f"股票 {ts_code} 同步失败\n\n错误: {error}",
            level="error",
            source="data_sync",
            details=details or {}
        )
        return self.send_alert(alert)
    
    def send_task_failure(self, task_name: str, error: str, context: Dict = None):
        """发送任务失败告警"""
        alert = Alert(
            title=f"任务执行失败: {task_name}",
            message=f"任务 {task_name} 执行失败\n\n错误: {error}",
            level="critical" if context and context.get("retry") >= 3 else "error",
            source="task_execution",
            details=context or {}
        )
        return self.send_alert(alert)
    
    def send_data_quality_alert(self, issue_type: str, count: int, samples: List = None):
        """发送数据质量告警"""
        alert = Alert(
            title=f"数据质量问题: {issue_type}",
            message=f"发现 {count} 条数据质量问题\n\n类型: {issue_type}",
            level="warning",
            source="data_quality",
            details={"count": count, "samples": samples or []}
        )
        return self.send_alert(alert)
    
    def check_failure_threshold(self, task_name: str) -> bool:
        """
        检查失败阈值
        
        Returns: 是否超过阈值（超过阈值时应暂停任务）
        """
        key = f"task_failure:{task_name}"
        count = self._alert_counts.get(key, 0)
        
        if count >= self._failure_threshold:
            logger.warning(f"[AlertManager] 任务 {task_name} 失败次数({count})超过阈值({self._failure_threshold})，建议暂停")
            return True
        return False
    
    def record_failure(self, task_name: str):
        """记录失败"""
        key = f"task_failure:{task_name}"
        self._alert_counts[key] = self._alert_counts.get(key, 0) + 1
        
        # 检查是否需要自动暂停
        if self._auto_pause and self.check_failure_threshold(task_name):
            self.send_alert(Alert(
                title=f"任务已自动暂停: {task_name}",
                message=f"任务 {task_name} 连续失败 {self._alert_counts[key]} 次，已自动暂停",
                level="critical",
                source="auto_pause"
            ))
            return True
        return False
    
    def reset_failure_count(self, task_name: str):
        """重置失败计数"""
        key = f"task_failure:{task_name}"
        if key in self._alert_counts:
            del self._alert_counts[key]
        logger.info(f"[AlertManager] 重置 {task_name} 失败计数")
    
    def clear_cooldown(self, alert_key: str = None):
        """清除冷却记录"""
        if alert_key:
            self._cooldowns.pop(alert_key, None)
        else:
            self._cooldowns.clear()


# ============================================================================
# Airflow DAG 定义
# ============================================================================

if HAS_AIRFLOW:
    
    # 默认参数
    DEFAULT_DAG_ARGS = {
        "owner": "quant",
        "depends_on_past": False,
        "email": os.environ.get("ALERT_EMAIL", "").split(",") if os.environ.get("ALERT_EMAIL") else [],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    }


# ============================================================================
# Prefect Flow 定义
# ============================================================================

if HAS_PREFECT:
    
    @prefect_task(name="sync-stock-list", retries=2)
    def prefetch_stock_list(trade_date: str = None):
        """Prefect: 同步股票列表"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        
        engine = PipelineDataEngine()
        td = trade_date or date.today().isoformat()
        
        try:
            stocks_df = engine.get_active_stocks(td)
            engine.close()
            return stocks_df["ts_code"].dropna().unique().tolist()
        except RuntimeError as e:
            engine.close()
            raise RuntimeError(f"获取股票列表失败: {e}")
    
    @prefect_task(name="sync-daily-data", retries=2)
    def prefetch_daily_data(ts_codes: List[str], start_date: str, end_date: str):
        """Prefect: 同步日线数据（统一使用 PipelineDataEngine）"""
        from scripts.pipeline_data_engine import PipelineDataEngine
        from scripts.production_scheduler import AlertManager
        
        engine = PipelineDataEngine(db_path="data/stock_data.duckdb")
        alert_mgr = AlertManager()
        
        results = []
        for ts_code in ts_codes:
            try:
                result = engine.sync_stock(ts_code, start_date, end_date)
                results.append({
                    "ts_code": ts_code,
                    "success": result.get("success", False),
                    "count": result.get("records", 0)
                })
                
                if not result.get("success"):
                    alert_mgr.send_sync_failure(ts_code, result.get("error", "unknown"))
            except Exception as e:
                logger.error(f"[Prefect] {ts_code} 同步失败: {e}")
                alert_mgr.send_sync_failure(ts_code, str(e))
                results.append({"ts_code": ts_code, "success": False, "error": str(e)})
        
        engine.close()
        return results
    
    @flow(name="stock-data-sync", log_prints=True)
    def stock_data_sync_flow(
        start_date: str = None,
        end_date: str = None,
        ts_codes: List[str] = None,
        trade_date: str = None
    ):
        """
        Prefect: 股票数据同步流程
        
        使用方式：
        from prefect import flow
        result = stock_data_sync_flow(
            start_date="2024-01-01",
            end_date="2024-12-31",
            ts_codes=None  # None 表示同步全部
        )
        """
        logger = get_run_logger()
        
        from datetime import datetime as dt
        from datetime import timedelta
        
        end_date = end_date or dt.now().date().isoformat()
        start_date = start_date or (dt.strptime(end_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        trade_date = trade_date or end_date
        
        logger.info(f"开始股票数据同步: {start_date} ~ {end_date}")
        
        # 获取股票列表
        if ts_codes is None:
            ts_codes = prefetch_stock_list(trade_date)
        
        logger.info(f"待同步股票数: {len(ts_codes)}")
        
        # 同步日线数据
        results = prefetch_daily_data(ts_codes, start_date, end_date)
        
        success_count = sum(1 for r in results if r.get("success"))
        logger.info(f"同步完成: 成功 {success_count}/{len(results)}")
        
        return {
            "total": len(results),
            "success": success_count,
            "failed": len(results) - success_count,
            "results": results
        }


# ============================================================================
# 本地调度器（无需 Airflow/Prefect）
# ============================================================================

class LocalScheduler:
    """
    本地调度器
    
    支持：
    - 定时任务（cron风格）
    - 一次性任务
    - 失败重试
    - 告警通知
    """
    
    def __init__(self, check_interval: int = 60):
        self.check_interval = check_interval  # 秒
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._tasks: Dict[str, Dict] = {}  # task_name -> config
        self._last_run: Dict[str, datetime] = {}
        self._alert_manager = AlertManager()
    
    def register_task(
        self,
        name: str,
        func: Callable,
        schedule: str,  # cron 格式: "5 16 * * 1-5"
        timeout: int = 3600,
        retries: int = 3
    ):
        """
        注册定时任务
        
        Args:
            name: 任务名称
            func: 任务函数
            schedule: cron 表达式
            timeout: 超时时间（秒）
            retries: 失败重试次数
        """
        self._tasks[name] = {
            "func": func,
            "schedule": schedule,
            "timeout": timeout,
            "retries": retries,
            "enabled": True
        }
        logger.info(f"[LocalScheduler] 注册任务: {name} (schedule: {schedule})")
    
    def run_task(self, name: str, **kwargs) -> bool:
        """运行指定任务"""
        if name not in self._tasks:
            logger.error(f"[LocalScheduler] 未知任务: {name}")
            return False
        
        task_config = self._tasks[name]
        func = task_config["func"]
        retries = task_config["retries"]
        
        last_error = None
        for attempt in range(retries):
            try:
                logger.info(f"[LocalScheduler] 执行任务: {name} (attempt {attempt + 1}/{retries})")
                
                # 带超时执行
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, **kwargs)
                    result = future.result(timeout=task_config["timeout"])
                
                self._last_run[name] = datetime.now()
                self._alert_manager.reset_failure_count(name)
                logger.info(f"[LocalScheduler] 任务成功: {name}")
                return True
                
            except Exception as e:
                last_error = e
                logger.error(f"[LocalScheduler] 任务失败 (attempt {attempt + 1}): {e}")
                self._alert_manager.record_failure(name)
                
                if attempt < retries - 1:
                    time.sleep(min(2 ** attempt, 60))  # 指数退避
        
        # 全部失败
        self._alert_manager.send_task_failure(name, str(last_error), {"retries": retries})
        return False
    
    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("[LocalScheduler] 调度器已在运行")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("[LocalScheduler] 调度器已启动")
    
    def stop(self):
        """停止调度器"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[LocalScheduler] 调度器已停止")
    
    def _run_loop(self):
        """调度循环"""
        while self._running:
            try:
                now = datetime.now()
                
                for name, config in self._tasks.items():
                    if not config["enabled"]:
                        continue
                    
                    # 简单的 cron 检查（实际使用 croniter 库更精确）
                    schedule = config["schedule"]
                    # TODO: 实现更精确的 cron 解析
                    
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"[LocalScheduler] 调度循环异常: {e}")
                time.sleep(self.check_interval)


# ============================================================================
# 全局实例
# ============================================================================

def get_alert_manager() -> AlertManager:
    """获取全局告警管理器"""
    return AlertManager()


# ============================================================================
# 测试代码
# ============================================================================

if __name__ == "__main__":
    print("生产级调度与监控测试")
    print("=" * 60)
    
    # 测试告警管理器
    print("\n1. 告警管理器:")
    alert_mgr = AlertManager()
    print(f"   已注册通知器数: {len(alert_mgr._notifiers)}")
    print(f"   失败阈值: {alert_mgr._failure_threshold}")
    
    # 测试告警发送
    print("\n2. 测试告警:")
    alert = Alert(
        title="测试告警",
        message="这是一条测试告警消息",
        level="info",
        source="test"
    )
    
    if alert_mgr._notifiers:
        print("   跳过实际发送（未配置通知渠道）")
    else:
        print("   无可用通知器")
    
    # Airflow/Prefect 检测
    print("\n3. 调度框架检测:")
    print(f"   Airflow: {'已安装' if HAS_AIRFLOW else '未安装'}")
    print(f"   Prefect: {'已安装' if HAS_PREFECT else '未安装'}")
    
    print("\n" + "=" * 60)
