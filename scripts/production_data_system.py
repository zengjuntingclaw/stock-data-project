"""生产级数据系统 V3 - 整合入口

整合所有生产级优化组件：
1. VersionedStorage - 点对点快照与PIT约束
2. PartitionedStorage - 热数据SQL + 冷数据Parquet
3. DataQAPipeline - 自动化数据体检
4. SurvivorshipBiasHandler V2 - 退市与换码自动处理
5. TradingFilter - 精确停牌处理

使用示例：
    from scripts.production_data_system import ProductionDataSystem
    
    pds = ProductionDataSystem()
    
    # 存储财务数据（自动PIT约束）
    pds.store_financial('000001.SZ', '2025-12-31', '2026-03-25', '年报', {...})
    
    # 查询PIT数据
    data = pds.get_financial_pit('000001.SZ', '2026-01-15')  # 返回None（年报未公告）
    data = pds.get_financial_pit('0000001.SZ', '2026-04-01')  # 返回年报数据
    
    # 运行数据QA
    report = pds.run_qa_check()
    
    # 归档冷数据
    pds.archive_cold_data(year=2020)
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import pandas as pd
from loguru import logger

# 导入所有组件
from scripts.data_engine import DataEngine
from scripts.versioned_storage import (
    VersionedStorage, 
    FinancialDataPITManager,
    DataSnapshotKey
)
from scripts.partitioned_storage import PartitionedStorage, TimeSeriesQueryOptimizer
from scripts.data_qa_pipeline import DataQAPipeline
from scripts.survivorship_bias import SurvivorshipBiasHandler
from scripts.trading_rules import TradingFilter, AShareTradingRules


class ProductionDataSystem:
    """
    生产级数据系统统一入口
    
    这是数据层的门面(Facade)模式实现，整合所有子系统，
    提供简洁统一的API。
    """
    
    def __init__(self, 
                 db_path: Optional[str] = None,
                 parquet_root: Optional[str] = None):
        """
        初始化生产级数据系统
        
        Parameters
        ----------
        db_path : str, optional
            DuckDB数据库路径
        parquet_root : str, optional
            Parquet文件根目录
        """
        logger.info("Initializing ProductionDataSystem V3...")
        
        # 1. 基础数据引擎（向后兼容）
        self.data_engine = DataEngine(db_path=db_path, parquet_dir=parquet_root)
        
        # 2. 版本化存储（PIT约束）
        self.versioned = VersionedStorage(db_path=db_path)
        self.financial_pit = FinancialDataPITManager(self.versioned)
        
        # 3. 分区存储（热/冷数据分层）
        self.partitioned = PartitionedStorage(db_path=db_path, parquet_root=parquet_root)
        self.ts_optimizer = TimeSeriesQueryOptimizer(self.partitioned)
        
        # 4. QA管道
        self.qa_pipeline = DataQAPipeline(self.data_engine)
        
        # 5. 幸存者偏差治理
        self.survivorship = SurvivorshipBiasHandler()
        
        # 6. 交易过滤器
        self.trading_filter = TradingFilter()
        
        logger.info("ProductionDataSystem V3 initialized successfully")
    
    # ═══════════════════════════════════════════════════════════
    # 财务数据PIT接口
    # ═══════════════════════════════════════════════════════════
    
    def store_financial(self,
                        ts_code: str,
                        end_date: str,          # 报告期
                        ann_date: str,          # 公告日（PIT关键）
                        report_type: str,       # 'Q1', 'Q2', 'Q3', '年报'
                        financials: Dict[str, float]):
        """
        存储财务数据（自动PIT约束）
        
        关键：使用ann_date作为effective_at，确保回测时只能看到已公告数据
        """
        return self.financial_pit.store_financial(
            ts_code=ts_code,
            end_date=end_date,
            ann_date=ann_date,
            report_type=report_type,
            financials=financials
        )
    
    def get_financial_pit(self,
                          ts_code: str,
                          as_of_date: str) -> Optional[Dict]:
        """
        获取PIT财务数据
        
        返回as_of_date之前最新已公告的财务数据
        """
        return self.financial_pit.get_financial_pit(ts_code, as_of_date)
    
    def get_batch_financial_pit(self,
                                 ts_codes: List[str],
                                 as_of_date: str) -> Dict[str, Dict]:
        """批量获取PIT财务数据"""
        return self.financial_pit.get_batch_financial_pit(ts_codes, as_of_date)
    
    # ═══════════════════════════════════════════════════════════
    # 分区存储接口
    # ═══════════════════════════════════════════════════════════
    
    def query_price_data(self,
                         start_date: str,
                         end_date: str,
                         ts_codes: Optional[List[str]] = None,
                         columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        统一价格数据查询（自动路由热/冷数据）
        """
        return self.partitioned.query(
            table='daily_quotes',
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            columns=columns
        )
    
    def get_rolling_window(self,
                          ts_code: str,
                          end_date: str,
                          window_days: int) -> pd.DataFrame:
        """
        获取滚动窗口数据（缓存优化）
        """
        return self.ts_optimizer.get_rolling_window(
            table='daily_quotes',
            ts_code=ts_code,
            end_date=end_date,
            window_days=window_days
        )
    
    def archive_cold_data(self, year: Optional[int] = None):
        """
        归档冷数据到Parquet
        
        Parameters
        ----------
        year : int, optional
            指定年份，默认归档所有超过2年的数据
        """
        return self.partitioned.archive_to_parquet(
            table='daily_quotes',
            year=year,
            compress=True
        )
    
    # ═══════════════════════════════════════════════════════════
    # QA管道接口
    # ═══════════════════════════════════════════════════════════
    
    def run_qa_check(self,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> Dict:
        """
        运行完整数据QA检查
        
        包括：
        - 价格逻辑校验
        - 成交量一致性校验
        - 数据完整性校验（Missing_Data_Report）
        - 停牌一致性校验
        - 统计异常检测
        """
        return self.qa_pipeline.run_full_check(start_date, end_date)
    
    def get_latest_qa_report(self) -> Optional[Dict]:
        """获取最新QA报告"""
        return self.qa_pipeline.get_latest_report()
    
    def has_critical_issues(self) -> bool:
        """检查是否有严重数据问题"""
        return self.qa_pipeline.has_critical_issues()
    
    # ═══════════════════════════════════════════════════════════
    # 幸存者偏差治理接口
    # ═══════════════════════════════════════════════════════════
    
    def load_stock_lifecycle(self, start_year: int = 2000, end_year: int = 2024):
        """加载股票生命周期数据（含退市股）"""
        self.survivorship.load_from_baostock(start_year, end_year)
    
    def get_tradable_status(self,
                           symbol: str,
                           date: datetime,
                           market_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        获取详细的可交易状态
        
        Returns
        -------
        {
            'is_tradable': bool,
            'can_buy': bool,
            'can_sell': bool,
            'suspension_type': str,
            'reason': str,
            'limit_price': Optional[float]
        }
        """
        return self.survivorship.get_tradable_status(symbol, date, market_data)
    
    def get_universe_at_date(self,
                            date: datetime,
                            include_delisted: bool = False) -> List[str]:
        """获取指定日期的有效股票池"""
        return self.survivorship.get_universe(date, include_delisted)
    
    # ═══════════════════════════════════════════════════════════
    # 交易过滤接口
    # ═══════════════════════════════════════════════════════════
    
    def check_tradable(self,
                      symbol: str,
                      date: datetime,
                      market_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查股票可交易状态（精确停牌处理）
        
        Returns
        -------
        {
            'is_tradable': bool,
            'can_buy': bool,
            'can_sell': bool,
            'is_suspend': bool,
            'is_limit_up': bool,
            'is_limit_down': bool,
            'limit_up_price': Optional[float],
            'limit_down_price': Optional[float],
            'reason': str,
        }
        """
        return self.trading_filter.check_tradable(symbol, date, market_data)
    
    def get_execution_price(self,
                           symbol: str,
                           date: datetime,
                           market_data: Dict[str, Any],
                           side: str) -> Optional[float]:
        """
        获取执行价格（考虑涨跌停限制）
        
        如果无法成交（如买入时涨停），返回None
        """
        return self.trading_filter.get_execution_price(
            symbol, date, market_data, side
        )
    
    # ═══════════════════════════════════════════════════════════
    # 系统维护接口
    # ═══════════════════════════════════════════════════════════
    
    def full_system_check(self) -> Dict:
        """
        完整系统健康检查
        
        包括：
        1. 数据QA检查
        2. 分区存储统计
        3. 版本化存储审计
        4. 缓存状态检查
        """
        logger.info("Running full system check...")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'qa_check': None,
            'partition_stats': None,
            'version_audit': None,
            'cache_stats': None,
            'overall_status': 'UNKNOWN'
        }
        
        # 1. QA检查
        try:
            results['qa_check'] = self.run_qa_check()
        except Exception as e:
            logger.error(f"QA check failed: {e}")
            results['qa_check'] = {'error': str(e)}
        
        # 2. 分区统计
        try:
            results['partition_stats'] = self.partitioned.get_partition_stats()
        except Exception as e:
            logger.error(f"Partition stats failed: {e}")
            results['partition_stats'] = {'error': str(e)}
        
        # 3. 版本审计
        try:
            results['version_audit'] = self.versioned.audit_data_integrity()
        except Exception as e:
            logger.error(f"Version audit failed: {e}")
            results['version_audit'] = {'error': str(e)}
        
        # 4. 缓存统计
        try:
            results['cache_stats'] = self.ts_optimizer.get_cache_stats()
        except Exception as e:
            logger.error(f"Cache stats failed: {e}")
            results['cache_stats'] = {'error': str(e)}
        
        # 综合状态
        has_critical = (
            results['qa_check'] and 
            isinstance(results['qa_check'], dict) and
            results['qa_check'].get('summary', {}).get('issues', {}).get('critical', 0) > 0
        )
        
        results['overall_status'] = 'HEALTHY' if not has_critical else 'CRITICAL'
        
        logger.info(f"System check completed: {results['overall_status']}")
        return results
    
    def optimize_storage(self):
        """优化存储布局"""
        logger.info("Optimizing storage layout...")
        self.partitioned.optimize_layout()
        logger.info("Storage optimization completed")
    
    def get_system_status(self) -> Dict:
        """获取系统状态摘要"""
        return {
            'data_engine': 'connected',
            'versioned_storage': 'active',
            'partitioned_storage': 'active',
            'qa_pipeline': 'ready',
            'survivorship_handler': f"{len(self.survivorship._stocks)} stocks loaded",
            'cache_stats': self.ts_optimizer.get_cache_stats()
        }


# 便捷函数
def create_production_data_system(db_path: Optional[str] = None,
                                   parquet_root: Optional[str] = None) -> ProductionDataSystem:
    """创建生产级数据系统实例"""
    return ProductionDataSystem(db_path, parquet_root)


# 向后兼容：保持原有DataEngine接口
__all__ = [
    'ProductionDataSystem',
    'create_production_data_system',
    'VersionedStorage',
    'PartitionedStorage',
    'DataQAPipeline',
    'SurvivorshipBiasHandler',
    'TradingFilter',
]
