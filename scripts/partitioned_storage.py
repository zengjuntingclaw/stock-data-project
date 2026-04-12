"""分区存储引擎 - 热数据SQL + 冷数据Parquet混合架构

随着回测周期拉长，查询效率将成为瓶颈。
本模块实现：
1. 分区存储：按trade_date或year分区，提升范围查询速度
2. Parquet混合架构：热数据(SQL) + 冷数据(Parquet列式存储)
3. 自动冷热分层：历史数据自动归档到Parquet
"""
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterator, Union
from dataclasses import dataclass
from enum import Enum
import json
import pandas as pd
import numpy as np
from loguru import logger



def _get_now():
    """获取当前时间（支持单测 mock）。"""
    return datetime.now()


class DataTier(Enum):
    """数据分层"""
    HOT = "hot"         # 热数据：最近2年，DuckDB
    WARM = "warm"       # 温数据：2-5年，DuckDB分区
    COLD = "cold"       # 冷数据：5年以上，Parquet
    ARCHIVE = "archive" # 归档：压缩Parquet


@dataclass
class PartitionInfo:
    """分区信息"""
    year: int
    tier: DataTier
    path: Path
    record_count: int
    min_date: str
    max_date: str
    size_bytes: int


class PartitionedStorage:
    """
    分区存储引擎
    
    架构设计：
    ```
    数据流向：
    实时数据 → DuckDB (热数据，最近2年)
              ↓
         自动归档 → Parquet/YYYY/ (冷数据，按年分区)
              ↓
         深度归档 → Parquet/archive/ (压缩，5年以上)
    
    查询路径：
    1. 查询2024年数据 → 直接从DuckDB热数据区读取
    2. 查询2020年数据 → 从Parquet/2020/读取
    3. 跨年度查询 → 合并DuckDB + 多个Parquet文件
    ```
    """
    
    # 分层阈值（年）
    HOT_YEARS = 2
    WARM_YEARS = 5
    
    def __init__(self, 
                 db_path: Optional[str] = None,
                 parquet_root: Optional[str] = None):
        """
        Parameters
        ----------
        db_path : str, optional
            DuckDB数据库路径
        parquet_root : str, optional
            Parquet文件根目录
        """
        import os
        
        project_root = Path(__file__).resolve().parent.parent
        
        self.db_path = Path(db_path or os.environ.get(
            'STOCK_DB_PATH', 
            str(project_root / 'data' / 'stock_data.duckdb')
        ))
        
        self.parquet_root = Path(parquet_root or os.environ.get(
            'STOCK_PARQUET_DIR',
            str(project_root / 'data' / 'parquet')
        ))
        
        # 创建目录结构
        self._ensure_directory_structure()
        
        # 分区索引缓存
        self._partition_cache: Dict[int, PartitionInfo] = {}
        self._refresh_partition_index()
        
        # 连接池（读写分离）
        import duckdb
        self._duckdb = duckdb
        self._write_pool: List = []
        self._read_pool: List = []
        self._pool_max = 5
        self._pool_lock = __import__('threading').Lock()
        
        logger.info(f"PartitionedStorage initialized")
        logger.info(f"  DB: {self.db_path}")
        logger.info(f"  Parquet: {self.parquet_root}")
    
    def _ensure_directory_structure(self):
        """确保目录结构存在"""
        # Parquet目录结构（使用显式映射）
        for table in self._ALLOWED_TABLES:
            self._get_parquet_dir(table).mkdir(parents=True, exist_ok=True)
        (self.parquet_root / "archive").mkdir(parents=True, exist_ok=True)
        
        # 元数据目录
        (self.parquet_root / "_meta").mkdir(parents=True, exist_ok=True)
    
    def _refresh_partition_index(self):
        """刷新分区索引"""
        self._partition_cache = {}
        
        # 扫描Parquet目录
        for tier_dir in ["daily", "financial", "valuation"]:
            tier_path = self.parquet_root / tier_dir
            if not tier_path.exists():
                continue
                
            for year_dir in tier_path.iterdir():
                if year_dir.is_dir() and year_dir.name.isdigit():
                    year = int(year_dir.name)
                    parquet_file = year_dir / f"{tier_dir}_{year}.parquet"
                    if parquet_file.exists():
                        stat = parquet_file.stat()
                        self._partition_cache[year] = PartitionInfo(
                            year=year,
                            tier=self._determine_tier(year),
                            path=parquet_file,
                            record_count=0,  # 延迟加载
                            min_date=f"{year}-01-01",
                            max_date=f"{year}-12-31",
                            size_bytes=stat.st_size
                        )
    
    def _determine_tier(self, year: int) -> DataTier:
        """根据年份确定数据分层"""
        current_year = _get_now().year
        age = current_year - year
        
        if age <= self.HOT_YEARS:
            return DataTier.HOT
        elif age <= self.WARM_YEARS:
            return DataTier.WARM
        else:
            return DataTier.COLD
    
    def archive_to_parquet(self, 
                          table: str = "daily_bar_adjusted",
                          year: Optional[int] = None,
                          compress: bool = True) -> Optional[Path]:
        """
        将DuckDB数据归档到Parquet
        
        Parameters
        ----------
        table : str
            表名
        year : int, optional
            指定年份，默认归档所有历史数据（year=None 时批量归档，返回 None）
        compress : bool
            是否使用压缩
            
        Returns
        -------
        Optional[Path] : 生成的Parquet文件路径；year=None 时批量归档并返回 None
        """
        import duckdb
        
        if year is None:
            # 归档所有超过HOT_YEARS的数据
            current_year = _get_now().year
            for y in range(current_year - self.HOT_YEARS - 5, current_year - self.HOT_YEARS):
                self.archive_to_parquet(table, y, compress)
            return None  # 显式返回 None，与类型注解 Optional[Path] 对齐
        
        # 确定输出路径
        tier = self._determine_tier(year)
        output_dir = self._get_parquet_dir(table) / str(year)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{table}_{year}.parquet"
        
        # 从DuckDB导出（连接池）
        with self._get_connection(read_only=True) as conn:
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            
            # 检查是否有数据
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE trade_date BETWEEN ? AND ?
            """, (start_date, end_date)).fetchone()
            
            if count_result[0] == 0:
                logger.info(f"No data to archive for {table} {year}")
                return output_file
            
            # 导出到Parquet
            compression = "zstd" if compress and tier == DataTier.COLD else "snappy"
            
            conn.execute(f"""
                COPY (
                    SELECT * FROM {table}
                    WHERE trade_date BETWEEN ? AND ?
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION '{compression}')
            """, (start_date, end_date))
            
            # 更新元数据
            self._update_partition_metadata(table, year, output_file)
            
            # 可选：从DuckDB删除已归档数据（节省空间）
            # conn.execute(f"""
            #     DELETE FROM {table}
            #     WHERE trade_date BETWEEN ? AND ?
            # """, (start_date, end_date))
            
            file_size = output_file.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Archived {table} {year}: {count_result[0]} records, {file_size:.1f} MB")
            
            return output_file
    
    def load_from_parquet(self,
                         table: str,
                         start_date: str,
                         end_date: str) -> pd.DataFrame:
        """
        从Parquet加载历史数据
        
        自动合并多个年度分区
        """
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        dfs = []
        for year in range(start_year, end_year + 1):
            parquet_file = self._get_parquet_dir(table) / str(year) / f"{table}_{year}.parquet"
            
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                # 过滤日期范围
                df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        return pd.concat(dfs, ignore_index=True)
    
    def query(self,
              table: str,
              start_date: str,
              end_date: str,
              ts_codes: Optional[List[str]] = None,
              columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        统一查询接口 - 自动路由到热数据或冷数据
        
        这是核心查询方法，根据日期范围自动选择最优数据源。
        """
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        current_year = _get_now().year
        
        # 判断数据分布
        hot_years = [y for y in range(start_year, end_year + 1) 
                     if current_year - y <= self.HOT_YEARS]
        cold_years = [y for y in range(start_year, end_year + 1) 
                      if current_year - y > self.HOT_YEARS]
        
        dfs = []
        
        # 查询热数据（DuckDB）- 修正：确保日期范围不重叠
        if hot_years:
            hot_start = max(start_date, f"{current_year - self.HOT_YEARS}-01-01")
            df_hot = self._query_duckdb(table, hot_start, end_date, ts_codes, columns)
            if not df_hot.empty:
                dfs.append(df_hot)
        
        # 查询冷数据（Parquet）- 修正：确保日期范围不重叠
        if cold_years:
            cold_end = min(end_date, f"{current_year - self.HOT_YEARS - 1}-12-31")
            # 如果热数据已覆盖，跳过冷数据查询
            if not hot_years or cold_end >= start_date:
                df_cold = self._query_parquet(table, start_date, cold_end, ts_codes, columns)
                if not df_cold.empty:
                    dfs.append(df_cold)
        
        if not dfs:
            return pd.DataFrame()
        
        result = pd.concat(dfs, ignore_index=True)
        
        # 去重（可能的热冷数据重叠）
        if 'trade_date' in result.columns and 'ts_code' in result.columns:
            result = result.drop_duplicates(subset=['ts_code', 'trade_date'])
        
        return result
    
    # 允许的表白名单（防止SQL注入）
    # 注意：旧表 daily_quotes/stock_basic/index_constituents 已废弃
    _ALLOWED_TABLES = frozenset({
        'daily_bar_adjusted', 'daily_bar_raw', 'stock_basic_history',
        'financial_data', 'index_constituents_history',
        'st_status_history', 'trade_calendar', 'daily_valuation'
    })
    
    # Parquet目录映射（表名 → 目录名，显式映射避免下划线规则歧义）
    _PARQUET_DIR_MAP = {
        'daily_bar_adjusted': 'daily_adj',
        'daily_bar_raw': 'daily_raw',
        'stock_basic_history': 'basic_hist',
        'financial_data': 'financial',
        'index_constituents_history': 'index_hist',
        'st_status_history': 'status',
        'trade_calendar': 'calendar',
        'daily_valuation': 'valuation',
    }
    
    def _get_parquet_dir(self, table: str) -> Path:
        """获取表对应的Parquet目录"""
        return self.parquet_root / self._PARQUET_DIR_MAP.get(table, table.replace('_', ''))
    
    def _get_connection(self, read_only: bool = False):
        """从连接池获取连接（上下文管理器，读写分离）"""
        import contextlib
        
        @contextlib.contextmanager
        def _conn():
            pool = self._read_pool if read_only else self._write_pool
            
            conn = None
            from_pool = False
            with self._pool_lock:
                if pool:
                    conn = pool.pop()
                    from_pool = True
            
            if conn is None:
                conn = self._duckdb.connect(str(self.db_path), read_only=read_only)
            
            try:
                yield conn
            finally:
                with self._pool_lock:
                    if from_pool and len(pool) < self._pool_max:
                        pool.append(conn)
                    else:
                        conn.close()
        
        return _conn()
    
    def close(self):
        """关闭所有连接池连接"""
        with self._pool_lock:
            for conn in self._write_pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._write_pool.clear()
            for conn in self._read_pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._read_pool.clear()
        logger.info("PartitionedStorage connection pools closed")
    
    def _query_duckdb(self,
                      table: str,
                      start_date: str,
                      end_date: str,
                      ts_codes: Optional[List[str]] = None,
                      columns: Optional[List[str]] = None) -> pd.DataFrame:
        """查询DuckDB热数据（连接池）"""
        # 表名白名单校验
        if table not in self._ALLOWED_TABLES:
            raise ValueError(f"Table '{table}' not in allowed list: {sorted(self._ALLOWED_TABLES)}")
        
        with self._get_connection(read_only=True) as conn:
            # 构建查询 - 列名白名单校验
            if columns:
                # 获取表的合法列名
                schema_df = conn.execute(f"PRAGMA table_info({table})").fetchdf()
                allowed_cols = set(schema_df['name'].tolist())
                invalid_cols = set(columns) - allowed_cols
                if invalid_cols:
                    raise ValueError(f"Invalid columns: {invalid_cols}")
                col_str = ", ".join(columns)
            else:
                col_str = "*"
            
            if ts_codes:
                # 使用参数化查询防止SQL注入
                codes_df = pd.DataFrame({'ts_code': ts_codes})
                conn.register('tmp_query_codes', codes_df)
                
                result = conn.execute(f"""
                    SELECT {col_str} FROM {table}
                    WHERE trade_date BETWEEN ? AND ?
                    AND ts_code IN (SELECT ts_code FROM tmp_query_codes)
                """, (start_date, end_date)).fetchdf()
                
                conn.execute("DROP VIEW IF EXISTS tmp_query_codes")
            else:
                result = conn.execute(f"""
                    SELECT {col_str} FROM {table}
                    WHERE trade_date BETWEEN ? AND ?
                """, (start_date, end_date)).fetchdf()
            
            return result
    
    def _query_parquet(self,
                       table: str,
                       start_date: str,
                       end_date: str,
                       ts_codes: Optional[List[str]] = None,
                       columns: Optional[List[str]] = None) -> pd.DataFrame:
        """查询Parquet冷数据 - 使用DuckDB进行列式扫描（连接池）"""
        # 表名白名单校验
        if table not in self._ALLOWED_TABLES:
            raise ValueError(f"Table '{table}' not in allowed list: {sorted(self._ALLOWED_TABLES)}")
        
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        
        # 收集需要读取的Parquet文件
        parquet_files = []
        for year in range(start_year, end_year + 1):
            pf = self._get_parquet_dir(table) / str(year) / f"{table}_{year}.parquet"
            if pf.exists():
                parquet_files.append(str(pf))
        
        if not parquet_files:
            return pd.DataFrame()
        
        # 使用DuckDB读取Parquet（支持谓词下推）
        with self._get_connection(read_only=True) as conn:
            # 注册Parquet文件
            files_str = ", ".join([f"'{f}'" for f in parquet_files])
            
            # 列名白名单校验（Parquet列名从第一个文件推断）
            if columns:
                sample_df = pd.read_parquet(parquet_files[0], columns=None, n_rows=0)
                allowed_cols = set(sample_df.columns.tolist())
                invalid_cols = set(columns) - allowed_cols
                if invalid_cols:
                    raise ValueError(f"Invalid columns: {invalid_cols}")
                col_str = ", ".join(columns)
            else:
                col_str = "*"
            
            if ts_codes:
                codes_df = pd.DataFrame({'ts_code': ts_codes})
                conn.register('tmp_query_codes', codes_df)
                
                result = conn.execute(f"""
                    SELECT {col_str} FROM read_parquet([{files_str}])
                    WHERE trade_date BETWEEN ? AND ?
                    AND ts_code IN (SELECT ts_code FROM tmp_query_codes)
                """, (start_date, end_date)).fetchdf()
                
                conn.execute("DROP VIEW IF EXISTS tmp_query_codes")
            else:
                result = conn.execute(f"""
                    SELECT {col_str} FROM read_parquet([{files_str}])
                    WHERE trade_date BETWEEN ? AND ?
                """, (start_date, end_date)).fetchdf()
            
            return result
    
    def get_partition_stats(self) -> Dict:
        """获取分区统计信息"""
        stats = {
            'partitions': [],
            'total_size_mb': 0,
            'by_tier': {t.value: {'count': 0, 'size_mb': 0} for t in DataTier}
        }
        
        for year, info in self._partition_cache.items():
            stats['partitions'].append({
                'year': year,
                'tier': info.tier.value,
                'size_mb': round(info.size_bytes / (1024 * 1024), 2),
                'path': str(info.path)
            })
            stats['total_size_mb'] += info.size_bytes / (1024 * 1024)
            stats['by_tier'][info.tier.value]['count'] += 1
            stats['by_tier'][info.tier.value]['size_mb'] += info.size_bytes / (1024 * 1024)
        
        # 四舍五入
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        for tier in stats['by_tier']:
            stats['by_tier'][tier]['size_mb'] = round(stats['by_tier'][tier]['size_mb'], 2)
        
        return stats
    
    def _update_partition_metadata(self, table: str, year: int, path: Path):
        """更新分区元数据"""
        meta_file = self.parquet_root / "_meta" / f"{table}_partitions.json"
        meta_file.parent.mkdir(parents=True, exist_ok=True)
        
        metadata = {}
        if meta_file.exists():
            with open(meta_file, 'r') as f:
                metadata = json.load(f)
        
        metadata[str(year)] = {
            'table': table,
            'year': year,
            'path': str(path),
            'created_at': _get_now().isoformat(),
            'size_bytes': path.stat().st_size
        }
        
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def optimize_layout(self):
        """
        优化存储布局
        
        1. 压缩冷数据
        2. 合并小文件
        3. 重建索引
        """
        logger.info("Starting storage optimization...")
        
        for year, info in list(self._partition_cache.items()):
            if info.tier == DataTier.COLD:
                # 检查压缩
                self._recompress_if_needed(info)
        
        self._refresh_partition_index()
        logger.info("Storage optimization completed")
    
    def _recompress_if_needed(self, info: PartitionInfo):
        """重新压缩如果需要"""
        # 检查当前压缩率
        file_size_mb = info.size_bytes / (1024 * 1024)
        
        # 如果文件大于100MB，使用更强的压缩
        if file_size_mb > 100:
            logger.info(f"Recompressing {info.path} with zstd...")
            
            df = pd.read_parquet(info.path)
            df.to_parquet(info.path, compression='zstd', compression_level=9)
            
            new_size = info.path.stat().st_size
            logger.info(f"  {info.size_bytes / (1024 * 1024):.1f} MB → {new_size / (1024 * 1024):.1f} MB")


class TimeSeriesQueryOptimizer:
    """
    时间序列查询优化器
    
    针对回测场景的查询优化：
    1. 预加载常用时间窗口
    2. 滑动窗口缓存
    3. 向量化批量查询
    """
    
    def __init__(self, storage: PartitionedStorage):
        self.storage = storage
        self._window_cache: Dict[str, pd.DataFrame] = {}
        self._cache_hits = 0
        self._cache_misses = 0
    
    def get_rolling_window(self,
                          table: str,
                          ts_code: str,
                          end_date: str,
                          window_days: int) -> pd.DataFrame:
        """
        获取滚动窗口数据（优化缓存）
        
        常用于计算移动平均线、波动率等
        """
        cache_key = f"{table}:{ts_code}:{end_date}:{window_days}"
        
        if cache_key in self._window_cache:
            self._cache_hits += 1
            return self._window_cache[cache_key]
        
        self._cache_misses += 1
        
        # 计算起始日期
        end = datetime.strptime(end_date, '%Y-%m-%d')
        start = end - timedelta(days=window_days * 2)  # 2倍缓冲
        start_date = start.strftime('%Y-%m-%d')
        
        # 查询数据
        df = self.storage.query(
            table=table,
            start_date=start_date,
            end_date=end_date,
            ts_codes=[ts_code]
        )
        
        if not df.empty:
            df = df.sort_values('trade_date')
        
        # 缓存（LRU策略）
        self._window_cache[cache_key] = df
        self._evict_cache_if_needed()
        
        return df
    
    def get_batch_rolling_windows(self,
                                   table: str,
                                   ts_codes: List[str],
                                   end_date: str,
                                   window_days: int) -> Dict[str, pd.DataFrame]:
        """批量获取滚动窗口"""
        end = datetime.strptime(end_date, '%Y-%m-%d')
        start = end - timedelta(days=window_days * 2)
        start_date = start.strftime('%Y-%m-%d')
        
        # 批量查询
        df = self.storage.query(
            table=table,
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes
        )
        
        if df.empty:
            return {}
        
        # 按股票分组
        result = {}
        for ts_code in ts_codes:
            code_df = df[df['ts_code'] == ts_code].sort_values('trade_date')
            if not code_df.empty:
                result[ts_code] = code_df
                # 更新缓存
                cache_key = f"{table}:{ts_code}:{end_date}:{window_days}"
                self._window_cache[cache_key] = code_df
        
        self._evict_cache_if_needed()
        return result
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        total = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total if total > 0 else 0
        
        return {
            'cache_size': len(self._window_cache),
            'hits': self._cache_hits,
            'misses': self._cache_misses,
            'hit_rate': f"{hit_rate:.1%}"
        }
    
    def _evict_cache_if_needed(self, max_size: int = 1000):
        """缓存淘汰"""
        if len(self._window_cache) > max_size:
            # 移除最早的20%
            keys_to_remove = list(self._window_cache.keys())[:max_size // 5]
            for k in keys_to_remove:
                del self._window_cache[k]


# 便捷函数
def create_partitioned_storage(db_path: Optional[str] = None,
                                parquet_root: Optional[str] = None) -> PartitionedStorage:
    """创建分区存储实例"""
    return PartitionedStorage(db_path, parquet_root)
