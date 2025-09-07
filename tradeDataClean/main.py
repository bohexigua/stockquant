#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗主程序
统一执行market和common目录中的数据脚本
"""

import sys
import os
import logging
import argparse
import importlib.util
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs/tradeDataClean')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
log_filename = os.path.join(logs_dir, f'data_clean_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DataCleanScheduler:
    """
    数据清洗调度器
    负责执行market和common目录中的数据清洗脚本
    """
    
    def __init__(self):
        """初始化调度器"""
        self.base_dir = Path(__file__).parent
        self.market_dir = self.base_dir / 'market'
        self.common_dir = self.base_dir / 'common'
        
        # 定义脚本执行顺序（依赖关系）
        self.execution_order = {
            'common': [
                'stock_concept_relation.py',  # 股票概念关系
                'stock_theme_relation.py'  # 股票主题关系
            ],
            'market': [
                'stock_basic_daily.py',  # 股票基础信息
                'stock_daily.py',  # 股票日行情
                'stock_fund_flow.py',  # 股票日资金流向数据
                'dc_concept.py',  # 东财概念板块
                'theme.py',  # 主题板块
                'dc_stock_hot.py',  # 东财热门股票
                'stock_auction_daily.py',  # 股票集合竞价
                'stock_60min.py',  # 股票60分钟数据
                'stock_cyq_daily.py',  # 股票筹码分布
                'index_daily.py'  # 指数日行情
            ]
        }
    
    def get_script_files(self, directory: str) -> List[Path]:
        """
        获取指定目录下的Python脚本文件
        
        Args:
            directory: 目录名称 ('market' 或 'common')
            
        Returns:
            脚本文件路径列表
        """
        if directory == 'market':
            target_dir = self.market_dir
        elif directory == 'common':
            target_dir = self.common_dir
        else:
            raise ValueError(f"不支持的目录: {directory}")
        
        if not target_dir.exists():
            logger.warning(f"目录不存在: {target_dir}")
            return []
        
        # 按照执行顺序返回脚本文件
        script_files = []
        for script_name in self.execution_order.get(directory, []):
            script_path = target_dir / script_name
            if script_path.exists():
                script_files.append(script_path)
            else:
                logger.warning(f"脚本文件不存在: {script_path}")
        
        return script_files
    
    def load_and_execute_script(self, script_path: Path) -> bool:
        """
        动态加载并执行脚本的main函数
        
        Args:
            script_path: 脚本文件路径
            
        Returns:
            执行是否成功
        """
        try:
            logger.info(f"开始执行脚本: {script_path.name}")
            
            # 动态加载模块
            spec = importlib.util.spec_from_file_location(
                script_path.stem, script_path
            )
            module = importlib.util.module_from_spec(spec)
            
            # 执行模块
            spec.loader.exec_module(module)
            
            # 检查是否有main函数
            if hasattr(module, 'main'):
                # 执行main函数
                module.main()
                logger.info(f"脚本执行成功: {script_path.name}")
                return True
            else:
                logger.warning(f"脚本没有main函数: {script_path.name}")
                return False
                
        except Exception as e:
            logger.error(f"执行脚本失败 {script_path.name}: {str(e)}")
            return False
    
    def execute_directory_scripts(self, directory: str, continue_on_error: bool = True) -> Dict[str, bool]:
        """
        执行指定目录下的所有脚本
        
        Args:
            directory: 目录名称 ('market' 或 'common')
            continue_on_error: 遇到错误是否继续执行
            
        Returns:
            执行结果字典 {脚本名: 是否成功}
        """
        logger.info(f"开始执行 {directory} 目录下的脚本")
        
        script_files = self.get_script_files(directory)
        results = {}
        
        for script_path in script_files:
            success = self.load_and_execute_script(script_path)
            results[script_path.name] = success
            
            if not success and not continue_on_error:
                logger.error(f"脚本执行失败，停止后续执行: {script_path.name}")
                break
        
        return results
    
    def execute_all_scripts(self, continue_on_error: bool = True) -> Dict[str, Dict[str, bool]]:
        """
        执行所有目录下的脚本
        
        Args:
            continue_on_error: 遇到错误是否继续执行
            
        Returns:
            执行结果字典 {目录名: {脚本名: 是否成功}}
        """
        logger.info("开始执行所有数据清洗脚本")
        start_time = datetime.now()
        
        all_results = {}
        
        # 先执行common目录（基础数据）
        all_results['common'] = self.execute_directory_scripts('common', continue_on_error)
        
        # 再执行market目录（市场数据）
        all_results['market'] = self.execute_directory_scripts('market', continue_on_error)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # 统计执行结果
        total_scripts = sum(len(results) for results in all_results.values())
        successful_scripts = sum(
            sum(1 for success in results.values() if success)
            for results in all_results.values()
        )
        
        logger.info(f"所有脚本执行完成，耗时: {duration}")
        logger.info(f"执行结果: {successful_scripts}/{total_scripts} 个脚本成功")
        
        return all_results
    
    def execute_specific_script(self, script_name: str) -> bool:
        """
        执行指定的脚本
        
        Args:
            script_name: 脚本名称（不含路径）
            
        Returns:
            执行是否成功
        """
        # 在market和common目录中查找脚本
        for directory in ['common', 'market']:
            script_files = self.get_script_files(directory)
            for script_path in script_files:
                if script_path.name == script_name:
                    return self.load_and_execute_script(script_path)
        
        logger.error(f"未找到脚本: {script_name}")
        return False


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='数据清洗主程序')
    parser.add_argument(
        '--directory', '-d',
        choices=['common', 'market', 'all'],
        default='all',
        help='要执行的目录 (default: all)'
    )
    parser.add_argument(
        '--script', '-s',
        help='执行指定的脚本文件名'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        default=True,
        help='遇到错误时继续执行后续脚本 (default: True)'
    )
    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        help='遇到错误时停止执行'
    )
    
    args = parser.parse_args()
    
    # 处理continue_on_error参数
    continue_on_error = args.continue_on_error and not args.stop_on_error
    
    scheduler = DataCleanScheduler()
    
    try:
        if args.script:
            # 执行指定脚本
            success = scheduler.execute_specific_script(args.script)
            if success:
                logger.info(f"脚本 {args.script} 执行成功")
            else:
                logger.error(f"脚本 {args.script} 执行失败")
                sys.exit(1)
        
        elif args.directory == 'all':
            # 执行所有脚本
            results = scheduler.execute_all_scripts(continue_on_error)
            
            # 打印详细结果
            for directory, script_results in results.items():
                logger.info(f"\n{directory} 目录执行结果:")
                for script_name, success in script_results.items():
                    status = "成功" if success else "失败"
                    logger.info(f"  {script_name}: {status}")
        
        else:
            # 执行指定目录
            results = scheduler.execute_directory_scripts(args.directory, continue_on_error)
            
            # 打印结果
            logger.info(f"\n{args.directory} 目录执行结果:")
            for script_name, success in results.items():
                status = "成功" if success else "失败"
                logger.info(f"  {script_name}: {status}")
    
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        sys.exit(1)
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()