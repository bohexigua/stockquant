#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算主程序
统一执行stock目录下的脚本以及concept.py和theme.py
"""

import os
import sys
import importlib.util
import argparse
import logging
from pathlib import Path
import pymysql
from typing import List, Dict, Any

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 创建logs目录
logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# 配置日志 - 输出到文件和控制台
from datetime import datetime
log_filename = os.path.join(logs_dir, f'factors_main_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class FactorCalculationScheduler:
    """
    因子计算调度器
    
    负责统一执行因子计算脚本:
    1. stock目录下的所有脚本
    2. concept.py
    3. theme.py
    """
    
    def __init__(self):
        """
        初始化因子计算调度器
        """
        self.base_dir = Path(__file__).parent
        self.stock_dir = self.base_dir / 'stock'
        
        # 定义脚本执行顺序
        self.script_order = {
            'stock': ['investment.py', 'momentum.py', 'stock_sector_correlation.py', 'intraday_momentum.py'],
            'root': [],
        }
        self.trade_date = self._get_current_trade_date()

    def _get_current_trade_date(self) -> str:
        try:
            from config import config
            conn = pymysql.connect(
                host=config.database.host,
                port=config.database.port,
                user=config.database.user,
                password=config.database.password,
                database=config.database.database,
                charset=config.database.charset,
                autocommit=True,
            )
            try:
                with conn.cursor() as c:
                    c.execute(
                        "SELECT MAX(cal_date) FROM trade_market_calendar WHERE is_open=1 AND cal_date<=CURDATE()"
                    )
                    r = c.fetchone()
                    if r and r[0]:
                        return r[0].strftime('%Y-%m-%d')
            finally:
                conn.close()
        except Exception:
            pass
        from datetime import datetime as _dt
        return _dt.now().strftime('%Y-%m-%d')
    
    def get_available_scripts(self) -> Dict[str, List[str]]:
        """
        获取所有可用的脚本
        
        Returns:
            Dict[str, List[str]]: 按目录分组的脚本列表
        """
        scripts = {'stock': [], 'root': []}
        
        # 获取stock目录下的脚本
        if self.stock_dir.exists():
            for script in self.script_order['stock']:
                script_path = self.stock_dir / script
                if script_path.exists():
                    scripts['stock'].append(script)
                else:
                    logger.warning(f"脚本不存在: {script_path}")
        
        # 获取根目录下的脚本
        for script in self.script_order['root']:
            script_path = self.base_dir / script
            if script_path.exists():
                scripts['root'].append(script)
            else:
                logger.warning(f"脚本不存在: {script_path}")
        
        return scripts
    
    def load_and_execute_script(self, script_path: Path) -> bool:
        """
        动态加载并执行脚本的main函数
        
        Args:
            script_path (Path): 脚本文件路径
            
        Returns:
            bool: 执行是否成功
        """
        try:
            logger.info(f"开始执行脚本: {script_path}")
            
            # 动态加载模块
            spec = importlib.util.spec_from_file_location(
                script_path.stem, script_path
            )
            module = importlib.util.module_from_spec(spec)
            
            # 执行模块
            spec.loader.exec_module(module)
            
            # 调用main函数，优先注入交易日参数
            if hasattr(module, 'main'):
                import sys as _sys
                orig_argv = list(_sys.argv)
                try:
                    _sys.argv = [script_path.name, '--date', self.trade_date]
                    module.main()
                    logger.info(f"脚本执行成功: {script_path} (date={self.trade_date})")
                    return True
                except SystemExit as ex:
                    if ex.code == 0:
                        logger.info(f"脚本执行成功: {script_path} (date={self.trade_date})")
                        return True
                    logger.warning(f"脚本不支持 --date 参数或参数错误，改用默认参数执行: {script_path}")
                    try:
                        _sys.argv = [script_path.name]
                        module.main()
                        logger.info(f"脚本执行成功: {script_path}")
                        return True
                    except SystemExit as ex2:
                        if ex2.code == 0:
                            logger.info(f"脚本执行成功: {script_path}")
                            return True
                        logger.error(f"脚本执行失败(默认参数): {script_path}")
                        return False
                    except Exception as e2:
                        logger.error(f"脚本执行异常(默认参数): {script_path}: {e2}")
                        return False
                finally:
                    _sys.argv = orig_argv
            else:
                logger.error(f"脚本缺少main函数: {script_path}")
                return False
                
        except Exception as e:
            logger.error(f"脚本执行失败 {script_path}: {e}")
            return False
    
    def execute_stock_scripts(self, continue_on_error: bool = True) -> bool:
        """
        执行stock目录下的所有脚本
        
        Args:
            continue_on_error (bool): 遇到错误时是否继续执行
            
        Returns:
            bool: 所有脚本是否都执行成功
        """
        logger.info("开始执行stock目录下的因子计算脚本")
        
        scripts = self.get_available_scripts()['stock']
        success_count = 0
        
        for script in scripts:
            script_path = self.stock_dir / script
            success = self.load_and_execute_script(script_path)
            
            if success:
                success_count += 1
            elif not continue_on_error:
                logger.error(f"脚本执行失败，停止执行: {script}")
                return False
        
        logger.info(f"stock脚本执行完成: {success_count}/{len(scripts)} 成功")
        return success_count == len(scripts)
    
    def execute_single_script(self, script_name: str) -> bool:
        """
        执行单个脚本
        
        Args:
            script_name (str): 脚本名称
            
        Returns:
            bool: 执行是否成功
        """
        # 先在stock目录中查找
        script_path = self.stock_dir / script_name
        if script_path.exists():
            return self.load_and_execute_script(script_path)
        
        # 再在根目录中查找
        script_path = self.base_dir / script_name
        if script_path.exists():
            return self.load_and_execute_script(script_path)
        
        logger.error(f"脚本不存在: {script_name}")
        return False
    
    def execute_all_scripts(self, continue_on_error: bool = True) -> bool:
        """
        执行所有脚本
        
        Args:
            continue_on_error (bool): 遇到错误时是否继续执行
            
        Returns:
            bool: 所有脚本是否都执行成功
        """
        logger.info("开始执行所有因子计算脚本")
        
        # 先执行stock目录下的脚本
        stock_success = self.execute_stock_scripts(continue_on_error)
        
        overall_success = stock_success
        
        if overall_success:
            logger.info("所有因子计算脚本执行成功")
        else:
            logger.warning("部分因子计算脚本执行失败")
        
        return overall_success
    
    def list_available_scripts(self):
        """
        列出所有可用的脚本
        """
        scripts = self.get_available_scripts()
        
        print("\n可用的因子计算脚本:")
        print("=" * 50)
        
        print("\nStock目录脚本:")
        for script in scripts['stock']:
            print(f"  - {script}")
        
        print("\n根目录脚本:")
        for script in scripts['root']:
            print(f"  - {script}")
        
        print("\n执行顺序:")
        print("1. Stock目录脚本 (按顺序执行)")
        print("2. 根目录脚本 (按顺序执行)")
        print("=" * 50)


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(
        description='因子计算脚本统一执行器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python main.py                          # 执行所有脚本
  python main.py -d stock                 # 只执行stock目录下的脚本
  python main.py -d root                  # 只执行根目录下的脚本
  python main.py -s hot.py                # 执行单个脚本
  python main.py --list                   # 列出所有可用脚本
  python main.py --stop-on-error          # 遇到错误时停止执行
        """
    )
    
    parser.add_argument(
        '-d', '--directory',
        choices=['stock', 'root', 'all'],
        default='all',
        help='指定执行的目录 (默认: all)'
    )
    
    parser.add_argument(
        '-s', '--script',
        help='执行单个脚本'
    )
    parser.add_argument(
        '--date',
        help='指定交易日期 (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='列出所有可用的脚本'
    )
    
    error_group = parser.add_mutually_exclusive_group()
    error_group.add_argument(
        '--continue-on-error',
        action='store_true',
        default=True,
        help='遇到错误时继续执行 (默认)'
    )
    
    error_group.add_argument(
        '--stop-on-error',
        action='store_true',
        help='遇到错误时停止执行'
    )
    
    args = parser.parse_args()
    
    # 创建调度器
    scheduler = FactorCalculationScheduler()
    if args.date:
        scheduler.trade_date = args.date
    
    # 列出可用脚本
    if args.list:
        scheduler.list_available_scripts()
        return
    
    # 确定错误处理策略
    continue_on_error = not args.stop_on_error
    
    try:
        # 执行单个脚本
        if args.script:
            success = scheduler.execute_single_script(args.script)
            if success:
                print(f"脚本 {args.script} 执行成功")
            else:
                print(f"脚本 {args.script} 执行失败")
                sys.exit(1)
            return
        
        # 根据目录参数执行脚本
        if args.directory == 'stock':
            success = scheduler.execute_stock_scripts(continue_on_error)
        else:  # all
            success = scheduler.execute_all_scripts(continue_on_error)
        
        if success:
            print("所有指定脚本执行成功")
        else:
            print("部分脚本执行失败")
            if not continue_on_error:
                sys.exit(1)
                
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        print("\n执行被用户中断")
    except Exception as e:
        logger.error(f"执行过程中发生错误: {e}")
        print(f"执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
