#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StockQuant主入口文件
提供命令行界面来运行不同的回测任务
"""

import argparse
import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core import BacktestEngine
from strategies import SimpleMovingAverageStrategy, DualMovingAverageStrategy
from data import DataManager
from utils import ConfigManager, LoggerSetup, PerformanceAnalyzer, ValidationHelper
from loguru import logger


def run_backtest(config_path=None, strategy_name=None, symbols=None, start_date=None, end_date=None):
    """
    运行回测
    
    Args:
        config_path: 配置文件路径
        strategy_name: 策略名称
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
    """
    try:
        # 加载配置
        config_manager = ConfigManager(config_path)
        config = config_manager.config
        
        # 验证配置
        is_valid, errors = ValidationHelper.validate_config(config)
        if not is_valid:
            logger.error("配置验证失败:")
            for error in errors:
                logger.error(f"  - {error}")
            return False
        
        # 覆盖配置参数
        if symbols:
            config['data']['symbols'] = symbols
        if start_date:
            config['backtest']['start_date'] = start_date
        if end_date:
            config['backtest']['end_date'] = end_date
        
        # 验证日期范围
        if not ValidationHelper.validate_date_range(
            config['backtest']['start_date'],
            config['backtest']['end_date']
        ):
            logger.error("日期范围无效")
            return False
        
        logger.info("开始回测...")
        logger.info(f"股票代码: {config['data']['symbols']}")
        logger.info(f"日期范围: {config['backtest']['start_date']} 到 {config['backtest']['end_date']}")
        
        # 创建回测引擎
        engine = BacktestEngine()
        engine.config = config
        engine._setup_cerebro()
        
        # 准备数据
        data_manager = DataManager(config)
        data_feeds = data_manager.prepare_data()
        
        if not data_feeds:
            logger.error("没有可用的数据源")
            return False
        
        # 添加数据到引擎
        for symbol, data_feed in data_feeds.items():
            engine.add_data(data_feed, symbol)
            logger.info(f"添加数据源: {symbol}")
        
        # 选择策略
        strategy_map = {
            'sma': SimpleMovingAverageStrategy,
            'dual_ma': DualMovingAverageStrategy
        }
        
        if strategy_name and strategy_name in strategy_map:
            strategy_class = strategy_map[strategy_name]
        else:
            strategy_class = SimpleMovingAverageStrategy
            logger.info(f"使用默认策略: {strategy_class.__name__}")
        
        # 获取策略参数
        strategy_params = config.get('strategy', {}).get('params', {})
        strategy_params['debug'] = False  # 命令行模式关闭调试输出
        
        # 添加策略
        engine.add_strategy(strategy_class, **strategy_params)
        
        # 运行回测
        start_time = datetime.now()
        results = engine.run()
        end_time = datetime.now()
        
        # 获取分析结果
        analysis = engine.get_analysis()
        
        # 生成报告
        if analysis:
            report = PerformanceAnalyzer.generate_report(analysis)
            print(report)
            
            # 保存结果
            results_path = config.get('output', {}).get('results_path', './results/')
            os.makedirs(results_path, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            result_file = os.path.join(results_path, f'backtest_{timestamp}.yaml')
            engine.save_results(result_file)
            
            logger.info(f"结果已保存到: {result_file}")
        
        duration = end_time - start_time
        logger.info(f"回测完成，耗时: {duration}")
        
        return True
        
    except Exception as e:
        logger.error(f"回测运行失败: {e}")
        return False


def list_strategies():
    """
    列出可用策略
    """
    strategies = {
        'sma': 'Simple Moving Average Strategy - 简单移动平均策略',
        'dual_ma': 'Dual Moving Average Strategy - 双移动平均策略'
    }
    
    print("可用策略:")
    for key, description in strategies.items():
        print(f"  {key}: {description}")


def create_sample_config():
    """
    创建示例配置文件
    """
    sample_config = {
        'backtest': {
            'cash': 100000,
            'commission': 0.001,
            'slippage': 0.0005,
            'start_date': '2022-01-01',
            'end_date': '2023-12-31'
        },
        'data': {
            'source': 'yahoo',
            'timeframe': '1d',
            'symbols': ['AAPL', 'MSFT'],
            'data_path': './data/'
        },
        'strategy': {
            'name': 'SimpleMovingAverageStrategy',
            'params': {
                'fast_period': 10,
                'slow_period': 30,
                'stop_loss': 0.05,
                'take_profit': 0.15
            }
        },
        'analyzers': {
            'sharpe_ratio': True,
            'drawdown': True,
            'returns': True,
            'trades': True
        },
        'output': {
            'plot': False,
            'save_results': True,
            'results_path': './results/',
            'log_level': 'INFO'
        }
    }
    
    config_file = './config/sample_config.yaml'
    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    
    import yaml
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(sample_config, f, default_flow_style=False, allow_unicode=True)
    
    print(f"示例配置文件已创建: {config_file}")


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(
        description='StockQuant - Backtrader回测框架',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python main.py run                                    # 使用默认配置运行
  python main.py run -c config/my_config.yaml          # 使用指定配置文件
  python main.py run -s sma -t AAPL,MSFT              # 使用SMA策略测试指定股票
  python main.py run --start 2023-01-01 --end 2023-12-31  # 指定日期范围
  python main.py list-strategies                        # 列出可用策略
  python main.py create-config                         # 创建示例配置文件
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 运行回测命令
    run_parser = subparsers.add_parser('run', help='运行回测')
    run_parser.add_argument('-c', '--config', help='配置文件路径')
    run_parser.add_argument('-s', '--strategy', choices=['sma', 'dual_ma'], help='策略名称')
    run_parser.add_argument('-t', '--symbols', help='股票代码，用逗号分隔')
    run_parser.add_argument('--start', help='开始日期 (YYYY-MM-DD)')
    run_parser.add_argument('--end', help='结束日期 (YYYY-MM-DD)')
    run_parser.add_argument('-v', '--verbose', action='store_true', help='详细输出')
    
    # 列出策略命令
    subparsers.add_parser('list-strategies', help='列出可用策略')
    
    # 创建配置文件命令
    subparsers.add_parser('create-config', help='创建示例配置文件')
    
    args = parser.parse_args()
    
    # 设置日志级别
    log_level = 'DEBUG' if getattr(args, 'verbose', False) else 'INFO'
    LoggerSetup.setup_logger(log_level)
    
    if args.command == 'run':
        # 处理股票代码参数
        symbols = None
        if args.symbols:
            symbols = [s.strip().upper() for s in args.symbols.split(',')]
        
        success = run_backtest(
            config_path=args.config,
            strategy_name=args.strategy,
            symbols=symbols,
            start_date=args.start,
            end_date=args.end
        )
        
        sys.exit(0 if success else 1)
        
    elif args.command == 'list-strategies':
        list_strategies()
        
    elif args.command == 'create-config':
        create_sample_config()
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main()