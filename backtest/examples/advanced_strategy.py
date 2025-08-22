#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高级策略示例
展示多股票、多策略的复杂回测场景
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backtest import BacktestEngine
from strategies import SimpleMovingAverageStrategy, DualMovingAverageStrategy
from data import DataManager
from utils import ConfigManager, LoggerSetup, PerformanceAnalyzer, FileHelper
from loguru import logger
import pandas as pd
from datetime import datetime


def run_strategy_comparison():
    """
    运行策略对比测试
    """
    logger.info("开始策略对比测试")
    
    # 加载配置
    config_manager = ConfigManager()
    config = config_manager.config
    
    # 策略列表
    strategies = [
        {
            'name': 'SimpleMA',
            'class': SimpleMovingAverageStrategy,
            'params': {'fast_period': 10, 'slow_period': 30, 'debug': False}
        },
        {
            'name': 'DualMA',
            'class': DualMovingAverageStrategy,
            'params': {'fast_period': 12, 'slow_period': 26, 'debug': False}
        }
    ]
    
    results_summary = []
    
    for strategy_info in strategies:
        logger.info(f"测试策略: {strategy_info['name']}")
        
        try:
            # 创建回测引擎
            engine = BacktestEngine()
            
            # 准备数据
            data_manager = DataManager(config)
            data_feeds = data_manager.prepare_data()
            
            if not data_feeds:
                logger.error("没有可用的数据源")
                continue
            
            # 添加数据
            for symbol, data_feed in data_feeds.items():
                engine.add_data(data_feed, symbol)
            
            # 添加策略
            engine.add_strategy(strategy_info['class'], **strategy_info['params'])
            
            # 运行回测
            results = engine.run()
            
            # 获取分析结果
            analysis = engine.get_analysis()
            
            # 计算关键指标
            final_value = engine.cerebro.broker.getvalue()
            initial_value = config.get('backtest', {}).get('cash', 100000)
            total_return = (final_value - initial_value) / initial_value
            
            strategy_result = {
                'strategy': strategy_info['name'],
                'final_value': final_value,
                'total_return': total_return,
                'analysis': analysis
            }
            
            results_summary.append(strategy_result)
            
            logger.info(f"{strategy_info['name']} 完成: 总收益率 {total_return:.2%}")
            
        except Exception as e:
            logger.error(f"策略 {strategy_info['name']} 运行失败: {e}")
    
    # 生成对比报告
    generate_comparison_report(results_summary)
    
    return results_summary


def generate_comparison_report(results_summary):
    """
    生成策略对比报告
    
    Args:
        results_summary: 结果汇总列表
    """
    if not results_summary:
        logger.warning("没有结果可供对比")
        return
    
    print("\n" + "="*60)
    print("                策略对比报告")
    print("="*60)
    
    # 创建对比表格
    comparison_data = []
    
    for result in results_summary:
        strategy_name = result['strategy']
        total_return = result['total_return']
        final_value = result['final_value']
        
        # 提取分析数据
        analysis = result.get('analysis', {})
        sharpe = analysis.get('sharpe', {}).get('sharperatio', 0) if 'sharpe' in analysis else 0
        max_dd = analysis.get('drawdown', {}).get('max', {}).get('drawdown', 0) if 'drawdown' in analysis else 0
        
        comparison_data.append({
            '策略': strategy_name,
            '最终资产': f"{final_value:,.2f}",
            '总收益率': f"{total_return:.2%}",
            '夏普比率': f"{sharpe:.2f}" if sharpe else "N/A",
            '最大回撤': f"{max_dd:.2%}" if max_dd else "N/A"
        })
    
    # 打印对比表格
    if comparison_data:
        df = pd.DataFrame(comparison_data)
        print(df.to_string(index=False))
    
    # 找出最佳策略
    best_strategy = max(results_summary, key=lambda x: x['total_return'])
    print(f"\n最佳策略: {best_strategy['strategy']}")
    print(f"最佳收益率: {best_strategy['total_return']:.2%}")
    
    print("="*60)
    
    # 保存详细结果
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f"./results/strategy_comparison_{timestamp}.json"
    FileHelper.save_json({
        'timestamp': timestamp,
        'comparison_data': comparison_data,
        'best_strategy': best_strategy['strategy'],
        'detailed_results': results_summary
    }, results_file)
    
    logger.info(f"对比结果已保存到: {results_file}")


def run_multi_symbol_test():
    """
    运行多股票测试
    """
    logger.info("开始多股票测试")
    
    # 自定义配置
    config = {
        'backtest': {
            'cash': 100000,
            'commission': 0.001,
            'start_date': '2022-01-01',
            'end_date': '2023-12-31'
        },
        'data': {
            'source': 'yahoo',
            'timeframe': '1d',
            'symbols': ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA'],
            'data_path': './data/'
        },
        'analyzers': {
            'sharpe_ratio': True,
            'drawdown': True,
            'returns': True,
            'trades': True
        }
    }
    
    try:
        # 创建回测引擎
        engine = BacktestEngine()
        engine.config = config
        engine._setup_cerebro()
        
        # 准备数据
        data_manager = DataManager(config)
        data_feeds = data_manager.prepare_data()
        
        logger.info(f"成功加载 {len(data_feeds)} 个股票数据")
        
        # 只添加第一个数据源进行测试
        if data_feeds:
            first_symbol = list(data_feeds.keys())[0]
            engine.add_data(data_feeds[first_symbol], first_symbol)
            
            # 添加策略
            engine.add_strategy(
                DualMovingAverageStrategy,
                fast_period=12,
                slow_period=26,
                debug=False
            )
            
            # 运行回测
            results = engine.run()
            
            # 获取分析结果
            analysis = engine.get_analysis()
            
            # 生成报告
            if analysis:
                report = PerformanceAnalyzer.generate_report(analysis)
                print(report)
            
            logger.info("多股票测试完成")
        
    except Exception as e:
        logger.error(f"多股票测试失败: {e}")


def main():
    """
    主函数
    """
    # 设置日志
    LoggerSetup.setup_logger('INFO')
    logger.info("开始运行高级策略示例")
    
    try:
        # 确保结果目录存在
        os.makedirs('./results', exist_ok=True)
        
        print("选择测试模式:")
        print("1. 策略对比测试")
        print("2. 多股票测试")
        print("3. 全部测试")
        
        choice = input("请输入选择 (1-3): ").strip()
        
        if choice == '1':
            run_strategy_comparison()
        elif choice == '2':
            run_multi_symbol_test()
        elif choice == '3':
            run_strategy_comparison()
            print("\n" + "-"*50 + "\n")
            run_multi_symbol_test()
        else:
            logger.info("运行默认策略对比测试")
            run_strategy_comparison()
        
        logger.info("所有测试完成")
        
    except KeyboardInterrupt:
        logger.info("用户中断测试")
    except Exception as e:
        logger.error(f"运行失败: {e}")
        raise


if __name__ == '__main__':
    main()