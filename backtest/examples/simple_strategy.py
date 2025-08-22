#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单策略示例
展示如何使用StockQuant框架进行回测
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import BacktestEngine
from strategies import SimpleMovingAverageStrategy
from data import DataManager
from utils import ConfigManager, LoggerSetup, PerformanceAnalyzer
from loguru import logger


def main():
    """
    主函数
    """
    # 设置日志
    LoggerSetup.setup_logger('INFO')
    logger.info("开始运行简单策略示例")
    
    try:
        # 加载配置
        config_manager = ConfigManager()
        config = config_manager.config
        
        logger.info("配置加载完成")
        return
        # 创建回测引擎
        engine = BacktestEngine()
        
        # 准备数据
        data_manager = DataManager(config)
        data_feeds = data_manager.prepare_data()
        
        if not data_feeds:
            logger.error("没有可用的数据源")
            return
        
        # 添加数据到引擎
        for symbol, data_feed in data_feeds.items():
            engine.add_data(data_feed, symbol)
            logger.info(f"添加数据源: {symbol}")
        
        # 获取策略参数
        strategy_params = config.get('strategy', {}).get('params', {})
        
        # 添加策略
        engine.add_strategy(
            SimpleMovingAverageStrategy,
            fast_period=strategy_params.get('fast_period', 10),
            slow_period=strategy_params.get('slow_period', 30),
            stop_loss=strategy_params.get('stop_loss', 0.05),
            take_profit=strategy_params.get('take_profit', 0.15),
            debug=True
        )
        
        # 运行回测
        logger.info("开始回测...")
        results = engine.run()
        
        # 获取分析结果
        analysis = engine.get_analysis()
        
        # 生成报告
        if analysis:
            report = PerformanceAnalyzer.generate_report(analysis)
            print(report)
            
            # 保存结果
            results_path = config.get('output', {}).get('results_path', './results/')
            if not os.path.exists(results_path):
                os.makedirs(results_path)
            
            timestamp = logger._core.handlers[0]._sink._stream.name if hasattr(logger, '_core') else 'latest'
            result_file = os.path.join(results_path, f'backtest_result_{timestamp}.yaml')
            engine.save_results(result_file)
        
        # 绘制图表（如果配置启用）
        if config.get('output', {}).get('plot', True):
            try:
                logger.info("绘制回测图表...")
                engine.plot()
            except Exception as e:
                logger.warning(f"图表绘制失败: {e}")
        
        logger.info("回测完成")
        
    except Exception as e:
        logger.error(f"运行失败: {e}")
        raise


if __name__ == '__main__':
    main()