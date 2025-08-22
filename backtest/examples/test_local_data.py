#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用本地CSV数据的测试示例
避免API限流问题，验证框架功能
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from utils import ConfigManager, LoggerSetup
from backtest import BacktestEngine
from data import DataLoader
from strategies import SimpleMovingAverageStrategy
import backtrader as bt
import pandas as pd

def main():
    """主函数"""
    try:
        # 设置日志
        LoggerSetup.setup_logger(log_level="INFO")
        logger.info("开始本地数据测试")
        
        # 创建回测引擎
        engine = BacktestEngine()
        
        # 手动配置回测参数
        engine.cerebro.broker.setcash(100000.0)
        engine.cerebro.broker.setcommission(commission=0.001)
        
        # 加载本地CSV数据
        data_loader = DataLoader()
        csv_file = "./data/sample_data.csv"
        
        if not os.path.exists(csv_file):
            logger.error(f"数据文件不存在: {csv_file}")
            return
            
        # 读取CSV数据
        df = pd.read_csv(csv_file, parse_dates=['Date'], index_col='Date')
        logger.info(f"加载数据: {len(df)} 条记录")
        
        # 创建Backtrader数据源
        data_feed = bt.feeds.PandasData(
            dataname=df,
            datetime=None,  # 使用索引作为日期
            open='Open',
            high='High', 
            low='Low',
            close='Close',
            volume='Volume',
            openinterest=None
        )
        
        # 添加数据到引擎
        engine.cerebro.adddata(data_feed, name='AAPL')
        logger.info("数据添加完成")
        
        # 添加策略
        engine.cerebro.addstrategy(
            SimpleMovingAverageStrategy,
            fast_period=5,
            slow_period=10,
            debug=True
        )
        logger.info("策略添加完成")
        
        # 添加分析器
        engine.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        engine.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        engine.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        
        # 运行回测
        logger.info("开始回测...")
        initial_value = engine.cerebro.broker.getvalue()
        logger.info(f"初始资金: ${initial_value:,.2f}")
        
        results = engine.cerebro.run()
        
        final_value = engine.cerebro.broker.getvalue()
        logger.info(f"最终资金: ${final_value:,.2f}")
        logger.info(f"总收益: ${final_value - initial_value:,.2f}")
        logger.info(f"收益率: {((final_value - initial_value) / initial_value) * 100:.2f}%")
        
        # 获取分析结果
        if results:
            strat = results[0]
            
            # 夏普比率
            if hasattr(strat.analyzers.sharpe, 'get_analysis'):
                sharpe = strat.analyzers.sharpe.get_analysis()
                if 'sharperatio' in sharpe and sharpe['sharperatio'] is not None:
                    logger.info(f"夏普比率: {sharpe['sharperatio']:.4f}")
                else:
                    logger.info("夏普比率: 无法计算（数据不足）")
            
            # 最大回撤
            if hasattr(strat.analyzers.drawdown, 'get_analysis'):
                drawdown = strat.analyzers.drawdown.get_analysis()
                if 'max' in drawdown and 'drawdown' in drawdown['max']:
                    logger.info(f"最大回撤: {drawdown['max']['drawdown']:.2f}%")
                else:
                    logger.info("最大回撤: 无法计算")
            
            # 年化收益率
            if hasattr(strat.analyzers.returns, 'get_analysis'):
                returns = strat.analyzers.returns.get_analysis()
                if 'rnorm100' in returns and returns['rnorm100'] is not None:
                    logger.info(f"年化收益率: {returns['rnorm100']:.2f}%")
                else:
                    logger.info("年化收益率: 无法计算（数据不足）")
        
        logger.info("回测完成！")
        
        # 可选：绘制图表（需要matplotlib）
        try:
            engine.cerebro.plot(style='candlestick', barup='green', bardown='red')
            logger.info("图表已生成")
        except Exception as e:
            logger.warning(f"图表生成失败: {e}")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()