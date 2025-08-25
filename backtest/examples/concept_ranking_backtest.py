#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
概念板块排名因子策略回测
基于概念板块排名和个股人气的选股策略回测
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
import yaml
import backtrader as bt
from datetime import datetime
import pandas as pd
import json

# 导入自定义模块
from core.engine import BacktestEngine
from data.multi_stock_manager import MultiStockDataManager
from strategies.concept_ranking_strategy import ConceptRankingStrategy


def setup_logger(log_path: str = './logs/'):
    """
    设置日志
    
    Args:
        log_path: 日志路径
    """
    os.makedirs(log_path, exist_ok=True)
    
    # 生成日志文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_path, f'concept_ranking_backtest_{timestamp}.log')
    
    # 配置日志
    logger.remove()  # 移除默认处理器
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="10 MB"
    )
    
    logger.info(f"日志文件: {log_file}")


def load_config(config_path: str) -> dict:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"配置文件加载成功: {config_path}")
        return config
    except Exception as e:
        logger.error(f"配置文件加载失败: {e}")
        raise


def prepare_data(config: dict) -> dict:
    """
    准备回测数据
    
    Args:
        config: 配置字典
        
    Returns:
        数据源字典
    """
    logger.info("开始准备回测数据...")
    
    # 创建数据管理器
    data_manager = MultiStockDataManager(config['database'])
    
    # 准备回测数据
    data_feeds = data_manager.prepare_backtest_data(
        start_date=config['backtest']['start_date'],
        end_date=config['backtest']['end_date'],
        stock_limit=config['data']['stock_limit'],
        min_days=config['data']['min_data_points']
    )
    
    logger.info(f"数据准备完成，共 {len(data_feeds)} 只股票")
    
    return data_feeds


def run_backtest(config: dict, data_feeds: dict) -> list:
    """
    运行回测
    
    Args:
        config: 配置字典
        data_feeds: 数据源字典
        
    Returns:
        回测结果
    """
    logger.info("开始运行回测...")
    
    # 创建回测引擎
    cerebro = bt.Cerebro()
    
    # 设置初始资金和手续费
    cerebro.broker.setcash(config['backtest']['cash'])
    cerebro.broker.setcommission(commission=config['backtest']['commission'])
    
    # 添加数据源
    for name, feed in data_feeds.items():
        cerebro.adddata(feed, name=name)
    
    logger.info(f"添加了 {len(data_feeds)} 个数据源")
    
    # 添加策略
    cerebro.addstrategy(
        ConceptRankingStrategy,
        **config['strategy']['params']
    )
    
    # 添加分析器
    if config['analyzers']['sharpe_ratio']:
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    
    if config['analyzers']['drawdown']:
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    if config['analyzers']['returns']:
        cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    
    if config['analyzers']['trades']:
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    
    # CalmarRatio分析器在某些版本的backtrader中不可用，暂时注释
    # if config['analyzers']['calmar']:
    #     cerebro.addanalyzer(bt.analyzers.CalmarRatio, _name='calmar')
    
    if config['analyzers']['sqn']:
        cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')
    
    # 记录初始资金
    initial_value = cerebro.broker.getvalue()
    logger.info(f"初始资金: ${initial_value:,.2f}")
    
    # 运行回测
    results = cerebro.run()
    
    # 记录最终资金
    final_value = cerebro.broker.getvalue()
    total_return = (final_value - initial_value) / initial_value * 100
    
    logger.info(f"最终资金: ${final_value:,.2f}")
    logger.info(f"总收益率: {total_return:.2f}%")
    
    return results


def analyze_results(results: list, config: dict) -> dict:
    """
    分析回测结果
    
    Args:
        results: 回测结果
        config: 配置字典
        
    Returns:
        分析结果字典
    """
    logger.info("开始分析回测结果...")
    
    if not results:
        logger.error("回测结果为空")
        return {}
    
    strategy = results[0]
    analysis = {}
    
    # 基础统计
    analysis['basic'] = {
        'initial_cash': config['backtest']['cash'],
        'final_value': strategy.broker.getvalue(),
        'total_return': (strategy.broker.getvalue() - config['backtest']['cash']) / config['backtest']['cash'] * 100
    }
    
    # 分析器结果
    analyzers = strategy.analyzers
    
    # 夏普比率
    if hasattr(analyzers, 'sharpe'):
        sharpe_analysis = analyzers.sharpe.get_analysis()
        analysis['sharpe_ratio'] = sharpe_analysis.get('sharperatio', None)
    
    # 回撤分析
    if hasattr(analyzers, 'drawdown'):
        dd_analysis = analyzers.drawdown.get_analysis()
        analysis['drawdown'] = {
            'max_drawdown': dd_analysis.get('max', {}).get('drawdown', 0),
            'max_drawdown_period': dd_analysis.get('max', {}).get('len', 0)
        }
    
    # 收益分析
    if hasattr(analyzers, 'returns'):
        returns_analysis = analyzers.returns.get_analysis()
        analysis['returns'] = {
            'total_return': returns_analysis.get('rtot', 0),
            'average_return': returns_analysis.get('ravg', 0)
        }
    
    # 交易分析
    if hasattr(analyzers, 'trades'):
        trades_analysis = analyzers.trades.get_analysis()
        analysis['trades'] = {
            'total_trades': trades_analysis.get('total', {}).get('total', 0),
            'winning_trades': trades_analysis.get('won', {}).get('total', 0),
            'losing_trades': trades_analysis.get('lost', {}).get('total', 0),
            'win_rate': trades_analysis.get('won', {}).get('total', 0) / max(trades_analysis.get('total', {}).get('total', 1), 1) * 100
        }
    
    # Calmar比率
    if hasattr(analyzers, 'calmar'):
        calmar_analysis = analyzers.calmar.get_analysis()
        analysis['calmar_ratio'] = calmar_analysis.get('calmarratio', None)
    
    # SQN
    if hasattr(analyzers, 'sqn'):
        sqn_analysis = analyzers.sqn.get_analysis()
        analysis['sqn'] = sqn_analysis.get('sqn', None)
    
    # 策略特定统计
    if hasattr(strategy, 'get_strategy_stats'):
        analysis['strategy_stats'] = strategy.get_strategy_stats()
    
    return analysis


def save_results(analysis: dict, config: dict):
    """
    保存回测结果
    
    Args:
        analysis: 分析结果
        config: 配置字典
    """
    # 创建结果目录
    results_path = config['output']['results_path']
    os.makedirs(results_path, exist_ok=True)
    
    # 生成文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 保存JSON结果
    json_file = os.path.join(results_path, f'concept_ranking_results_{timestamp}.json')
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"结果已保存到: {json_file}")
    
    # 生成报告
    report_file = os.path.join(results_path, f'concept_ranking_report_{timestamp}.md')
    generate_report(analysis, report_file)
    
    logger.info(f"报告已生成: {report_file}")


def generate_report(analysis: dict, report_file: str):
    """
    生成回测报告
    
    Args:
        analysis: 分析结果
        report_file: 报告文件路径
    """
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# 概念板块排名因子策略回测报告\n\n")
        
        # 基础统计
        if 'basic' in analysis:
            basic = analysis['basic']
            f.write("## 基础统计\n\n")
            f.write(f"- 初始资金: ${basic['initial_cash']:,.2f}\n")
            f.write(f"- 最终资金: ${basic['final_value']:,.2f}\n")
            f.write(f"- 总收益率: {basic['total_return']:.2f}%\n\n")
        
        # 风险指标
        f.write("## 风险指标\n\n")
        if 'sharpe_ratio' in analysis and analysis['sharpe_ratio']:
            f.write(f"- 夏普比率: {analysis['sharpe_ratio']:.4f}\n")
        
        if 'drawdown' in analysis:
            dd = analysis['drawdown']
            f.write(f"- 最大回撤: {dd['max_drawdown']:.2f}%\n")
            f.write(f"- 最大回撤期间: {dd['max_drawdown_period']} 天\n")
        
        if 'calmar_ratio' in analysis and analysis['calmar_ratio']:
            f.write(f"- Calmar比率: {analysis['calmar_ratio']:.4f}\n")
        
        f.write("\n")
        
        # 交易统计
        if 'trades' in analysis:
            trades = analysis['trades']
            f.write("## 交易统计\n\n")
            f.write(f"- 总交易次数: {trades['total_trades']}\n")
            f.write(f"- 盈利交易: {trades['winning_trades']}\n")
            f.write(f"- 亏损交易: {trades['losing_trades']}\n")
            f.write(f"- 胜率: {trades['win_rate']:.2f}%\n\n")
        
        # 策略统计
        if 'strategy_stats' in analysis:
            stats = analysis['strategy_stats']
            f.write("## 策略统计\n\n")
            f.write(f"- 调仓次数: {stats.get('rebalance_count', 0)}\n")
            f.write(f"- 概念选择次数: {stats.get('concept_selections_count', 0)}\n")
            f.write(f"- 平均每次调仓股票数: {stats.get('avg_stocks_per_rebalance', 0)}\n\n")
        
        f.write(f"\n报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


def main():
    """
    主函数
    """
    try:
        # 配置文件路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, '..', 'config', 'concept_ranking_config.yaml')
        
        # 加载配置
        config = load_config(config_path)
        
        # 设置日志
        setup_logger(config['output']['log_path'])
        
        logger.info("=" * 50)
        logger.info("概念板块排名因子策略回测开始")
        logger.info("=" * 50)
        
        # 准备数据
        data_feeds = prepare_data(config)
        
        if not data_feeds:
            logger.error("数据准备失败，退出回测")
            return
        
        # 运行回测
        results = run_backtest(config, data_feeds)
        
        # 分析结果
        analysis = analyze_results(results, config)
        
        # 保存结果
        save_results(analysis, config)
        
        logger.info("=" * 50)
        logger.info("概念板块排名因子策略回测完成")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"回测执行失败: {e}")
        raise


if __name__ == "__main__":
    main()