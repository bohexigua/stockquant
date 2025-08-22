#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测引擎模块
提供基于Backtrader的回测框架核心功能
"""

import backtrader as bt
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
import yaml
import os


class BacktestEngine:
    """
    回测引擎类
    负责管理回测的整个生命周期
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化回测引擎
        
        Args:
            config_path: 配置文件路径
        """
        self.cerebro = bt.Cerebro()
        self.config = self._load_config(config_path)
        self.results = None
        self._setup_cerebro()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"配置文件加载成功: {config_path}")
            return config
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """
        获取默认配置
        
        Returns:
            默认配置字典
        """
        return {
            'backtest': {
                'cash': 100000,
                'commission': 0.001,
                'slippage': 0.0005
            },
            'analyzers': {
                'sharpe_ratio': True,
                'drawdown': True,
                'returns': True,
                'trades': True
            }
        }
    
    def _setup_cerebro(self):
        """
        设置Cerebro引擎
        """
        # 设置初始资金
        cash = self.config.get('backtest', {}).get('cash', 100000)
        self.cerebro.broker.setcash(cash)
        logger.info(f"设置初始资金: {cash}")
        
        # 设置手续费
        commission = self.config.get('backtest', {}).get('commission', 0.001)
        self.cerebro.broker.setcommission(commission=commission)
        logger.info(f"设置手续费率: {commission}")
        
        # 添加分析器
        self._add_analyzers()
        
        # 添加观察器
        self._add_observers()
    
    def _add_analyzers(self):
        """
        添加分析器
        """
        analyzers_config = self.config.get('analyzers', {})
        
        if analyzers_config.get('sharpe_ratio', True):
            self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
            
        if analyzers_config.get('drawdown', True):
            self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            
        if analyzers_config.get('returns', True):
            self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
            
        if analyzers_config.get('trades', True):
            self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
            
        logger.info("分析器添加完成")
    
    def _add_observers(self):
        """
        添加观察器
        """
        self.cerebro.addobserver(bt.observers.Broker)
        self.cerebro.addobserver(bt.observers.Trades)
        self.cerebro.addobserver(bt.observers.BuySell)
        logger.info("观察器添加完成")
    
    def add_strategy(self, strategy_class, **kwargs):
        """
        添加策略
        
        Args:
            strategy_class: 策略类
            **kwargs: 策略参数
        """
        self.cerebro.addstrategy(strategy_class, **kwargs)
        logger.info(f"策略添加成功: {strategy_class.__name__}")
    
    def add_data(self, data, name: str = None):
        """
        添加数据源
        
        Args:
            data: 数据源
            name: 数据名称
        """
        self.cerebro.adddata(data, name=name)
        logger.info(f"数据源添加成功: {name or 'unnamed'}")
    
    def run(self) -> List[Any]:
        """
        运行回测
        
        Returns:
            回测结果
        """
        logger.info("开始回测...")
        start_time = datetime.now()
        
        # 记录初始资金
        start_value = self.cerebro.broker.getvalue()
        logger.info(f"初始资金: {start_value:.2f}")
        
        # 运行回测
        self.results = self.cerebro.run()
        
        # 记录最终资金
        end_value = self.cerebro.broker.getvalue()
        logger.info(f"最终资金: {end_value:.2f}")
        logger.info(f"总收益: {end_value - start_value:.2f}")
        logger.info(f"收益率: {(end_value - start_value) / start_value * 100:.2f}%")
        
        end_time = datetime.now()
        logger.info(f"回测完成，耗时: {end_time - start_time}")
        
        return self.results
    
    def get_analysis(self) -> Dict[str, Any]:
        """
        获取分析结果
        
        Returns:
            分析结果字典
        """
        if not self.results:
            logger.warning("请先运行回测")
            return {}
        
        analysis = {}
        strategy = self.results[0]
        
        # 获取各种分析结果
        for analyzer_name in ['sharpe', 'drawdown', 'returns', 'trades']:
            if hasattr(strategy.analyzers, analyzer_name):
                analyzer = getattr(strategy.analyzers, analyzer_name)
                analysis[analyzer_name] = analyzer.get_analysis()
        
        return analysis
    
    def plot(self, **kwargs):
        """
        绘制回测图表
        
        Args:
            **kwargs: 绘图参数
        """
        if not self.results:
            logger.warning("请先运行回测")
            return
        
        # 设置默认绘图参数
        plot_kwargs = {
            'style': 'candlestick',
            'barup': 'red',
            'bardown': 'green',
            'volume': False
        }
        plot_kwargs.update(kwargs)
        
        try:
            self.cerebro.plot(**plot_kwargs)
            logger.info("图表绘制完成")
        except Exception as e:
            logger.error(f"图表绘制失败: {e}")
    
    def save_results(self, filepath: str):
        """
        保存回测结果
        
        Args:
            filepath: 保存路径
        """
        if not self.results:
            logger.warning("请先运行回测")
            return
        
        try:
            analysis = self.get_analysis()
            
            # 创建结果字典
            results_dict = {
                'config': self.config,
                'analysis': analysis,
                'timestamp': datetime.now().isoformat()
            }
            
            # 保存为YAML文件
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(results_dict, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"结果保存成功: {filepath}")
        except Exception as e:
            logger.error(f"结果保存失败: {e}")