#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
概念板块排名因子策略
基于概念板块排名和个股人气的选股策略
"""

import backtrader as bt
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 导入数据加载器和配置
from backtest.strategies.base_strategy import BaseStrategy
from backtest.data.database_loader import DatabaseLoader

class ConceptRankingStrategy(BaseStrategy):
    """
    概念板块排名因子策略
    
    策略逻辑:
    - 每个交易日开盘前选择人气TOP3的概念板块
    - 在这些概念板块中选择个股人气排名靠前的股票
    - 等权重买入选中的股票
    - 每日调仓，卖出不在当日选股池的股票
    """
    
    params = (
        ('top_concepts', 3),          # 选择前N个概念板块
        ('stocks_per_concept', 2),    # 每个概念板块选择的股票数
        ('rebalance_freq', 1),        # 调仓频率（天）
        ('position_size', 0.95),      # 总仓位比例
        ('min_volume', 1000000),      # 最小成交量过滤
        ('debug', True),              # 调试模式
    )
    
    def __init__(self):
        """
        初始化策略
        """
        super().__init__()
        
        # 数据库连接 - 从策略参数中获取
        db_config = {
            'host': self.params.db_host,
            'port': self.params.db_port,
            'user': self.params.db_user,
            'password': self.params.db_password,
            'database': self.params.db_database
        }
        self.db_loader = DatabaseLoader(db_config)
        
        # 策略状态
        self.current_positions = {}  # 当前持仓
        self.target_stocks = []      # 目标股票池
        self.last_rebalance_date = None
        self.trading_dates = []
        
        # 统计信息
        self.rebalance_count = 0
        self.concept_selections = []  # 记录每次选择的概念板块
        
        self.log(f'概念板块排名策略初始化完成')
        self.log(f'选择概念板块数: {self.params.top_concepts}')
        self.log(f'每个概念选股数: {self.params.stocks_per_concept}')
    
    def prenext(self):
        """
        数据不足时的处理
        """
        pass
    
    def next(self):
        """
        策略主逻辑
        """
        current_date = self.datas[0].datetime.date(0)
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        # 检查是否需要调仓
        if self._should_rebalance(current_date):
            self.log(f'开始调仓: {current_date_str}')
            
            # 获取目标股票池
            target_stocks = self._get_target_stocks(current_date_str)
            
            if target_stocks:
                # 执行调仓
                self._rebalance_portfolio(target_stocks)
                self.last_rebalance_date = current_date
                self.rebalance_count += 1
                
                self.log(f'调仓完成，目标股票: {target_stocks}')
            else:
                self.log(f'未找到目标股票，跳过调仓')
    
    def _should_rebalance(self, current_date: datetime) -> bool:
        """
        判断是否需要调仓
        
        Args:
            current_date: 当前日期
            
        Returns:
            是否需要调仓
        """
        if self.last_rebalance_date is None:
            return True
            
        days_since_rebalance = (current_date - self.last_rebalance_date).days
        return days_since_rebalance >= self.params.rebalance_freq
    
    def _get_target_stocks(self, trade_date: str) -> List[str]:
        """
        获取目标股票池
        
        Args:
            trade_date: 交易日期
            
        Returns:
            目标股票代码列表
        """
        try:
            with self.db_loader:
                # 1. 获取TOP概念板块
                top_concepts = self.db_loader.get_concept_ranking(
                    trade_date, self.params.top_concepts
                )
                
                if not top_concepts:
                    self.log(f'未找到概念板块数据: {trade_date}')
                    return []
                
                concept_codes = [concept['code'] for concept in top_concepts]
                self.log(f'选中概念板块: {[(c["code"], c["name"]) for c in top_concepts]}')
                
                # 记录概念板块选择
                self.concept_selections.append({
                    'date': trade_date,
                    'concepts': top_concepts
                })
                
                # 2. 获取概念板块成分股
                concept_stocks = self.db_loader.get_concept_stocks(concept_codes)
                
                # 3. 为每个概念板块选择热门股票
                target_stocks = []
                for concept_code in concept_codes:
                    stocks_in_concept = concept_stocks.get(concept_code, [])
                    if stocks_in_concept:
                        # 获取人气排名靠前的股票
                        hot_stocks = self.db_loader.get_stock_hot_ranking(
                            trade_date, stocks_in_concept, self.params.stocks_per_concept
                        )
                        target_stocks.extend(hot_stocks)
                        
                        self.log(f'概念 {concept_code} 选中股票: {hot_stocks}')
                
                return list(set(target_stocks))  # 去重
                
        except Exception as e:
            self.log(f'获取目标股票失败: {e}')
            return []
    
    def _rebalance_portfolio(self, target_stocks: List[str]):
        """
        执行投资组合调仓
        
        Args:
            target_stocks: 目标股票列表
        """
        if not target_stocks:
            # 清空所有持仓
            self._close_all_positions()
            return
        
        # 计算目标权重
        target_weight = self.params.position_size / len(target_stocks)
        current_value = self.broker.getvalue()
        
        # 获取当前持仓
        current_positions = {}
        for data in self.datas:
            if hasattr(data, '_name') and data._name:
                position = self.getposition(data)
                if position.size != 0:
                    current_positions[data._name] = position
        
        # 卖出不在目标池中的股票
        for stock_code, position in current_positions.items():
            if stock_code not in target_stocks:
                data = self._get_data_by_name(stock_code)
                if data and position.size > 0:
                    self.sell(data=data, size=position.size)
                    self.log(f'卖出 {stock_code}: {position.size} 股')
        
        # 买入目标股票
        for stock_code in target_stocks:
            data = self._get_data_by_name(stock_code)
            if data:
                current_position = self.getposition(data)
                target_value = current_value * target_weight
                current_price = data.close[0]
                
                if current_price > 0:
                    target_size = int(target_value / current_price)
                    size_diff = target_size - current_position.size
                    
                    if size_diff > 0:
                        self.buy(data=data, size=size_diff)
                        self.log(f'买入 {stock_code}: {size_diff} 股，价格: {current_price:.2f}')
                    elif size_diff < 0:
                        self.sell(data=data, size=abs(size_diff))
                        self.log(f'减仓 {stock_code}: {abs(size_diff)} 股，价格: {current_price:.2f}')
    
    def _close_all_positions(self):
        """
        清空所有持仓
        """
        for data in self.datas:
            position = self.getposition(data)
            if position.size > 0:
                self.sell(data=data, size=position.size)
                stock_name = getattr(data, '_name', 'Unknown')
                self.log(f'清仓 {stock_name}: {position.size} 股')
    
    def _get_data_by_name(self, name: str):
        """
        根据名称获取数据源
        
        Args:
            name: 数据源名称
            
        Returns:
            数据源对象
        """
        for data in self.datas:
            if hasattr(data, '_name') and data._name == name:
                return data
        return None
    
    def buy_signal(self) -> bool:
        """
        买入信号（由调仓逻辑控制，这里返回False）
        """
        return False
    
    def sell_signal(self) -> bool:
        """
        卖出信号（由调仓逻辑控制，这里返回False）
        """
        return False
    
    def stop(self):
        """
        策略结束时的处理
        """
        super().stop()
        
        self.log(f'策略执行完成')
        self.log(f'总调仓次数: {self.rebalance_count}')
        
        # 输出概念板块选择统计
        if self.concept_selections:
            self.log('概念板块选择记录:')
            for record in self.concept_selections[-5:]:  # 显示最后5次
                concepts_info = [(c['code'], c['name']) for c in record['concepts']]
                self.log(f"  {record['date']}: {concepts_info}")
    
    def get_strategy_stats(self) -> Dict[str, Any]:
        """
        获取策略统计信息
        
        Returns:
            策略统计字典
        """
        stats = super().get_stats()
        stats.update({
            'rebalance_count': self.rebalance_count,
            'concept_selections_count': len(self.concept_selections),
            'avg_stocks_per_rebalance': len(self.target_stocks) if self.target_stocks else 0
        })
        return stats