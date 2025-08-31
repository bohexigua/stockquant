#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
辅助工具模块
提供常用的工具函数和辅助类
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import os
import json
from pathlib import Path
import quantstats
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.loader import Loader

def is_valid_data(value: Any) -> bool:
    """
    判断数据是否有效
    
    Args:
        value: 要检查的数据值
        
    Returns:
        bool: 数据是否有效
        
    Rules:
        - 对于数字类型(int, float, np.number)：不能是None和NaN
        - 对于其他类型：不能是None
    """
    # 检查是否为None
    if value is None:
        return False
    
    # 检查是否为数字类型
    if isinstance(value, (int, float, np.number)):
        # 数字类型还需要检查是否为NaN
        if pd.isna(value) or np.isnan(value):
            return False
    
    return True

class BacktestResultSaver:
    """
    回测结果保存器
    用于保存回测的统计明细、图表和交割单
    """
    
    def __init__(self, base_path: str = '/Users/zwldqp/work/stockquant/backtest/returns'):
        """
        初始化回测结果保存器
        
        Args:
            base_path: 结果保存的基础路径
        """
        self.base_path = Path(base_path)
        self.loader = Loader()
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def get_stock_names(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, str]:
        """
        从数据库获取股票名称
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票代码到名称的映射字典
        """
        try:
            # 使用loader从trade_market_stock_basic_daily表获取股票基本信息
            df = self.loader.load_data(start_date, end_date, 'trade_market_stock_basic_daily')
            
            if df is not None and not df.empty:
                # 筛选指定的股票代码
                filtered_df = df[df['code'].isin(stock_codes)]
                
                # 创建代码到名称的映射，去重并取最新的名称
                stock_names = {}
                for code in stock_codes:
                    code_df = filtered_df[filtered_df['code'] == code]
                    if not code_df.empty:
                        # 取最新日期的股票名称
                        latest_name = code_df.sort_values('datetime', ascending=False)['name'].iloc[0]
                        stock_names[code] = latest_name
                    else:
                        stock_names[code] = '未知'
                
                return stock_names
            else:
                # 如果查询失败，返回默认映射
                return {code: '未知' for code in stock_codes}
                
        except Exception as e:
            print(f"获取股票名称失败: {e}")
            return {code: '未知' for code in stock_codes}
    
    def create_result_folder(self, strategy_name: str) -> Path:
        """
        为策略创建结果文件夹
        
        Args:
            strategy_name: 策略名称
            
        Returns:
            结果文件夹路径
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_name = f"{strategy_name}_{timestamp}"
        result_path = self.base_path / folder_name
        result_path.mkdir(parents=True, exist_ok=True)
        return result_path
    
    def save_backtest_results(self, result_path: Path, strategy_name: str, strat, 
                             start_date: str, end_date: str, initial_cash: float,
                             returns: pd.Series = None, transactions: pd.DataFrame = None):
        """
        保存回测结果到指定文件夹
        
        Args:
            result_path: 结果保存路径
            strategy_name: 策略名称
            strat: 策略对象
            start_date: 开始日期
            end_date: 结束日期
            initial_cash: 初始资金
            returns: 收益率序列
            transactions: 交易记录
        """
        # 1. 保存统计明细
        final_value = strat.result['final_value']
        
        # 获取最大回撤
        max_drawdown = 'N/A'
        if strat is not None and hasattr(strat, 'analyzers') and hasattr(strat.analyzers, 'drawdown'):
            if hasattr(strat.analyzers.drawdown, 'get_analysis'):
                drawdown = strat.analyzers.drawdown.get_analysis()
                max_drawdown = drawdown.get('max', {}).get('drawdown', 'N/A')
        
        stats = {
            'strategy_name': strategy_name,
            'start_date': start_date,
            'end_date': end_date,
            'initial_cash': initial_cash,
            'final_value': final_value,
            'total_return': (final_value - initial_cash),
            'return_pct': ((final_value - initial_cash) / initial_cash) * 100 if initial_cash > 0 else 0,
            'max_drawdown': max_drawdown,
            'total_trades': len(strat.trade_log) if strat is not None and hasattr(strat, 'trade_log') else 0,
            'winning_trades': len([t for t in strat.trade_log if t['pnl'] > 0]) if strat is not None and hasattr(strat, 'trade_log') else 0,
            'win_rate': (len([t for t in strat.trade_log if t['pnl'] > 0]) / len(strat.trade_log) * 100) if strat is not None and hasattr(strat, 'trade_log') and strat.trade_log else 0,
            'avg_pnl': sum([t['pnl'] for t in strat.trade_log]) / len(strat.trade_log) if strat is not None and hasattr(strat, 'trade_log') and strat.trade_log else 0
        }
        
        with open(result_path / 'statistics.json', 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        # 3. 保存收益数据
        if returns is not None and len(returns) > 0:
            returns.to_csv(result_path / 'returns.csv', encoding='utf-8-sig')
        
        # 5. 保存交易记录（包含股票名称）
        if transactions is not None and len(transactions) > 0:
            transactions_copy = transactions.copy()
            
            # 从数据库获取股票名称
            if 'symbol' in transactions_copy.columns:
                stock_codes = transactions_copy['symbol'].unique().tolist()
                stock_names = self.get_stock_names(stock_codes, start_date, end_date)
                
                # 添加股票名称映射
                transactions_copy['stock_name'] = transactions_copy['symbol'].map(stock_names).fillna('未知')
            
            transactions_copy.to_csv(result_path / 'transactions.csv', encoding='utf-8-sig')
        
        logger.info(f"回测结果已保存到: {result_path}")
        return result_path
    
    def save_charts(self, result_path: Path, returns: pd.Series, strategy_name: str = 'Strategy'):
        """
        保存图表
        
        Args:
            result_path: 结果保存路径
            returns: 收益率序列
            strategy_name: 策略名称
        """
        try:
            # 保存性能快照图
            logger.info(f"性能快照图准备生成...")
            snapshot_path = result_path / 'performance_snapshot.png'
            quantstats.plots.snapshot(returns, title=f'{strategy_name} Performance', 
                                    show=False, savefig=str(snapshot_path))
            logger.info(f"性能快照图已保存: {snapshot_path}")
            
        except Exception as e:
            logger.error(f"生成图表时出错: {e}")
            logger.info(f"请查看保存的结果文件夹: {result_path}")
    
    def save_complete_results(self, strategy_name: str, strat, start_date: str, end_date: str, 
                             initial_cash: float, returns: pd.Series = None, 
                             transactions: pd.DataFrame = None) -> Path:
        """
        保存完整的回测结果（包括数据和图表）
        
        Args:
            strategy_name: 策略名称
            strat: 策略对象
            start_date: 开始日期
            end_date: 结束日期
            initial_cash: 初始资金
            returns: 收益率序列
            transactions: 交易记录
            
        Returns:
            结果保存路径
        """
        # 创建结果文件夹
        result_path = self.create_result_folder(strategy_name)
        
        # 保存回测结果
        self.save_backtest_results(result_path, strategy_name, strat, start_date, end_date, initial_cash,
                                 returns, transactions)
        
        self.save_charts(result_path, returns, strategy_name)
        
        return result_path