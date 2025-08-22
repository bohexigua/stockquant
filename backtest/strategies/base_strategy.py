#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略基类模板
提供所有策略的基础功能和通用接口
"""

import backtrader as bt
from abc import abstractmethod
from typing import Dict, Any, Optional
from loguru import logger


class BaseStrategy(bt.Strategy):
    """
    策略基类
    所有自定义策略都应该继承此类
    """
    
    # 默认参数
    params = (
        ('stop_loss', 0.05),      # 止损比例
        ('take_profit', 0.15),    # 止盈比例
        ('position_size', 0.1),   # 仓位大小
        ('debug', False),         # 调试模式
    )
    
    def __init__(self):
        """
        初始化策略
        """
        super().__init__()
        
        # 数据引用
        self.dataclose = self.data.close
        self.dataopen = self.data.open
        self.datahigh = self.data.high
        self.datalow = self.data.low
        self.datavolume = self.data.volume
        
        # 策略状态
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        
        # 统计信息
        self.trade_count = 0
        self.win_count = 0
        self.lose_count = 0
        
        # 日志设置
        if self.params.debug:
            logger.info(f"策略初始化: {self.__class__.__name__}")
            logger.info(f"策略参数: {dict(self.params._getpairs())}")
    
    def log(self, txt: str, dt=None):
        """
        日志记录函数
        
        Args:
            txt: 日志内容
            dt: 日期时间
        """
        dt = dt or self.datas[0].datetime.date(0)
        if self.params.debug:
            logger.info(f'{dt.isoformat()}: {txt}')
    
    def notify_order(self, order):
        """
        订单状态通知
        
        Args:
            order: 订单对象
        """
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交/已接受
            return
        
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行, 价格: {order.executed.price:.2f}, '
                        f'数量: {order.executed.size}, '
                        f'手续费: {order.executed.comm:.2f}')
                self.buy_price = order.executed.price
                self.buy_comm = order.executed.comm
            else:
                self.log(f'卖出执行, 价格: {order.executed.price:.2f}, '
                        f'数量: {order.executed.size}, '
                        f'手续费: {order.executed.comm:.2f}')
                
                # 计算盈亏
                if self.buy_price:
                    profit = (order.executed.price - self.buy_price) * order.executed.size
                    profit -= (self.buy_comm + order.executed.comm)
                    self.log(f'交易盈亏: {profit:.2f}')
                    
                    # 更新统计
                    self.trade_count += 1
                    if profit > 0:
                        self.win_count += 1
                    else:
                        self.lose_count += 1
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单取消/保证金不足/拒绝: {order.status}')
        
        # 重置订单
        self.order = None
    
    def notify_trade(self, trade):
        """
        交易状态通知
        
        Args:
            trade: 交易对象
        """
        if not trade.isclosed:
            return
        
        self.log(f'交易完成, 毛利润: {trade.pnl:.2f}, 净利润: {trade.pnlcomm:.2f}')
    
    def get_position_size(self, price: float) -> int:
        """
        计算仓位大小
        
        Args:
            price: 当前价格
            
        Returns:
            仓位大小
        """
        cash = self.broker.getcash()
        max_size = int(cash * self.params.position_size / price)
        return max_size
    
    def check_stop_loss(self, current_price: float) -> bool:
        """
        检查止损条件
        
        Args:
            current_price: 当前价格
            
        Returns:
            是否触发止损
        """
        if not self.position or not self.buy_price:
            return False
        
        if self.position.size > 0:  # 多头仓位
            loss_pct = (self.buy_price - current_price) / self.buy_price
            return loss_pct >= self.params.stop_loss
        
        return False
    
    def check_take_profit(self, current_price: float) -> bool:
        """
        检查止盈条件
        
        Args:
            current_price: 当前价格
            
        Returns:
            是否触发止盈
        """
        if not self.position or not self.buy_price:
            return False
        
        if self.position.size > 0:  # 多头仓位
            profit_pct = (current_price - self.buy_price) / self.buy_price
            return profit_pct >= self.params.take_profit
        
        return False
    
    def can_buy(self) -> bool:
        """
        检查是否可以买入
        
        Returns:
            是否可以买入
        """
        return not self.position and not self.order
    
    def can_sell(self) -> bool:
        """
        检查是否可以卖出
        
        Returns:
            是否可以卖出
        """
        return self.position and not self.order
    
    def buy_signal(self) -> bool:
        """
        买入信号
        子类需要实现此方法
        
        Returns:
            是否产生买入信号
        """
        return False
    
    def sell_signal(self) -> bool:
        """
        卖出信号
        子类需要实现此方法
        
        Returns:
            是否产生卖出信号
        """
        return False
    
    def next(self):
        """
        策略主逻辑
        每个数据点都会调用此方法
        """
        # 记录当前价格
        current_price = self.dataclose[0]
        self.log(f'当前价格: {current_price:.2f}')
        
        # 检查订单状态
        if self.order:
            return
        
        # 检查止损止盈
        if self.position:
            if self.check_stop_loss(current_price):
                self.log('触发止损')
                self.order = self.sell()
                return
            
            if self.check_take_profit(current_price):
                self.log('触发止盈')
                self.order = self.sell()
                return
        
        # 检查买入信号
        if self.can_buy() and self.buy_signal():
            size = self.get_position_size(current_price)
            if size > 0:
                self.log(f'买入信号, 数量: {size}')
                self.order = self.buy(size=size)
        
        # 检查卖出信号
        elif self.can_sell() and self.sell_signal():
            self.log('卖出信号')
            self.order = self.sell()
    
    def stop(self):
        """
        策略结束时调用
        """
        final_value = self.broker.getvalue()
        self.log(f'策略结束, 最终资产: {final_value:.2f}')
        
        if self.trade_count > 0:
            win_rate = self.win_count / self.trade_count * 100
            self.log(f'交易统计: 总交易{self.trade_count}次, '
                    f'盈利{self.win_count}次, 亏损{self.lose_count}次, '
                    f'胜率{win_rate:.1f}%')
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取策略统计信息
        
        Returns:
            统计信息字典
        """
        win_rate = self.win_count / self.trade_count * 100 if self.trade_count > 0 else 0
        
        return {
            'strategy_name': self.__class__.__name__,
            'total_trades': self.trade_count,
            'win_trades': self.win_count,
            'lose_trades': self.lose_count,
            'win_rate': win_rate,
            'final_value': self.broker.getvalue()
        }