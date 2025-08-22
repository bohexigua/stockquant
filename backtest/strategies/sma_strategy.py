#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单移动平均策略
基于快慢均线交叉的经典策略
"""

import backtrader as bt
from .base_strategy import BaseStrategy


class SimpleMovingAverageStrategy(BaseStrategy):
    """
    简单移动平均策略
    
    策略逻辑:
    - 当快速均线上穿慢速均线时买入
    - 当快速均线下穿慢速均线时卖出
    - 支持止损止盈
    """
    
    params = (
        ('fast_period', 10),      # 快速均线周期
        ('slow_period', 30),      # 慢速均线周期
        ('stop_loss', 0.05),      # 止损比例
        ('take_profit', 0.15),    # 止盈比例
        ('position_size', 0.1),   # 仓位大小
        ('debug', True),          # 调试模式
    )
    
    def __init__(self):
        """
        初始化策略
        """
        super().__init__()
        
        # 添加移动平均线指标
        self.sma_fast = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.fast_period
        )
        self.sma_slow = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.slow_period
        )
        
        # 添加交叉信号指标
        self.crossover = bt.indicators.CrossOver(
            self.sma_fast, self.sma_slow
        )
        
        # 添加其他技术指标
        self.rsi = bt.indicators.RelativeStrengthIndex(
            self.datas[0], period=14
        )
        
        self.log(f'快速均线周期: {self.params.fast_period}')
        self.log(f'慢速均线周期: {self.params.slow_period}')
    
    def buy_signal(self) -> bool:
        """
        买入信号判断
        
        Returns:
            是否产生买入信号
        """
        # 金叉信号：快线上穿慢线
        golden_cross = self.crossover[0] > 0
        
        # RSI不超买（可选的过滤条件）
        rsi_ok = self.rsi[0] < 70
        
        # 价格在慢速均线之上（趋势确认）
        price_above_slow = self.dataclose[0] > self.sma_slow[0]
        
        signal = golden_cross and rsi_ok and price_above_slow
        
        if signal:
            self.log(f'买入信号: 金叉, RSI={self.rsi[0]:.1f}, '
                    f'快线={self.sma_fast[0]:.2f}, 慢线={self.sma_slow[0]:.2f}')
        
        return signal
    
    def sell_signal(self) -> bool:
        """
        卖出信号判断
        
        Returns:
            是否产生卖出信号
        """
        # 死叉信号：快线下穿慢线
        death_cross = self.crossover[0] < 0
        
        # RSI超买（可选的过滤条件）
        rsi_overbought = self.rsi[0] > 80
        
        # 价格跌破慢速均线（趋势确认）
        price_below_slow = self.dataclose[0] < self.sma_slow[0]
        
        signal = death_cross or rsi_overbought or price_below_slow
        
        if signal:
            reason = []
            if death_cross:
                reason.append('死叉')
            if rsi_overbought:
                reason.append('RSI超买')
            if price_below_slow:
                reason.append('跌破慢线')
            
            self.log(f'卖出信号: {",".join(reason)}, RSI={self.rsi[0]:.1f}, '
                    f'快线={self.sma_fast[0]:.2f}, 慢线={self.sma_slow[0]:.2f}')
        
        return signal
    
    def next(self):
        """
        策略主逻辑
        """
        # 确保有足够的数据
        if len(self.datas[0]) < self.params.slow_period:
            return
        
        # 调用父类的next方法
        super().next()


class DualMovingAverageStrategy(BaseStrategy):
    """
    双移动平均策略（改进版）
    
    策略逻辑:
    - 使用EMA代替SMA，响应更快
    - 添加成交量确认
    - 添加ATR止损
    """
    
    params = (
        ('fast_period', 12),      # 快速EMA周期
        ('slow_period', 26),      # 慢速EMA周期
        ('volume_period', 20),    # 成交量均线周期
        ('atr_period', 14),       # ATR周期
        ('atr_multiplier', 2.0),  # ATR止损倍数
        ('position_size', 0.1),   # 仓位大小
        ('debug', True),          # 调试模式
    )
    
    def __init__(self):
        """
        初始化策略
        """
        super().__init__()
        
        # 添加EMA指标
        self.ema_fast = bt.indicators.ExponentialMovingAverage(
            self.datas[0], period=self.params.fast_period
        )
        self.ema_slow = bt.indicators.ExponentialMovingAverage(
            self.datas[0], period=self.params.slow_period
        )
        
        # 添加MACD指标
        self.macd = bt.indicators.MACD(
            self.datas[0],
            period_me1=self.params.fast_period,
            period_me2=self.params.slow_period,
            period_signal=9
        )
        
        # 添加成交量指标
        if hasattr(self.datas[0], 'volume'):
            self.volume_sma = bt.indicators.SimpleMovingAverage(
                self.datas[0].volume, period=self.params.volume_period
            )
        else:
            self.volume_sma = None
        
        # 添加ATR指标用于动态止损
        self.atr = bt.indicators.AverageTrueRange(
            self.datas[0], period=self.params.atr_period
        )
        
        self.log(f'快速EMA周期: {self.params.fast_period}')
        self.log(f'慢速EMA周期: {self.params.slow_period}')
    
    def buy_signal(self) -> bool:
        """
        买入信号判断
        
        Returns:
            是否产生买入信号
        """
        # EMA金叉
        ema_cross = (self.ema_fast[0] > self.ema_slow[0] and 
                    self.ema_fast[-1] <= self.ema_slow[-1])
        
        # MACD金叉
        macd_cross = (self.macd.macd[0] > self.macd.signal[0] and 
                     self.macd.macd[-1] <= self.macd.signal[-1])
        
        # 成交量放大（如果有成交量数据）
        volume_ok = True
        if self.volume_sma:
            volume_ok = self.datas[0].volume[0] > self.volume_sma[0] * 1.2
        
        signal = ema_cross and macd_cross and volume_ok
        
        if signal:
            self.log(f'买入信号: EMA金叉+MACD金叉, '
                    f'快EMA={self.ema_fast[0]:.2f}, 慢EMA={self.ema_slow[0]:.2f}')
        
        return signal
    
    def sell_signal(self) -> bool:
        """
        卖出信号判断
        
        Returns:
            是否产生卖出信号
        """
        # EMA死叉
        ema_cross = (self.ema_fast[0] < self.ema_slow[0] and 
                    self.ema_fast[-1] >= self.ema_slow[-1])
        
        # MACD死叉
        macd_cross = (self.macd.macd[0] < self.macd.signal[0] and 
                     self.macd.macd[-1] >= self.macd.signal[-1])
        
        signal = ema_cross or macd_cross
        
        if signal:
            reason = []
            if ema_cross:
                reason.append('EMA死叉')
            if macd_cross:
                reason.append('MACD死叉')
            
            self.log(f'卖出信号: {",".join(reason)}, '
                    f'快EMA={self.ema_fast[0]:.2f}, 慢EMA={self.ema_slow[0]:.2f}')
        
        return signal
    
    def check_stop_loss(self, current_price: float) -> bool:
        """
        使用ATR动态止损
        
        Args:
            current_price: 当前价格
            
        Returns:
            是否触发止损
        """
        if not self.position or not self.buy_price:
            return False
        
        if self.position.size > 0:  # 多头仓位
            # 使用ATR计算动态止损位
            stop_loss_price = self.buy_price - (self.atr[0] * self.params.atr_multiplier)
            return current_price <= stop_loss_price
        
        return False
    
    def next(self):
        """
        策略主逻辑
        """
        # 确保有足够的数据
        if len(self.datas[0]) < max(self.params.slow_period, self.params.atr_period):
            return
        
        # 调用父类的next方法
        super().next()