#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Strategies模块
提供各种交易策略
"""

from .base_strategy import BaseStrategy
from .sma_strategy import SimpleMovingAverageStrategy, DualMovingAverageStrategy

__all__ = [
    'BaseStrategy',
    'SimpleMovingAverageStrategy', 
    'DualMovingAverageStrategy'
]