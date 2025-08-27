#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Strategies模块
提供各种交易策略
"""

from .strong_sector_low_stock_arbitrage import StrongSectorLowStockArbitrageStrategy

__all__ = [
    'StrongSectorLowStockArbitrageStrategy',
]