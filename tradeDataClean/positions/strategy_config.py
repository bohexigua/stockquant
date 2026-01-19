# -*- coding: utf-8 -*-

# 策略配置
STRATEGY_CONFIG = [
    {
        'key': 'leading_stock_arbitrage',
        'enabled': True,
        'execution_windows': [('09:29:40', '11:31:00'), ('12:59:00', '15:01:00')],
        'execution_interval': 20,
        'watchlist_func': 'get_watchlist_from_user_pool'
    },
    {
        'key': 'leading_stock_arbitrage_backtest',
        'enabled': False,
        'execution_windows': [('09:29:40', '11:31:00'), ('12:59:00', '15:01:00')],
        'execution_interval': 120,
        'watchlist_func': 'get_watchlist_by_theme'
    }
]
