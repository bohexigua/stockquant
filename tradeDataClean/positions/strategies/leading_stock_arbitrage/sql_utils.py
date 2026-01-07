from datetime import datetime
from .constants import strategy_name as STRATEGY_NAME

def get_subquery_stock_tick(now_dt: datetime) -> str:
    """
    实时数据: trade_date <= today AND trade_time <= now
    """
    d = now_dt.strftime('%Y-%m-%d')
    t = now_dt.strftime('%H:%M:%S')
    return f"(SELECT * FROM trade_market_stock_tick WHERE trade_date <= '{d}' AND trade_time <= '{t}')"

def get_subquery_stock_daily(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_market_stock_daily WHERE trade_date < '{d}')"

def get_subquery_stock_5min(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_market_stock_5min WHERE trade_date < '{d}')"

def get_subquery_positions() -> str:
    return f"(SELECT * FROM ptm_quant_positions WHERE related_strategy = '{STRATEGY_NAME}')"

def get_subquery_intraday_momentum(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_factor_stock_intraday_momentum WHERE trade_date < '{d}')"

def get_subquery_related_theme(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_factor_most_related_theme WHERE trade_date < '{d}')"
