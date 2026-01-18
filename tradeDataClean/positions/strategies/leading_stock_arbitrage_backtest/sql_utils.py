from datetime import datetime
from .constants import strategy_name as STRATEGY_NAME

def get_subquery_stock_tick(now_dt: datetime) -> str:
    """
    实时数据: trade_date <= today AND trade_time <= now
    映射 5min 表字段到 tick 表字段:
    - close -> price
    - vol -> volume
    - 关联 daily 表获取 pre_close
    """
    d = now_dt.strftime('%Y-%m-%d')
    t = now_dt.strftime('%H:%M:%S')
    return f"""(
        SELECT 
            t5.trade_date,
            t5.code,
            t5.name,
            t5.trade_time,
            t5.open,
            d.pre_close,
            t5.close as price,
            t5.high,
            t5.low,
            t5.vol as volume,
            t5.amount
        FROM trade_market_stock_5min t5
        LEFT JOIN trade_market_stock_daily d ON t5.code = d.code AND t5.trade_date = d.trade_date
        WHERE t5.trade_date = '{d}' AND t5.trade_time <= '{t}'
    )"""

def get_subquery_stock_daily(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_market_stock_daily WHERE trade_date < '{d}')"

def get_subquery_stock_basic_daily(now_dt: datetime) -> str:
    """
    T+1离线数据: trade_date < today
    """
    d = now_dt.strftime('%Y-%m-%d')
    return f"(SELECT * FROM trade_market_stock_basic_daily WHERE trade_date < '{d}')"

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
