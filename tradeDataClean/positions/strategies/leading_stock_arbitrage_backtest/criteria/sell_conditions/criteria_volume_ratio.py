from .common import calc_volume_ratio
from datetime import datetime
from tradeDataClean.positions.strategies.common.watchlist import get_watchlist_by_theme

def check(strategy, code: str, stock_name: str, now_dt=None):
    """
    量比条件:
    1. 不在自选列表中，当日开始下跌且当日量比<0.6时则立即卖出
    2. 在自选列表中，当日开始下跌且当日量比<0.5时则立即卖出
    """
    try:
        ok, ratio, msg, data = calc_volume_ratio(strategy, code, now_dt)
        if not ok:
            return False, f'数据获取失败:{msg}', {}
        
        price = data['price']
        pre_close = data['pre_close']

        if price <= 0.0 or pre_close <= 0.0:
            return False, '价格数据无效(<=0)', data
            
        # 1. 检查是否在自选股
        current_date_str = now_dt.strftime('%Y-%m-%d') if now_dt else datetime.now().strftime('%Y-%m-%d')
        
        if not hasattr(strategy, 'daily_watchlist_cache') or strategy.daily_watchlist_cache.get('date') != current_date_str:
            with strategy.db.cursor() as c:
                dt_to_use = now_dt if now_dt else datetime.now()
                watchlist = get_watchlist_by_theme(c, dt_to_use)
                strategy.daily_watchlist_cache = {
                    'date': current_date_str,
                    'data': watchlist
                }
        
        watchlist = strategy.daily_watchlist_cache['data']
        
        threshold = 0.6
        is_in_watchlist = False
        if code in watchlist:
            threshold = 0.5
            is_in_watchlist = True
        
        # 当日开始下跌 (Price < PreClose)
        if price < pre_close and ratio < threshold:
            wl_status = "在自选" if is_in_watchlist else "不在自选"
            return True, f'下跌且量比极低({wl_status}, Price:{price}<Pre:{pre_close}, Ratio:{ratio:.2f}<{threshold})', data
            
        return False, '未触发卖出条件', data

    except Exception as e:
        return False, f'策略异常:{str(e)}', {}
