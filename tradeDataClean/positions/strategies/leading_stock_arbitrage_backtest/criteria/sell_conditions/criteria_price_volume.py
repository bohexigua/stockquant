from .common import calc_volume_ratio, is_limit_up
from datetime import datetime, time
from tradeDataClean.positions.strategies.common.watchlist import get_watchlist_by_theme
from tradeDataClean.positions.strategies.leading_stock_arbitrage_backtest import sql_utils

def check(strategy, code: str, stock_name: str, now_dt=None):
    """
    量价条件:
    1. 不在自选列表中
       - 当日9:35后非涨停则立即卖出
    2. 在自选列表中
       - 当日10:30后非涨停立即卖出
       - 当日9:30后开始有过涨停，但跌破<7%则立即卖出
    """
    try:
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
        is_in_watchlist = code in watchlist

        # 获取行情
        ok, ratio, msg, data = calc_volume_ratio(strategy, code, now_dt)
        if not ok:
            return False, f'数据获取失败:{msg}', {}
        
        price = data['price']
        pre_close = data['pre_close']

        if price <= 0.0 or pre_close <= 0.0:
            return False, '价格数据无效(<=0)', data

        current_time = now_dt.time() if now_dt else datetime.now().time()
        is_limit = is_limit_up(price, pre_close, code, stock_name)
        
        # ---------------- 条件1: 不在自选列表 ----------------
        if not is_in_watchlist:
            # 当日9:35后非涨停则立即卖出
            if current_time >= time(9, 35) and not is_limit:
                 return True, f'不在自选且9:35后非涨停({current_time})', data
        
        # ---------------- 条件2: 在自选列表 ----------------
        else:
            # 1. 当日10:30后非涨停立即卖出
            if current_time >= time(10, 30) and not is_limit:
                 return True, f'在自选且10:30后非涨停({current_time})', data
            
            # 2. 当日9:30后开始有过涨停，但跌破<7%则立即卖出
            if current_time >= time(9, 30):
                # 检查当日是否触及过涨停
                view_tick = sql_utils.get_subquery_stock_tick(now_dt)
                limit_price = round(pre_close * 1.1, 2) # 简化的涨停价计算，实际应根据板块和ST规则，但这里用预估
                if code.startswith('30') or code.startswith('68'): # 创业板/科创板 20%
                    limit_price = round(pre_close * 1.2, 2)
                
                # 更严谨的涨停价获取方式:
                # 尝试直接查询最高价
                # 从 get_subquery_stock_tick 返回的视图中可能包含 high，但需要确认是当天的high
                # 由于 get_subquery_stock_tick 是 union/subquery 比较复杂，我们直接查询 trade_market_stock_5min 
                # 但回测环境需要模拟此刻之前的最高价
                
                d = now_dt.strftime('%Y-%m-%d')
                t = now_dt.strftime('%H:%M:%S')
                
                with strategy.db.cursor() as c:
                    c.execute(f"""
                        SELECT MAX(high) 
                        FROM {view_tick} as t
                        WHERE code=%s AND trade_date='{d}' AND trade_time <= '{t}'
                    """, (code,))
                    row = c.fetchone()
                    today_high = float(row[0]) if row and row[0] else 0.0
                
                # 判断是否摸过涨停 (接近涨停价)
                # 涨停幅度: 主板10%, 科创/创业20%, ST 5%
                # 简单判定: (High - PreClose) / PreClose >= 0.098 (主板) 或 >= 0.198 (20cm)
                limit_ratio = 0.098
                if code.startswith('30') or code.startswith('68'):
                    limit_ratio = 0.198
                
                has_touched_limit = False
                if today_high > 0 and (today_high - pre_close) / pre_close >= limit_ratio:
                    has_touched_limit = True
                
                if has_touched_limit:
                    current_rise = (price - pre_close) / pre_close
                    if current_rise < 0.07:
                        return True, f'在自选,曾涨停但现跌破7%(High:{today_high}, CurrRise:{current_rise:.2%})', data

        return False, '未触发卖出条件', data

    except Exception as e:
        return False, f'策略异常:{str(e)}', {}
