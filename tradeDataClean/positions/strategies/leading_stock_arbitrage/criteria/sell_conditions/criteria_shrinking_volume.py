from .common import calc_volume_ratio, is_limit_up
from datetime import datetime

def check(strategy, code: str, stock_name: str, now_dt=None):
    """
    卖出条件2: 连续2个交易日缩量，且当日价格对比2个交易日前涨幅<7%
    - 当日开盘是涨的且非涨停则立即卖出
    - 否则当日量比开始<=0.8时则立即卖出
    """
    try:
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        if now_dt is None:
            now_dt = datetime.now()
            
        view_daily = sql_utils.get_subquery_stock_daily(now_dt)
        
        # 获取最近3个交易日的日线数据 (T-1, T-2, T-3)
        # 这里的 view_daily 已经是 trade_date < today
        with strategy.db.cursor() as c:
            # 取最近3条
            c.execute(
                f"SELECT trade_date, vol, close FROM {view_daily} as t WHERE code=%s ORDER BY trade_date DESC LIMIT 3",
                (code,)
            )
            rows = c.fetchall()
            if len(rows) < 3:
                return False, '历史数据不足3天', {}
            
            # rows[0] is T-1, rows[1] is T-2, rows[2] is T-3
            vol_1 = float(rows[0][1] or 0)
            vol_2 = float(rows[1][1] or 0)
            vol_3 = float(rows[2][1] or 0)
            
            close_2 = float(rows[1][2] or 0) # Price at T-2 (Close)
            
            # 连续2个交易日缩量: V(T-1) < V(T-2) AND V(T-2) < V(T-3)
            # 或者 T-1 < T-2 即可？ "连续2个交易日缩量" 通常指 T-1和T-2都比前一天缩量
            # 即 V(T-1) < V(T-2) AND V(T-2) < V(T-3)
            is_shrinking = (vol_1 < vol_2) and (vol_2 < vol_3)
            
            if not is_shrinking:
                return False, '未满足连续2日缩量', {}
                
            # 获取当前行情
            ok, ratio, msg, data = calc_volume_ratio(strategy, code, now_dt)
            if not ok:
                return False, f'数据获取失败:{msg}', {}
            
            price = data['price']
            pre_close = data['pre_close']
            open_price = data['open']
            
            # 当日价格对比2个交易日前涨幅 < 7%
            # (Price - Close_2) / Close_2
            if close_2 <= 0:
                 growth = 0
            else:
                 growth = (price - close_2) / close_2
                 
            if growth >= 0.07:
                return False, f'涨幅过高({growth:.2%}>=7%)', data
                
            # 满足前置条件，检查卖出触发
            is_limit = is_limit_up(price, pre_close, code, stock_name)
            
            if open_price > pre_close and not is_limit:
                return True, f'缩量滞涨且高开非涨停(Open:{open_price}>Pre:{pre_close})', data
            
            if ratio <= 0.8:
                return True, f'缩量滞涨且量比低({ratio:.2f}<=0.8)', data
                
            return False, '未触发卖出条件', data

    except Exception as e:
        return False, f'策略异常:{str(e)}', {}
