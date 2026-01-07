from datetime import datetime, time

def get_stock_limit_up_price(code: str, pre_close: float, stock_name: str = '') -> float:
    """
    计算涨停价（近似）
    """
    if not pre_close:
        return 0.0
    
    ratio = 1.10
    if code.startswith('300') or code.startswith('688'):
        ratio = 1.20
    elif code.startswith('8') or code.startswith('4'): # 北交所
        ratio = 1.30
    elif 'ST' in stock_name:
        ratio = 1.05
        
    # 简单的四舍五入逻辑，实际交易所逻辑更复杂（价格档位等）
    # 这里做宽泛判断，只要接近涨停价即视为涨停
    return float(f"{pre_close * ratio:.2f}")

def is_limit_up(price: float, pre_close: float, code: str, stock_name: str = '') -> bool:
    limit_price = get_stock_limit_up_price(code, pre_close, stock_name)
    # 考虑到精度问题，如果价格 >= 涨停价 - 0.01 则视为涨停
    return price >= (limit_price - 0.01)

def calc_volume_ratio(strategy, code: str, now_dt: datetime):
    """
    计算量比
    返回: (ok, ratio, reason)
    """
    try:
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        view_tick = sql_utils.get_subquery_stock_tick(now_dt)
        view_5min = sql_utils.get_subquery_stock_5min(now_dt)

        with strategy.db.cursor() as c:
            now_t = now_dt.time()
            is_trading_day = False
            c.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = %s LIMIT 1", (now_dt.date(),))
            cal_r = c.fetchone()
            if cal_r and int(cal_r[0]) == 1:
                is_trading_day = True

            if now_t >= time(9, 0, 0) and is_trading_day:
                tdate = now_dt.date()
            else:
                c.execute(
                    f"SELECT MAX(trade_date) FROM {view_tick} as t WHERE code=%s AND trade_date<%s",
                    (code, now_dt.date()),
                )
                drow = c.fetchone()
                if not drow or not drow[0]:
                    return False, 0.0, '无交易日期'
                tdate = drow[0]

            # 当日最新分时的累计量能
            c.execute(
                f"SELECT trade_time, volume, price, pre_close, open FROM {view_tick} as t WHERE code=%s AND trade_date=%s ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            vrow = c.fetchone()
            if not vrow:
                return False, 0.0, '当日分时数据缺失'
            curr_time, pre_vol, price, pre_close, open_price = vrow[0], float(vrow[1] or 0.0), float(vrow[2] or 0.0), float(vrow[3] or 0.0), float(vrow[4] or 0.0)

            # 昨日分时量能
            c.execute(
                f"SELECT MAX(trade_date) FROM {view_5min} as t WHERE code=%s AND trade_date<%s",
                (code, tdate),
            )
            pdrow = c.fetchone()
            prev_date = pdrow[0] if pdrow and pdrow[0] else None
            if not prev_date:
                # 尝试取tick的上一日
                c.execute(
                     f"SELECT MAX(trade_date) FROM {view_tick} as t WHERE code=%s AND trade_date<%s",
                     (code, tdate),
                )
                tick_pdrow = c.fetchone()
                if not tick_pdrow or not tick_pdrow[0]:
                    return False, 0.0, '昨日数据缺失'
                # 如果5min没数据，这里简化处理返回失败，或者需要降级逻辑
                # 为了保持与原逻辑一致，若5min缺失则失败
                return False, 0.0, '昨日分时数据缺失'

            c.execute(
                f"SELECT MIN(trade_time) FROM {view_5min} as t WHERE code=%s AND trade_date=%s AND trade_time>=%s",
                (code, prev_date, curr_time),
            )
            ntrow = c.fetchone()
            target_time = ntrow[0]
            if not target_time:
                c.execute(
                    f"SELECT MAX(trade_time) FROM {view_5min} as t WHERE code=%s AND trade_date=%s",
                    (code, prev_date),
                )
                ltrow = c.fetchone()
                target_time = ltrow[0]

            c.execute(
                f"SELECT COALESCE(SUM(vol),0) FROM {view_5min} as t WHERE code=%s AND trade_date=%s AND trade_time<=%s",
                (code, prev_date, target_time),
            )
            y_cum_vol = float(c.fetchone()[0] or 0.0)
            if y_cum_vol <= 0:
                return False, 0.0, '昨日分时量能异常'

            ratio_pct = pre_vol / y_cum_vol
            return True, ratio_pct, '', {
                'price': price,
                'pre_close': pre_close,
                'open': open_price,
                'curr_time': curr_time,
                'trade_date': tdate
            }

    except Exception as e:
        return False, 0.0, f'量比计算异常: {str(e)}'
