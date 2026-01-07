def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        with strategy.db.cursor() as c:
            from datetime import datetime, time
            if now_dt is None:
                now_dt = datetime.now()

            from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
            view_tick = sql_utils.get_subquery_stock_tick(now_dt)
            view_daily = sql_utils.get_subquery_stock_daily(now_dt)
            
            now_t = now_dt.time()
            is_trading_day = False
            # Check if now_dt is a trading day
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
                tdate = drow[0]
            c.execute(
                f"SELECT trade_time, price, pre_close, volume FROM {view_tick} as t WHERE code=%s AND trade_date=%s AND trade_time<='09:30:00' ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            trow = c.fetchone()
            if not trow:
                return False, '竞价无数据', {}
            trade_time, price, pre_close, pre_vol = trow[0], trow[1], trow[2], trow[3]
            c.execute(
                f"SELECT vol FROM {view_daily} as t WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM {view_daily} as tt WHERE code=%s AND trade_date<%s)",
                (code, code, tdate),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                pre_ratio = 0.0
            else:
                y_vol = float(yrow[0])
                pre_ratio = 0.0 if y_vol <= 0 else (float(pre_vol) / 100.0) / y_vol
    except Exception as e:
        return False, '竞价数据获取异常', {}
    if pre_close is None or price is None:
        return False, '竞价价格缺失', {}
    rise = (float(price) - float(pre_close)) / float(pre_close)
    if pre_ratio < 0.01:
        return False, f'竞价量能不足，竞价量能占比:{pre_ratio:.2}', {'pre_ratio': pre_ratio}
    if rise > 0.05:
        return False, f'竞价涨幅过大:{rise:.2%}，竞价量能占比:{pre_ratio:.2}', {'rise': rise, 'pre_ratio': pre_ratio}
    return True, '', {'rise': rise, 'pre_close': float(pre_close), 'trade_date': tdate, 'trade_time': trade_time, 'price': float(price), 'pre_ratio': pre_ratio}
