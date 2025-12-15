def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            from datetime import datetime, time
            now_t = datetime.now().time()
            if now_t >= time(9, 0, 0):
                c.execute("SELECT CURDATE()")
                drow = c.fetchone()
                tdate = drow[0]
            else:
                c.execute(
                    "SELECT MAX(trade_date) FROM trade_market_stock_tick WHERE code=%s AND trade_date<CURDATE()",
                    (code,),
                )
                drow = c.fetchone()
                tdate = drow[0]
            c.execute(
                "SELECT trade_time, price, pre_close, volume FROM trade_market_stock_tick WHERE code=%s AND trade_date=%s AND trade_time<='10:15:00' ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            trow = c.fetchone()
            if not trow:
                return False, '竞价无数据', {}
            trade_time, price, pre_close, pre_vol = trow[0], trow[1], trow[2], trow[3]
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<%s)",
                (code, code, tdate),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                pre_ratio = 0.0
            else:
                y_vol = float(yrow[0])
                pre_ratio = 0.0 if y_vol <= 0 else (float(pre_vol) / 100.0) / y_vol
    except Exception:
        return False, '竞价数据获取异常', {}
    if pre_close is None or price is None:
        return False, '竞价价格缺失', {}
    rise = (float(price) - float(pre_close)) / float(pre_close)
    if pre_ratio < 0.01:
        return False, f'竞价量能不足，竞价量能占比:{pre_ratio:.2}', {'pre_ratio': pre_ratio}
    if rise > 0.05:
        return False, f'竞价涨幅过大:{rise:.2%}，竞价量能占比:{pre_ratio:.2}', {'rise': rise, 'pre_ratio': pre_ratio}
    return True, '', {'rise': rise, 'pre_close': float(pre_close), 'trade_date': tdate, 'trade_time': trade_time, 'price': float(price), 'pre_ratio': pre_ratio}
