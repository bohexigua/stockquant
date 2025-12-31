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
                "SELECT trade_time, price, pre_close, volume FROM trade_market_stock_tick WHERE code=%s AND trade_date=%s AND trade_time<='10:30:00' ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            row = c.fetchone()
            if not row:
                return False, '10:30前无分时', {}
            trade_time, price, pre_close, vol = row[0], row[1], row[2], row[3]
            if price is None or pre_close is None or pre_close <= 0:
                return False, '价格缺失', {}
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<%s)",
                (code, code, tdate),
            )
            yrow = c.fetchone()
            y_vol = float(yrow[0]) if yrow and yrow[0] is not None else 0.0
            pre_ratio = 0.0 if y_vol <= 0 else (float(vol or 0.0) / 100.0) / y_vol
            rise = (float(price) - float(pre_close)) / float(pre_close)
            reason = f"10:30前跌幅:{rise:.2%};竞价量能:{pre_ratio:.2}"
            if (trade_time and str(trade_time) <= '10:30:00') and (rise <= -0.03) and (pre_ratio < 0.01):
                return True, reason, {'trade_date': tdate, 'trade_time': trade_time, 'price': float(price), 'rise': rise, 'pre_ratio': pre_ratio}
            return False, reason, {'trade_date': tdate, 'trade_time': trade_time, 'price': float(price), 'rise': rise, 'pre_ratio': pre_ratio}
    except Exception:
        return False, '卖出条件计算异常', {}
