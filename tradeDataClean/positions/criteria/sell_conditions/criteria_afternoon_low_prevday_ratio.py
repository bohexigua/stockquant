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
                "SELECT trade_time, price, volume FROM trade_market_stock_tick WHERE code=%s AND trade_date=%s AND trade_time<='14:00:00' ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            vrow = c.fetchone()
            if not vrow:
                return False, '14:00前无分时', {'ratio': 0.0}
            curr_time, price, vol = vrow[0], vrow[1], float(vrow[2] or 0.0)
            c.execute(
                "SELECT MAX(trade_date) FROM trade_market_stock_5min WHERE code=%s AND trade_date<%s",
                (code, tdate),
            )
            pdrow = c.fetchone()
            prev_date = pdrow[0] if pdrow and pdrow[0] else None
            if not prev_date:
                return False, '昨日分时缺失', {'ratio': 0.0}
            c.execute(
                "SELECT MIN(trade_time) FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s AND trade_time>=%s",
                (code, prev_date, curr_time),
            )
            ntrow = c.fetchone()
            target_time = ntrow[0]
            if not target_time:
                c.execute(
                    "SELECT MAX(trade_time) FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s",
                    (code, prev_date),
                )
                ltrow = c.fetchone()
                target_time = ltrow[0]
            c.execute(
                "SELECT COALESCE(SUM(vol),0) FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s AND trade_time<=%s",
                (code, prev_date, target_time),
            )
            y_cum_vol = float(c.fetchone()[0] or 0.0)
            if y_cum_vol <= 0:
                return False, '昨日量能异常', {'ratio': 0.0}
            ratio = vol / y_cum_vol if y_cum_vol > 0 else 0.0
            reason = f"昨量比:{ratio:.2f};时点:{curr_time}->{target_time}"
            if (curr_time and str(curr_time) <= '14:00:00') and (ratio <= 0.30):
                return True, reason, {'ratio': ratio, 'trade_date': tdate, 'trade_time': curr_time, 'price': float(price) if price is not None else None}
            return False, reason, {'ratio': ratio, 'trade_date': tdate, 'trade_time': curr_time, 'price': float(price) if price is not None else None}
    except Exception:
        return False, '卖出条件计算异常', {'ratio': 0.0}
