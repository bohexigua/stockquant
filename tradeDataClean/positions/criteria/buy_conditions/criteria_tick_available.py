def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT trade_date, trade_time, price FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<='10:15:00' ORDER BY trade_time DESC LIMIT 1",
                (code,),
            )
            trow = c.fetchone()
            if not trow:
                return False, '竞价无数据', {}
            tdate, ttime, price = trow
            from datetime import datetime, timedelta
            if isinstance(ttime, timedelta):
                ttime = (datetime.min + ttime).time()
            elif isinstance(ttime, datetime):
                ttime = ttime.time()
            elif isinstance(ttime, str):
                try:
                    ttime = datetime.strptime(ttime, "%H:%M:%S").time()
                except ValueError:
                    ttime = datetime.strptime(ttime, "%H:%M").time()
            c.execute(
                "SELECT pre_close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE())",
                (code, code),
            )
            prow = c.fetchone()
            pre_close = float(prow[0]) if prow and prow[0] is not None else None
            trade_dt = datetime.combine(tdate, ttime)
            return True, '', {'tick': (trade_dt, float(price), pre_close)}
    except Exception:
        return False, '竞价数据获取异常', {}
