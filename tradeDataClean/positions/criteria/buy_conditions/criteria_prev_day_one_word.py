def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE()",
                (code,),
            )
            drow = c.fetchone()
            trade_date = drow[0]

            c.execute(
                "SELECT high, low FROM trade_market_stock_daily WHERE code=%s AND trade_date=%s",
                (code, trade_date),
            )
            r = c.fetchone()
            if not r:
                return True, '', {'trade_date': trade_date}
            high, low = r
            if high is None or low is None:
                return True, '', {'trade_date': trade_date}
            if float(high) == float(low):
                return False, '前一日一字板', {'trade_date': trade_date}
            return True, '', {'trade_date': trade_date}
    except Exception:
        return True, '', {'trade_date': None}
