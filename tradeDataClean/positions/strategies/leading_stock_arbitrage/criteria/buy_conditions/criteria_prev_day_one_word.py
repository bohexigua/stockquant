def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        from datetime import datetime
        if now_dt is None:
            now_dt = datetime.now()
        
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        view_daily = sql_utils.get_subquery_stock_daily(now_dt)

        with strategy.db.cursor() as c:
            c.execute(
                f"SELECT MAX(trade_date) FROM {view_daily} as t WHERE code=%s AND trade_date<%s",
                (code, now_dt.date()),
            )
            drow = c.fetchone()
            trade_date = drow[0]

            c.execute(
                f"SELECT high, low FROM {view_daily} as t WHERE code=%s AND trade_date=%s",
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
