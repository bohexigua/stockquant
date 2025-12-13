def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT high, low FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE())",
                (code, code),
            )
            r = c.fetchone()
            if not r:
                return True, '', {}
            high, low = r
            if high is None or low is None:
                return True, '', {}
            if float(high) == float(low):
                return False, '前一日一字板', {}
            return True, '', {}
    except Exception:
        return True, '', {}
