def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT COUNT(*) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date<CURDATE()) AND main_action='主力拉升'",
                (code, code),
            )
            r = c.fetchone()
            cnt = int(r[0]) if r and r[0] is not None else 0
            if cnt > 0:
                return True, '', {'main_lift': True, 'lift_count': cnt}
            return False, '前一日无主力拉升', {'main_lift': False, 'lift_count': 0}
    except Exception:
        return False, '前一日无主力拉升', {'main_lift': False, 'lift_count': 0}
