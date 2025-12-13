def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT position_qty_after FROM ptm_quant_positions WHERE stock_code=%s ORDER BY trade_date DESC, trade_time DESC LIMIT 1",
                (code,),
            )
            r = c.fetchone()
            qty = int(r[0]) if r and r[0] is not None else 0
            if qty > 0:
                return False, '已持仓，跳过买入', {'position_qty_after': qty}
            return True, '', {'position_qty_after': qty}
    except Exception:
        return True, '', {}
