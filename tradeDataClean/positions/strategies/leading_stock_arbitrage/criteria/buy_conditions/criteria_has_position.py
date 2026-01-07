def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        from datetime import datetime
        if now_dt is None:
            now_dt = datetime.now()
                
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        subquery_positions = sql_utils.get_subquery_positions()

        with strategy.db.cursor() as c:
            # 增加时间限制，只查询当前时间之前的持仓记录
            c.execute(
                f"SELECT qty, created_time FROM {subquery_positions} as t WHERE stock_code=%s ORDER BY created_time DESC LIMIT 1",
                (code,),
            )
            r = c.fetchone()
            qty = int(r[0]) if r and r[0] is not None else 0
            if qty > 0:
                # 如果持仓更新时间早于今日（即昨日或更早买入），允许今日继续买入
                created_time = r[1]
                if created_time and hasattr(created_time, 'date') and created_time.date() < now_dt.date():
                    return True, '', {'position_qty_after': qty}
                return False, '已持仓，跳过买入', {'position_qty_after': qty}
            return True, '', {'position_qty_after': qty}
    except Exception:
        return True, '', {}
