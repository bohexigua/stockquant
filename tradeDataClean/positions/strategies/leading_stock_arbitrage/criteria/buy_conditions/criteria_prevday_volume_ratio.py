def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        from datetime import datetime, time
        if now_dt is None:
            now_dt = datetime.now()
        
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        view_tick = sql_utils.get_subquery_stock_tick(now_dt)
        view_5min = sql_utils.get_subquery_stock_5min(now_dt)

        with strategy.db.cursor() as c:
            now_t = now_dt.time()
            is_trading_day = False
            c.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = %s LIMIT 1", (now_dt.date(),))
            cal_r = c.fetchone()
            if cal_r and int(cal_r[0]) == 1:
                is_trading_day = True

            if now_t >= time(9, 0, 0) and is_trading_day:
                tdate = now_dt.date()
            else:
                c.execute(
                    f"SELECT MAX(trade_date) FROM {view_tick} as t WHERE code=%s AND trade_date<%s",
                    (code, now_dt.date()),
                )
                drow = c.fetchone()
                tdate = drow[0]
            # 当日最新分时的累计量能（tick volume 假设为累计）与对应时间
            c.execute(
                f"SELECT trade_time, volume FROM {view_tick} as t WHERE code=%s AND trade_date=%s ORDER BY trade_time DESC LIMIT 1",
                (code, tdate),
            )
            vrow = c.fetchone()
            if not vrow:
                return False, '当日分时数据缺失', {'ratio': 0.0}
            curr_time, pre_vol = vrow[0], float(vrow[1] or 0.0)
            # 昨日分时量能（5分钟粒度）：选择首个 trade_time >= 当前时间 的bar，取该时点之前的累计量能以对齐口径
            c.execute(
                f"SELECT MAX(trade_date) FROM {view_5min} as t WHERE code=%s AND trade_date<%s",
                (code, tdate),
            )
            pdrow = c.fetchone()
            prev_date = pdrow[0] if pdrow and pdrow[0] else None
            if not prev_date:
                return False, '昨日分时数据缺失', {'ratio': 0.0}
            c.execute(
                f"SELECT MIN(trade_time) FROM {view_5min} as t WHERE code=%s AND trade_date=%s AND trade_time>=%s",
                (code, prev_date, curr_time),
            )
            ntrow = c.fetchone()
            target_time = ntrow[0]
            if not target_time:
                # 若不存在比当前时间更晚的昨日bar，则使用昨日最后一个bar作为比较
                c.execute(
                    f"SELECT MAX(trade_time) FROM {view_5min} as t WHERE code=%s AND trade_date=%s",
                    (code, prev_date),
                )
                ltrow = c.fetchone()
                target_time = ltrow[0]
            c.execute(
                f"SELECT COALESCE(SUM(vol),0) FROM {view_5min} as t WHERE code=%s AND trade_date=%s AND trade_time<=%s",
                (code, prev_date, target_time),
            )
            y_cum_vol = float(c.fetchone()[0] or 0.0)
            if y_cum_vol <= 0:
                return False, '昨日分时量能异常', {'ratio': 0.0}
            ratio_pct = ((pre_vol) / y_cum_vol)
            reason = f"昨量比{'充分' if ratio_pct >= 1.0 else '不足'}:{ratio_pct:.2f};时点:{curr_time}->{target_time}"
            if ratio_pct >= 1.2:
                return True, reason, {'ratio': ratio_pct}
            return False, reason, {'ratio': ratio_pct}
    except Exception:
        return False, '昨量比计算异常', {'ratio': 0.0}
