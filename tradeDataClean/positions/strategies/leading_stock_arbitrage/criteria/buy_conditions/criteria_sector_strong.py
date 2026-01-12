def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        from datetime import datetime, time
        if now_dt is None:
            now_dt = datetime.now()
        
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        view_tick = sql_utils.get_subquery_stock_tick(now_dt)
        view_theme = sql_utils.get_subquery_related_theme(now_dt)

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

            c.execute(
                f"SELECT all_themes_name, trade_date FROM {view_theme} as t WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 1",
                (code,),
            )
            trow = c.fetchone()
            if not trow or not trow[0]:
                return False, '同板块无强势股', {}
            raw = [x.strip() for x in str(trow[0]).split(',') if x and x.strip()]
            theme1 = raw[0] if len(raw) > 0 else ''
            theme2 = raw[1] if len(raw) > 1 else ''
            def _get_peers(theme: str):
                if not theme:
                    return set()
                like = f"%{theme}%"
                c.execute(
                    f"SELECT t.stock_code FROM {view_theme} as t "
                    f"WHERE t.all_themes_name LIKE %s "
                    f"AND t.trade_date = (SELECT MAX(tt.trade_date) FROM {view_theme} as tt WHERE tt.stock_code=t.stock_code)",
                    (like,),
                )
                rows = c.fetchall()
                ps = set()
                for r in rows:
                    if r and r[0] and r[0] != code:
                        ps.add(r[0])
                return ps
            peers_t1 = _get_peers(theme1)
            peers_t2 = _get_peers(theme2)
            def _count(peers: set):
                strong = 0
                items = []
                for peer in peers:
                    # 参考 tick 数据源：取竞价末条（<=当前时间）价格与昨收
                    c.execute(
                        f"SELECT trade_date, trade_time, price, pre_close, name FROM {view_tick} as t "
                        "WHERE code=%s AND trade_date=%s "
                        "ORDER BY trade_time DESC LIMIT 1",
                        (peer, tdate),
                    )
                    kt = c.fetchone()
                    if not kt:
                        continue
                    try:
                        price = float(kt[2]) if kt[2] is not None else None
                    except Exception:
                        price = None
                    try:
                        pre_close = float(kt[3]) if kt[3] is not None else None
                    except Exception:
                        pre_close = None
                    nm = str(kt[4]) if len(kt) > 4 and kt[4] else peer
                    if price is None or pre_close is None or pre_close <= 0:
                        continue
                    rise = (price - pre_close) / pre_close
                    items.append({'name': nm, 'rise': rise})
                    if rise >= 0.095:
                        strong += 1
                items.sort(key=lambda x: x['rise'], reverse=True)
                return strong, items
            strong1, peers1 = _count(peers_t1)
            strong2, peers2 = _count(peers_t2)
            strong_total = strong1 + strong2
            # 判定条件修改：第一个板块至少1个强势股，或者第二个板块至少2个强势股
            if not (strong1 >= 1 or strong2 >= 2):
                return False, '同板块无强势股', {
                    'strong_count': strong_total,
                    'theme1': theme1,
                    'strong1': strong1,
                    'theme2': theme2,
                    'strong2': strong2,
                    'peers1': peers1,
                    'peers2': peers2,
                    'trade_date': tdate,
                }
            return True, '', {
                'strong_count': strong_total,
                'theme1': theme1,
                'strong1': strong1,
                'theme2': theme2,
                'strong2': strong2,
                'peers1': peers1,
                'peers2': peers2,
                'trade_date': tdate,
            }
    except Exception as e:
        print(f'获取板块数据异常: {e}')
        return False, '同板块无强势股', {}
