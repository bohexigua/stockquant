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
                "SELECT all_themes_name, trade_date FROM trade_factor_most_related_theme WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 1",
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
                    "SELECT t.stock_code FROM trade_factor_most_related_theme t "
                    "WHERE t.all_themes_name LIKE %s "
                    "AND t.trade_date = (SELECT MAX(tt.trade_date) FROM trade_factor_most_related_theme tt WHERE tt.stock_code=t.stock_code)",
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
                    # 参考 tick 数据源：取竞价末条（<=10:15）价格与昨收
                    c.execute(
                        "SELECT trade_date, trade_time, price, pre_close, name FROM trade_market_stock_tick "
                        "WHERE code=%s AND trade_date=%s AND trade_time<='11:00:00' "
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
            if strong_total <= 0:
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
    except Exception:
        return False, '同板块无强势股', {}
