def check(strategy, code: str, stock_name: str):
    try:
        with strategy.db.cursor() as c:
            c.execute(
                "SELECT all_themes_name, trade_date FROM trade_factor_most_related_theme WHERE stock_code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_factor_most_related_theme WHERE stock_code=%s AND trade_date<=CURDATE())",
                (code, code),
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
                    "INNER JOIN (SELECT stock_code, MAX(trade_date) AS max_date FROM trade_factor_most_related_theme GROUP BY stock_code) m "
                    "ON t.stock_code=m.stock_code AND t.trade_date=m.max_date "
                    "WHERE t.all_themes_name LIKE %s",
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
                max_rise = 0.0
                names = []
                for peer in peers:
                    rise = strategy.ds.peer_preopen_rise(peer)
                    if rise is None:
                        continue
                    if rise >= 0.095:
                        strong += 1
                        nm = strategy.ds.get_stock_name(peer)
                        names.append(nm)
                    if rise > max_rise:
                        max_rise = rise
                return strong, max_rise, names
            strong1, max1, names1 = _count(peers_t1)
            strong2, max2, names2 = _count(peers_t2)
            max_peer_rise = max(max1, max2)
            strong_total = strong1 + strong2
            if strong_total <= 0:
                return False, '同板块无强势股', {}
            return True, '', {
                'strong_count': strong_total,
                'max_peer_rise': max_peer_rise,
                'theme1': theme1,
                'strong1': strong1,
                'theme2': theme2,
                'strong2': strong2,
                'names1': names1,
                'names2': names2,
            }
    except Exception:
        return False, '同板块无强势股', {}
