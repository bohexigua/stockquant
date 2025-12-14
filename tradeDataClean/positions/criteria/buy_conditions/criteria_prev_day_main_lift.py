def check(strategy, code: str, stock_name: str, trade_date: str = None):
    try:
        with strategy.db.cursor() as c:
            # 主力拉升计数
            c.execute(
                "SELECT COUNT(*) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=%s AND main_action='主力拉升'",
                (code, trade_date),
            )
            r1 = c.fetchone()
            lift_cnt = int(r1[0]) if r1 and r1[0] is not None else 0

            # 主力出货计数
            c.execute(
                "SELECT COUNT(*) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=%s AND main_action='主力出货'",
                (code, trade_date),
            )
            r2 = c.fetchone()
            dump_cnt = int(r2[0]) if r2 and r2[0] is not None else 0

            # 比较“主力拉升”与“主力出货”的最后发生时刻，决定最后一次是在做拉升还是出货
            c.execute(
                "SELECT MAX(trade_time) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=%s AND main_action='主力拉升'",
                (code, trade_date),
            )
            lt_row = c.fetchone()
            last_lift_time = lt_row[0] if lt_row and lt_row[0] is not None else None

            c.execute(
                "SELECT MAX(trade_time) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=%s AND main_action='主力出货'",
                (code, trade_date),
            )
            dt_row = c.fetchone()
            last_dump_time = dt_row[0] if dt_row and dt_row[0] is not None else None

            def _to_sec(t):
                try:
                    if t is None:
                        return None
                    if hasattr(t, 'hour'):
                        return t.hour*3600 + t.minute*60 + t.second
                    from datetime import timedelta as _td
                    if isinstance(t, _td):
                        total = t.total_seconds()
                        return int(total)
                    return None
                except Exception:
                    return None

            ls = _to_sec(last_lift_time)
            ds = _to_sec(last_dump_time)
            if ls is None and ds is None:
                last_action = None
            elif ls is None:
                last_action = '主力出货'
            elif ds is None:
                last_action = '主力拉升'
            else:
                last_action = '主力拉升' if ls >= ds else '主力出货'

            net_lift = lift_cnt - dump_cnt

            data = {
                'main_lift': lift_cnt > 0,
                'lift_count': lift_cnt,
                'main_dump': dump_cnt > 0,
                'dump_count': dump_cnt,
                'net_lift': net_lift,
                'last_action': last_action,
                'last_lift_time': last_lift_time,
                'last_dump_time': last_dump_time,
            }
            # True/False 以当日最后一次主力动作判定：最后一次为拉升则 True，否则 False
            def _fmt_time(t):
                try:
                    if t is None:
                        return '-'
                    if hasattr(t, 'strftime'):
                        return t.strftime('%H:%M:%S')
                    return str(t)
                except Exception:
                    return '-'
            dd = trade_date or '当日'
            reason = f"{dd}主力: 拉升{lift_cnt}次/出货{dump_cnt}次, 净拉升{net_lift}; 最后动作:{last_action or '无'}; 最后拉升:{_fmt_time(last_lift_time)} 最后出货:{_fmt_time(last_dump_time)}"
            if net_lift >= 0:
                return True, reason, data
            return False, reason, data
    except Exception:
        return False, '主力数据获取异常', {'main_lift': False, 'lift_count': 0, 'main_dump': False, 'dump_count': 0, 'net_lift': 0, 'last_action': None}
