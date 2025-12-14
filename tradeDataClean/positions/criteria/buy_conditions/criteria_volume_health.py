def check(strategy, code: str, stock_name: str, df, trade_date: str = None):
    try:
        if df is None or df.empty:
            vol_summary = '量能:数据缺失'
            return False, vol_summary, {'trade_date': trade_date, 'vol_summary': vol_summary}
        window = 5
        k = min(window, len(df))
        tail = df.tail(k)
        if len(tail) < 3:
            vol_summary = '量能:数据不足'
            return False, vol_summary, {'trade_date': trade_date, 'vol_summary': vol_summary}
        import pandas as pd
        closes = pd.to_numeric(tail['close'], errors='coerce')
        opens = pd.to_numeric(tail['open'], errors='coerce')
        vols = pd.to_numeric(tail['vol'], errors='coerce')
        if closes.isna().any() or opens.isna().any() or vols.isna().any():
            vol_summary = '量能:数据异常'
            return False, vol_summary, {'trade_date': trade_date, 'vol_summary': vol_summary}
        # 收盘价逐日抬升与量能递减统计
        # 计算末端连续连升天数（以收盘价为准）
        rise_len = 1
        for i in range(len(tail)-1, 0, -1):
            if closes.iloc[i] > closes.iloc[i-1]:
                rise_len += 1
            else:
                break
        if rise_len < 2:
            vol_summary = f"量能:连升{rise_len}日"
            return False, vol_summary, {'trade_date': trade_date, 'vol_summary': vol_summary}
        # 在该连续上涨区间内统计量能放大/缩量天数（相邻比较）
        start_idx = len(tail) - rise_len
        vol_inc = 0
        vol_dec = 0
        for i in range(start_idx, len(tail)):
            if i == 0:
                continue
            if vols.iloc[i] > vols.iloc[i-1]:
                vol_inc += 1
            elif vols.iloc[i] < vols.iloc[i-1]:
                vol_dec += 1
        # 条件放宽规则：
        # 连升3日: 量能≥2日放大，缩量≤1日
        # 连升4日: 量能≥3日放大，缩量≤1日
        # 连升5日: 量能≥3日放大，缩量≤2日
        # 连升2日: 量能≥2日放大
        cond_ok = False
        if rise_len >= 2:
            cond_ok = (vol_inc >= 2 and vol_dec == 0)
        if rise_len >= 5:
            cond_ok = (vol_inc >= 3 and vol_dec <= 2)
        elif rise_len >= 4:
            cond_ok = (vol_inc >= 3 and vol_dec <= 1)
        elif rise_len >= 3:
            cond_ok = (vol_inc >= 2 and vol_dec <= 1)
        if not cond_ok:
            base_summary = f"量能:连升{rise_len}日,放大{vol_inc}天,缩量{vol_dec}天"
            return False, base_summary, {
                'trade_date': trade_date,
                'rise_len': rise_len,
                'vol_inc': vol_inc,
                'vol_dec': vol_dec,
                'vol_summary': base_summary,
            }
        # 回溯最近的强阳线（涨幅≥4%）
        big_idx = None
        for i in range(len(tail)-1, -1, -1):
            o = opens.iloc[i]
            c = closes.iloc[i]
            if o is None or c is None or o <= 0:
                continue
            if c > o and (c - o) / o >= 0.04:
                big_idx = i
                break
        # 强阳线开盘价作为“台阶底部”，最新收盘不应跌破
        support_ok = True
        if big_idx is not None:
            last_close = closes.iloc[-1]
            bottom = opens.iloc[big_idx]
            support_ok = bool(last_close >= bottom)
            if not support_ok:
                vol_summary = f"量能:连升{rise_len}日,放大{vol_inc}天,缩量{vol_dec}天;强阳支撑:无"
                return False, vol_summary, {
                    'trade_date': trade_date,
                    'rise_len': rise_len,
                    'vol_inc': vol_inc,
                    'vol_dec': vol_dec,
                    'support_ok': support_ok,
                    'vol_summary': vol_summary,
                }
        vol_summary = f"量能:连升{rise_len}日,放大{vol_inc}天,缩量{vol_dec}天;强阳支撑:{'有效' if support_ok else '无'}"
        return True, vol_summary, {
            'trade_date': trade_date,
            'rise_len': rise_len,
            'vol_inc': vol_inc,
            'vol_dec': vol_dec,
            'support_ok': support_ok,
            'vol_summary': vol_summary,
        }
    except Exception:
        vol_summary = '量能:计算异常'
        return False, vol_summary, {'trade_date': trade_date, 'vol_summary': vol_summary}
