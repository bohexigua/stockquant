def check(strategy, code: str, stock_name: str, df):
    try:
        if df is None or df.empty:
            return False, '量能不健康', {}
        window = 5
        k = min(window, len(df))
        tail = df.tail(k)
        if len(tail) < 3:
            return False, '量能不健康', {}
        import pandas as pd
        closes = pd.to_numeric(tail['close'], errors='coerce')
        opens = pd.to_numeric(tail['open'], errors='coerce')
        vols = pd.to_numeric(tail['vol'], errors='coerce')
        if closes.isna().any() or opens.isna().any() or vols.isna().any():
            return False, '量能不健康', {}
        up_all = True
        dec_cnt = 0
        for i in range(1, len(tail)):
            if not (closes.iloc[i] > closes.iloc[i-1]):
                up_all = False
                break
            if vols.iloc[i] < vols.iloc[i-1]:
                dec_cnt += 1
        if not up_all:
            return False, '量能不健康', {}
        if dec_cnt > 1:
            return False, '量能不健康', {}
        big_idx = None
        for i in range(len(tail)-1, -1, -1):
            o = opens.iloc[i]
            c = closes.iloc[i]
            if o is None or c is None or o <= 0:
                continue
            if c > o and (c - o) / o >= 0.04:
                big_idx = i
                break
        if big_idx is not None:
            last_close = closes.iloc[-1]
            bottom = opens.iloc[big_idx]
            if not (last_close >= bottom):
                return False, '量能不健康', {}
        return True, '', {}
    except Exception:
        return False, '量能不健康', {}
