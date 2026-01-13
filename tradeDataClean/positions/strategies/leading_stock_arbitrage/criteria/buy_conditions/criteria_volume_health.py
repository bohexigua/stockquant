def check(strategy, code: str, stock_name: str, now_dt=None):
    try:
        from datetime import datetime
        if now_dt is None:
            now_dt = datetime.now()
        from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
        view_daily = sql_utils.get_subquery_stock_daily(now_dt)
        # 获取最近5条日线数据
        with strategy.db.cursor() as c:
            c.execute(
                f"SELECT open, close, vol FROM {view_daily} as t WHERE code=%s ORDER BY trade_date DESC LIMIT 5",
                (code,),
            )
            rows = c.fetchall()
            if not rows:
                vol_summary = '量能:数据缺失'
                return False, vol_summary, {'vol_summary': vol_summary}
            
            # rows 是倒序的 (最新在前)，转为正序处理
            rows = list(reversed(rows))
            import pandas as pd
            df = pd.DataFrame(rows, columns=['open', 'close', 'vol'])
        
        if df.empty:
            vol_summary = '量能:数据缺失'
            return False, vol_summary, {'vol_summary': vol_summary}
        
        k = len(df)
        if k < 3:
            vol_summary = '量能:数据不足'
            return False, vol_summary, {'vol_summary': vol_summary}
            
        closes = pd.to_numeric(df['close'], errors='coerce')
        opens = pd.to_numeric(df['open'], errors='coerce')
        vols = pd.to_numeric(df['vol'], errors='coerce')
        
        if closes.isna().any() or opens.isna().any() or vols.isna().any():
            vol_summary = '量能:数据异常'
            return False, vol_summary, {'vol_summary': vol_summary}
            
        # 连续5个交易日观察量能放大/缩量情况（相邻比较）
        vol_inc = 0
        vol_dec = 0
        for i in range(1, len(df)):
            if vols.iloc[i] > vols.iloc[i-1]:
                vol_inc += 1
            elif vols.iloc[i] < vols.iloc[i-1] * 0.88:
                vol_dec += 1

        # 判定规则：5日窗口中放大≥2天，且缩量≤2天
        cond_ok = (vol_dec <= 2) and (vol_inc >= 2)
            
        if not cond_ok:
            base_summary = f"量能:放大{vol_inc}天,缩量{vol_dec}天"
            return False, base_summary, {
                'vol_inc': vol_inc,
                'vol_dec': vol_dec,
                'vol_summary': base_summary,
            }
            
        # 回溯最近的强阳线（涨幅≥7%）
        big_idx = None
        for i in range(len(df)-1, -1, -1):
            o = opens.iloc[i]
            c = closes.iloc[i]
            if o is None or c is None or o <= 0:
                continue
            if c > o and (c - o) / o >= 0.07:
                big_idx = i
                break
                
        # 强阳线开盘价作为“台阶底部”，最新收盘不应跌破
        support_ok = True
        if big_idx is not None:
            last_close = closes.iloc[-1]
            bottom = opens.iloc[big_idx]
            support_ok = bool(last_close >= bottom)
            if not support_ok:
                vol_summary = f"量能:放大{vol_inc}天,缩量{vol_dec}天;强阳支撑:无"
                return False, vol_summary, {
                    'vol_inc': vol_inc,
                    'vol_dec': vol_dec,
                    'support_ok': support_ok,
                    'vol_summary': vol_summary,
                }
        
        vol_summary = f"量能:放大{vol_inc}天,缩量{vol_dec}天;强阳支撑:{'有效' if support_ok else '无'}"
        return True, vol_summary, {
            'vol_inc': vol_inc,
            'vol_dec': vol_dec,
            'support_ok': support_ok,
            'vol_summary': vol_summary,
        }
    except Exception as e:
        vol_summary = '量能:计算异常'
        print(f'{stock_name} {code} {vol_summary}: {e}')
        return False, vol_summary, {'vol_summary': vol_summary}
