def compute_pct(change5: float):
    if change5 <= 0:
        return 0.0
    if change5 < 0.05:
        return 0.2
    if change5 < 0.10:
        return 0.4
    if change5 < 0.15:
        return 0.6
    return 0.8

def check(strategy, code: str, stock_name: str, df, cash_before: float, price: float, rise: float, pre_ratio: float):
    # compute five day change from df
    import pandas as pd
    if df is None or df.empty or len(df) < 5:
        change5 = 0.0
    else:
        start_open = pd.to_numeric(df.iloc[-5]['open'], errors='coerce')
        end_close = pd.to_numeric(df.iloc[-1]['close'], errors='coerce')
        if pd.isna(start_open) or pd.isna(end_close) or float(start_open) <= 0:
            change5 = 0.0
        else:
            change5 = float((float(end_close) - float(start_open)) / float(start_open))
    pct = compute_pct(change5)
    target_value = pct * cash_before
    qty_units = int(target_value // max(price, 0.01))
    qty_to_buy = (qty_units // 100) * 100
    if qty_to_buy < 100:
        return False, f'买入数量为0，近5日涨幅:{change5:.2%}，仓位:{pct:.0%}，竞价涨幅:{rise:.2%}，竞价量能占比:{pre_ratio:.2}', {
            'change5': change5,
            'pct': pct,
            'qty_to_buy': qty_to_buy,
        }
    return True, '', {
        'change5': change5,
        'pct': pct,
        'qty_to_buy': qty_to_buy,
    }
