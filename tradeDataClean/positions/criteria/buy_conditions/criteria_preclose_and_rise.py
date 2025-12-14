def check(strategy, code: str, stock_name: str, tick):
    trade_dt, price, pre_close = tick
    if pre_close is None:
        return False, '昨收缺失', {}
    pre_ratio = strategy.get_preopen_volume_ratio(code)
    if pre_ratio < 0.01:
        return False, f'竞价量能不足，竞价量能占比:{pre_ratio:.2}', {'pre_ratio': pre_ratio}
    rise = (price - pre_close) / pre_close
    if rise > 0.05:
        return False, f'竞价涨幅过大:{rise:.2%}，竞价量能占比:{pre_ratio:.2}', {'rise': rise, 'pre_ratio': pre_ratio}
    return True, '', {'rise': rise, 'pre_close': pre_close, 'trade_dt': trade_dt, 'price': price, 'pre_ratio': pre_ratio}
