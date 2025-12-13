def check(strategy, code: str, stock_name: str):
    pre_ratio = strategy.ds.get_preopen_volume_ratio(code)
    if pre_ratio < 0.01:
        return False, f'竞价量能不足，竞价量能占比:{pre_ratio:.2}', {'pre_ratio': pre_ratio}
    return True, '', {'pre_ratio': pre_ratio}
