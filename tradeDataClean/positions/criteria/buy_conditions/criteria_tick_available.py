def check(strategy, code: str, stock_name: str):
    tick = strategy.ds.get_preopen_info(code)
    if tick is None:
        return False, '竞价无数据', {}
    return True, '', {'tick': tick}
