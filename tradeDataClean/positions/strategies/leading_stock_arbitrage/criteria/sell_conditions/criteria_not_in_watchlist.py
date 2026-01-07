from .common import calc_volume_ratio, is_limit_up

def check(strategy, code: str, stock_name: str, now_dt=None):
    """
    卖出条件1: 不在自选列表中
    - 当日开盘是涨的且非涨停则立即卖出
    - 否则当日量比开始<=0.8时则立即卖出
    """
    try:
        # 1. 检查是否在自选股
        with strategy.db.cursor() as c:
            c.execute("SELECT count(*) FROM ptm_user_watchlist WHERE stock_code=%s AND is_active=1", (code,))
            r = c.fetchone()
            if r and r[0] > 0:
                return False, '在自选股中', {}

        # 2. 获取行情和量比
        ok, ratio, msg, data = calc_volume_ratio(strategy, code, now_dt)
        if not ok:
            return False, f'数据获取失败:{msg}', {}
        
        price = data['price']
        pre_close = data['pre_close']
        open_price = data['open']
        
        # 3. 判断条件
        # 当日开盘是涨的 (Open > PreClose) 且非涨停
        is_limit = is_limit_up(price, pre_close, code, stock_name)
        if open_price > pre_close and not is_limit:
            return True, f'不在自选且高开非涨停(Open:{open_price}>Pre:{pre_close}, Price:{price})', data
        
        # 否则当日量比<=0.8
        if ratio <= 0.8:
            return True, f'不在自选且量比低({ratio:.2f}<=0.8)', data
            
        return False, '未触发卖出条件', data

    except Exception as e:
        return False, f'策略异常:{str(e)}', {}
