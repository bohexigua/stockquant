from .common import calc_volume_ratio

def check(strategy, code: str, stock_name: str, now_dt=None):
    """
    卖出条件3: 当日开始下跌且当日量比<0.5时则立即卖出
    """
    try:
        ok, ratio, msg, data = calc_volume_ratio(strategy, code, now_dt)
        if not ok:
            return False, f'数据获取失败:{msg}', {}
        
        price = data['price']
        pre_close = data['pre_close']
        
        # 当日开始下跌 (Price < PreClose)
        if price < pre_close and ratio < 0.5:
            return True, f'下跌且量比极低(Price:{price}<Pre:{pre_close}, Ratio:{ratio:.2f}<0.5)', data
            
        return False, '未触发卖出条件', data

    except Exception as e:
        return False, f'策略异常:{str(e)}', {}
