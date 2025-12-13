import os
import sys
from datetime import datetime, timedelta
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class BuyStrategy:
    def __init__(self, db, data_source=None):
        self.db = db
        if data_source is None:
            from tradeDataClean.positions.data_source import TickPreopenDataSource
            self.ds = TickPreopenDataSource(db)
        else:
            self.ds = data_source
    def write_strategy_evaluation(self, code: str, stock_name: str, decision_side: str, will_execute: int, summary: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "DELETE FROM ptm_quant_strategy_evaluations WHERE trade_date=CURDATE() AND stock_code=%s AND strategy_name=%s AND decision_side=%s AND HOUR(eval_time)=HOUR(NOW())",
                    (code, 'BuyStrategy', decision_side),
                )
                c.execute(
                    "INSERT INTO ptm_quant_strategy_evaluations (trade_date, stock_code, stock_name, strategy_name, decision_side, will_execute, summary) "
                    "VALUES (CURDATE(), %s, %s, %s, %s, %s, %s)",
                    (code, stock_name, 'BuyStrategy', decision_side, will_execute, summary),
                )
        except Exception:
            pass

    def get_daily_recent(self, code: str, days: int = 14) -> pd.DataFrame:
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_date, open, high, low, close, pre_close, vol, amount, chg_val, chg_pct FROM trade_market_stock_daily WHERE code=%s ORDER BY trade_date DESC LIMIT %s",
                (code, days),
            )
            rows = c.fetchall()
            cols = ['trade_date','open','high','low','close','pre_close','vol','amount','chg_val','chg_pct']
            df = pd.DataFrame(rows, columns=cols)
            return df[::-1]

    def get_tick_preopen(self, code: str):
        return self.ds.get_preopen_info(code)

    def preopen_volume_ratio_ok(self, code: str) -> bool:
        ratio = self.ds.get_preopen_volume_ratio(code)
        return ratio >= 0.01

    def get_preopen_volume_ratio(self, code: str) -> float:
        return self.ds.get_preopen_volume_ratio(code)


    def decide_buy(self, code: str, cash_before: float, stock_name: str):
        df = self.get_daily_recent(code)
        from tradeDataClean.positions.criteria.buy_conditions.criteria_has_position import check as c_has_pos
        ok, reason, _ = c_has_pos(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        if df.empty:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '无日线数据')
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_prev_day_one_word import check as c_one_word
        ok, reason, _ = c_one_word(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_prev_day_main_lift import check as c_main_lift
        ok, reason, _ = c_main_lift(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_sector_strong import check as c_sector
        ok, reason, sec = c_sector(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_volume_health import check as c_vol
        ok, reason, _ = c_vol(self, code, stock_name, df)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_tick_available import check as c_tick
        ok, reason, tkv = c_tick(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        tick = tkv['tick']
        from tradeDataClean.positions.criteria.buy_conditions.criteria_preopen_volume import check as c_prevol
        ok, reason, pv = c_prevol(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_preclose_and_rise import check as c_rise
        ok, reason, rv = c_rise(self, code, stock_name, tick)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_qty_to_buy import check as c_qty
        ok, reason, qv = c_qty(self, code, stock_name, df, cash_before, rv['price'], rv['rise'], pv['pre_ratio'])
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        pre_ratio = self.get_preopen_volume_ratio(code)
        list1 = ','.join(sec['names1']) if sec['names1'] else '无'
        list2 = ','.join(sec['names2']) if sec['names2'] else '无'
        reason = f"梯队({sec['theme1']})强势:{sec['strong1']}只[{list1}];梯队({sec['theme2']})强势:{sec['strong2']}只[{list2}];梯队最大涨幅:{sec['max_peer_rise']:.2%};量能健康;昨主力拉升;竞价量能:{pre_ratio:.2}≥昨量0.01;竞价涨幅:{rv['rise']:.2%}≤5%;近5日涨幅:{qv['change5']:.2%},仓位:{qv['pct']:.0%}"
        self.write_strategy_evaluation(code, stock_name, 'BUY', 1, reason)
        return rv['trade_dt'], rv['price'], qv['qty_to_buy'], reason
