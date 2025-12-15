import os
import sys
from datetime import datetime, timedelta
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class BuyStrategy:
    def __init__(self, db):
        self.db = db
    def _calc_layers_and_qty(self, sec: dict, cash_before: float, price: float):
        strong_total = 0
        try:
            strong_total = int(sec.get('strong_count') or (sec.get('strong1', 0) + sec.get('strong2', 0)))
        except Exception:
            strong_total = (sec.get('strong1', 0) + sec.get('strong2', 0))
        if strong_total >= 7:
            layers = 5
        elif strong_total >= 5:
            layers = 4
        elif strong_total >= 3:
            layers = 3
        else:
            layers = 2
        pct = layers / 10.0
        qty_to_buy = int((cash_before * pct) // max(price, 0.01) // 100) * 100
        return layers, pct, qty_to_buy

    def _get_prev_trade_date(self, code: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE()",
                    (code,),
                )
                r = c.fetchone()
                return r[0].strftime('%Y-%m-%d') if r and r[0] else None
        except Exception:
            return None
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
        prev_date = self._get_prev_trade_date(code)
        from tradeDataClean.positions.criteria.buy_conditions.criteria_prev_day_one_word import check as c_one_word
        ok, reason, _ = c_one_word(self, code, stock_name, prev_date)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_prev_day_main_lift import check as c_main_lift
        ok, main_lift_reason, _ = c_main_lift(self, code, stock_name, prev_date)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, main_lift_reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_sector_strong import check as c_sector
        ok, reason, sec = c_sector(self, code, stock_name, prev_date)
        peers1 = sec.get('peers1') or []
        peers2 = sec.get('peers2') or []
        list1 = ','.join([f"{p['name']}:{p['rise']:.2%}" for p in peers1[:5]]) if peers1 else '无'
        list2 = ','.join([f"{p['name']}:{p['rise']:.2%}" for p in peers2[:5]]) if peers2 else '无'
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_volume_health import check as c_vol
        ok, reason, vol_data = c_vol(self, code, stock_name, df, prev_date)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_prevday_volume_ratio import check as c_vr
        ok, reason, vr = c_vr(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        from tradeDataClean.positions.criteria.buy_conditions.criteria_preclose_and_rise import check as c_rise
        ok, reason, rv = c_rise(self, code, stock_name)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason)
            return None
        layers, pct, qty_to_buy = self._calc_layers_and_qty(sec, cash_before, rv['price'])
        reason = f"梯队({sec['theme1']})强势:{sec['strong1']}只[{list1}];梯队({sec['theme2']})强势:{sec['strong2']}只[{list2}];{vol_data.get('vol_summary', '')};{main_lift_reason};昨量比:{vr['ratio']:.2f}≥5;竞价量能:{rv['pre_ratio']:.2}≥昨量0.01;竞价涨幅:{rv['rise']:.2%}≤5%;建议仓位:{layers}层"
        self.write_strategy_evaluation(code, stock_name, 'BUY', 1, reason)
        return rv['trade_date'], rv.get('trade_time'), rv['price'], qty_to_buy, reason
