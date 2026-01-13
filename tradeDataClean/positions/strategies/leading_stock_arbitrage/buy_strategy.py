import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from .constants import strategy_name as STRATEGY_NAME

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
            layers = 7
        elif strong_total >= 3:
            layers = 5
        else:
            layers = 3
        pct = layers / 10.0
        qty_to_buy = int((cash_before * pct) // max(price, 0.01) // 100) * 100
        return layers, pct, qty_to_buy
    
    def write_strategy_evaluation(self, code: str, stock_name: str, decision_side: str, will_execute: int, summary: str, execute_qty: int = 0, now_dt: datetime = None):
        try:
            with self.db.cursor() as c:
                trade_date = (now_dt.date() if now_dt else datetime.now().date())
                trade_time = (now_dt if now_dt else datetime.now())
                c.execute(
                    "INSERT INTO ptm_quant_strategy_evaluations (trade_date, stock_code, stock_name, decision_side, will_execute, summary, related_strategy, execute_qty, created_time, updated_time) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE summary=VALUES(summary), execute_qty=VALUES(execute_qty), updated_time=VALUES(updated_time)",
                    (trade_date, code, stock_name, decision_side, will_execute, summary, STRATEGY_NAME, execute_qty, trade_time, trade_time),
                )
        except Exception:
            pass

    def decide_buy(self, code: str, cash_before: float, stock_name: str, now_dt: datetime = None):
        if now_dt is None:
            now_dt = datetime.now()
        from .criteria.buy_conditions.criteria_has_position import check as c_has_pos
        ok, reason, _ = c_has_pos(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None
        from .criteria.buy_conditions.criteria_prev_day_one_word import check as c_one_word
        ok, reason, _ = c_one_word(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None
        from .criteria.buy_conditions.criteria_prev_day_main_lift import check as c_main_lift
        ok, main_lift_reason, _ = c_main_lift(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, main_lift_reason, now_dt=now_dt)
            return None
        from .criteria.buy_conditions.criteria_sector_strong import check as c_sector
        ok, reason, sec = c_sector(self, code, stock_name, now_dt)
        peers1 = sec.get('peers1') or []
        peers2 = sec.get('peers2') or []
        list1 = ','.join([f"{p['name']}:{p['rise']:.2%}" for p in peers1[:5]]) if peers1 else '无'
        list2 = ','.join([f"{p['name']}:{p['rise']:.2%}" for p in peers2[:5]]) if peers2 else '无'
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None

        from .criteria.buy_conditions.criteria_sector_limit import check as c_sector_limit
        ok, reason, _ = c_sector_limit(self, code, stock_name, sec.get('theme1'), sec.get('theme2'), now_dt)
        if not ok:
             self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
             return None

        from .criteria.buy_conditions.criteria_volume_health import check as c_vol
        ok, reason, vol_data = c_vol(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None
        from .criteria.buy_conditions.criteria_prevday_volume_ratio import check as c_vr
        ok, reason, vr = c_vr(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None
        from .criteria.buy_conditions.criteria_preclose_and_rise import check as c_rise
        ok, reason, rv = c_rise(self, code, stock_name, now_dt)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, reason, now_dt=now_dt)
            return None
        layers, pct, qty_to_buy = self._calc_layers_and_qty(sec, cash_before, rv['price'])
        if qty_to_buy <= 0:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '仓位不足', now_dt=now_dt)
            return None
        reason = f"梯队({sec['theme1']})强势:{sec['strong1']}只[{list1}];梯队({sec['theme2']})强势:{sec['strong2']}只[{list2}];{vol_data.get('vol_summary', '')};{main_lift_reason};昨量比:{vr['ratio']:.2f}≥1.2;竞价量能:{rv['pre_ratio']:.2}≥昨量0.01;现价涨幅:{rv['rise']:.2%}≤5%;建议仓位:{layers}层"
        self.write_strategy_evaluation(code, stock_name, 'BUY', 1, reason, qty_to_buy, now_dt=now_dt)
        return now_dt, rv['price'], qty_to_buy, reason
