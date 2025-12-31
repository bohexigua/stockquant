import os
import sys
from datetime import datetime, timedelta

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class SellStrategy:
    def __init__(self, db):
        self.db = db

    def write_strategy_evaluation(self, code: str, stock_name: str, decision_side: str, will_execute: int, summary: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "DELETE FROM ptm_quant_strategy_evaluations WHERE trade_date=CURDATE() AND stock_code=%s AND strategy_name=%s AND decision_side=%s AND HOUR(eval_time)=HOUR(NOW())",
                    (code, 'SellStrategy', decision_side),
                )
                c.execute(
                    "INSERT INTO ptm_quant_strategy_evaluations (trade_date, stock_code, stock_name, strategy_name, decision_side, will_execute, summary) "
                    "VALUES (CURDATE(), %s, %s, %s, %s, %s, %s)",
                    (code, stock_name, 'SellStrategy', decision_side, will_execute, summary),
                )
        except Exception:
            pass

    def get_latest_position(self, code: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT position_qty_after FROM ptm_quant_positions WHERE stock_code=%s ORDER BY trade_date DESC, trade_time DESC LIMIT 1",
                    (code,),
                )
                r = c.fetchone()
                qty = int(r[0]) if r and r[0] is not None else 0
                return qty
        except Exception:
            return 0

    def get_last_buy_date(self, code: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT MAX(trade_date) FROM ptm_quant_positions WHERE stock_code=%s AND trade_side='BUY'",
                    (code,),
                )
                r = c.fetchone()
                return r[0] if r and r[0] else None
        except Exception:
            return None

    def t_plus_one_available(self, code: str) -> bool:
        last_buy_date = self.get_last_buy_date(code)
        if last_buy_date is None:
            return False
        try:
            with self.db.cursor() as c:
                c.execute("SELECT cal_date FROM trade_market_calendar WHERE is_open=1 AND cal_date=CURDATE() LIMIT 1")
                r = c.fetchone()
                if not r:
                    return False
            return last_buy_date < datetime.now().date()
        except Exception:
            return False

    def decide_sell(self, code: str, stock_name: str):
        qty = self.get_latest_position(code)
        if qty <= 0:
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '无持仓，跳过卖出')
            return None
        if not self.t_plus_one_available(code):
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '未满足T+1，跳过卖出')
            return None
        try:
            from tradeDataClean.positions.criteria.sell_conditions.criteria_morning_drop_low_preopen import check as s_morning
            ok, reason, data = s_morning(self, code, stock_name)
            if ok:
                try:
                    td = data.get('trade_date')
                    tt = data.get('trade_time')
                    from datetime import datetime as _dt
                    trade_dt = _dt.combine(td, tt) if hasattr(td, 'strftime') and hasattr(tt, 'strftime') else _dt.now()
                except Exception:
                    trade_dt = datetime.now()
                price = float(data.get('price') or 0.0)
                self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason)
                return trade_dt, price, qty, reason
        except Exception:
            pass
        try:
            from tradeDataClean.positions.criteria.sell_conditions.criteria_afternoon_low_prevday_ratio import check as s_afternoon
            ok, reason, data = s_afternoon(self, code, stock_name)
            if ok:
                try:
                    td = data.get('trade_date')
                    tt = data.get('trade_time')
                    from datetime import datetime as _dt
                    trade_dt = _dt.combine(td, tt) if hasattr(td, 'strftime') and hasattr(tt, 'strftime') else _dt.now()
                except Exception:
                    trade_dt = datetime.now()
                price = float(data.get('price') or 0.0)
                self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason)
                return trade_dt, price, qty, reason
        except Exception:
            pass
        self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '未触发卖出条件')
        return None
