import os
import sys
from datetime import datetime, timedelta
from .constants import strategy_name as STRATEGY_NAME

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class SellStrategy:
    def __init__(self, db):
        self.db = db

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

    def get_latest_position(self, code: str):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT qty FROM ptm_quant_positions WHERE stock_code=%s AND related_strategy=%s ORDER BY created_time DESC LIMIT 1",
                    (code, STRATEGY_NAME),
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
                    "SELECT MAX(deal_date) FROM ptm_quant_delivery_orders WHERE stock_code=%s AND deal_type='BUY' AND related_strategy=%s",
                    (code, STRATEGY_NAME),
                )
                r = c.fetchone()
                return r[0] if r and r[0] else None
        except Exception:
            return None

    def t_plus_one_available(self, code: str, now_dt: datetime = None) -> bool:
        if now_dt is None:
            now_dt = datetime.now()
        last_buy_date = self.get_last_buy_date(code)
        if last_buy_date is None:
            return False
        try:
            with self.db.cursor() as c:
                c.execute("SELECT cal_date FROM trade_market_calendar WHERE is_open=1 AND cal_date=%s LIMIT 1", (now_dt.date(),))
                r = c.fetchone()
                if not r:
                    return False
            return last_buy_date < now_dt.date()
        except Exception:
            return False

    def decide_sell(self, code: str, stock_name: str, now_dt: datetime = None):
        qty = self.get_latest_position(code)
        if qty <= 0:
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '无持仓，跳过卖出', now_dt=now_dt)
            return None
        if not self.t_plus_one_available(code, now_dt):
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '未满足T+1，跳过卖出', now_dt=now_dt)
            return None
            
        # 1. 不在自选列表中
        try:
            from .criteria.sell_conditions import criteria_not_in_watchlist
            ok, reason, data = criteria_not_in_watchlist.check(self, code, stock_name, now_dt)
            if ok:
                price = float(data.get('price') or 0.0)
                trade_dt = now_dt if now_dt else datetime.now()
                self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason, qty, now_dt=now_dt)
                return trade_dt, price, qty, reason
        except Exception:
            pass

        # 2. 连续2个交易日缩量
        try:
            from .criteria.sell_conditions import criteria_shrinking_volume
            ok, reason, data = criteria_shrinking_volume.check(self, code, stock_name, now_dt)
            if ok:
                price = float(data.get('price') or 0.0)
                trade_dt = now_dt if now_dt else datetime.now()
                self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason, qty, now_dt=now_dt)
                return trade_dt, price, qty, reason
        except Exception:
            pass

        # 3. 当日开始下跌且量比低
        try:
            from .criteria.sell_conditions import criteria_drop_low_volume
            ok, reason, data = criteria_drop_low_volume.check(self, code, stock_name, now_dt)
            if ok:
                price = float(data.get('price') or 0.0)
                trade_dt = now_dt if now_dt else datetime.now()
                self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason, qty, now_dt=now_dt)
                return trade_dt, price, qty, reason
        except Exception:
            pass
            
        self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '未触发卖出条件', now_dt=now_dt)
        return None
