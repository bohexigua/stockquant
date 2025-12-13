import os
import sys
from datetime import datetime, timedelta
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class SellStrategy:
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
        # 弱化条件判断
        weak_reasons = []
        # 1) 前一日分时存在主力出货或试探
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT COUNT(*) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_factor_stock_intraday_momentum WHERE code=%s AND trade_date<CURDATE()) AND main_action IN ('主力出货','出货试探')",
                    (code, code),
                )
                r = c.fetchone()
                if r and int(r[0]) > 0:
                    weak_reasons.append('昨有主力出货/试探')
        except Exception:
            pass
        # 2) 近5日走势转弱（昨收 vs 5日前开盘）
        def _five_day_change() -> float:
            try:
                with self.db.cursor() as c:
                    c.execute(
                        "SELECT trade_date, open, close FROM trade_market_stock_daily WHERE code=%s ORDER BY trade_date DESC LIMIT 5",
                        (code,),
                    )
                    rows = c.fetchall()
                    if not rows or len(rows) < 5:
                        return 0.0
                    # rows desc, index 4 is oldest
                    start_open = float(rows[4][1]) if rows[4][1] is not None else 0.0
                    end_close = float(rows[0][2]) if rows[0][2] is not None else 0.0
                    if start_open <= 0 or end_close <= 0:
                        return 0.0
                    return (end_close - start_open) / start_open
            except Exception:
                return 0.0
        change5 = _five_day_change()
        if change5 <= 0:
            weak_reasons.append(f'近5日回落:{change5:.2%}')
        # 3) 支撑失守（最新收盘低于最近大阳线开盘）
        def _support_broken() -> bool:
            try:
                with self.db.cursor() as c:
                    c.execute(
                        "SELECT trade_date, open, close FROM trade_market_stock_daily WHERE code=%s ORDER BY trade_date DESC LIMIT 5",
                        (code,),
                    )
                    rows = c.fetchall()
                    if not rows:
                        return False
                    # find last big bullish (close>=open*1.04)
                    big_open = None
                    for row in rows:
                        o = row[1]
                        cl = row[2]
                        if o is None or cl is None or float(o) <= 0:
                            continue
                        if float(cl) > float(o) and (float(cl) - float(o)) / float(o) >= 0.04:
                            big_open = float(o)
                            break
                    if big_open is None:
                        return False
                    last_close = float(rows[0][2]) if rows[0][2] is not None else 0.0
                    return not (last_close >= big_open)
            except Exception:
                return False
        if _support_broken():
            weak_reasons.append('支撑失守')
        # 4) 竞价显著低开
        tick = self.ds.get_preopen_info(code)
        if tick is None:
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '竞价无数据，跳过卖出')
            return None
        trade_dt, price, pre_close = tick
        if pre_close and pre_close > 0:
            drop = (price - pre_close) / pre_close
            if drop <= -0.03:
                weak_reasons.append(f'竞价低开:{drop:.2%}')
        if not weak_reasons:
            self.write_strategy_evaluation(code, stock_name, 'SELL', 0, '未触发走弱条件，跳过卖出')
            return None
        reason = f"触发走弱卖出: {';'.join(weak_reasons)}; 价格:{price:.2f}，数量:{qty}"
        self.write_strategy_evaluation(code, stock_name, 'SELL', 1, reason)
        return trade_dt, price, qty, reason
