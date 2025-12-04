import os
import sys
from datetime import datetime
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)


class BuyStrategy:
    def __init__(self, db):
        self.db = db

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
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_date, trade_time, price FROM trade_market_stock_tick WHERE code=%s AND trade_time < '09:30:00' AND trade_date=CURDATE() ORDER BY trade_time DESC LIMIT 1",
                (code,),
            )
            trow = c.fetchone()
            if not trow:
                return None
            tdate, ttime, price = trow
            c.execute(
                "SELECT pre_close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date < CURDATE())",
                (code, code),
            )
            prow = c.fetchone()
            pre_close = float(prow[0]) if prow and prow[0] is not None else None
            trade_dt = datetime.combine(tdate, ttime)
            return trade_dt, float(price), pre_close

    def preopen_volume_ratio_ok(self, code: str) -> bool:
        with self.db.cursor() as c:
            c.execute(
                "SELECT COALESCE(SUM(volume),0) FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<'09:30:00'",
                (code,),
            )
            vrow = c.fetchone()
            pre_vol = float(vrow[0]) if vrow and vrow[0] is not None else 0.0
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date < CURDATE())",
                (code, code),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                return False
            y_vol = float(yrow[0])
            if y_vol <= 0:
                return False
            return (pre_vol / 100 / y_vol) >= 0.01

    def get_preopen_volume_ratio(self, code: str) -> float:
        with self.db.cursor() as c:
            c.execute(
                "SELECT COALESCE(SUM(volume),0) FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<'09:30:00'",
                (code,),
            )
            vrow = c.fetchone()
            pre_vol = float(vrow[0]) if vrow and vrow[0] is not None else 0.0
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date < CURDATE())",
                (code, code),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                return 0.0
            y_vol = float(yrow[0])
            if y_vol <= 0:
                return 0.0
            return pre_vol / 100 / y_vol

    def prev_day_is_one_word(self, code: str) -> bool:
        with self.db.cursor() as c:
            c.execute(
                "SELECT high, low FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date < CURDATE())",
                (code, code),
            )
            r = c.fetchone()
            if not r:
                return False
            high, low = r
            if high is None or low is None:
                return False
            return float(high) == float(low)

    def sector_has_strong_movers(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT all_themes_name, trade_date FROM trade_factor_most_related_theme WHERE stock_code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_factor_most_related_theme WHERE stock_code=%s AND trade_date<=CURDATE())",
                (code, code),
            )
            trow = c.fetchone()
            if not trow or not trow[0]:
                return False, 0, 0.0
            all_themes, theme_date = trow
            raw_themes = [x.strip() for x in str(all_themes).split(',') if x and x.strip()]
            peers_set = set()
            for th in raw_themes:
                like = f"%{th}%"
                c.execute(
                    "SELECT t.stock_code FROM trade_factor_most_related_theme t \
                     INNER JOIN (SELECT stock_code, MAX(trade_date) AS max_date FROM trade_factor_most_related_theme GROUP BY stock_code) m \
                     ON t.stock_code = m.stock_code AND t.trade_date = m.max_date \
                     WHERE t.all_themes_name LIKE %s",
                    (like,),
                )
                rows = c.fetchall()
                for r in rows:
                    if r and r[0] and r[0] != code:
                        peers_set.add(r[0])
            peers = list(peers_set)
            if not peers:
                return False, 0, 0.0
            strong_count = 0
            max_rise = 0.0
            for peer in peers:
                c.execute(
                    "SELECT trade_date, trade_time, price FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<'09:30:00' ORDER BY trade_time DESC LIMIT 1",
                    (peer,),
                )
                kt = c.fetchone()
                if not kt:
                    continue
                tdate, ttime, price = kt
                c.execute(
                    "SELECT pre_close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE())",
                    (peer, peer),
                )
                pdrow = c.fetchone()
                if not pdrow or pdrow[0] is None:
                    continue
                pre_close = float(pdrow[0])
                if pre_close <= 0:
                    continue
                rise = (float(price) - pre_close) / pre_close
                if rise >= 0.095 or rise >= 0.07:
                    strong_count += 1
                if rise > max_rise:
                    max_rise = rise
            return (strong_count > 0), strong_count, max_rise

    def volume_health(self, df: pd.DataFrame, window: int = 5) -> bool:
        if df is None or df.empty:
            return False
        k = min(window, len(df))
        tail = df.tail(k)
        if len(tail) < 3:
            return False
        closes = pd.to_numeric(tail['close'], errors='coerce')
        opens = pd.to_numeric(tail['open'], errors='coerce')
        vols = pd.to_numeric(tail['vol'], errors='coerce')
        if closes.isna().any() or opens.isna().any() or vols.isna().any():
            return False
        up_all = True
        dec_cnt = 0
        for i in range(1, len(tail)):
            if not (closes.iloc[i] > closes.iloc[i-1]):
                up_all = False
                break
            if vols.iloc[i] < vols.iloc[i-1]:
                dec_cnt += 1
        if not up_all:
            return False
        if dec_cnt > 1:
            return False
        big_idx = None
        for i in range(len(tail)-1, -1, -1):
            o = opens.iloc[i]
            c = closes.iloc[i]
            if o is None or c is None or o <= 0:
                continue
            if c > o and (c - o) / o >= 0.04:
                big_idx = i
                break
        if big_idx is not None:
            last_close = closes.iloc[-1]
            bottom = opens.iloc[big_idx]
            if not (last_close >= bottom):
                return False
        return True

    def five_day_change(self, df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return 0.0
        last5 = df.tail(5)
        return float(pd.to_numeric(last5['chg_pct'], errors='coerce').fillna(0).sum())

    def decide_buy(self, code: str, cash_before: float):
        df = self.get_daily_recent(code)
        if df.empty:
            return None
        if self.prev_day_is_one_word(code):
            return None
        ok, strong_count, max_peer_rise = self.sector_has_strong_movers(code)
        if not ok:
            return None
        if not self.volume_health(df):
            return None
        tick = self.get_tick_preopen(code)
        if tick is None:
            return None
        if not self.preopen_volume_ratio_ok(code):
            return None
        trade_dt, price, pre_close = tick
        if pre_close is None:
            return None
        rise = (price - pre_close) / pre_close
        if rise > 0.05:
            return None
        change5 = self.five_day_change(df)
        if change5 <= 0:
            pct = 0.0
        elif change5 < 0.05:
            pct = 0.2
        elif change5 < 0.10:
            pct = 0.4
        elif change5 < 0.15:
            pct = 0.6
        else:
            pct = 0.8
        target_value = pct * cash_before
        qty_to_buy = int(target_value // max(price, 0.01))
        if qty_to_buy <= 0:
            return None
        pre_ratio = self.get_preopen_volume_ratio(code)
        reason = f"同板块强势:{strong_count}只,最大涨幅:{max_peer_rise:.2%};量能健康;竞价量能:{pre_ratio:.2%}≥昨量1%;竞价涨幅:{rise:.2%}≤5%;近5日涨幅:{change5:.2%},仓位:{pct:.0%}"
        return trade_dt, price, qty_to_buy, reason
