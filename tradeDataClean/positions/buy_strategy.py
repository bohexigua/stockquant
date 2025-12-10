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

    def has_position(self, code: str) -> bool:
        try:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT position_qty_after FROM ptm_quant_positions WHERE stock_code=%s ORDER BY trade_date DESC, trade_time DESC LIMIT 1",
                    (code,),
                )
                r = c.fetchone()
                qty = int(r[0]) if r and r[0] is not None else 0
                return qty > 0
        except Exception:
            return False

    def get_tick_preopen(self, code: str):
        return self.ds.get_preopen_info(code)

    def preopen_volume_ratio_ok(self, code: str) -> bool:
        ratio = self.ds.get_preopen_volume_ratio(code)
        return ratio >= 0.01

    def get_preopen_volume_ratio(self, code: str) -> float:
        return self.ds.get_preopen_volume_ratio(code)

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
                return False, 0, 0.0, '', 0, '', 0
            raw_themes = [x.strip() for x in str(trow[0]).split(',') if x and x.strip()]
            theme1 = raw_themes[0] if len(raw_themes) > 0 else ''
            theme2 = raw_themes[1] if len(raw_themes) > 1 else ''
            peers_t1 = self._get_theme_peers(c, theme1, code)
            peers_t2 = self._get_theme_peers(c, theme2, code)
            strong1, max1, names1 = self._count_strong_and_max(c, peers_t1)
            strong2, max2, names2 = self._count_strong_and_max(c, peers_t2)
            max_rise = max(max1, max2)
            strong_total = strong1 + strong2
            ok = strong_total > 0
            return ok, strong_total, max_rise, theme1, strong1, theme2, strong2, names1, names2

    def _get_theme_peers(self, c, theme: str, code: str):
        if not theme:
            return set()
        like = f"%{theme}%"
        c.execute(
            "SELECT t.stock_code FROM trade_factor_most_related_theme t \
             INNER JOIN (SELECT stock_code, MAX(trade_date) AS max_date FROM trade_factor_most_related_theme GROUP BY stock_code) m \
             ON t.stock_code = m.stock_code AND t.trade_date = m.max_date \
             WHERE t.all_themes_name LIKE %s",
            (like,),
        )
        rows = c.fetchall()
        peers = set()
        for r in rows:
            if r and r[0] and r[0] != code:
                peers.add(r[0])
        return peers

    def _count_strong_and_max(self, c, peers: set):
        strong = 0
        max_rise = 0.0
        names = []
        for peer in peers:
            rise = self.ds.peer_preopen_rise(peer)
            if rise is None:
                continue
            if rise >= 0.095:
                strong += 1
                nm = self.ds.get_stock_name(peer)
                names.append(nm)
            if rise > max_rise:
                max_rise = rise
        return strong, max_rise, names

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
        if df is None or df.empty or len(df) < 5:
            return 0.0
        start_open = pd.to_numeric(df.iloc[-5]['open'], errors='coerce')
        end_close = pd.to_numeric(df.iloc[-1]['close'], errors='coerce')
        if pd.isna(start_open) or pd.isna(end_close) or float(start_open) <= 0:
            return 0.0
        return float((float(end_close) - float(start_open)) / float(start_open))

    def decide_buy(self, code: str, cash_before: float, stock_name: str):
        df = self.get_daily_recent(code)
        if self.has_position(code):
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '已持仓，跳过买入')
            return None
        if df.empty:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '无日线数据')
            return None
        if self.prev_day_is_one_word(code):
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '前一日一字板')
            return None
        ok, strong_count, max_peer_rise, theme1, strong1, theme2, strong2, names1, names2 = self.sector_has_strong_movers(code)
        if not ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '同板块无强势股')
            return None
        vol_ok = self.volume_health(df)
        if not vol_ok:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '量能不健康')
            return None
        tick = self.get_tick_preopen(code)
        if tick is None:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '竞价无数据')
            return None
        if not self.preopen_volume_ratio_ok(code):
            pre_ratio = self.get_preopen_volume_ratio(code)
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, f'竞价量能不足，竞价量能占比:{pre_ratio:.2}')
            return None
        trade_dt, price, pre_close = tick
        if pre_close is None:
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, '昨收缺失')
            return None
        rise = (price - pre_close) / pre_close
        if rise > 0.05:
            pre_ratio = self.get_preopen_volume_ratio(code)
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, f'竞价涨幅过大:{rise:.2%}，竞价量能占比:{pre_ratio:.2}')
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
        qty_units = int(target_value // max(price, 0.01))
        qty_to_buy = (qty_units // 100) * 100
        if qty_to_buy < 100:
            pre_ratio = self.get_preopen_volume_ratio(code)
            self.write_strategy_evaluation(code, stock_name, 'BUY', 0, f'买入数量为0，近5日涨幅:{change5:.2%}，仓位:{pct:.0%}，竞价涨幅:{rise:.2%}，竞价量能占比:{pre_ratio:.2}')
            return None
        pre_ratio = self.get_preopen_volume_ratio(code)
        list1 = ','.join(names1) if names1 else '无'
        list2 = ','.join(names2) if names2 else '无'
        reason = f"梯队({theme1})强势:{strong1}只[{list1}];梯队({theme2})强势:{strong2}只[{list2}];最大涨幅:{max_peer_rise:.2%};量能健康;竞价量能:{pre_ratio:.2}≥昨量0.01;竞价涨幅:{rise:.2%}≤5%;近5日涨幅:{change5:.2%},仓位:{pct:.0%}"
        self.write_strategy_evaluation(code, stock_name, 'BUY', 1, reason)
        return trade_dt, price, qty_to_buy, reason
