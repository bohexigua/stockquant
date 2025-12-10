from datetime import datetime, timedelta


class BasePreopenDataSource:
    def __init__(self, db):
        self.db = db

    def get_preopen_info(self, code: str):
        raise NotImplementedError

    def get_preopen_volume_ratio(self, code: str) -> float:
        raise NotImplementedError

    def peer_preopen_rise(self, code: str):
        raise NotImplementedError

    def get_stock_name(self, code: str):
        raise NotImplementedError


class TickPreopenDataSource(BasePreopenDataSource):
    def get_preopen_info(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_date, trade_time, price FROM trade_market_stock_tick WHERE code=%s AND trade_time<='10:15:00' AND trade_date=CURDATE() ORDER BY trade_time DESC LIMIT 1",
                (code,),
            )
            trow = c.fetchone()
            if not trow:
                return None
            tdate, ttime, price = trow
            if isinstance(ttime, timedelta):
                ttime = (datetime.min + ttime).time()
            elif isinstance(ttime, datetime):
                ttime = ttime.time()
            elif isinstance(ttime, str):
                try:
                    ttime = datetime.strptime(ttime, "%H:%M:%S").time()
                except ValueError:
                    ttime = datetime.strptime(ttime, "%H:%M").time()
            c.execute(
                "SELECT pre_close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE())",
                (code, code),
            )
            prow = c.fetchone()
            pre_close = float(prow[0]) if prow and prow[0] is not None else None
            trade_dt = datetime.combine(tdate, ttime)
            return trade_dt, float(price), pre_close

    def get_preopen_volume_ratio(self, code: str) -> float:
        with self.db.cursor() as c:
            c.execute(
                "SELECT volume FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<='10:15:00'",
                (code,),
            )
            vrow = c.fetchone()
            pre_vol = float(vrow[0]) if vrow and vrow[0] is not None else 0.0
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<CURDATE())",
                (code, code),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                return 0.0
            y_vol = float(yrow[0])
            if y_vol <= 0:
                return 0.0
            return (pre_vol / 100.0) / y_vol

    def peer_preopen_rise(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_date, trade_time, price, pre_close FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() AND trade_time<='10:15:00' ORDER BY trade_time DESC LIMIT 1",
                (code,),
            )
            kt = c.fetchone()
            if not kt:
                return None
            price = float(kt[2])
            pre_close = float(kt[3]) if kt[3] is not None else None
            if pre_close is None:
                return None
            if pre_close <= 0:
                return None
            return (price - pre_close) / pre_close

    def get_stock_name(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT name FROM trade_market_stock_tick WHERE code=%s AND trade_date=CURDATE() ORDER BY trade_time DESC LIMIT 1",
                (code,),
            )
            r = c.fetchone()
            if r and r[0]:
                return str(r[0])
            c.execute(
                "SELECT name FROM trade_market_stock_basic_daily WHERE code=%s ORDER BY trade_date DESC LIMIT 1",
                (code,),
            )
            rr = c.fetchone()
            return str(rr[0]) if rr and rr[0] else code


class Stock60MinPreopenDataSource(BasePreopenDataSource):
    def __init__(self, db, target_date: str):
        super().__init__(db)
        self.target_date = target_date

    def get_preopen_info(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_time, open, close FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s ORDER BY trade_time ASC LIMIT 1",
                (code, self.target_date),
            )
            r = c.fetchone()
            if not r:
                return None
            ttime, o, cl = r
            price = float(o if o is not None else cl)
            c.execute(
                "SELECT pre_close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<%s)",
                (code, code, self.target_date),
            )
            prow = c.fetchone()
            pre_close = float(prow[0]) if prow and prow[0] is not None else None
            tdate = datetime.strptime(self.target_date, "%Y-%m-%d").date()
            if isinstance(ttime, timedelta):
                ttime = (datetime.min + ttime).time()
            elif isinstance(ttime, datetime):
                ttime = ttime.time()
            elif isinstance(ttime, str):
                ttime = datetime.strptime(ttime, "%H:%M:%S").time()
            trade_dt = datetime.combine(tdate, ttime)
            return trade_dt, price, pre_close

    def get_preopen_volume_ratio(self, code: str) -> float:
        with self.db.cursor() as c:
            c.execute(
                "SELECT vol FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s AND trade_time<='10:15:00'",
                (code, self.target_date),
            )
            vrow = c.fetchone()
            pre_vol = float(vrow[0]) if vrow and vrow[0] is not None else 0.0
            c.execute(
                "SELECT vol FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<%s)",
                (code, code, self.target_date),
            )
            yrow = c.fetchone()
            if not yrow or yrow[0] is None:
                return 0.0
            y_vol = float(yrow[0])
            if y_vol <= 0:
                return 0.0
            return pre_vol / 100 / y_vol

    def peer_preopen_rise(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT trade_time, open, close FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s ORDER BY trade_time ASC LIMIT 1",
                (code, self.target_date),
            )
            r = c.fetchone()
            if not r:
                return None
            ttime, o, cl = r
            price = float(o if o is not None else cl)
            c.execute(
                "SELECT close FROM trade_market_stock_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<%s)",
                (code, code, self.target_date),
            )
            pdrow = c.fetchone()
            if not pdrow or pdrow[0] is None:
                return None
            pre_close = float(pdrow[0])
            if pre_close <= 0:
                return None
            return (price - pre_close) / pre_close

    def get_stock_name(self, code: str):
        with self.db.cursor() as c:
            c.execute(
                "SELECT name FROM trade_market_stock_5min WHERE code=%s AND trade_date=%s ORDER BY trade_time ASC LIMIT 1",
                (code, self.target_date),
            )
            r = c.fetchone()
            if r and r[0]:
                return str(r[0])
            c.execute(
                "SELECT name FROM trade_market_stock_basic_daily WHERE code=%s AND trade_date=(SELECT MAX(trade_date) FROM trade_market_stock_basic_daily WHERE code=%s AND trade_date<=%s)",
                (code, code, self.target_date),
            )
            rr = c.fetchone()
            return str(rr[0]) if rr and rr[0] else code
