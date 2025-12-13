import sys
import os
import logging
from datetime import datetime

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

import pandas as pd
import pymysql
import tushare as ts
from config import config
from typing import Optional, List

logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_filename = os.path.join(logs_dir, f'stock_tick_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, encoding='utf-8'), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


class StockTickWriter:
    def __init__(self):
        self.db_config = config.database
        self.tushare_token = config.tushare.token
        self.connection = None
        self._init_tushare()
        self._init_database()

    def _init_tushare(self):
        ts.set_token(self.tushare_token)
        logger.info('Tushare初始化完成')

    def _init_database(self):
        self.connection = pymysql.connect(host=self.db_config.host, port=self.db_config.port, user=self.db_config.user, password=self.db_config.password, database=self.db_config.database, charset=self.db_config.charset, autocommit=True)
        logger.info('数据库连接初始化成功')

    def is_trading_day(self) -> bool:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = CURDATE() LIMIT 1")
                row = cursor.fetchone()
                return bool(row and int(row[0]) == 1)
        except Exception as e:
            logger.error(f'检查交易日失败: {e}')
            return False

    def fetch_realtime(self, ts_code: str, src: str = 'sina') -> pd.DataFrame:
        logger.info(f'获取实时tick: ts_code={ts_code} src={src}')
        df = ts.realtime_quote(ts_code=ts_code, src=src)
        if df is None or df.empty:
            logger.info('实时数据为空')
            return pd.DataFrame()
        return df

    @staticmethod
    def _latest_per_code(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if 'TIME' not in df.columns or 'TS_CODE' not in df.columns:
            return df
        df2 = df.sort_values(['TS_CODE', 'DATE', 'TIME']).groupby('TS_CODE', as_index=False).tail(1)
        return df2

    @staticmethod
    def _to_records(df: pd.DataFrame, src: str) -> list:
        if df.empty:
            return []
        cols = ['NAME', 'TS_CODE', 'DATE', 'TIME', 'OPEN', 'PRE_CLOSE', 'PRICE', 'HIGH', 'LOW', 'BID', 'ASK', 'VOLUME', 'AMOUNT', 'B1_V', 'B1_P', 'B2_V', 'B2_P', 'B3_V', 'B3_P', 'B4_V', 'B4_P', 'B5_V', 'B5_P', 'A1_V', 'A1_P', 'A2_V', 'A2_P', 'A3_V', 'A3_P', 'A4_V', 'A4_P', 'A5_V', 'A5_P']
        for c in cols:
            if c not in df.columns:
                df[c] = None
        recs = []
        for _, r in df.iterrows():
            try:
                td = datetime.strptime(str(r['DATE']), '%Y%m%d').date()
            except Exception:
                td = None
            try:
                tt = datetime.strptime(str(r['TIME']), '%H:%M:%S').time()
            except Exception:
                tt = None
            recs.append((
                td,
                str(r['TS_CODE']) if r['TS_CODE'] is not None else None,
                str(r['NAME']) if r['NAME'] is not None else None,
                tt,
                StockTickWriter._to_decimal(r['OPEN']),
                StockTickWriter._to_decimal(r['PRE_CLOSE']),
                StockTickWriter._to_decimal(r['PRICE']),
                StockTickWriter._to_decimal(r['HIGH']),
                StockTickWriter._to_decimal(r['LOW']),
                StockTickWriter._to_decimal(r['BID']),
                StockTickWriter._to_decimal(r['ASK']),
                StockTickWriter._to_int(r['VOLUME']),
                StockTickWriter._to_decimal(r['AMOUNT']),
                StockTickWriter._to_int(r['B1_V']),
                StockTickWriter._to_decimal(r['B1_P']),
                StockTickWriter._to_int(r['B2_V']),
                StockTickWriter._to_decimal(r['B2_P']),
                StockTickWriter._to_int(r['B3_V']),
                StockTickWriter._to_decimal(r['B3_P']),
                StockTickWriter._to_int(r['B4_V']),
                StockTickWriter._to_decimal(r['B4_P']),
                StockTickWriter._to_int(r['B5_V']),
                StockTickWriter._to_decimal(r['B5_P']),
                StockTickWriter._to_int(r['A1_V']),
                StockTickWriter._to_decimal(r['A1_P']),
                StockTickWriter._to_int(r['A2_V']),
                StockTickWriter._to_decimal(r['A2_P']),
                StockTickWriter._to_int(r['A3_V']),
                StockTickWriter._to_decimal(r['A3_P']),
                StockTickWriter._to_int(r['A4_V']),
                StockTickWriter._to_decimal(r['A4_P']),
                StockTickWriter._to_int(r['A5_V']),
                StockTickWriter._to_decimal(r['A5_P']),
                src
            ))
        return recs

    @staticmethod
    def _to_decimal(v):
        try:
            if v is None or v == '':
                return None
            return float(v)
        except Exception:
            return None

    @staticmethod
    def _to_int(v):
        try:
            if v is None or v == '':
                return None
            return int(float(v))
        except Exception:
            return None

    def insert_ticks(self, records: list):
        if not records:
            logger.info('无记录可写入')
            return True
        sql_ins = (
            "INSERT INTO trade_market_stock_tick "
            "(trade_date, code, name, trade_time, open, pre_close, price, high, low, bid, ask, volume, amount, "
            "b1_v, b1_p, b2_v, b2_p, b3_v, b3_p, b4_v, b4_p, b5_v, b5_p, a1_v, a1_p, a2_v, a2_p, a3_v, a3_p, a4_v, a4_p, a5_v, a5_p, data_source) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        sql_get_two = (
            "SELECT trade_time FROM trade_market_stock_tick WHERE code=%s AND trade_date=%s ORDER BY trade_time DESC LIMIT 2"
        )
        sql_del_top1 = (
            "DELETE FROM trade_market_stock_tick WHERE code=%s AND trade_date=%s AND trade_time=%s"
        )
        try:
            with self.connection.cursor() as cursor:
                for rec in records:
                    td, code, tt_new = rec[0], rec[1], rec[3]
                    cursor.execute(sql_get_two, (code, td))
                    rows = cursor.fetchall()
                    if rows and len(rows) >= 2:
                        t1 = rows[0][0]
                        t2 = rows[1][0]
                        def _to_time_obj(t):
                            try:
                                if hasattr(t, 'time'):
                                    return t.time()
                                from datetime import datetime as _dt, timedelta as _td
                                import pandas as _pd
                                if isinstance(t, _td):
                                    return (_dt.min + t).time()
                                if isinstance(t, _pd.Timedelta):
                                    return (_dt.min + t.to_pytimedelta()).time()
                                if isinstance(t, str):
                                    try:
                                        return _dt.strptime(t, '%H:%M:%S').time()
                                    except Exception:
                                        return _dt.strptime(t, '%H:%M').time()
                                return t
                            except Exception:
                                return t
                        t1o = _to_time_obj(t1)
                        t2o = _to_time_obj(t2)
                        def _sec(t):
                            return t.hour*3600 + t.minute*60 + t.second
                        if t1o and t2o:
                            diff = _sec(t1o) - _sec(t2o)
                            if diff < 300:
                                # 删除最新一条，再插入当前记录
                                cursor.execute(sql_del_top1, (code, td, t1o))
                    cursor.execute(sql_ins, rec)
            logger.info(f'成功写入{len(records)}条tick记录')
            return True
        except Exception as e:
            logger.error(f'写入tick失败: {e}')
            return False

    def run_once(self, ts_code: str, src: str = 'sina') -> bool:
        df = self.fetch_realtime(ts_code, src)
        if df.empty:
            return True
        df_latest = self._latest_per_code(df)
        recs = self._to_records(df_latest, src)
        return self.insert_ticks(recs)

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('数据库连接已关闭')


# 固定调度配置：每天 09:15:00 - 11:30:00，间隔 20s
FIXED_INTERVAL = 18
FIXED_WINDOWS = [('09:14:00', '11:31:00'), ('12:59:00', '15:01:00')]
FIXED_SRC = 'sina'
FIXED_GROUP = None


def main():
    w = None
    try:
        w = StockTickWriter()
        if not w.is_trading_day():
            logger.info('今日为非交易日，跳过执行')
            return
        ok = schedule_loop(w, FIXED_SRC, FIXED_INTERVAL, FIXED_WINDOWS, FIXED_GROUP, None)
        if ok:
            logger.info('tick调度完成')
        else:
            logger.error('tick调度失败')
    except Exception as e:
        logger.error(f'程序执行失败: {e}')
    finally:
        if w:
            w.close()



def _time_in_windows(now: datetime, windows: List[tuple]) -> bool:
    t = now.time()
    for s_str, e_str in windows:
        s = datetime.strptime(s_str, '%H:%M:%S').time()
        e = datetime.strptime(e_str, '%H:%M:%S').time()
        if s <= t <= e:
            return True
    return False

def _chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def schedule_loop(w: StockTickWriter, src: str, interval: int, windows: List[tuple], group_name: Optional[str], max_loops: Optional[int]) -> bool:
    try:
        loop_count = 0
        last_end = datetime.strptime(windows[-1][1], '%H:%M:%S').time()
        while True:
            now = datetime.now()
            if not _time_in_windows(now, windows):
                if now.time() > last_end:
                    logger.info('已过结束时间窗口，退出调度')
                    return True
                # 等待进入开始窗口
                logger.info('未到开始时间窗口，等待中...')
                import time as _t
                _t.sleep(min(interval, 20))
                continue

            # 获取自选股列表
            codes = fetch_watchlist_codes(w, group_name)
            if not codes:
                logger.info('自选股为空，跳过本轮')
            else:
                # Tushare sina源一次最多50只
                for batch in _chunk(codes, 50):
                    ts_code = ','.join(batch)
                    w.run_once(ts_code, src)

            loop_count += 1
            if max_loops is not None and loop_count >= max_loops:
                logger.info('达到最大循环次数，退出调度')
                return True

            import time as _t
            _t.sleep(interval)
    except Exception as e:
        logger.error(f'调度运行失败: {e}')
        return False

def fetch_watchlist_codes(w: StockTickWriter, group_name: Optional[str]) -> List[str]:
    try:
        q = 'SELECT stock_code FROM ptm_user_watchlist WHERE is_active = 1'
        params = ()
        if group_name:
            q += ' AND group_name = %s'
            params = (group_name,)
        with w.connection.cursor() as cursor:
            cursor.execute(q, params)
            rows = cursor.fetchall()
            codes = [r[0] for r in rows if r and r[0]]
            return codes
    except Exception as e:
        logger.error(f'获取自选股失败: {e}')
        return []

if __name__ == '__main__':
    main()
