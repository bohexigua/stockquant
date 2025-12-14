import os
import sys
import logging
import time
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

import pymysql
from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy

logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_filename = os.path.join(logs_dir, f'quant_trading_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, encoding='utf-8'), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


class TradingScheduler:
    def __init__(self, test_mode: bool = False, test_date: str = None):
        self.db = None
        self.db_config = config.database
        self.test_mode = test_mode
        self.test_date = test_date
        self._init_db()

    def _init_db(self):
        self.db = pymysql.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
            charset=self.db_config.charset,
            autocommit=True,
        )
        logger.info('数据库连接初始化成功')

    def is_trading_day(self) -> bool:
        try:
            if self.test_mode:
                return True
            with self.db.cursor() as c:
                c.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = CURDATE() LIMIT 1")
                r = c.fetchone()
                return bool(r and int(r[0]) == 1)
        except Exception as e:
            logger.error(f'检查交易日失败: {e}')
            return False

    def get_watchlist(self) -> List[Tuple[str, str]]:
        try:
            with self.db.cursor() as c:
                c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
                rows = c.fetchall()
                return [(r[0], r[1]) for r in rows if r and r[0]]
        except Exception as e:
            logger.error(f'获取自选股失败: {e}')
            return []


    def position_before(self, code: str) -> Tuple[int, float, float]:
        try:
            with self.db.cursor() as c:
                # latest position qty
                c.execute(
                    "SELECT position_qty_after FROM ptm_quant_positions WHERE stock_code=%s ORDER BY trade_date DESC, trade_time DESC LIMIT 1",
                    (code,),
                )
                r = c.fetchone()
                qty = int(r[0]) if r and r[0] is not None else 0
                # latest account cash
                c.execute(
                    "SELECT current_cash FROM ptm_quant_account_balances WHERE account_code=%s ORDER BY trade_date DESC, trade_time DESC LIMIT 1",
                    ('DEFAULT',),
                )
                br = c.fetchone()
                cash = float(br[0]) if br and br[0] is not None else 100000.0
                # initial account cash (earliest recorded)
                c.execute(
                    "SELECT current_cash FROM ptm_quant_account_balances WHERE account_code=%s ORDER BY trade_date ASC, trade_time ASC LIMIT 1",
                    ('DEFAULT',),
                )
                ir = c.fetchone()
                init_cash = float(ir[0]) if ir and ir[0] is not None else 100000.0
                return qty, cash, init_cash
        except Exception:
            return 0, 100000.0, 100000.0

    def write_position(self, trade_date: datetime.date, trade_time: datetime, qty: int, price: float, code: str, name: str, side: str, pos_after: int, realized_pnl: Optional[float], trade_reason: Optional[str], new_cash: float):
        try:
            with self.db.cursor() as c:
                c.execute(
                    "INSERT INTO ptm_quant_positions (trade_date, trade_time, trade_qty, trade_price, stock_code, stock_name, trade_side, trade_reason, position_qty_after, realized_pnl) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (trade_date, trade_time.time() if hasattr(trade_time, 'time') else trade_time, qty, price, code, name, side, trade_reason, pos_after, realized_pnl),
                )
                # upsert account balance
                try:
                    c.execute(
                        "INSERT INTO ptm_quant_account_balances (account_code, trade_date, trade_time, current_cash) VALUES (%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE current_cash=VALUES(current_cash), trade_time=VALUES(trade_time)",
                        ('DEFAULT', trade_date, trade_time, new_cash),
                    )
                except Exception:
                    pass
                try:
                    c.execute(
                        "INSERT INTO ptm_quant_strategy_evaluations (trade_date, stock_code, stock_name, strategy_name, decision_side, will_execute, summary) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (trade_date, code, name, 'BuyStrategy', side, 1, trade_reason),
                    )
                except Exception:
                    pass
                logger.info(f'写入交割: {code} {side} qty={qty} price={price} pos_after={pos_after}')
        except Exception as e:
            logger.error(f'写入交割失败 {code}: {e}')

    def decide_and_execute(self, code: str, name: str):
        qty_before, cash_before, init_cash = self.position_before(code)
        strat = BuyStrategy(self.db)
        res = strat.decide_buy(code, cash_before, name)
        if res is None:
            return
        trade_dt, price, qty_to_buy, trade_reason = res
        new_cash = cash_before - qty_to_buy * price
        pos_after = qty_before + qty_to_buy
        self.write_position(trade_dt.date(), trade_dt, qty_to_buy, price, code, name, 'BUY', pos_after, None, trade_reason, new_cash)

    def run_once(self):
        if not self.is_trading_day():
            logger.info('非交易日，跳过')
            return
        watchlist = self.get_watchlist()
        if not watchlist:
            logger.info('自选股为空')
            return
        for code, name in watchlist:
            self.decide_and_execute(code, name)


def _time_in_windows(now: datetime, windows: List[Tuple[str, str]]) -> bool:
    t = now.time()
    for s_str, e_str in windows:
        s = datetime.strptime(s_str, '%H:%M:%S').time()
        e = datetime.strptime(e_str, '%H:%M:%S').time()
        if s <= t <= e:
            return True
    return False


FIXED_WINDOWS = [('09:14:00', '11:31:00'), ('12:59:00', '15:01:00')]
FIXED_INTERVAL = 20


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-mode', action='store_true')
    parser.add_argument('--test-date', type=str, default=None)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()

    s = None
    try:
        s = TradingScheduler(test_mode=args.test_mode, test_date=args.test_date)
        if args.test_mode:
            s.run_once()
            return
        if not s.is_trading_day():
            logger.info('非交易日，跳过')
            return
        if args.once:
            s.run_once()
            return
        while True:
            now = datetime.now()
            if not _time_in_windows(now, FIXED_WINDOWS):
                last_end = datetime.strptime(FIXED_WINDOWS[-1][1], '%H:%M:%S').time()
                if now.time() > last_end:
                    logger.info('超过结束时间，退出')
                    break
                time.sleep(min(FIXED_INTERVAL, 20))
                continue
            s.run_once()
            time.sleep(FIXED_INTERVAL)
    except Exception as e:
        logger.error(f'程序失败: {e}')
    finally:
        try:
            if s and s.db:
                s.db.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
