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
from tradeDataClean.positions.strategies import strategies

logs_dir = os.path.join(project_root, 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_filename = os.path.join(logs_dir, f'quant_trading_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename, encoding='utf-8'), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


class TradingScheduler:
    def __init__(self, test_mode: bool = False):
        self.db = None
        self.db_config = config.database
        self.test_mode = test_mode
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

    def is_trading_day(self, target_date: Optional[datetime.date] = None) -> bool:
        try:
            if self.test_mode:
                return True
            with self.db.cursor() as c:
                if target_date:
                    c.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = %s LIMIT 1", (target_date,))
                else:
                    c.execute("SELECT is_open FROM trade_market_calendar WHERE cal_date = CURDATE() LIMIT 1")
                r = c.fetchone()
                return bool(r and int(r[0]) == 1)
        except Exception as e:
            logger.error(f'检查交易日失败: {e}')
            return False

    def get_watchlist_and_positions(self) -> List[Tuple[str, str]]:
        try:
            with self.db.cursor() as c:
                # 获取自选股
                c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
                rows = c.fetchall()
                watchlist_codes = {r[0]: r[1] for r in rows if r and r[0]}
                
                # 获取持仓股
                c.execute("SELECT DISTINCT stock_code, stock_name FROM ptm_quant_positions WHERE qty > 0")
                p_rows = c.fetchall()
                for r in p_rows:
                    if r and r[0]:
                        if r[0] not in watchlist_codes:
                            watchlist_codes[r[0]] = r[1]
                
                return list(watchlist_codes.items())
        except Exception as e:
            logger.error(f'获取自选股/持仓股失败: {e}')
            return []


    def _get_account_code(self, strategy_name: str) -> str:
        return f"A_MARKET_{strategy_name}"

    def position_before(self, code: str, strategy_name: str) -> Tuple[int, float, float]:
        try:
            with self.db.cursor() as c:
                # latest position qty
                c.execute(
                    "SELECT qty FROM ptm_quant_positions WHERE stock_code=%s AND related_strategy=%s ORDER BY created_time DESC LIMIT 1",
                    (code, strategy_name),
                )
                r = c.fetchone()
                qty = int(r[0]) if r and r[0] is not None else 0
                # latest account cash
                account_code = self._get_account_code(strategy_name)
                c.execute(
                    "SELECT current_cash FROM ptm_quant_account_balances WHERE account_code=%s ORDER BY created_time DESC LIMIT 1",
                    (account_code,)
                )
                br = c.fetchone()
                cash = float(br[0]) if br and br[0] is not None else 100000.0
                # initial account cash (earliest recorded)
                c.execute(
                    "SELECT current_cash FROM ptm_quant_account_balances WHERE account_code=%s ORDER BY created_time ASC LIMIT 1",
                    (account_code,)
                )
                ir = c.fetchone()
                init_cash = float(ir[0]) if ir and ir[0] is not None else 100000.0
                return qty, cash, init_cash
        except Exception:
            return 0, 100000.0, 100000.0

    def write_position(self, trade_date: datetime.date, trade_time: datetime, qty: int, price: float, code: str, name: str, side: str, pos_after: int, trade_reason: Optional[str], new_cash: float, strategy_name: str = 'Manual'):
        try:
            # calculate average cost price if buying
            final_price = price
            if side == 'BUY':
                 try:
                    with self.db.cursor() as c:
                        c.execute("SELECT qty, price FROM ptm_quant_positions WHERE stock_code=%s AND related_strategy=%s", (code, strategy_name))
                        row = c.fetchone()
                        if row:
                            curr_qty = float(row[0])
                            curr_price = float(row[1])
                            total_cost = curr_qty * curr_price + float(qty) * float(price)
                            final_price = total_cost / float(pos_after) if pos_after > 0 else 0.0
                 except Exception:
                     pass
            
            with self.db.cursor() as c:
                c.execute(
                    "INSERT INTO ptm_quant_positions (qty, price, stock_code, stock_name, related_strategy, created_time, updated_time) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE qty=VALUES(qty), price=VALUES(price), related_strategy=VALUES(related_strategy), updated_time=VALUES(updated_time)",
                    (pos_after, final_price, code, name, strategy_name, trade_time, trade_time),
                )
                # insert account balance log
                account_code = self._get_account_code(strategy_name)
                c.execute(
                    "INSERT INTO ptm_quant_account_balances (account_code, current_cash, change_reason, created_time, updated_time) VALUES (%s,%s,%s,%s,%s)",
                    (account_code, new_cash, trade_reason, trade_time, trade_time), 
                )
                try:
                    amount = float(qty) * float(price)
                    c.execute(
                        "INSERT INTO ptm_quant_delivery_orders (deal_date, deal_time, stock_code, stock_name, deal_type, price, qty, amount, related_strategy, summary) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (str(trade_date), trade_time.strftime('%H:%M:%S'), code, name, side, price, qty, amount, strategy_name, trade_reason),
                    )
                except Exception:
                    pass
                logger.info(f'写入交割: {code} {side} qty={qty} price={price} pos_after={pos_after}')
        except Exception as e:
            logger.error(f'写入交割失败 {code}: {e}')

    def _combine_date_time(self, trade_date, trade_time) -> datetime:
        return datetime.combine(trade_date, datetime.strptime(str(trade_time), '%H:%M:%S').time())

    def execute_leading_stock_arbitrage(self, code: str, name: str, now_dt: Optional[datetime] = None):
        if now_dt is None:
            now_dt = datetime.now()
        strategy_name = strategies.leading_stock_arbitrage.name
        qty_before, cash_before, init_cash = self.position_before(code, strategy_name)

        # 优先判断卖出
        if qty_before > 0:
            s_strat = strategies.leading_stock_arbitrage.sell(self.db)
            s_res = s_strat.decide_sell(code, name, now_dt)
            if s_res:
                trade_dt, price, qty_to_sell, reason = s_res
                new_cash = cash_before + qty_to_sell * price
                pos_after = qty_before - qty_to_sell
                self.write_position(trade_dt.date(), trade_dt, qty_to_sell, price, code, name, 'SELL', pos_after, reason, new_cash, strategy_name)
                return

        strat = strategies.leading_stock_arbitrage.buy(self.db)
        res = strat.decide_buy(code, cash_before, name, now_dt)
        if res is None:
            return
        trade_dt, price, qty_to_buy, trade_reason = res
        new_cash = cash_before - qty_to_buy * price
        pos_after = qty_before + qty_to_buy

        self.write_position(trade_dt.date(), trade_dt, qty_to_buy, price, code, name, 'BUY', pos_after, trade_reason, new_cash, strategy_name)

    def run_once(self, now_dt: Optional[datetime] = None):
        if now_dt is None:
            now_dt = datetime.now()
        if not self.is_trading_day(now_dt.date()):
            logger.info(f'{now_dt.date()} 非交易日，跳过')
            return
        watchlist = self.get_watchlist_and_positions()
        if not watchlist:
            logger.info('自选股为空')
            return
        for code, name in watchlist:
            self.execute_leading_stock_arbitrage(code, name, now_dt)


def _time_in_windows(now: datetime, windows: List[Tuple[str, str]]) -> bool:
    t = now.time()
    for s_str, e_str in windows:
        s = datetime.strptime(s_str, '%H:%M:%S').time()
        e = datetime.strptime(e_str, '%H:%M:%S').time()
        if s <= t <= e:
            return True
    return False


FIXED_WINDOWS = [('09:26:00', '11:31:00'), ('12:59:00', '15:01:00')]
FIXED_INTERVAL = 20


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-mode', action='store_true')
    parser.add_argument('--sim-date-start', type=str, help='Simulation start date (YYYY-MM-DD)')
    parser.add_argument('--sim-date-end', type=str, help='Simulation end date (YYYY-MM-DD)')
    args = parser.parse_args()

    s = None
    try:
        s = TradingScheduler(test_mode=args.test_mode)
        
        if args.sim_date_start and args.sim_date_end:
            start_date = datetime.strptime(args.sim_date_start, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.sim_date_end, '%Y-%m-%d').date()
            
            logger.info(f"开始模拟: {start_date} -> {end_date}")
            
            curr_date = start_date
            while curr_date <= end_date:
                # 遍历当天的所有交易时间点
                for start_time_str, end_time_str in FIXED_WINDOWS:
                    start_dt = datetime.combine(curr_date, datetime.strptime(start_time_str, '%H:%M:%S').time())
                    end_dt = datetime.combine(curr_date, datetime.strptime(end_time_str, '%H:%M:%S').time())
                    
                    curr_dt = start_dt
                    while curr_dt <= end_dt:
                        logger.info(f"Simulating {curr_dt}")
                        s.run_once(curr_dt)
                        curr_dt += timedelta(seconds=FIXED_INTERVAL)
                
                curr_date += timedelta(days=1)
            
            logger.info("模拟结束")
            return

        if args.test_mode:
            s.run_once()
            return
        if not s.is_trading_day():
            logger.info('非交易日，跳过')
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
