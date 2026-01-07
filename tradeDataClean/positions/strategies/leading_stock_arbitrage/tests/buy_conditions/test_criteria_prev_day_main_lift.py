from datetime import datetime
import sys
import os
import pymysql

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from config import config
from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.buy_conditions import criteria_prev_day_main_lift

class StrategyMock:
    def __init__(self):
        self.db_config = config.database
        self.db = pymysql.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
            charset=self.db_config.charset,
            autocommit=True,
        )

    def close(self):
        if self.db:
            self.db.close()

def test_real_db():
    strategy = StrategyMock()
    try:
        now_dt = datetime(2026, 1, 7, 9, 25, 0)
        code = '002342.SZ'
        name = '巨力索具'
        
        print(f"Testing {code} {name} at {now_dt}...")
        ok, reason, data = criteria_prev_day_main_lift.check(strategy, code, name, now_dt)
        print(f"Result: ok={ok}")
        print(f"Reason: {reason}")
        print(f"Data: {data}")
        
    finally:
        strategy.close()

if __name__ == '__main__':
    test_real_db()
