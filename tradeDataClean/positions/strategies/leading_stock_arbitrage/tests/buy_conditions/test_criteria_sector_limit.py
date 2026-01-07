from datetime import datetime
import sys
import os
import pymysql

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from config import config
from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.buy_conditions import criteria_sector_limit

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
        now_dt = datetime(2026, 1, 7, 9, 30, 0)
        code = '002342.SZ'
        name = '巨力索具'
        
        print(f"Testing {code} {name} at {now_dt}...")
        # Assume some themes for testing, or fetch from DB if needed. Here we use placeholders or try to fetch.
        # For simplicity, pass None to test basic logic or specific themes if known.
        # Let's try to fetch themes for this stock first to make it realistic
        theme1, theme2 = None, None
        with strategy.db.cursor() as c:
             c.execute("SELECT theme_name FROM trade_market_stock_theme_relation WHERE stock_code=%s LIMIT 2", (code,))
             rows = c.fetchall()
             if len(rows) > 0: theme1 = rows[0][0]
             if len(rows) > 1: theme2 = rows[1][0]
        
        print(f"Themes: {theme1}, {theme2}")
        ok, reason, data = criteria_sector_limit.check(strategy, code, name, theme1, theme2, now_dt)
        print(f"Result: ok={ok}")
        print(f"Reason: {reason}")
        print(f"Data: {data}")
        
    finally:
        strategy.close()

if __name__ == '__main__':
    test_real_db()
