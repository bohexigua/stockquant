import sys
import os
import pymysql
import json
from datetime import datetime
from unittest.mock import MagicMock

# Add project root to path
# Use absolute path calculation based on current file location
# File is at: /Users/zwldqp/work/stockquant/tradeDataClean/positions/strategies/common_tests/test_sql_utils.py
# Root is at: /Users/zwldqp/work/stockquant
# ../../../../../ goes to root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))) # Also add tradeDataClean parent just in case
sys.path.append('/Users/zwldqp/work/stockquant') # Explicit fallback


from config import config
from tradeDataClean.positions.strategies.common import watchlist

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

def test_real_db(test_date_str=None):
    print("\nTesting against REAL DB...")
    strategy = StrategyMock()
    try:
        with strategy.db.cursor() as c:
            # print("\n1. get_watchlist_from_user_pool:")
            # res1 = watchlist.get_watchlist_from_user_pool(c)
            # print(f"Found {len(res1)} stocks")
            # print(list(res1.items())[:5])

            print("\n2. get_watchlist_by_theme:")
            if test_date_str:
                now_dt = datetime.strptime(test_date_str, '%Y-%m-%d')
                print(f"Testing date: {now_dt.date()}")
            else:
                now_dt = datetime.now()
                print(f"Testing date: {now_dt.date()} (Today)")

            res2 = watchlist.get_watchlist_by_theme(c, now_dt)
            print(f"Found {len(res2)} stocks")
            if res2:
                print("Sample:", list(res2.items()))
                print(",".join([k for k in res2.keys()]))
            else:
                print("No stocks found (might be no data or no matching themes)")
                
    finally:
        strategy.close()

if __name__ == '__main__':
    try:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--date', type=str, help='Test date YYYY-MM-DD')
        args = parser.parse_args()
        
        test_real_db(args.date)
    except Exception as e:
        print(f"Real DB test skipped or failed: {e}")
