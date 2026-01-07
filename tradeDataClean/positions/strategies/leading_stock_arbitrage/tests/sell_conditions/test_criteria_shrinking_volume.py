from datetime import datetime, timedelta
import sys
import os
import pymysql

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from config import config
from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.sell_conditions import criteria_shrinking_volume

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
        base_date = datetime(2026, 1, 7)
        code = '002342.SZ'
        name = '巨力索具'
        
        windows = [('09:14:00', '11:31:00'), ('12:59:00', '15:01:00')]
        time_ranges = []
        for s_str, e_str in windows:
            s_time = datetime.strptime(s_str, '%H:%M:%S').time()
            e_time = datetime.strptime(e_str, '%H:%M:%S').time()
            time_ranges.append((s_time, e_time))
            
        start_t = time_ranges[0][0]
        end_t = time_ranges[-1][1]
        
        curr_dt = datetime.combine(base_date.date(), start_t)
        end_dt = datetime.combine(base_date.date(), end_t)
        
        while curr_dt <= end_dt:
            curr_time = curr_dt.time()
            in_window = False
            for s, e in time_ranges:
                if s <= curr_time <= e:
                    in_window = True
                    break
            
            if in_window:
                ok, reason, data = criteria_shrinking_volume.check(strategy, code, name, curr_dt)
                if ok:
                    print(f"[PASS] {curr_dt}: {reason} Data: {data}")
            
            curr_dt += timedelta(seconds=20)
        
    finally:
        strategy.close()

if __name__ == '__main__':
    test_real_db()
