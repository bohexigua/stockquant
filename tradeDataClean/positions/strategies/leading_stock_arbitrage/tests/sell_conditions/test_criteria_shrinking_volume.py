import unittest
from unittest.mock import MagicMock
from datetime import datetime, date, time
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.sell_conditions import criteria_shrinking_volume

class TestCriteriaShrinkingVolume(unittest.TestCase):
    def setUp(self):
        self.strategy = MagicMock()
        self.cursor = MagicMock()
        self.strategy.db.cursor.return_value.__enter__.return_value = self.cursor
        self.now_dt = datetime(2025, 1, 1, 10, 0, 0)
        self.code = '000001.SZ'
        self.name = '平安银行'

    def mock_volume_ratio_calls(self, ratio, price, pre_close, open_price, has_tick=True, has_prev=True):
        y_cum_vol = 10000.0
        pre_vol = y_cum_vol * ratio
        return_values = []
        return_values.append((1,)) 
        if has_tick:
            return_values.append((time(10, 0), pre_vol, price, pre_close, open_price))
        else:
            return_values.append(None)
        if has_prev:
            return_values.append((date(2024, 12, 31),))
        else:
            return_values.append((None,))
        return_values.append((time(10, 5),))
        return_values.append((y_cum_vol,))
        return return_values

    def test_shrinking_volume(self):
        # Vol: 100(T-1) < 200(T-2) < 300(T-3) -> Shrinking
        daily_mock = [(date(2024,12,31), 100, 10.5), (date(2024,12,30), 200, 10.0), (date(2024,12,29), 300, 9.5)]
        
        # Case 1: Shrinking, Growth < 7% (Price 10.5 vs Close2 10.0 -> 5%), Open > Pre -> SELL
        self.cursor.fetchall.return_value = daily_mock
        self.cursor.fetchone.side_effect = self.mock_volume_ratio_calls(1.0, 10.5, 10.4, 10.5) 
        
        ok, reason, data = criteria_shrinking_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertTrue(ok)
        self.assertIn('缩量滞涨且高开', reason)

        # Case 2: Not shrinking
        daily_mock_bad = [(date(2024,12,31), 300, 10.5), (date(2024,12,30), 200, 10.0), (date(2024,12,29), 100, 9.5)]
        self.cursor.fetchall.return_value = daily_mock_bad
        ok, reason, data = criteria_shrinking_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertFalse(ok)
        self.assertIn('未满足连续2日缩量', reason)
        
        # Case 3: High Growth (>7%)
        self.cursor.fetchall.return_value = daily_mock
        self.cursor.fetchone.side_effect = self.mock_volume_ratio_calls(1.0, 11.0, 10.4, 10.5)
        ok, reason, data = criteria_shrinking_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertFalse(ok)
        self.assertIn('涨幅过高', reason)

if __name__ == '__main__':
    unittest.main()
