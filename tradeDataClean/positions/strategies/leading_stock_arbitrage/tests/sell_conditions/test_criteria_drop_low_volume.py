import unittest
from unittest.mock import MagicMock
from datetime import datetime, date, time
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.sell_conditions import criteria_drop_low_volume

class TestCriteriaDropLowVolume(unittest.TestCase):
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

    def test_drop_low_volume(self):
        # Case 1: Drop (Price < PreClose) AND Ratio < 0.5 -> SELL
        self.cursor.fetchone.side_effect = self.mock_volume_ratio_calls(0.4, 9.8, 10.0, 10.0)
        ok, reason, data = criteria_drop_low_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertTrue(ok)
        self.assertIn('下跌且量比极低', reason)
        
        # Case 2: Drop but Ratio High -> NO SELL
        self.cursor.fetchone.side_effect = self.mock_volume_ratio_calls(0.6, 9.8, 10.0, 10.0)
        ok, reason, data = criteria_drop_low_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertFalse(ok)

        # Case 3: Rise -> NO SELL
        self.cursor.fetchone.side_effect = self.mock_volume_ratio_calls(0.4, 10.1, 10.0, 10.0)
        ok, reason, data = criteria_drop_low_volume.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertFalse(ok)

if __name__ == '__main__':
    unittest.main()
