import unittest
from unittest.mock import MagicMock
from datetime import datetime, date, time
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../')))

from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.sell_conditions import criteria_not_in_watchlist

class TestCriteriaNotInWatchlist(unittest.TestCase):
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

    def test_not_in_watchlist(self):
        # Case 1: Not in watchlist, Open > PreClose, Not Limit Up -> SELL
        watchlist_mock = [(0,)]
        ratio_mock = self.mock_volume_ratio_calls(1.0, 10.5, 10.0, 10.2)
        self.cursor.fetchone.side_effect = watchlist_mock + ratio_mock
        
        ok, reason, data = criteria_not_in_watchlist.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertTrue(ok)
        self.assertIn('高开非涨停', reason)

        # Case 2: Not in watchlist, Ratio <= 0.8 -> SELL
        self.cursor.fetchone.side_effect = [(0,)] + self.mock_volume_ratio_calls(0.7, 10.1, 10.0, 10.0)
        ok, reason, data = criteria_not_in_watchlist.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertTrue(ok)
        self.assertIn('量比低', reason)

        # Case 3: In watchlist -> NO SELL
        self.cursor.fetchone.side_effect = [(1,)] 
        ok, reason, data = criteria_not_in_watchlist.check(self.strategy, self.code, self.name, self.now_dt)
        self.assertFalse(ok)
        self.assertIn('在自选股中', reason)

if __name__ == '__main__':
    unittest.main()
