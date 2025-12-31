
import sys
import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, date

# Mock config
sys.modules['config'] = MagicMock()
sys.modules['config.config'] = MagicMock()

# Mock pymysql
sys.modules['pymysql'] = MagicMock()

# Add project root to path
sys.path.append('/Users/zwldqp/work/stockquant')

from tradeDataClean.positions.quant_trading import TradingScheduler

class TestQuantTrading(unittest.TestCase):
    @patch('tradeDataClean.positions.quant_trading.pymysql')
    @patch('tradeDataClean.positions.quant_trading.BuyStrategy')
    @patch('tradeDataClean.positions.quant_trading.SellStrategy')
    def test_decide_and_execute_sell(self, MockSellStrategy, MockBuyStrategy, MockPyMySQL):
        # Setup
        scheduler = TradingScheduler(test_mode=True)
        scheduler.db = MagicMock()
        scheduler.position_before = MagicMock(return_value=(100, 50000.0, 100000.0)) # Holding 100 shares
        scheduler.write_position = MagicMock()

        # Mock Sell Strategy behavior
        mock_sell_instance = MockSellStrategy.return_value
        trade_dt = datetime(2023, 1, 1, 10, 0, 0)
        mock_sell_instance.decide_sell.return_value = (trade_dt, 10.0, 100, "Sell Reason")

        # Execute
        scheduler.decide_and_execute('000001', 'TestStock')

        # Verify
        mock_sell_instance.decide_sell.assert_called_with('000001', 'TestStock')
        scheduler.write_position.assert_called_once()
        
        # Check write_position args
        args, _ = scheduler.write_position.call_args
        self.assertEqual(args[0], trade_dt.date()) # trade_date
        self.assertEqual(args[1], trade_dt)        # trade_time
        self.assertEqual(args[2], 100)             # qty
        self.assertEqual(args[3], 10.0)            # price
        self.assertEqual(args[6], 'SELL')          # side
        self.assertEqual(args[7], 0)               # pos_after (100 - 100)
        self.assertEqual(args[9], "Sell Reason")   # reason
        self.assertEqual(args[10], 51000.0)        # new_cash (50000 + 100*10)

        # Verify Buy Strategy was NOT called
        MockBuyStrategy.assert_not_called()

    @patch('tradeDataClean.positions.quant_trading.pymysql')
    @patch('tradeDataClean.positions.quant_trading.BuyStrategy')
    @patch('tradeDataClean.positions.quant_trading.SellStrategy')
    def test_decide_and_execute_no_sell_then_buy(self, MockSellStrategy, MockBuyStrategy, MockPyMySQL):
        # Setup
        scheduler = TradingScheduler(test_mode=True)
        scheduler.db = MagicMock()
        scheduler.position_before = MagicMock(return_value=(100, 50000.0, 100000.0))
        scheduler.write_position = MagicMock()

        # Mock Sell Strategy (No sell)
        mock_sell_instance = MockSellStrategy.return_value
        mock_sell_instance.decide_sell.return_value = None

        # Mock Buy Strategy (Buy)
        mock_buy_instance = MockBuyStrategy.return_value
        mock_buy_instance.decide_buy.return_value = (date(2023, 1, 1), '10:00:00', 10.0, 100, "Buy Reason")

        # Execute
        scheduler.decide_and_execute('000001', 'TestStock')

        # Verify
        mock_sell_instance.decide_sell.assert_called()
        mock_buy_instance.decide_buy.assert_called()
        
        # Check write_position args for BUY
        args, _ = scheduler.write_position.call_args
        self.assertEqual(args[6], 'BUY')
        self.assertEqual(args[7], 200) # pos_after (100 + 100)

if __name__ == '__main__':
    unittest.main()
