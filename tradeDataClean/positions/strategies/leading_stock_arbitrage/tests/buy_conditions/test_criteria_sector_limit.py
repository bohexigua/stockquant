import unittest
from unittest.mock import MagicMock
import sys
import os

# Adjust path to find project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))))
sys.path.append(project_root)

from tradeDataClean.positions.strategies.leading_stock_arbitrage.criteria.buy_conditions.criteria_sector_limit import check

class TestCriteriaSectorLimit(unittest.TestCase):
    def setUp(self):
        self.strategy = MagicMock()
        self.cursor = MagicMock()
        self.strategy.db.cursor.return_value.__enter__.return_value = self.cursor

    def test_no_themes(self):
        ok, reason, data = check(self.strategy, '000001', 'Test', None, None)
        self.assertTrue(ok)
        self.assertEqual(reason, '')

    def test_no_positions(self):
        # Mock no held positions
        self.cursor.fetchall.return_value = []
        
        ok, reason, data = check(self.strategy, '000001', 'Test', 'ThemeA', 'ThemeB')
        self.assertTrue(ok)

    def test_limit_not_reached(self):
        # Mock 1 held position matching ThemeA
        self.cursor.fetchall.return_value = [('000002',)]
        # Mock theme query for held stock
        self.cursor.fetchone.return_value = ('ThemeA, ThemeC', )
        
        ok, reason, data = check(self.strategy, '000001', 'Test', 'ThemeA', 'ThemeB')
        self.assertTrue(ok)
        self.assertEqual(data['count'], 1)

    def test_limit_reached(self):
        # Mock 2 held positions matching ThemeA
        self.cursor.fetchall.return_value = [('000002',), ('000003',)]
        # Mock theme query for held stocks
        # 1st call for 000002: ThemeA
        # 2nd call for 000003: ThemeA
        self.cursor.fetchone.side_effect = [('ThemeA, ThemeC',), ('ThemeA, ThemeD',)]
        
        ok, reason, data = check(self.strategy, '000001', 'Test', 'ThemeA', 'ThemeB')
        self.assertFalse(ok)
        self.assertIn('上限', reason)
        self.assertEqual(data['count'], 2)

    def test_limit_reached_mixed_themes(self):
        # Mock 2 held positions, one matches ThemeA, one matches ThemeB
        self.cursor.fetchall.return_value = [('000002',), ('000003',)]
        self.cursor.fetchone.side_effect = [('ThemeA, ThemeC',), ('ThemeB, ThemeD',)]
        
        ok, reason, data = check(self.strategy, '000001', 'Test', 'ThemeA', 'ThemeB')
        self.assertFalse(ok)
        self.assertEqual(data['count'], 2)

    def test_db_exception(self):
        self.strategy.db.cursor.side_effect = Exception("DB Error")
        ok, reason, data = check(self.strategy, '000001', 'Test', 'ThemeA', None)
        self.assertTrue(ok) # Should pass on error

if __name__ == '__main__':
    unittest.main()
