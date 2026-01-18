from .leading_stock_arbitrage.buy_strategy import BuyStrategy as LeadingStockBuy
from .leading_stock_arbitrage.sell_strategy import SellStrategy as LeadingStockSell
from .leading_stock_arbitrage import constants

from .leading_stock_arbitrage_backtest.buy_strategy import BuyStrategy as LeadingStockBacktestBuy
from .leading_stock_arbitrage_backtest.sell_strategy import SellStrategy as LeadingStockBacktestSell
from .leading_stock_arbitrage_backtest import constants as backtest_constants

class Strategy:
    def __init__(self, name, buy, sell):
        self.name = name
        self.buy = buy
        self.sell = sell

class Strategies:
    def __init__(self):
        self.leading_stock_arbitrage = Strategy(
            name=constants.strategy_name,
            buy=LeadingStockBuy,
            sell=LeadingStockSell
        )
        self.leading_stock_arbitrage_backtest = Strategy(
            name=backtest_constants.strategy_name,
            buy=LeadingStockBacktestBuy,
            sell=LeadingStockBacktestSell
        )

strategies = Strategies()

__all__ = ['strategies']
