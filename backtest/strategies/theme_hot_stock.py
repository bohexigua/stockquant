import backtrader as bt
import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.theme import ThemeDataLoader

class ThemeHotStockStrategy(bt.Strategy):
    """
    题材热门股票策略
    基于题材排名选择热门股票进行交易
    """
    
    params = (
        ('top_themes', 3),  # 选择前N个题材
        ('stocks_per_theme', 2),  # 每个题材选择的股票数量
        ('rebalance_days', 5),  # 调仓周期（天）
        ('stop_loss', 0.1),  # 止损比例
        ('take_profit', 0.2),  # 止盈比例
    )
    
    def __init__(self):
        """
        初始化策略
        """
        self.theme_loader = ThemeDataLoader()
        
        # 记录持仓信息
        self.position_dict = {}
        self.rebalance_counter = 0
        
        # 记录交易信息
        self.trade_log = []
        
        print(f"策略初始化完成，数据源数量: {len(self.datas)}")
    
    def log(self, txt, dt=None):
        """
        日志记录函数
        """
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}: {txt}')
    
    def next(self):
        """
        策略主逻辑
        """
        current_date = self.datas[0].datetime.date(0).strftime('%Y-%m-%d')

        # 每N天调仓一次，第一天也要调仓
        self.rebalance_counter += 1
        if self.rebalance_counter != 1 and self.rebalance_counter % self.params.rebalance_days != 0:
            return
        
        self.log(f'执行调仓逻辑，当前日期: {current_date}')
        
        try:
            # 获取当日TOP题材
            top_themes = self.theme_loader.get_top_themes_by_rank(
                current_date, 
                top_n=self.params.top_themes
            )
            
            if top_themes is None or top_themes.empty:
                self.log(f'未获取到题材数据，跳过调仓')
                return
            
            # 获取题材关联的股票
            target_stocks = set()
            theme_codes = top_themes['code'].tolist()
            theme_stock_map = self.theme_loader.get_theme_related_stocks(theme_codes)
            
            if theme_stock_map:
                for theme_code in theme_codes:
                    if theme_code in theme_stock_map:
                        stocks = theme_stock_map[theme_code]
                        if stocks:
                            # 每个题材选择前N只股票
                            target_stocks.update(stocks[:self.params.stocks_per_theme])
            
            self.log(f'目标股票池: {list(target_stocks)[:10]}...')  # 只显示前10个
            
            # 执行调仓
            self.rebalance_portfolio(target_stocks)
            
        except Exception as e:
            self.log(f'调仓过程出错: {e}')
    
    def rebalance_portfolio(self, target_stocks):
        """
        执行组合调仓
        """
        # 获取当前持仓股票
        current_positions = set()
        for data in self.datas:
            if hasattr(data, '_name') and self.getposition(data).size > 0:
                current_positions.add(data._name)
        
        # 计算需要卖出的股票
        stocks_to_sell = current_positions - target_stocks
        
        # 计算需要买入的股票
        stocks_to_buy = target_stocks - current_positions
        
        # 卖出不在目标池中的股票
        for data in self.datas:
            if hasattr(data, '_name') and data._name in stocks_to_sell:
                position = self.getposition(data)
                if position.size > 0:
                    self.close(data)
                    self.log(f'卖出股票: {data._name}, 持仓: {position.size}')
        
        # 计算可用资金
        available_cash = self.broker.getcash()
        
        # 买入新股票
        if stocks_to_buy and available_cash > 0:
            position_size = available_cash / len(stocks_to_buy) * 0.95  # 保留5%现金
            
            for data in self.datas:
                if hasattr(data, '_name') and data._name in stocks_to_buy:
                    if data.close[0] > 0:  # 确保有有效价格
                        shares = int(position_size / data.close[0] / 100) * 100  # 按手买入
                        if shares > 0:
                            self.buy(data, size=shares)
                            self.log(f'买入股票: {data._name}, 股数: {shares}, 价格: {data.close[0]:.2f}')
    
    def notify_order(self, order):
        """
        订单状态通知
        """
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: {order.data._name}, 价格: {order.executed.price:.2f}, 数量: {order.executed.size}')
            else:
                self.log(f'卖出执行: {order.data._name}, 价格: {order.executed.price:.2f}, 数量: {order.executed.size}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单失败: {order.data._name}, 状态: {order.status}')
    
    def notify_trade(self, trade):
        """
        交易完成通知
        """
        if trade.isclosed:
            pnl = trade.pnl
            pnl_pct = (trade.pnl / trade.value) * 100 if trade.value != 0 else 0
            self.log(f'交易完成: {trade.data._name}, 盈亏: {pnl:.2f}, 盈亏率: {pnl_pct:.2f}%')
            
            # 记录交易信息
            self.trade_log.append({
                'stock': trade.data._name,
                'pnl': pnl,
                'pnl_pct': pnl_pct,
                'value': trade.value
            })
