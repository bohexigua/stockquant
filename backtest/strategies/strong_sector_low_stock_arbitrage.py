import backtrader as bt
import pandas as pd
import pdb
import sys
import os
from datetime import datetime, timedelta
# 导入日志配置
from backtest.utils.logger import setup_logger

# 配置日志
logger = setup_logger(__name__, "strategies")

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.theme import ThemeDataLoader
from backtest.data.Calendar import Calendar
from backtest.utils.helpers import is_valid_data

class StrongSectorLowStockArbitrageStrategy(bt.Strategy):
    """
    强势板块低位套利（恐高）策略
    
    策略逻辑：
    1. 当日买入前一日热门题材（TOP）中的人气票（东财TOP）且流动市值在x亿-y亿之间且换手率在z%以上且量比在w%以上且股价在p1-p2之间
    2. 买入时机要保证相对位置不高（<6%）
    3. 次日如果竞价量能不及预期（<2%），开盘价卖出
    4. 次日未封板且0轴以上持续2h横盘（上下波动<3%），则卖出
    5. 次日尾盘0轴以下，止损卖出
    """
    
    params = (
        ('top_themes', 5),  # 选择前N个热门题材
        ('market_cap_range', (100 * 10000, 500 * 10000)),  # 流动市值范围(最小值, 最大值)
        ('max_rank', 30),  # 人气票排名TOP
        ('max_relative_position', 0.06),  # 最大相对位置6%
        ('min_auction_volume', 0.02),  # 最小竞价量能2%
        ('sideways_threshold', 0.03),  # 横盘波动阈值3%
        ('sideways_hours', 2),  # 横盘持续时间2小时
        ('stock_price_range', (0.0, 50.0)),  # 股价范围(最小值, 最大值)
        ('min_turnover_rate', 25.0),  # 最小换手率
        ('min_volume_ratio', 0.7),  # 最小量比
    )
    
    def __init__(self):
        """
        初始化策略
        """
        self.theme_loader = ThemeDataLoader()
        self.calendar = Calendar()  # 交易日历
        
        # 记录持仓信息和买入时间
        self.position_dict = {}  # {stock_code: {'buy_date': date, 'buy_price': price, 'sideways_start': None}}
        
        # 记录交易信息
        self.trade_log = []
        
        # 缓存前一日的题材和股票数据
        self.prev_day_themes = None
        self.prev_day_stocks = {}
        
        logger.info(f"强势板块低位套利策略初始化完成，数据源数量: {len(self.datas)}")
    
    def log(self, txt, dt=None):
        """
        日志记录函数
        """
        dt = dt or self.datas[0].datetime.date(0)
        logger.info(f'{dt.isoformat()}: {txt}')
    
    def next(self):
        """
        策略主逻辑
        """
        current_date = self.datas[0].datetime.date(0)
        current_time = self.datas[0].datetime.time(0)
        current_datetime = self.datas[0].datetime.datetime(0)
        
        # 处理卖出逻辑（持仓股票的卖出条件检查）
        self.check_sell_conditions(current_date, current_time)
        
        # 买入逻辑：只在每日开盘时执行
        if current_time.hour == 9 and current_time.minute == 30:
            self.check_buy_conditions(current_date)
        
        # 更新横盘监控
        self.update_sideways_monitoring(current_datetime)
    
    def check_buy_conditions(self, current_date):
        """
        检查买入条件
        使用前一日的题材和股票数据来避免未来函数
        """
        try:
            # 获取前一个交易日的题材数据
            current_date_str = current_date.strftime('%Y-%m-%d')
            prev_date = self.calendar.get_previous_trading_day(current_date_str)
            
            if prev_date is None:
                # 如果无法获取前一个交易日，使用自然日减1作为备选
                prev_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')

            # 获取前一日TOP题材
            top_themes = self.theme_loader.get_top_themes_by_rank(
                prev_date, 
                top_n=self.params.top_themes
            )
            
            if top_themes is None or top_themes.empty:
                return
            
            # 获取题材关联的股票
            theme_codes = top_themes['code'].tolist()
            theme_stock_map = self.theme_loader.get_theme_related_stocks(theme_codes)
            
            if not theme_stock_map:
                return
            
            # 筛选符合条件的股票
            candidate_stocks = set()
            for theme_code in theme_codes:
                if theme_code in theme_stock_map:
                    candidate_stocks.update(theme_stock_map[theme_code])
            
            # 检查每只候选股票的买入条件
            for data in self.datas:
                if not hasattr(data, '_name') or data._name not in candidate_stocks:
                    continue
                
                # 检查是否已持仓
                if self.getposition(data).size > 0:
                    continue
                
                # 检查买入条件
                if self.should_buy_stock(data):
                    self.execute_buy(data)
                    
        except Exception as e:
            self.log(f'买入条件检查出错: {e}')
    
    def should_buy_stock(self, data):
        """
        检查单只股票是否满足买入条件
        """
        try:
            # 1. 检查人气排名（使用前一日数据）
            if hasattr(data, 'rank_today') and is_valid_data(data.rank_today[-1]):
                if data.rank_today[-1] > self.params.max_rank:
                    return False
            else:
                return False  # 没有排名数据的股票不买入
            
            # 2. 检查流动市值
            if hasattr(data, 'circ_mv') and is_valid_data(data.circ_mv[-1]):
                min_cap, max_cap = self.params.market_cap_range
                if data.circ_mv[-1] >= max_cap:
                    return False
                if data.circ_mv[-1] <= min_cap:
                    return False
            
            # 3. 检查股价范围
            current_price = data.open[0]
            min_price, max_price = self.params.stock_price_range
            if current_price >= max_price:
                return False
            if current_price <= min_price:
                return False
            
            # 4. 检查前一日换手率
            if hasattr(data, 'turnover_rate') and is_valid_data(data.turnover_rate[-1]):
                if data.turnover_rate[-1] <= self.params.min_turnover_rate:
                    return False
            else:
                return False  # 没有换手率数据的股票不买入
            
            # 5. 检查前一日量比
            if hasattr(data, 'volume_ratio') and is_valid_data(data.volume_ratio[-1]):
                if data.volume_ratio[-1] <= self.params.min_volume_ratio:
                    return False
            else:
                return False  # 没有量比数据的股票不买入
            
            # 6. 检查当前开盘价相对于昨日收盘价的涨幅（不超过6%）
            if hasattr(data, 'auction_pre_close') and data.auction_pre_close[0] > 0:
                prev_close = data.auction_pre_close[0]
                daily_change_pct = (current_price - prev_close) / prev_close
                if daily_change_pct > 0.06:  # 相对昨日收盘价涨幅超过6%则不买入
                    return False
            
            return True
            
        except Exception as e:
            self.log(f'股票 {data._name} 买入条件检查出错: {e}')
            return False
    
    def execute_buy(self, data):
        """
        执行买入操作
        """
        try:
            available_cash = self.broker.getcash()
            if available_cash > 5000:  # 至少保留5000现金
                position_size = min(available_cash * 1, 10000)  # 每次最多买入1万或可用资金的100%
                shares = int(position_size / data.open[0] / 100) * 100  # 按手买入
                
                if shares > 0:
                    order = self.buy(data, size=shares)
                    if order:
                        # 记录买入信息
                        self.position_dict[data._name] = {
                            'buy_date': self.datas[0].datetime.date(0),
                            'buy_price': data.open[0],
                            'sideways_start': None
                        }
                        self.log(f'买入股票: {data._name}, 股数: {shares}, 价格: {data.open[0]:.2f}')
                        
        except Exception as e:
            self.log(f'买入执行出错: {e}')
    
    def check_sell_conditions(self, current_date, current_time):
        """
        检查卖出条件
        """
        for data in self.datas:
            if not hasattr(data, '_name') or data._name not in self.position_dict:
                continue
                
            position = self.getposition(data)
            if position.size <= 0:
                continue
            
            stock_info = self.position_dict[data._name]
            buy_date = stock_info['buy_date']
            
            # 次日卖出逻辑
            if current_date > buy_date:
                sell_reason = self.get_sell_reason(data, current_time, stock_info)
                if sell_reason:
                    self.close(data)
                    self.log(f'卖出股票: {data._name}, 原因: {sell_reason}, 价格: {data.close[0]:.2f}')
                    del self.position_dict[data._name]
    
    def get_sell_reason(self, data, current_time, stock_info):
        """
        获取卖出原因
        """
        try:
            # 1. 开盘时检查竞价量能
            if current_time.hour == 9 and current_time.minute == 30:
                if hasattr(data, 'auction_volume_ratio') and is_valid_data(data.auction_volume_ratio[0]):
                    if data.auction_volume_ratio[0] < self.params.min_auction_volume:
                        return '竞价量能不足'
            
            # 计算当前涨跌幅（基于当前开盘价和昨日收盘价）
            current_price = data.open[0]
            chg_pct = 0
            if hasattr(data, 'auction_pre_close') and data.auction_pre_close[0] > 0:
                prev_close = data.auction_pre_close[0]
                chg_pct = (current_price - prev_close) / prev_close * 100
            
            # 2. 检查是否封板（涨停）
            if chg_pct >= 9.8:  # 接近涨停
                return None  # 封板不卖出
            
            # 3. 检查横盘条件（0轴以上持续2小时横盘）
            if chg_pct > 0:  # 0轴以上
                if self.is_sideways_trading(data, stock_info):
                    return '0轴以上横盘2小时'
            
            # 4. 尾盘0轴以下止损
            if current_time.hour == 14 and current_time.minute == 00:  # 临近尾盘时间
                if chg_pct < 0:  # 0轴以下
                    return '尾盘0轴以下止损'
            
            return None
            
        except Exception as e:
            self.log(f'卖出条件检查出错: {e}')
            return None
    
    def is_sideways_trading(self, data, stock_info):
        """
        检查是否横盘交易
        """
        try:
            # 检查最近2小时（2个60分钟K线）的价格波动
            if len(data.close) >= 2:
                recent_prices = [data.close[-i] for i in range(2)]
                price_range = (max(recent_prices) - min(recent_prices)) / min(recent_prices)
                
                if price_range <= self.params.sideways_threshold:
                    return True
            
            return False
            
        except Exception as e:
            self.log(f'横盘检查出错: {e}')
            return False
    
    def update_sideways_monitoring(self, current_datetime):
        """
        更新横盘监控
        """
        # 这里可以添加更精确的横盘时间监控逻辑
        pass
    
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
            status_msg = {
                order.Canceled: '已取消',
                order.Margin: '保证金不足',
                order.Rejected: '被拒绝'
            }.get(order.status, f'未知状态({order.status})')
            self.log(f'订单失败: {order.data._name}, 状态: {status_msg}, 原因: {getattr(order, "info", {}).get("reason", "未知原因")}')
    
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

    def stop(self):
        """
        策略结束时调用
        """
        # 回测结束时，记录当前策略的参数和 broker 数据
        self.result = {
            'params': self.params._getkwargs(),  # 当前参数组合（字典形式）
            'final_value': self.broker.getvalue(),  # 最终总资产
            'cash': self.broker.getcash(),  # 最终现金
            'pnl': self.broker.getvalue() - self.broker.startingcash,  # 净收益
            'trade_log': self.trade_log  # 交易记录
        }
