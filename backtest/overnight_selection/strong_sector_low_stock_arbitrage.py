#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势板块低位套利（恐高）隔夜选股脚本
基于策略逻辑筛选明日需要实盘操作的股票
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backtest.data.stock_60min import Stock60minDataLoader
from backtest.data.theme import ThemeDataLoader
from backtest.data.trading_calendar import Calendar
from backtest.utils.helpers import is_valid_data


class StrongSectorLowStockSelector:
    """
    强势板块低位套利选股器
    根据策略逻辑筛选符合条件的股票
    """
    
    def __init__(self):
        """
        初始化选股器
        """
        self.stock_loader = Stock60minDataLoader()
        self.theme_loader = ThemeDataLoader()
        self.calendar = Calendar()
        
        # 策略参数（与策略保持一致）
        self.params = {
            'top_themes': 5,  # 选择前N个热门题材
            'market_cap_range': (100 * 10000, 500 * 10000),  # 流动市值范围(最小值, 最大值)
            'max_rank': 30,  # 人气票排名TOP
            'stock_price_range': (0.0, 50.0),  # 股价范围(最小值, 最大值)
            'min_turnover_rate': 25.0,  # 最小换手率
            'min_volume_ratio': 0.7,  # 最小量比
        }
    
    def get_candidate_stocks(self, target_date: str) -> List[str]:
        """
        获取候选股票列表
        
        Args:
            target_date: 目标交易日期，格式：'YYYY-MM-DD'
            
        Returns:
            List[str]: 符合条件的股票代码列表
        """
        try:
            # 获取前一个交易日
            prev_date = self.calendar.get_previous_trading_day(target_date)
            if prev_date is None:
                # 如果无法获取前一个交易日，使用自然日减1作为备选
                target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
                prev_date = (target_datetime - timedelta(days=1)).strftime('%Y-%m-%d')
            
            print(f"目标交易日: {target_date}")
            print(f"使用前一交易日数据: {prev_date}")
            
            # 获取前一日TOP题材
            top_themes = self.theme_loader.get_top_themes_by_rank(
                prev_date, 
                top_n=self.params['top_themes']
            )
            
            if top_themes is None or top_themes.empty:
                print("未获取到热门题材数据")
                return []
            
            print(f"\n=== TOP {self.params['top_themes']} 热门题材 ===")
            for _, theme in top_themes.iterrows():
                print(f"{theme['code']} - {theme['name']} (排名值: {theme.get('rank_value', 'N/A')})")
            
            # 获取题材关联的股票
            theme_codes = top_themes['code'].tolist()
            theme_stock_map = self.theme_loader.get_theme_related_stocks(theme_codes)
            
            if not theme_stock_map:
                print("未获取到题材关联股票")
                return []
            
            # 收集所有候选股票
            candidate_stocks = set()
            for theme_code in theme_codes:
                if theme_code in theme_stock_map:
                    stocks = theme_stock_map[theme_code]
                    candidate_stocks.update(stocks)
                    print(f"题材 {theme_code} 关联股票数量: {len(stocks)}")
            
            print(f"\n总候选股票数量: {len(candidate_stocks)}")
            
            # 加载股票数据进行筛选
            selected_stocks = self.filter_stocks_by_conditions(
                list(candidate_stocks), prev_date, target_date
            )
            
            return selected_stocks
            
        except Exception as e:
            print(f"获取候选股票出错: {e}")
            return []
    
    def filter_stocks_by_conditions(self, stock_codes: List[str], prev_date: str, target_date: str) -> List[str]:
        """
        根据策略条件筛选股票
        
        Args:
            stock_codes: 候选股票代码列表
            prev_date: 前一交易日
            target_date: 目标交易日
            
        Returns:
            List[str]: 符合条件的股票代码列表
        """
        try:
            # 加载股票数据（需要包含前一日和目标日的数据）
            stock_data = self.stock_loader.load_merged_stock_60min_data(prev_date, target_date)
            if stock_data is None or stock_data.empty:
                print("未获取到股票数据")
                return []
            
            selected_stocks = []
            
            print(f"\n=== 开始筛选股票 ===")
            
            for stock_code in stock_codes:
                # 获取该股票的数据
                stock_df = stock_data[stock_data['code'] == stock_code].copy()
                if stock_df.empty:
                    continue
                
                # 获取前一日的数据（用于条件检查）
                prev_day_data = stock_df[stock_df['datetime'].dt.date == datetime.strptime(prev_date, '%Y-%m-%d').date()]
                if prev_day_data.empty:
                    continue
                
                # 获取目标日的开盘数据（用于价格检查）
                target_day_data = stock_df[stock_df['datetime'].dt.date == datetime.strptime(target_date, '%Y-%m-%d').date()]
                if target_day_data.empty:
                    continue
                
                # 使用最新的前一日数据
                latest_prev_data = prev_day_data.iloc[-1]
                # 使用目标日的第一条数据（开盘）
                target_open_data = target_day_data.iloc[0]
                
                if self.check_stock_conditions(latest_prev_data, target_open_data, stock_code):
                    selected_stocks.append(stock_code)
            
            print(f"\n筛选完成，符合条件的股票数量: {len(selected_stocks)}")
            return selected_stocks
            
        except Exception as e:
            print(f"筛选股票出错: {e}")
            return []
    
    def check_stock_conditions(self, prev_data: pd.Series, target_data: pd.Series, stock_code: str) -> bool:
        """
        检查单只股票是否满足买入条件
        
        Args:
            prev_data: 前一日数据
            target_data: 目标日开盘数据
            stock_code: 股票代码
            
        Returns:
            bool: 是否符合条件
        """
        try:
            stock_name = prev_data.get('name', stock_code)
            
            # 1. 检查人气排名（使用前一日数据）
            if 'rank_today' in prev_data and is_valid_data(prev_data['rank_today']):
                if prev_data['rank_today'] > self.params['max_rank']:
                    print(f"❌ {stock_code}({stock_name}) - 人气排名过低: {prev_data['rank_today']} > {self.params['max_rank']}")
                    return False
            else:
                print(f"❌ {stock_code}({stock_name}) - 缺少人气排名数据")
                return False
            
            # 2. 检查流动市值
            if 'circ_mv' in prev_data and is_valid_data(prev_data['circ_mv']):
                min_cap, max_cap = self.params['market_cap_range']
                if prev_data['circ_mv'] >= max_cap:
                    print(f"❌ {stock_code}({stock_name}) - 流动市值过大: {prev_data['circ_mv']/10000:.2f}万 >= {max_cap/10000:.2f}万")
                    return False
                if prev_data['circ_mv'] <= min_cap:
                    print(f"❌ {stock_code}({stock_name}) - 流动市值过小: {prev_data['circ_mv']/10000:.2f}万 <= {min_cap/10000:.2f}万")
                    return False
            else:
                print(f"❌ {stock_code}({stock_name}) - 缺少流动市值数据")
                return False
            
            # 3. 检查股价范围
            current_price = target_data['open']
            min_price, max_price = self.params['stock_price_range']
            if current_price >= max_price:
                print(f"❌ {stock_code}({stock_name}) - 股价过高: {current_price:.2f} >= {max_price:.2f}")
                return False
            if current_price <= min_price:
                print(f"❌ {stock_code}({stock_name}) - 股价过低: {current_price:.2f} <= {min_price:.2f}")
                return False
            
            # 4. 检查前一日换手率
            if 'turnover_rate' in prev_data and is_valid_data(prev_data['turnover_rate']):
                if prev_data['turnover_rate'] <= self.params['min_turnover_rate']:
                    print(f"❌ {stock_code}({stock_name}) - 换手率过低: {prev_data['turnover_rate']:.2f}% <= {self.params['min_turnover_rate']:.2f}%")
                    return False
            else:
                print(f"❌ {stock_code}({stock_name}) - 缺少换手率数据")
                return False
            
            # 5. 检查前一日量比
            if 'volume_ratio' in prev_data and is_valid_data(prev_data['volume_ratio']):
                if prev_data['volume_ratio'] <= self.params['min_volume_ratio']:
                    print(f"❌ {stock_code}({stock_name}) - 量比过低: {prev_data['volume_ratio']:.2f} <= {self.params['min_volume_ratio']:.2f}")
                    return False
            else:
                print(f"❌ {stock_code}({stock_name}) - 缺少量比数据")
                return False
            
            # 6. 检查当前开盘价相对于昨日收盘价的涨幅（不超过6%）
            if 'auction_pre_close' in target_data and target_data['auction_pre_close'] > 0:
                prev_close = target_data['auction_pre_close']
                daily_change_pct = (current_price - prev_close) / prev_close
                if daily_change_pct > 0.06:
                    print(f"❌ {stock_code}({stock_name}) - 开盘涨幅过大: {daily_change_pct*100:.2f}% > 6.00%")
                    return False
            
            # 所有条件都满足
            print(f"✅ {stock_code}({stock_name}) - 符合所有条件")
            print(f"   人气排名: {prev_data['rank_today']}, 流动市值: {prev_data['circ_mv']/10000:.2f}万")
            print(f"   股价: {current_price:.2f}, 换手率: {prev_data['turnover_rate']:.2f}%, 量比: {prev_data['volume_ratio']:.2f}")
            
            return True
            
        except Exception as e:
            print(f"❌ {stock_code} - 条件检查出错: {e}")
            return False
    
    def select_stocks_for_tomorrow(self, target_date: str = None) -> List[Dict]:
        """
        选择明日需要操作的股票
        
        Args:
            target_date: 目标交易日期，默认为明日
            
        Returns:
            List[Dict]: 选中的股票信息列表
        """
        if target_date is None:
            # 默认选择明日
            tomorrow = datetime.now() + timedelta(days=1)
            target_date = tomorrow.strftime('%Y-%m-%d')
        
        print(f"\n{'='*50}")
        print(f"强势板块低位套利（恐高）隔夜选股")
        print(f"目标交易日期: {target_date}")
        print(f"{'='*50}")
        
        selected_stocks = self.get_candidate_stocks(target_date)
        
        if not selected_stocks:
            print("\n❌ 未找到符合条件的股票")
            return []
        
        # 获取股票详细信息
        stock_info_list = []
        prev_date = self.calendar.get_previous_trading_day(target_date)
        if prev_date is None:
            target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
            prev_date = (target_datetime - timedelta(days=1)).strftime('%Y-%m-%d')
        
        stock_data = self.stock_loader.load_merged_stock_60min_data(prev_date, target_date)
        if stock_data is not None:
            for stock_code in selected_stocks:
                stock_df = stock_data[stock_data['code'] == stock_code]
                if not stock_df.empty:
                    latest_data = stock_df.iloc[-1]
                    stock_info = {
                        'code': stock_code,
                        'name': latest_data.get('name', ''),
                        'rank_today': latest_data.get('rank_today', 0),
                        'circ_mv': latest_data.get('circ_mv', 0),
                        'turnover_rate': latest_data.get('turnover_rate', 0),
                        'volume_ratio': latest_data.get('volume_ratio', 0),
                        'close_price': latest_data.get('close', 0)
                    }
                    stock_info_list.append(stock_info)
        
        # 按人气排名排序
        stock_info_list.sort(key=lambda x: x['rank_today'])
        
        print(f"\n{'='*50}")
        print(f"✅ 选股结果 (共{len(stock_info_list)}只)")
        print(f"{'='*50}")
        
        for i, stock in enumerate(stock_info_list, 1):
            print(f"{i:2d}. {stock['code']} - {stock['name']}")
            print(f"    人气排名: {stock['rank_today']}, 流动市值: {stock['circ_mv']/10000:.2f}万")
            print(f"    换手率: {stock['turnover_rate']:.2f}%, 量比: {stock['volume_ratio']:.2f}")
            print(f"    收盘价: {stock['close_price']:.2f}")
            print()
        
        return stock_info_list


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='强势板块低位套利隔夜选股')
    parser.add_argument('--date', type=str, help='目标交易日期 (YYYY-MM-DD)，默认为明日')
    
    args = parser.parse_args()
    
    selector = StrongSectorLowStockSelector()
    selected_stocks = selector.select_stocks_for_tomorrow(args.date)
    
    if selected_stocks:
        print(f"\n🎯 明日建议关注 {len(selected_stocks)} 只股票")
        print("请根据实盘情况和风险控制进行操作决策")
    else:
        print("\n📝 明日暂无符合条件的股票")


if __name__ == '__main__':
    main()