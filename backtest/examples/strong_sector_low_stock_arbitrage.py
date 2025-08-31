#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
题材热门股票策略示例
使用backtrader框架实现多股票回测
"""

import sys
import os
import pdb
import pandas as pd
import backtrader as bt
from datetime import datetime

# 导入日志配置
from backtest.utils.logger import setup_logger

# 配置日志
logger = setup_logger(__name__, "examples")

# 添加项目根目录到路径
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from backtest.data.stock_60min import Stock60minDataLoader, Stock60min
from backtest.data.trading_calendar import Calendar
from backtest.strategies.strong_sector_low_stock_arbitrage import (
    StrongSectorLowStockArbitrageStrategy,
)
from backtest.utils.helpers import BacktestResultSaver, is_valid_data

# 近2周
fromdate = datetime(2025, 8, 15)
todate = datetime(2025, 8, 31)
fromdate_str = fromdate.strftime("%Y-%m-%d")
todate_str = todate.strftime("%Y-%m-%d")


def load_stock_data_by_codes(fromdate, todate):
    """
    根据股票代码列表加载60分钟数据

    Args:
        fromdate: 开始日期
        todate: 结束日期

    Returns:
        dict: 股票代码到DataFrame的映射
    """
    stock_60min_loader = Stock60minDataLoader()

    # 加载所有股票60分钟数据
    all_stock_data = stock_60min_loader.load_merged_stock_60min_data(fromdate, todate)
    if all_stock_data is None:
        return {}

    # 按股票代码分组 - 使用all_stock_data中的所有个股
    stock_data_dict = {}
    unique_codes = all_stock_data["code"].unique()

    for code in unique_codes:
        stock_data = all_stock_data[all_stock_data["code"] == code].copy()
        if not stock_data.empty:
            # 确保datetime列格式正确，但不设置为索引
            stock_data["datetime"] = pd.to_datetime(stock_data["datetime"])
            # 按datetime排序
            stock_data = stock_data.sort_values("datetime")
            # 重置索引
            stock_data = stock_data.set_index("datetime", drop=True)
            stock_data_dict[code] = stock_data

    # 打印第一个股票的前五行数据用于调试
    if stock_data_dict:
        first_stock_code = list(stock_data_dict.keys())[0]
        first_stock_data = stock_data_dict[first_stock_code]
        logger.info(f"\n=== {first_stock_code} 股票60分钟数据前5行 ===")
        logger.info(first_stock_data.head())
        logger.info(f"数据形状: {first_stock_data.shape}")
        logger.info(f"列名: {list(first_stock_data.columns)}")

    return stock_data_dict


def run_backtest():
    """
    运行回测
    """
    logger.info("开始运行题材热门股票策略回测...")

    initial_cash = 20000  # 2万初始资金

    # 加载股票数据
    stock_data_dict = load_stock_data_by_codes(fromdate_str, todate_str)

    if not stock_data_dict:
        logger.error("未能加载到股票数据")
        return

    logger.info(f"成功加载 {len(stock_data_dict)} 只股票的数据")

    # 创建Cerebro引擎
    cerebro = bt.Cerebro(optreturn=False)

    # 添加策略优化
    # cerebro.optstrategy(
    #     StrongSectorLowStockArbitrageStrategy,
    #     max_rank=[30, 50],
    #     market_cap_range=[(200 * 10000, 1000 * 10000), (0 * 10000, 200 * 10000)],
    #     top_themes=[1, 3, 5],
    #     min_turnover_rate=[15.0, 25.0, 30.0],
    #     min_volume_ratio=[0.7, 1.0],
    # )

    cerebro.optstrategy(
        StrongSectorLowStockArbitrageStrategy,
        max_rank=[50],
        market_cap_range=[(200 * 10000, 1000 * 10000)],
        top_themes=[1],
        min_turnover_rate=[30.0],
        min_volume_ratio=[0.7],
    )

    # 获取回测期间的交易日数量
    calendar = Calendar()
    expected_trading_days = calendar.get_trading_days(fromdate_str, todate_str)
    # 每天 5 个 60 分钟 K 线
    expected_trading_count = len(expected_trading_days) * 5
    logger.info(f"回测期间预期交易日数量: {expected_trading_count}")

    # 添加股票数据源
    added_stocks = 0
    for stock_code, stock_data in stock_data_dict.items():
        if len(stock_data) > 0:
            # 检查股票实际交易日数量是否满足要求
            actual_trading_count = len(stock_data)
            if actual_trading_count >= expected_trading_count:
                # 创建60分钟数据源
                data_feed = Stock60min(
                    dataname=stock_data, fromdate=fromdate, todate=todate
                )
                data_feed._name = stock_code  # 设置股票代码标识
                cerebro.adddata(data_feed)
                added_stocks += 1
                logger.info(f"添加数据源: {stock_code}, 数据量: {actual_trading_count}")
            else:
                logger.info(
                     f"跳过股票 {stock_code}: 数据量不足 ({actual_trading_count} < {expected_trading_count})"
                 )

    logger.info(f"总共添加了 {added_stocks} 只股票作为数据源")

    # 设置初始资金
    cerebro.broker.setcash(initial_cash)

    # 设置手续费
    cerebro.broker.setcommission(commission=0.0005)  # 0.05%手续费

    # 添加分析器
    cerebro.addanalyzer(
        bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days
    )
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name="pyfolio")

    logger.info(f"回测设置完成，初始资金: {initial_cash:,.0f}")
    logger.info(f"回测期间: {fromdate} 到 {todate}")
    logger.info(f"数据源数量: {len(stock_data_dict)}")

    # 运行回测
    logger.info("\n开始执行回测...")
    results = cerebro.run(maxcpus=1)  # 禁用多进程以避免序列化错误

    # 处理优化结果
    logger.info(f"\n=== 策略优化结果 ===")
    logger.info(f"共测试了 {len(results)} 个参数组合")

    best_result = None
    best_return = -float("inf")

    # 遍历所有优化结果，找到最佳的那个
    for i, result in enumerate(results):
        strat = result[0]  # OptReturn对象中的第一个元素是策略实例
        final_value = strat.result["final_value"]

        total_return = final_value - initial_cash
        return_pct = ((final_value / initial_cash) - 1) * 100 if initial_cash > 0 else 0

        # 获取夏普比率
        sharpe_analysis = strat.analyzers.sharpe.get_analysis()
        sharpe_ratio = (
            sharpe_analysis.get("sharperatio", 0.0) if sharpe_analysis else 0.0
        )

        # 获取最大回撤
        drawdown_analysis = strat.analyzers.drawdown.get_analysis()
        max_drawdown = (
            drawdown_analysis.get("max", {}).get("drawdown", 0.0)
            if drawdown_analysis
            else 0.0
        )

        # 从策略结果获取参数
        params = strat.result["params"]
        max_rank = params.get("max_rank", "N/A")
        market_cap_range = params.get("market_cap_range", "N/A")
        top_themes = params.get("top_themes", "N/A")
        min_turnover_rate = params.get("min_turnover_rate", "N/A")
        min_volume_ratio = params.get("min_volume_ratio", "N/A")

        try:
            logger.info(
                f"参数组合 {i+1}: max_rank={max_rank}, market_cap_range={market_cap_range}, top_themes={top_themes}, min_turnover_rate={min_turnover_rate}, min_volume_ratio={min_volume_ratio}, 收益率={return_pct:.2f}%, 夏普比率={sharpe_ratio:.4f}, 最大回撤={max_drawdown:.2f}%, 最终资金={final_value:,.0f}"
            )

            if return_pct > best_return:
                best_return = return_pct
                best_result = result
        except Exception as e:
            logger.error(f"参数组合 {i+1}: 处理结果时出错 - {str(e)}")
            continue

    # 使用最佳结果进行详细分析
    if best_result:
        strat = best_result[0]

        # 计算最佳策略的收益率
        best_final_value = strat.result["final_value"]
        best_return_pct = (
            ((best_final_value / initial_cash) - 1) * 100 if initial_cash > 0 else 0
        )

        logger.info(f"\n=== 最佳参数组合结果（按收益率选择）===")
        logger.info(f"收益率: {best_return_pct:.2f}%")
        logger.info(f"初始资金: {initial_cash:,.0f}")
        logger.info(f"最终资金: {best_final_value:,.0f}")

    if hasattr(strat.analyzers.drawdown, "get_analysis"):
        drawdown = strat.analyzers.drawdown.get_analysis()
        max_drawdown = drawdown.get("max", {}).get("drawdown", "N/A")
        logger.info(f"最大回撤: {max_drawdown:.2f}%")

    # 输出交易统计
    if hasattr(strat, "trade_log") and strat.trade_log:
        total_trades = len(strat.trade_log)
        profitable_trades = len([t for t in strat.trade_log if t["pnl"] > 0])
        win_rate = (profitable_trades / total_trades) * 100 if total_trades > 0 else 0

        logger.info(f"\n=== 交易统计（不包括持仓中）===")
        logger.info(f"总交易次数: {total_trades}")
        logger.info(f"盈利交易: {profitable_trades}")
        logger.info(f"胜率: {win_rate:.2f}%")

        if total_trades > 0:
            avg_pnl = sum([t["pnl"] for t in strat.trade_log]) / total_trades
            logger.info(f"平均每笔盈亏: {avg_pnl:.2f}")

        portfolio_stats = strat.analyzers.getbyname("pyfolio")
        returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
        logger.info(f"回测完成，收益数据已生成，共{len(returns)}个交易日")

        # 使用BacktestResultSaver保存回测结果
        saver = BacktestResultSaver()
        strategy_name = f"StrongSectorLowStockArbitrageStrategy"
        saver.save_complete_results(
            strategy_name=strategy_name,
            strat=strat,
            start_date=fromdate_str,
            end_date=todate_str,
            initial_cash=initial_cash,
            returns=returns,
            transactions=transactions,
        )
    else:
        logger.error("未找到有效的优化结果")


if __name__ == "__main__":
    run_backtest()
    # print("=== 测试股票数据加载 ===")
    # load_stock_data_by_codes(fromdate_str, todate_str)
