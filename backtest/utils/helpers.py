#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
辅助工具模块
提供常用的工具函数和辅助类
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import yaml
import os
import json


class ConfigManager:
    """
    配置管理器
    负责加载和管理配置文件
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
        
        self.config_path = config_path
        self.config = self.load_config()
        print('config: ', self.config)
    
    def load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"配置文件加载成功: {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def update(self, key: str, value: Any):
        """
        更新配置值
        
        Args:
            key: 配置键
            value: 新值
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save_config(self, filepath: str = None):
        """
        保存配置到文件
        
        Args:
            filepath: 保存路径，默认为原路径
        """
        if filepath is None:
            filepath = self.config_path
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
            logger.info(f"配置已保存到: {filepath}")
        except Exception as e:
            logger.error(f"保存配置失败: {e}")


class PerformanceAnalyzer:
    """
    性能分析器
    提供回测结果的详细分析
    """
    
    @staticmethod
    def calculate_metrics(returns: pd.Series) -> Dict[str, float]:
        """
        计算性能指标
        
        Args:
            returns: 收益率序列
            
        Returns:
            性能指标字典
        """
        if returns.empty:
            return {}
        
        # 基础统计
        total_return = (1 + returns).prod() - 1
        annual_return = (1 + returns.mean()) ** 252 - 1
        volatility = returns.std() * np.sqrt(252)
        
        # 夏普比率
        sharpe_ratio = annual_return / volatility if volatility > 0 else 0
        
        # 最大回撤
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # 胜率
        win_rate = (returns > 0).mean()
        
        # 盈亏比
        positive_returns = returns[returns > 0]
        negative_returns = returns[returns < 0]
        
        avg_win = positive_returns.mean() if len(positive_returns) > 0 else 0
        avg_loss = abs(negative_returns.mean()) if len(negative_returns) > 0 else 0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        
        # Calmar比率
        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio,
            'calmar_ratio': calmar_ratio,
            'total_trades': len(returns)
        }
    
    @staticmethod
    def generate_report(analysis: Dict[str, Any]) -> str:
        """
        生成性能报告
        
        Args:
            analysis: 分析结果
            
        Returns:
            格式化的报告字符串
        """
        report = "\n" + "="*50 + "\n"
        report += "           回测性能报告\n"
        report += "="*50 + "\n"
        
        # 基础信息
        if 'returns' in analysis:
            metrics = PerformanceAnalyzer.calculate_metrics(analysis['returns'])
            
            report += f"总收益率:     {metrics.get('total_return', 0):.2%}\n"
            report += f"年化收益率:   {metrics.get('annual_return', 0):.2%}\n"
            report += f"年化波动率:   {metrics.get('volatility', 0):.2%}\n"
            report += f"夏普比率:     {metrics.get('sharpe_ratio', 0):.2f}\n"
            report += f"最大回撤:     {metrics.get('max_drawdown', 0):.2%}\n"
            report += f"胜率:         {metrics.get('win_rate', 0):.2%}\n"
            report += f"盈亏比:       {metrics.get('profit_loss_ratio', 0):.2f}\n"
            report += f"Calmar比率:   {metrics.get('calmar_ratio', 0):.2f}\n"
            report += f"总交易次数:   {metrics.get('total_trades', 0)}\n"
        
        # 交易分析
        if 'trades' in analysis:
            trades = analysis['trades']
            report += "\n" + "-"*30 + "\n"
            report += "交易分析\n"
            report += "-"*30 + "\n"
            
            total_trades = trades.get('total', {}).get('total', 0)
            won_trades = trades.get('won', {}).get('total', 0)
            lost_trades = trades.get('lost', {}).get('total', 0)
            
            report += f"总交易数:     {total_trades}\n"
            report += f"盈利交易:     {won_trades}\n"
            report += f"亏损交易:     {lost_trades}\n"
            
            if total_trades > 0:
                win_rate = won_trades / total_trades
                report += f"胜率:         {win_rate:.2%}\n"
        
        report += "="*50 + "\n"
        return report


class DateTimeHelper:
    """
    日期时间辅助类
    """
    
    @staticmethod
    def parse_date(date_str: str) -> datetime:
        """
        解析日期字符串
        
        Args:
            date_str: 日期字符串
            
        Returns:
            datetime对象
        """
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%d-%m-%Y']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"无法解析日期: {date_str}")
    
    @staticmethod
    def get_trading_days(start_date: str, end_date: str) -> List[datetime]:
        """
        获取交易日列表（排除周末）
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        start = DateTimeHelper.parse_date(start_date)
        end = DateTimeHelper.parse_date(end_date)
        
        trading_days = []
        current = start
        
        while current <= end:
            # 排除周末（周六=5，周日=6）
            if current.weekday() < 5:
                trading_days.append(current)
            current += timedelta(days=1)
        
        return trading_days
    
    @staticmethod
    def format_duration(seconds: float) -> str:
        """
        格式化时间间隔
        
        Args:
            seconds: 秒数
            
        Returns:
            格式化的时间字符串
        """
        if seconds < 60:
            return f"{seconds:.1f}秒"
        elif seconds < 3600:
            return f"{seconds/60:.1f}分钟"
        else:
            return f"{seconds/3600:.1f}小时"


class FileHelper:
    """
    文件操作辅助类
    """
    
    @staticmethod
    def ensure_dir(filepath: str):
        """
        确保目录存在
        
        Args:
            filepath: 文件路径
        """
        directory = os.path.dirname(filepath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
    
    @staticmethod
    def save_json(data: Dict[str, Any], filepath: str):
        """
        保存JSON文件
        
        Args:
            data: 要保存的数据
            filepath: 文件路径
        """
        FileHelper.ensure_dir(filepath)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"JSON文件保存成功: {filepath}")
        except Exception as e:
            logger.error(f"保存JSON文件失败: {e}")
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any]:
        """
        加载JSON文件
        
        Args:
            filepath: 文件路径
            
        Returns:
            JSON数据
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"JSON文件加载成功: {filepath}")
            return data
        except Exception as e:
            logger.error(f"加载JSON文件失败: {e}")
            return {}
    
    @staticmethod
    def get_file_size(filepath: str) -> str:
        """
        获取文件大小
        
        Args:
            filepath: 文件路径
            
        Returns:
            格式化的文件大小
        """
        try:
            size = os.path.getsize(filepath)
            
            if size < 1024:
                return f"{size}B"
            elif size < 1024 * 1024:
                return f"{size/1024:.1f}KB"
            elif size < 1024 * 1024 * 1024:
                return f"{size/(1024*1024):.1f}MB"
            else:
                return f"{size/(1024*1024*1024):.1f}GB"
        except Exception:
            return "未知"


class LoggerSetup:
    """
    日志设置类
    """
    
    @staticmethod
    def setup_logger(log_level: str = 'INFO', log_file: str = None):
        """
        设置日志配置
        
        Args:
            log_level: 日志级别
            log_file: 日志文件路径
        """
        # 移除默认处理器
        logger.remove()
        
        # 添加控制台处理器
        logger.add(
            sink=lambda msg: print(msg, end=''),
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>"
        )
        
        # 添加文件处理器
        if log_file:
            FileHelper.ensure_dir(log_file)
            logger.add(
                sink=log_file,
                level=log_level,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
                rotation="10 MB",
                retention="30 days"
            )
        
        logger.info(f"日志系统初始化完成，级别: {log_level}")


class ValidationHelper:
    """
    数据验证辅助类
    """
    
    @staticmethod
    def validate_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        验证配置文件
        
        Args:
            config: 配置字典
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors = []
        
        # 检查必要的配置项
        required_sections = ['backtest', 'data', 'strategy']
        for section in required_sections:
            if section not in config:
                errors.append(f"缺少必要配置节: {section}")
        
        # 检查回测配置
        if 'backtest' in config:
            backtest_config = config['backtest']
            
            if 'cash' not in backtest_config:
                errors.append("缺少初始资金配置")
            elif not isinstance(backtest_config['cash'], (int, float)) or backtest_config['cash'] <= 0:
                errors.append("初始资金必须为正数")
            
            if 'start_date' not in backtest_config:
                errors.append("缺少开始日期配置")
            
            if 'end_date' not in backtest_config:
                errors.append("缺少结束日期配置")
        
        # 检查数据配置
        if 'data' in config:
            data_config = config['data']
            
            if 'symbols' not in data_config:
                errors.append("缺少股票代码配置")
            elif not isinstance(data_config['symbols'], list) or len(data_config['symbols']) == 0:
                errors.append("股票代码必须为非空列表")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> bool:
        """
        验证日期范围
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            日期范围是否有效
        """
        try:
            start = DateTimeHelper.parse_date(start_date)
            end = DateTimeHelper.parse_date(end_date)
            return start < end
        except Exception:
            return False