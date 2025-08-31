import logging
import os
import sys
from datetime import datetime

def setup_logger(module_name, log_prefix="backtest"):
    """
    设置日志配置
    
    Args:
        module_name: 模块名称，通常使用 __name__
        log_prefix: 日志文件前缀
    
    Returns:
        logger: 配置好的logger对象
    """
    # 创建日志目录
    log_dir = '/Users/zwldqp/work/stockquant/logs/backtest'
    os.makedirs(log_dir, exist_ok=True)
    
    # 生成日志文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'{log_dir}/{log_prefix}_{timestamp}.log'
    
    # 创建logger
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    
    # 清除已有的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger