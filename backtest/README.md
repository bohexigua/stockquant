# StockQuant - Backtrader回测脚手架

基于Backtrader的量化交易回测框架，提供完整的策略开发、回测和分析工具。

## 功能特性

- 🚀 基于Backtrader的高性能回测引擎
- 📊 多种数据源支持（Yahoo Finance、CCXT等）
- 📈 丰富的技术指标库
- 🎯 策略模板和示例
- 📋 详细的回测报告和可视化
- ⚙️ 灵活的配置管理

## 项目结构

```
stockquant/
├── config/                 # 配置文件
├── data/                   # 数据存储
├── strategies/             # 交易策略
├── indicators/             # 自定义指标
├── utils/                  # 工具类
├── backtest/              # 回测框架
├── examples/              # 示例代码
└── tests/                 # 测试文件
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行示例

```bash
# 运行本地数据测试（推荐首次使用）
python examples/test_local_data.py

# 运行简单策略示例（需要网络连接）
python examples/simple_strategy.py

# 运行高级策略示例
python examples/advanced_strategy.py
```

### 3. 使用命令行界面

```bash
# 查看帮助
python main.py --help

# 列出可用策略
python main.py list-strategies

# 创建示例配置文件
python main.py create-config

# 使用默认配置运行回测
python main.py run

# 使用指定配置文件运行
python main.py run -c config/sample_config.yaml
```

### 4. 自定义策略

参考 `strategies/` 目录下的策略模板，创建自己的交易策略。

## 使用说明

详细的使用说明请参考各模块的文档和示例代码。

## 故障排除

### 常见问题

1. **Yahoo Finance API限流错误**
   ```
   ERROR: Too Many Requests. Rate limited. Try after a while.
   ```
   **解决方案**: 使用本地数据测试 `python examples/test_local_data.py`

2. **元类冲突错误**
   ```
   TypeError: metaclass conflict
   ```
   **解决方案**: 已在最新版本中修复，确保使用最新的代码

3. **数据文件不存在**
   ```
   ERROR: 数据文件不存在
   ```
   **解决方案**: 确保 `data/sample_data.csv` 文件存在，或运行 `python examples/test_local_data.py` 自动创建

4. **依赖包安装失败**
   ```
   ERROR: Failed building wheel for TA-Lib
   ```
   **解决方案**: 
   - macOS: `brew install ta-lib`
   - Ubuntu: `sudo apt-get install libta-lib-dev`
   - Windows: 下载预编译包或使用conda

### 性能优化建议

- 使用本地缓存的数据文件避免重复下载
- 对于大量数据，考虑使用数据库存储
- 调整日志级别以减少输出量
- 使用多进程进行参数优化

## 许可证

MIT License