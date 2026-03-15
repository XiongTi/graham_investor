# Graham Intelligent Investor - 量化选股模型

基于本杰明·格雷厄姆《聪明的投资者》核心原则构建的美股量化筛选与评分系统。

## 核心评分维度

| 维度 | 指标 | Graham 标准 | 权重 |
|------|------|-------------|------|
| 估值 | P/E ratio | < 15 | 20% |
| 估值 | P/B ratio | < 1.5 | 15% |
| 估值 | Graham Number | 市价 < Graham Number | 20% |
| 财务 | Current Ratio | > 2.0 | 10% |
| 财务 | Debt/Equity | < 0.5 | 10% |
| 盈利 | 连续盈利年数 | ≥ 5年 | 10% |
| 分红 | 连续分红年数 | ≥ 5年 | 5% |
| 成长 | 盈利增长率 | > 33% (近5年) | 10% |

## 使用方式

### 安装依赖
```bash
pip install yfinance pandas
```

### 运行模型
```bash
# 使用默认股票池（约 42 只美股）并输出完整报告
python -m graham_investor.model
```

### 常用选项
- **显示前 10 只值得投资的股票**（已在报告中自动展示）
- **仅返回前 10 名**（可使用 `--top10` 参数，仅展示 "值得投资的 10 只股票" 表）

```bash
python -m graham_investor.model --top10
```

### 自定义股票列表
可以在命令行直接传入股票代码，以空格分隔。例如筛选 AAPL、MSFT、GOOGL：
```bash
python -m graham_investor.model AAPL MSFT GOOGL
```

### 结果说明
- **完整排名**：所有筛选股票按照总分降序排列。
- **值得投资的 10 只股票**：根据总分挑选的前十名，展示代码、公司、价格、总分、评级与安全边际。
- **Graham 推荐 (A/B 级)**：符合 Graham 标准的高分股票。
- **安全边际最高 Top 5**：安全边际为正的前五只。

### 示例输出（截取）
```
【完整排名】
代码 公司 价格 P/E P/B … 总分 评级
...（省略）

【值得投资的 10 只股票】
代码 公司 价格 总分 评级 安全边际%
BRK-B Berkshire Hathaway I 490.03 70.6 B 97.4
BAC Bank of America Corp 46.72 61.1 C 18.6
...（后续）
```

如需进一步分析或集成到其他系统，可直接调用 `select_top10()` 函数获取前十名的 DataFrame。
