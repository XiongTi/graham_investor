# Graham Intelligent Investor - 量化选股模型

基于本杰明·格雷厄姆《聪明的投资者》核心原则构建的美股量化筛选与评分系统。

## 核心特性

- **自动发现** — 通过 Yahoo Finance 批量预筛全美股，自动挖掘符合 Graham 标准的候选股票，无需手动维护股票清单
- **多维评分** — 12 个维度加权评分：P/E、P/B、Graham Number、流动比率、D/E、盈利稳定性、分红、盈利增长、营收增长、ROE、净利率、FCF 收益率
- **两阶段筛选** — 第一阶段粗筛（批量 API，快速过滤）→ 第二阶段精细评分（逐只深度分析）

## 评分维度

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
| 成长 | 营收增长率 | > 10% | 10% |
| 质量 | ROE | > 15% | 12% |
| 质量 | 净利率 | > 10% | 8% |
| 质量 | FCF 收益率 | > 5% | 7% |

## 安装依赖

```bash
pip install yfinance pandas
```

## 使用方式

### 自动发现模式（默认，推荐）

自动从全美股中预筛候选，然后逐只精细评分：

```bash
python -m graham_investor.model
```

### 仅显示前 10 名

```bash
python -m graham_investor.model --top10
```

### 使用备用股票池

如果自动发现不可用或想快速测试，可回退到内置的 42 只股票池：

```bash
python -m graham_investor.model --watchlist
```

### 指定股票代码

直接传入股票代码，跳过自动发现：

```bash
python -m graham_investor.model AAPL MSFT GOOGL BRK-B
```

## 预筛选参数

在 `config.py` 的 `SCREENER_CONFIG` 中可调整自动发现的粗筛标准：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `exchanges` | NMS, NYQ | NASDAQ + NYSE |
| `min_market_cap` | 10亿美元 | 过滤微型股 |
| `min_avg_volume` | 30万 | 确保流动性 |
| `pre_screen_pe_max` | 18 | P/E 粗筛上限（比 Graham 的 15 略宽松） |
| `pre_screen_pb_max` | 2.0 | P/B 粗筛上限（比 Graham 的 1.5 略宽松） |
| `pre_screen_roe_min` | 1% | ROE 为正，做基础质量过滤 |
| `pre_screen_net_margin_min` | 1% | 净利率为正，避免明显低质公司 |
| `max_candidates` | 200 | 进入精细评分的最大数量 |

## 结果说明

- **完整排名**：所有筛选股票按照总分降序排列
- **值得投资的 10 只股票**：总分前十名
- **Graham 推荐 (A/B 级)**：符合 Graham 标准的高分股票
- **安全边际最高 Top 5**：安全边际为正的前五只

⚠ 声明: 本模型仅供学习研究，不构成投资建议。投资有风险，入市需谨慎。
