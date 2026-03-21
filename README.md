# Graham Investor — 美股量化选股与组合跟踪系统

基于本杰明·格雷厄姆《聪明的投资者》防御型投资策略，融合巴菲特质量因子，构建的美股量化筛选、评分与组合跟踪系统。

## 项目背景

格雷厄姆在《聪明的投资者》中为防御型投资者提出了一套严格的选股标准：低估值（P/E < 15, P/B < 1.5）、财务稳健（流动比率 > 2）、长期盈利与分红记录等。本项目将这些经典原则数字化，并加入巴菲特偏好的质量因子（ROE、净利率、自由现金流），形成一套完整的量化选股流水线。

## 系统架构

```
┌─────────────┐      ┌─────────────┐      ┌────────────┐
│ screener.py │ ───▶ │  model.py   │ ───▶ │  终端报告   │
│  批量预筛    │      │  逐只评分    │      │  排名/推荐  │
└─────────────┘      └─────────────┘      └────────────┘
       ▲                    ▲
       └──── config.py ─────┘

┌─────────────┐      ┌─────────────┐
│ monitor.py  │ ───▶ │ data/*.csv  │
│  组合监控    │      │  历史记录    │
└─────────────┘      └─────────────┘
```

| 模块 | 职责 |
|------|------|
| `config.py` | 所有配置参数：Graham 标准阈值、评分权重、惩罚系数、预筛参数、备用股票池 |
| `screener.py` | 第一阶段：通过 Yahoo Finance EquityQuery API 从全美股中批量预筛候选 |
| `model.py` | 第二阶段：逐只获取详细数据，12 维加权评分，生成排名与推荐报告 |
| `monitor.py` | 独立模块：跟踪两组固定组合的每日收益与累计收益，持久化到 CSV |

## 安装

**环境要求**：Python 3.9+

```bash
# 克隆项目
git clone git@github.com:XiongTi/graham_investor.git
cd graham_investor

# 安装依赖
pip install -r requirements.txt
```

依赖列表（`requirements.txt`）：

| 依赖 | 用途 |
|------|------|
| `yfinance >= 0.2.31` | 股票数据获取 + EquityQuery 批量筛选 |
| `pandas >= 2.0` | 数据处理、CSV 读写、报告格式化 |

## 使用方式

### 选股模型

```bash
# 自动发现模式（默认） — 从全美股预筛 → 精细评分
python -m graham_investor.model

# 仅显示前 10 名
python -m graham_investor.model --top10

# 使用内置备用股票池（42 只蓝筹 + 价值股）
python -m graham_investor.model --watchlist

# 指定股票代码
python -m graham_investor.model AAPL MSFT GOOGL BRK-B
```

**输出示例**：

```
══════════════════════════════════════════════════════════════════════════
  📊 Graham 聪明投资者 - 量化选股报告
══════════════════════════════════════════════════════════════════════════

【值得投资的 10 只股票】

  代码   公司                 价格   总分  评级  安全边际%
  XXXX   Example Corp        45.20  82.5    A      35.2
  ...

【Graham 推荐 (A/B 级)】共 N 只
  ...

【安全边际最高 Top 5】
  ...
```

### 组合监控

从 2026-03-18 建仓日起，跟踪两组固定组合的每日表现：

```bash
# 自动模式 — 根据当前时间推算美东交易日
python -m graham_investor.monitor

# 指定观测日期
python -m graham_investor.monitor --date 2026-03-20

# 自定义初始资金（默认 $30,000）
python -m graham_investor.monitor --capital 50000
```

**监控的两组组合**（各等权买入 10 只）：

| 组合 | 股票 | 选股来源 |
|------|------|---------|
| `fallback_top10` | BRK-B, JPM, BAC, GOOGL, MSFT, MRK, T, DIS, JNJ, IBM | 人工挑选的蓝筹股 |
| `market_top10` | INVA, SLDE, JHG, TROW, LUXE, TSLX, PAGS, VICI, RCI, STNG | 模型自动筛选结果 |

监控脚本会自动处理：
- 北京时间 → 美东时间转换（含夏令时）
- 周末与美国交易所假日检测（动态计算，支持任意年份）
- 建仓价确定：取建仓日或之后第一个可用收盘价
- 数据持久化：追加或更新 `data/daily_positions.csv` 和 `data/daily_portfolios.csv`

## 评分体系

### 12 维加权评分

分为 4 大类，合计 100%：

| 类别 | 维度 | 指标 | Graham/Buffett 标准 | 权重 |
|------|------|------|-------------------|------|
| **估值** (30%) | P/E | 市盈率 | ≤ 15 | 10% |
| | P/B | 市净率 | ≤ 1.5 | 8% |
| | Graham Number | 安全边际 | 市价 < √(22.5 × EPS × BVPS) | 12% |
| **财务** (15%) | Current Ratio | 流动比率 | ≥ 2.0 | 8% |
| | D/E | 债务权益比 | ≤ 0.5 | 7% |
| **成长与稳定** (28%) | Earnings Stability | 连续盈利年数 | ≥ 5 年 | 8% |
| | Dividend | 连续分红年数 | ≥ 5 年 | 4% |
| | Earnings Growth | 盈利增长率 | ≥ 33% | 8% |
| | Revenue Growth | 营收增长率 | ≥ 10% | 8% |
| **质量因子** (27%) | ROE | 净资产收益率 | ≥ 15% (良好) / ≥ 20% (优秀) | 12% |
| | Net Margin | 净利润率 | ≥ 10% (良好) / ≥ 20% (优秀) | 8% |
| | FCF Yield | 自由现金流收益率 | ≥ 5% | 7% |

### 盈利衰退惩罚

当盈利增长为负时，对加权总分施加乘数惩罚，防止"便宜但衰退"的股票获得高分：

| 盈利下滑幅度 | 惩罚系数 | 效果 |
|------------|---------|------|
| 0% ~ -10% | × 0.80 | 轻度惩罚 |
| -10% ~ -30% | × 0.60 | 中度惩罚 |
| > -30% | × 0.45 | 重度惩罚 |

### 评级标准

| 总分 | 评级 | 含义 |
|------|------|------|
| ≥ 80 | **A** | 强烈推荐 — 高度符合 Graham 标准 |
| ≥ 65 | **B** | 推荐 — 多数维度表现良好 |
| ≥ 50 | **C** | 中性 — 部分指标达标 |
| ≥ 35 | **D** | 谨慎 — 估值或质量存在隐患 |
| < 35 | **F** | 不推荐 — 不符合 Graham 标准 |

## 预筛参数

`config.py` 中 `SCREENER_CONFIG` 控制第一阶段粗筛的过滤条件（宽于 Graham 标准以保证召回率）：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `exchanges` | NMS, NYQ | NASDAQ + NYSE |
| `min_market_cap` | 10 亿美元 | 过滤微型股 |
| `min_avg_volume` | 30 万 | 3 个月日均成交量，确保流动性 |
| `pre_screen_pe_max` | 18 | P/E 粗筛上限（Graham 标准为 15） |
| `pre_screen_pb_max` | 2.0 | P/B 粗筛上限（Graham 标准为 1.5） |
| `pre_screen_roe_min` | 1% | ROE 为正即可，基础质量过滤 |
| `pre_screen_net_margin_min` | 1% | 净利率为正，排除明显亏损公司 |
| `max_candidates` | 200 | 进入精细评分的最大候选数量 |

## 数据文件

监控模块生成的 CSV 文件存放在 `data/` 目录下：

**`daily_positions.csv`** — 每只股票每个交易日的持仓明细：

| 字段 | 说明 |
|------|------|
| `observation_date` | 观测日期 |
| `group_name` | 组合名称 |
| `ticker` | 股票代码 |
| `shares` | 持有股数 |
| `buy_price` / `buy_price_date` | 建仓价及日期 |
| `latest_close` / `latest_price_date` | 最新收盘价及日期 |
| `day_pnl_usd` / `day_return_pct` | 当日盈亏（金额/百分比） |
| `total_pnl_usd` / `total_return_pct` | 累计盈亏（金额/百分比） |
| `current_value_usd` | 当前市值 |

**`daily_portfolios.csv`** — 每组组合每个交易日的汇总：

| 字段 | 说明 |
|------|------|
| `observation_date` | 观测日期 |
| `group_name` | 组合名称 |
| `initial_capital_usd` | 初始资金 |
| `cost_basis_usd` | 成本基础 |
| `current_value_usd` | 当前总市值 |
| `day_pnl_usd` / `day_return_pct` | 当日盈亏 |
| `total_pnl_usd` / `total_return_pct` | 累计盈亏 |

## 项目结构

```
graham_investor/
├── __init__.py          # Python 包标记
├── config.py            # 配置参数中心
├── screener.py          # 自动发现模块（批量预筛）
├── model.py             # 核心选股引擎（评分 + 报告）
├── monitor.py           # 组合监控（每日收益跟踪）
├── requirements.txt     # Python 依赖
├── README.md
├── data/
│   ├── daily_positions.csv    # 每只股票每日持仓数据
│   └── daily_portfolios.csv   # 组合级每日汇总数据
└── docs/
    └── plans/
        └── 2026-03-18-long-term-observation-design.md
```

## 免责声明

本项目仅供学习研究，不构成任何投资建议。股票投资存在风险，过往表现不代表未来收益。使用本工具所做的任何投资决策，责任由使用者自行承担。
