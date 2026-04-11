# WealthKeeper

WealthKeeper 是一个基于格雷厄姆防御型策略的多市场选股、持仓跟踪与钱包管理工具，支持美股、A 股、港股。

## 快速开始

**环境要求：** Python 3.9+

```bash
pip install -r requirements.txt
```

安装后可直接使用 `ws` 命令：

```bash
pip install -e .
ws insight --market us
ws insight --market us --top 10
ws analyze --market us AAPL MSFT GOOGL
ws track --market us
ws copilot --market us --top 10
ws wallet deposit --market us --amount 5000 --note "入金"
ws wallet show --market us
ws buy --market us --ticker AAPL --shares 10 --price 185.2 --fees 3 --note "首次建仓"
ws sell --market us --ticker AAPL --shares 5 --price 192.0
ws wallet withdraw --market us --amount 1000 --note "提现"
```

不想安装时，也可以直接运行仓库内脚本：

```bash
./ws --help
python -m wealthkeeper --help
```

## 常用命令

- `ws refresh`：手动刷新本地可投资股票池缓存
- `ws insight`：全量评分本地股票池；不传 `--top` 时输出完整排名
- `ws track`：查看真实持仓总收益和个股明细
- `ws buy`：记录一笔真实买入交易
- `ws sell`：记录一笔真实卖出交易
- `ws wallet show|deposit|withdraw`：查看余额、入金、提现
- `ws analyze`：分析指定股票
- `ws copilot`：结合当前持仓和市场候选给出继续持有、卖出、买入和调仓建议

## 数据存储

`data/wealthkeeper.db`：钱包、交易记录与运行数据。

首次运行时如果检测到旧库 `data/graham_investor.db`，程序会自动迁移到新文件名。

以下缓存文件可安全删除：

```
data/universe/*_raw.csv         ← 规范化后的原始股票池
data/universe/*_investable.csv  ← 过滤后的可投资股票池
data/universe/*_snapshots.csv   ← 预筛阶段的行情快照缓存
```

以下种子数据需要保留：

```
data/universe/sources/us_seed.csv
data/universe/sources/cn_seed.csv
data/universe/sources/hk_seed.csv
```

## 测试

```bash
python -m pytest
```

## 免责声明

仅供学习研究，不构成投资建议。投资有风险，入市需谨慎。
