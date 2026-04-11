# WealthKeeper CLI Restructure Design

## Goal

将当前偏“研究脚本”的 CLI 重构为更清晰的产品化命令集合，明确区分：

- 市场洞察
- 持仓跟踪
- 真实交易
- 钱包管理
- 智能调仓建议

本次设计目标是让命令语义和用户心智一致，避免继续使用语义不准确的 `build`。

## Final Command Set

```bash
ws refresh --market {us,cn,hk,all}

ws insight --market {us,cn,hk} [--top 10]

ws track --market {us,cn,hk,all}

ws buy --market us --ticker AAPL --shares 10 [--price 185.2] [--fees 3] [--note "..."]
ws sell --market us --ticker AAPL --shares 5 [--price 192] [--fees 2] [--note "..."]

ws wallet show --market us
ws wallet deposit --market us --amount 5000 [--note "..."]
ws wallet withdraw --market us --amount 1000 [--note "..."]

ws analyze --market us AAPL MSFT

ws copilot --market {us,cn,hk,all} [--top 10]
```

## Command Responsibilities

### `ws refresh`

职责：

- 生成并更新各市场的本地可投资股票池

行为：

- 读取种子股票池
- 补全元数据
- 生成 `raw` 和 `investable` universe 文件

不负责：

- 输出推荐股票
- 写交易记录
- 写策略快照

### `ws insight`

职责：

- 输出市场候选股票排名
- 对候选股票给出大白话说明

行为：

- 基于当前市场的 universe 和评分模型筛选候选股
- 按分数排序
- 输出前 `N` 只股票
- 给出易理解的推荐理由

不负责：

- 读取当前持仓
- 给出调仓建议
- 记录交易
- 写策略快照

说明：

`insight` 只看市场，不看用户仓位。这与 `copilot` 严格分开。

### `ws track`

职责：

- 查看真实持仓的总收益和个股收益明细

行为：

- 读取钱包与持仓
- 计算现金余额、持仓市值、总资产
- 输出组合总收益和每只股票的收益明细

不负责：

- 市场候选排名
- 策略组对照
- 调仓建议

说明：

当前 `track` 中“策略快照 vs 对照组”的逻辑需要下线。重构后 `track` 只看真实持仓。

### `ws buy`

职责：

- 按指定价格与数量记录真实买入交易

输入：

- 必填：`--market` `--ticker` `--shares`
- 可选：`--price` `--fees` `--note`

行为：

- 不再接受 `--date`
- 不再接受 `--cash-in`
- 默认使用当前市场日期作为交易日期
- `--price` 不传时自动获取当前价格

### `ws sell`

职责：

- 按指定价格与数量记录真实卖出交易

输入：

- 必填：`--market` `--ticker` `--shares`
- 可选：`--price` `--fees` `--note`

行为：

- 不再接受 `--date`
- 默认使用当前市场日期作为交易日期
- `--price` 不传时自动获取当前价格

### `ws wallet`

职责：

- 管理钱包现金，不负责持仓分析

子命令：

- `ws wallet show --market us`
- `ws wallet deposit --market us --amount 5000 [--note "..."]`
- `ws wallet withdraw --market us --amount 1000 [--note "..."]`

行为：

- `show`：显示现金余额、净入金、持仓市值、总资产
- `deposit`：记录入金
- `withdraw`：记录出金

说明：

钱包入金/提现与买卖交易彻底拆开。买入命令不再隐式充值。

### `ws analyze`

职责：

- 分析一只或多只指定股票

行为：

- 输出分数、评级和原因解释

不负责：

- 排名市场候选
- 调仓建议

### `ws copilot`

职责：

- 结合当前持仓和市场候选给出调仓建议

行为：

- 读取真实持仓
- 调用最新市场候选结果
- 判断：
  - 哪些股票建议继续持有
  - 哪些股票建议卖出
  - 哪些股票建议买入或调入
- 输出推荐调仓方案和理由

不负责：

- 自动执行交易

说明：

`copilot` 第一版采用规则驱动，不依赖在线大模型或外部智能服务。

## Command Boundary Rules

为避免再次出现职责重叠，重构后遵守以下边界：

- `refresh` 只更新股票池
- `insight` 只做市场候选
- `track` 只看真实持仓收益
- `buy/sell` 只记录真实交易
- `wallet` 只管理现金账户
- `analyze` 只分析指定股票
- `copilot` 只生成建议，不自动调仓

## Migration Plan

### Remove

- 删除 `ws build`
- 删除用户可见的策略快照“建仓”概念
- 删除交易命令中的 `--date`
- 删除买入命令中的 `--cash-in`

### Rename / Reuse

- `refresh` 保留，继续复用现有 universe 刷新逻辑
- `analyze` 保留，继续复用现有单股分析逻辑
- `buy/sell` 保留，复用现有交易逻辑并收口参数

### Rebuild

- `insight` 复用当前选股与评分逻辑，但不再写策略快照
- `track` 重写为“真实持仓视角”
- `wallet` 基于现有数据库能力新增 show/deposit/withdraw
- `copilot` 基于规则引擎新增调仓建议能力

## Implementation Phases

### Phase 1

- 删除 `build`
- 新增 `insight`
- 收口 `buy/sell` 参数
- 新增 `wallet`
- 更新 README 与帮助文案

### Phase 2

- 重写 `track` 为真实持仓视角
- 去除策略组对照输出

### Phase 3

- 新增 `copilot`
- 输出建议持有、建议卖出、建议买入和推荐调仓方案

## Testing Scope

需要覆盖以下测试：

- CLI 命令解析
- `buy/sell` 不再接受 `--date`
- `buy` 不再接受 `--cash-in`
- `wallet show/deposit/withdraw` 的数据更新正确
- `insight` 不写策略快照
- `track` 仅输出真实持仓结果
- `copilot` 对典型仓位给出稳定建议

## Recommendation

按 Phase 1 -> Phase 2 -> Phase 3 顺序实施。

原因：

- 先把命令边界和用户体验收口
- 再处理历史上职责混杂最严重的 `track`
- 最后做 `copilot`，避免在旧边界上叠加新能力
