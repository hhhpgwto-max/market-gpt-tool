# Market GPT Tool

这是给你的自定义 GPT 用的“行情小工具”。

它的作用很简单：

```text
你问 GPT 股票问题
GPT 调用这个小工具
小工具查询行情数据
GPT 把结果讲给你听
```

## 已有接口

```text
GET /health
GET /search?keyword=茅台
GET /quote?symbol=600519
GET /kline?symbol=600519&period=daily
```

## 免费部署

这个仓库带了 `render.yaml`，用于 Render 免费 Web Service。

Render 上要注意：

```text
Plan 选 Free
不要点付费
不要填信用卡
看到 Starter / Pro / Billing 就停
```

## 环境变量

Render 里需要设置：

```text
MARKET_TOOL_TOKEN=你自己随便设置的一串密码
MARKET_TOOL_NAME=market-gpt-tool
```

GPT Action 里也要用同一串 `MARKET_TOOL_TOKEN`，作为请求头：

```text
x-api-key: 你的密码
```

## GPT 使用要求

给自定义 GPT 的 Instructions 可以写：

```text
当用户询问股票、行情、K线、涨跌幅、成交量、成交额等实时市场数据时，必须调用 Market GPT Tool。
不要凭记忆回答实时行情。
回答中必须说明数据来源和接口返回时间。
不要给出确定性买卖建议，只能做信息整理和风险提示。
```

## 说明

当前版本使用 `efinance` 获取行情数据，适合个人学习和测试。数据仅供参考，不构成投资建议。
