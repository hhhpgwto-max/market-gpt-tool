# Market GPT Tool MCP

这是给普通 ChatGPT 对话使用的只读 A 股行情 MCP 服务。

```text
普通 ChatGPT 对话
调用盘中哨兵 MCP 应用
MCP 从行情数据源查询数据
ChatGPT 整理并解释结果
```

## 线上地址

```text
MCP：https://market-gpt-tool.onrender.com/mcp
健康检查：https://market-gpt-tool.onrender.com/health
```

ChatGPT Business 自定义应用填写方式：

```text
名称：盘中哨兵
服务器 URL：https://market-gpt-tool.onrender.com/mcp
身份验证：无身份验证
```

## MCP 工具

- `search_a_share`：按股票代码或名称搜索 A 股。
- `get_a_share_quote`：查询一只 A 股的最新行情。
- `get_a_share_kline`：查询最多 30 条近期 K 线数据。
- `get_a_share_intraday`：查询最多 240 条当日一分钟分时数据。
- `get_a_share_fund_flow`：查询最多 10 个交易日的公开资金流估算。
- `get_a_share_financials`：查询最多 4 期公开财务关键指标。
- `get_a_share_news`：查询最多 10 条提及该股票代码的公开新闻。
- `get_a_share_market_overview`：查询上证、深证、创业板指数，以及可用时的行业板块表现。

全部工具都标记为只读，不创建、修改或删除任何数据。

## 时间与稳定性

- 报价中的 `market_time` 是数据源给出的实际行情时间；`queried_at` 只是本服务向数据源发起查询的时间。周末或休市时，必须按 `market_time` 判断行情所属交易日。
- K 线中的 `latest_market_time` 是最后一根 K 线的真实数据时间；K 线优先直连东方财富公开接口，连接失败时自动改用腾讯公开接口，不再依赖 `efinance` 的 K 线通道。

## 本地运行

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app:app --reload
```

本地测试：

```powershell
.\.venv\Scripts\python.exe tests\test_mcp.py
```

## Render 部署

仓库推送到 GitHub 后，Render 免费 Web Service 自动部署：

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

只保留一个普通环境变量：

```text
MARKET_TOOL_NAME=market-gpt-tool
```

## 数据说明

当前使用 `efinance`，并以腾讯、新浪和东方财富公开接口作为行情和资讯来源。免费公开数据源可能延迟、限流或临时不可用；新闻检索可能包含仅提及该股票代码的文章，行业板块表现会在可用时按成分股行情计算。

东方财富 Choice SDK 暂未接入线上服务。数据仅供信息参考，不构成投资建议。
