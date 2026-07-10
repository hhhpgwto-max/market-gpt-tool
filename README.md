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

三个工具都标记为只读，不创建、修改或删除任何数据。

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

当前使用 `efinance`，并以腾讯、新浪和东方财富公开行情接口作为备用来源。

东方财富 Choice SDK 暂未接入线上服务。数据仅供信息参考，不构成投资建议。
