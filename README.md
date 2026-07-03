# Market GPT Tool 行情小工具

这是给你的自定义 GPT 用的“行情查询小工具”。

你不用直接操作它。以后它会放到云端，GPT 会自动调用它。

## 它能干什么

```text
你问 GPT 股票问题
GPT 调用这个小工具
小工具查询行情数据
GPT 把结果解释给你
```

## 当前已有接口

```text
GET /health
检查小工具是否正常运行

GET /search?keyword=茅台
按股票名称或代码搜索股票

GET /quote?symbol=600519
查询某只股票的实时行情

GET /kline?symbol=600519&period=daily
查询某只股票的 K 线数据
```

## 本地运行方式

这些命令主要是我用的，你不用背。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app:app --reload
```

打开这个地址可以检查是否启动成功：

```text
http://127.0.0.1:8000/health
```

## 密码设置

云端需要设置一个小工具密码：

```text
MARKET_TOOL_TOKEN=你自己设置的一串密码
MARKET_TOOL_NAME=market-gpt-tool
```

以后 GPT 调用小工具时，也要带同一个密码：

```text
x-api-key: 你设置的那串密码
```

这个密码不是你的 GitHub 密码，也不是邮箱密码，只是保护这个小工具的访问密码。

## Render 免费部署注意

我们只用免费版。

看到这些字样就先停：

```text
Starter
Pro
Scale
Billing
Credit card
Upgrade
```

不要点付费，不要填信用卡。

## 给 GPT 的说明草稿

以后可以把这段放进你的自定义 GPT Instructions：

```text
当用户询问股票、A 股、行情、涨跌幅、成交量、成交额、K 线或实时市场数据时，必须先调用 Market GPT Tool。不要凭记忆回答实时行情。回答中必须说明数据来源和接口返回时间。不要给出确定性买卖建议，只能做信息整理和风险提示。
```

## 数据说明

当前第一版使用 `efinance` 查询数据，适合个人测试和学习。

数据只供参考，不构成投资建议。
