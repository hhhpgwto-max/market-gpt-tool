# Market GPT Tool MCP

这是给普通 ChatGPT 对话使用的只读 A 股及交易所基金行情 MCP 服务。

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

- `search_a_share`：按代码或名称搜索 A 股、ETF、LOF。
- `get_a_share_quote`：查询一只 A 股、ETF 或 LOF 的最新行情。
- `get_a_share_batch_quotes`：一次查询最多 20 只 A 股、ETF、LOF；成功项目来自同一公开批量快照，个别无效代码单独返回错误。指数需要写成 `index:000300`，以免与同代码股票混淆。
- `get_a_share_kline`：查询最多 30 条近期股票、ETF 或 LOF K 线数据。
- `get_a_share_intraday`：查询最多 240 条当日一分钟股票、ETF 或 LOF 分时数据，并返回 5/15/30 分钟涨跌、日内高低点距离、均价偏离、相对开盘涨跌和成交速度等机械指标。
- `get_a_share_auction`：查询可核实的开盘集合竞价结果；公开源没有提供的竞价过程、撤单和未匹配委托会明确返回为空。
- `filter_a_share_securities`：按调用方指定的涨跌幅、成交额、换手率、是否高于均价和市值条件机械筛选普通 A 股，不加入评分或推荐。
- `get_a_share_fund_flow`：查询最多 10 个交易日的公开资金流估算。
- `get_a_share_financials`：查询最多 4 期公开财务关键指标。
- `get_a_share_news`：查询最多 10 条提及该股票代码的公开新闻。
- `get_a_share_sector_rankings`：按涨跌幅或成交额机械排列行业或概念板块；行业默认只返回二级分类，避免把二级、三级分类混在一起。
- `get_a_share_market_overview`：查询三大指数、主要风格指数、全市场广度、成交额、涨跌停/炸板统计和行业板块快照。
- `get_a_share_announcements`：查询最近 1—90 天的交易所官方公告，并按业绩、分红、回购、股东变动、解禁、停复牌、风险提示、监管、重大交易、融资和治理等公开关键词做机械标签。
- `get_a_share_relative_strength`：比较目标证券与基准指数、可选同类证券的当日涨跌幅差，最多合计 20 个标识；结果是当前百分比点差，不是预测。
- `scan_a_share_intraday_anomalies`：一次按调用方阈值扫描最多 20 个标识的日内异动，包括涨跌幅、量比、换手率、跳空、接近日内高低点和相对基准变化。
- `get_market_data_health`：查询本进程已观察到的公开源可用性、成功率、平均耗时、缓存状态和降级状态；它不会伪造一次实时探测，也不包含投资判断。

全部工具都标记为只读，不创建、修改或删除任何数据。

## 时间与稳定性

- 报价中的 `trade_date` 表示行情所属交易日，`quote_time` 表示该交易日内最后可能成交的时间，`source_updated_at` 表示数据源刷新快照的时间；收盘后的刷新时间不会再冒充成交时间。`queried_at` 仅表示本服务查询时间。
- 报价中的 `volume` 已统一换算为股，`volume_unit` 固定为 `share`；`turnover` 已统一换算为人民币元，`turnover_unit` 固定为 `CNY`。
- 代码会先识别证券类别和交易所，再转换为各数据源需要的格式。例如上交所 ETF `512760` 会使用 `sh512760`（腾讯/新浪）和 `1.512760`（东方财富），不会误当成深市股票。股票、ETF 和 LOF 名称搜索优先使用腾讯轻量接口，结果为空时自动回退新浪。
- K 线中的 `volume` 已统一换算为股，`volume_unit` 固定为 `share`；`turnover` 使用人民币元，`turnover_unit` 固定为 `CNY`。`latest_trade_date` 是最后一根 K 线所属交易日。K 线优先直连东方财富，失败时自动回退腾讯。
- 分钟线中的 `volume` 同样统一为股，`volume_unit` 固定为 `share`；`turnover_unit` 固定为 `CNY`。
- 分钟线的 `average_price` 会说明口径：东方财富提供时是来源报告的当日累计均价；腾讯回退没有该字段时，只能用已返回分钟数据计算窗口 VWAP，并会明确标注不是完整当日均价。午休不会虚构分钟线，最后一根若与当前交易分钟相同会标记为 `is_current_minute_unfinished`。
- `return_from_open_pct` 固定使用来源提供的正式开盘价，即使最多 240 条的返回窗口从 09:31 或 09:32 才开始也不会改变基准；窗口首条的变化另列为 `return_from_first_returned_minute_pct`。`opening_price`、`opening_price_scope` 和 `first_returned_minute_time` 会同时说明口径。全日 `high`/`low` 也会在截取窗口前保留。
- 分钟线会在截取数量和计算指标之前强制过滤为 `09:30—11:30`、`13:00—15:00`（Asia/Shanghai）。来源在收盘后重复生成的 `15:01`、`15:11` 等记录不会进入结果，也不会参与 5/15/30 分钟涨跌、高低点和成交速度计算。
- 分时请求会同时尝试东方财富和腾讯，优先返回最先取得有效交易分钟的一路；根据 Render 线上实测延迟，总等待预算为 9 秒，成功结果缓存 15 秒。若两个实时源同时短暂失败，可在 120 秒内返回最近一次成功结果，但会强制标记 `is_stale=true`、`stale_reason=live_sources_failed_using_recent_cache`，不会冒充实时数据。
- 大盘概览会分别获取三大指数（上证、深证、创业板）和风格指数（科创 50、沪深 300、中证 500/1000/2000、上证 50、中证红利）；行业板块会快速依次尝试东方财富的多个公开入口，临时不可用时再从全市场公开行情计算，避免因单一入口或指数来源切换而返回空数组。
- 大盘概览的指数、行业板块和全市场宽度并发获取；根据 Render 到国内公开源的实测延迟，总响应预算为 9 秒，成功组件缓存 30 秒。慢组件超过预算时优先使用最近一次成功缓存，否则明确返回 `unavailable_within_response_budget`。`component_status` 会逐项说明是实时、有效缓存、旧缓存还是本次未取得。
- 指数和全市场快照保留 `source_updated_at` 作为来源刷新时间，但 `market_time` 会限制在真实交易时段内；例如收盘后 16:14 刷新的快照，其行情时间仍为 15:00。
- 腾讯指数回退一次请求主要指数和风格指数，并补充指数日内 `high`/`low`；取得 3 个主要指数和至少 6 个风格指数后即可返回，不再等待较慢来源。若腾讯不提供中证 2000，会在 `missing_style_index_symbols` 中列出 `932000`，并把 `style_indices_status` 标成 `partial_data`。
- 行业板块默认会合并同名的二级、三级行业，优先保留层级更高的记录，并返回 `industry_name` 与 `industry_level` 说明层级。
- 市场广度默认只统计普通 A 股，不含 ETF、基金、B 股、退市整理股票和没有现价的行；沪市、深市、北交所及全市场都会分别给出上涨、下跌、平盘、±3/5/7%、涨跌停、炸板和 ST 涨跌停数量。
- 全市场宽度并发尝试东方财富三个轻量入口，用一次请求取得沪、深、北三市 A 股上涨/下跌/平盘家数与成交额；主响应不再启动约 56 页的个股明细抓取，避免后台请求堆积拖慢后续调用。
- `turnover.previous_trade_day_same_time` 目前会明确返回 `null`：公开来源没有稳定的上一交易日全市场同时间序列，服务不会用前一天的全天成交额冒充可比数据。`estimated_full_day` 只是按已交易分钟数的机械估算。
- 板块快照提供成交额、上涨/下跌家数和来源报告的领涨成分股；5/15/30 分钟动量目前不提供排名，因为公开板块分钟线接口不稳定，不能用不完整样本冒充完整排行。
- 快速全市场聚合只保证沪、深、北三市上涨/下跌/平盘家数与成交额；需要逐只股票扫描的 ±3/5/7%、涨跌停和炸板字段会列入 `unavailable_breadth_detail_fields`，`market_breadth_detail_status` 标为 `aggregate_only`。主响应不会重新抓取约 56 页个股明细拖慢交易时段调用。
- 批量报价的 `market_time_range` 说明同一批公开请求内各证券的来源更新时间范围，`queried_at` 是本服务统一完成查询的时间。单个失败不会影响同批其他结果。
- 所有 MCP 工具统一返回 `ok`、`market_status`、`trade_date`、`market_time`、`queried_at`、`source`、`source_errors`、`is_stale`、`stale_reason`、`data_age_seconds`、`latency_ms` 和嵌套的 `data`。原有的主要字段仍保留在顶层，避免旧调用失效。
- `source_errors` 是结构化的部分失败记录，会带来源、错误类别和说明；一个备用来源失败不会丢掉其他来源已经取得的有效数据。
- 实时类工具只缓存成功结果，报价/批量报价为 2 秒，大盘概览为 5 秒，分时为 15 秒；命中缓存时会明确返回 `cache_hit`、`cache_created_at` 和 `cache_age_seconds`，不会把缓存伪装为新请求。
- 每个公开上游请求默认 3 秒超时；单只报价同时尝试东方财富、腾讯和新浪，优先返回最先成功的结果，总等待预算为 6 秒。
- 条件筛选当前只覆盖普通 A 股；`above_average_price=true` 使用公开快照的成交额除以成交量计算 VWAP，不会把它解释成买卖信号。
- 竞价工具只提供来源可核实的开盘价及相对昨收变化。9:15—9:25 的逐笔价格、竞价成交额、未匹配买卖单和撤单变化不属于当前公开稳定数据，均保持为空。
- 公告优先且只使用交易所官方披露：沪市来自上交所，深市来自深交所。公告事件日期是披露日期，机械标签不代表事件实际发生日，也不构成重要性判断。北交所公开页面目前会拦截云端请求，因此会明确返回 `data_status=unavailable`，不会拿第三方公告冒充官方数据。
- 异动扫描是“调用时立即扫描”，不会在后台持续监听或主动推送。它使用同批行情快照；其中量比是来源提供的当日量比，不会伪装成 5 分钟突然放量。
- 相对强弱只比较当前快照的涨跌幅百分比点差，可指定 `index:000001` 等基准和同类标的；正负仅表示相对表现，不代表买卖建议。

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

当前以腾讯、新浪和东方财富公开接口作为行情和资讯来源，官方公告使用上海证券交易所和深圳证券交易所公开接口，并在市场总览的行业快照降级路径中保留 `efinance` 计算。免费公开数据源可能延迟、限流或临时不可用；新闻检索可能包含仅提及该股票代码的文章。服务会明确返回数据来源、部分失败、数据年龄、缓存标记和过期状态，以便区分行情时间与服务查询时间。

东方财富 Choice SDK 暂未接入线上服务。数据仅供信息参考，不构成投资建议。
