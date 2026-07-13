import importlib
import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


fake_efinance = types.ModuleType("efinance")
fake_efinance.stock = types.SimpleNamespace()
sys.modules.setdefault("efinance", fake_efinance)

fake_pandas = types.ModuleType("pandas")
fake_pandas.Series = object
fake_pandas.DataFrame = object
fake_pandas.isna = lambda value: value is None
sys.modules.setdefault("pandas", fake_pandas)

market_app = importlib.import_module("app")


def rpc_request(request_id: int, method: str, params: dict | None = None) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params or {},
    }


def fake_get_quote_data(symbol: str) -> dict:
    return {
        "quote": {
            "symbol": symbol,
            "name": "Test Stock",
            "price": 123.45,
            "change": 1.23,
            "change_pct": 1.01,
            "open": 122.0,
            "high": 124.0,
            "low": 121.5,
            "previous_close": 122.22,
            "volume": 1000000,
            "volume_unit": "share",
            "turnover": 123456789,
            "turnover_unit": "CNY",
            "pe_dynamic": 99.9,
            "total_market_value": 999999999,
            "trade_date": "2026-07-10",
            "quote_time": "2026-07-10T15:00:00+08:00",
            "source_updated_at": "2026-07-10T16:14:42+08:00",
        },
        "source": "test",
        "queried_at": "2026-07-10T00:05:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_search_stock_data(keyword: str, limit: int) -> dict:
    return {
        "keyword": keyword,
        "count": 1,
        "results": [{"symbol": "603993", "name": "洛阳钼业", "market": "沪A"}],
        "source": "tencent_search",
        "queried_at": "2026-07-10T00:05:00+00:00",
    }


def fake_get_kline_data(symbol: str, period: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "period": period,
        "count": limit,
        "items": [
            {
                "date": "2026-07-10",
                "close": 123.45,
                "volume": 100000,
                "volume_unit": "share",
                "turnover": None,
                "turnover_unit": "CNY",
            }
        ],
        "source": "tencent",
        "latest_trade_date": "2026-07-10",
        "queried_at": "2026-07-10T00:05:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_intraday_data(symbol: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "name": "Test Stock",
        "count": limit,
        "items": [{"time": "2026-07-10 09:30", "price": 123.45}],
        "source": "test",
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_fund_flow_data(symbol: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "name": "Test Stock",
        "count": limit,
        "items": [{"date": "2026-07-10", "main_net_inflow": 1000}],
        "source": "test",
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_financial_data(symbol: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "name": "Test Stock",
        "count": limit,
        "items": [{"report_period": "2026-03-31", "basic_eps": 1.23}],
        "source": "test",
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_news_data(symbol: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "count": limit,
        "items": [{"title": "Test news", "url": "https://example.com/news"}],
        "source": "test",
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_market_overview_data(limit: int) -> dict:
    return {
        "indices": [{"name": "Test Index", "price": 100}],
        "industry_boards": [],
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def test_kline_source_parsers() -> None:
    original_reader = market_app.read_public_json
    original_eastmoney = market_app.get_eastmoney_kline
    original_tencent = market_app.get_tencent_kline
    try:
        market_app.read_public_json = lambda *_: {
            "data": {
                "klines": [
                    "2026-07-10,120.0,123.45,124.0,119.5,1000,123456,3.5,1.2,1.45,0.6"
                ]
            }
        }
        eastmoney = market_app.get_eastmoney_kline("600519", "daily", 101, 1)
        assert eastmoney["source"] == "eastmoney"
        assert eastmoney["latest_trade_date"] == "2026-07-10"
        assert eastmoney["items"][0]["close"] == 123.45
        assert eastmoney["items"][0]["volume"] == 100000
        assert eastmoney["items"][0]["volume_unit"] == "share"
        assert eastmoney["items"][0]["turnover_unit"] == "CNY"

        market_app.read_public_json = lambda *_: {
            "data": {
                "sh600519": {
                    "qfqday": [["2026-07-10", "120.0", "123.45", "124.0", "119.5", "1000"]]
                }
            }
        }
        tencent = market_app.get_tencent_kline("600519", "daily", 1)
        assert tencent["source"] == "tencent"
        assert tencent["latest_trade_date"] == "2026-07-10"
        assert tencent["items"][0]["close"] == 123.45
        assert tencent["items"][0]["volume"] == 100000
        assert tencent["items"][0]["volume_unit"] == "share"

        market_app.get_eastmoney_kline = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="connection closed")
        )
        market_app.get_tencent_kline = lambda *_: tencent
        fallback = market_app.get_fallback_kline("600519", "daily", 101, 1)
        assert fallback["source"] == "tencent"
    finally:
        market_app.read_public_json = original_reader
        market_app.get_eastmoney_kline = original_eastmoney
        market_app.get_tencent_kline = original_tencent


def test_search_source_parser() -> None:
    original_text_reader = market_app.read_market_text
    try:
        market_app.read_market_text = lambda *_: (
            r'v_hint="sh~603993~\u6d1b\u9633\u94bc\u4e1a~lymy~GP-A'
            r'^hk~03993~\u6d1b\u9633\u94bc\u4e1a~lymy~GP";'
        )
        result = market_app.search_stock_data("洛阳钼业", 5)
        assert result["source"] == "tencent_search"
        assert result["count"] == 1
        assert result["results"][0]["symbol"] == "603993"
        assert result["results"][0]["name"] == "洛阳钼业"

        market_app.read_market_text = lambda url, *_: (
            'var suggestdata="洛阳钼业,11,603993,sh603993,洛阳钼业,,洛阳钼业,99,1,ESG,,";'
            if "suggest3.sinajs.cn" in url
            else 'v_hint="";'
        )
        sina_fallback = market_app.search_stock_data("洛阳钼业", 5)
        assert sina_fallback["source"] == "sina_search"
        assert sina_fallback["count"] == 1
        assert sina_fallback["results"][0]["symbol"] == "603993"
    finally:
        market_app.read_market_text = original_text_reader


def test_etf_market_routing_and_search() -> None:
    etf = market_app.security_metadata("512760")
    assert etf["security_type"] == "etf"
    assert etf["exchange"] == "SSE"
    assert market_app.market_symbol("512760") == "sh512760"
    assert market_app.eastmoney_secid("512760") == "1.512760"

    assert market_app.security_metadata("159915")["security_type"] == "etf"
    assert market_app.market_symbol("159915") == "sz159915"
    assert market_app.eastmoney_secid("159915") == "0.159915"
    assert market_app.security_metadata("501050")["security_type"] == "lof"

    original_text_reader = market_app.read_market_text
    original_json_reader = market_app.read_public_json
    try:
        market_app.read_market_text = lambda *_: (
            r'v_hint="sh~512760~\u56fd\u6cf0\u534a\u5bfc\u4f53ETF~gtbdt~ETF";'
        )
        tencent = market_app.search_tencent_stock("512760", 5)
        assert tencent == [
            {
                "symbol": "512760",
                "name": "国泰半导体ETF",
                "market": "Shanghai Stock Exchange",
                "security_type": "etf",
            }
        ]

        market_app.read_market_text = lambda *_: (
            'var suggestdata="国泰半导体ETF,14,512760,sh512760,国泰半导体ETF,,国泰半导体ETF,99,1,,";'
        )
        sina = market_app.search_sina_stock("512760", 5)
        assert sina[0]["symbol"] == "512760"
        assert sina[0]["security_type"] == "etf"

        quote_urls: list[str] = []
        market_app.read_market_text = lambda url, *_: (
            quote_urls.append(url)
            or 'var hq_str_sh512760="Test ETF,10.00,10.10,10.20,10.30,9.90,0,0,100,1000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-07-10,15:00:00";'
        )
        quote = market_app.get_sina_quote("512760")
        assert quote["symbol"] == "512760"
        assert quote_urls == ["http://hq.sinajs.cn/list=sh512760"]

        kline_urls: list[str] = []
        market_app.read_public_json = lambda url, *_: (
            kline_urls.append(url)
            or {
                "data": {
                    "sh512760": {
                        "qfqday": [["2026-07-10", "10.0", "10.2", "10.3", "9.9", "1000"]]
                    }
                }
            }
        )
        kline = market_app.get_tencent_kline("512760", "daily", 1)
        assert kline["items"][0]["close"] == 10.2
        assert "sh512760" in kline_urls[0]
    finally:
        market_app.read_market_text = original_text_reader
        market_app.read_public_json = original_json_reader


def test_quote_unit_normalization() -> None:
    result = market_app.normalize_quote_units(
        {"volume": 2603164, "turnover": 458798}, "tencent"
    )
    assert result["volume"] == 260316400
    assert result["volume_unit"] == "share"
    assert result["turnover"] == 4587980000
    assert result["turnover_unit"] == "CNY"


def test_quote_timestamp_semantics() -> None:
    result = market_app.derive_quote_timestamps("2026-07-10T16:14:42+08:00")
    assert result["trade_date"] == "2026-07-10"
    assert result["quote_time"] == "2026-07-10T15:00:00+08:00"
    assert result["source_updated_at"] == "2026-07-10T16:14:42+08:00"


def test_industry_board_parser() -> None:
    original_json = market_app.read_public_json
    try:
        calls: list[str] = []

        def read_with_host_fallback(url: str, *_: object) -> dict:
            calls.append(url)
            if "push2.eastmoney.com" in url:
                raise market_app.HTTPException(status_code=502, detail="blocked")
            return {
                "data": {
                    "diff": [
                        {
                            "f12": "BK0001",
                            "f14": "Test Industry",
                            "f2": 100.5,
                            "f3": 2.3,
                            "f4": 2.25,
                        }
                    ]
                }
            }

        market_app.read_public_json = read_with_host_fallback
        boards = market_app.get_eastmoney_industry_boards(5)
        assert "push2.eastmoney.com" in calls[0]
        assert "push2delay.eastmoney.com" in calls[1]
        assert boards == [
            {
                "symbol": "BK0001",
                "name": "Test Industry",
                "price": 100.5,
                "change_pct": 2.3,
                "change": 2.25,
            }
        ]

        market_app.read_public_json = lambda *_: {
            "data": {
                "diff": [
                    {"f12": "BK0001", "f14": "Test Industry", "f2": 100.5, "f3": 2.3, "f4": 2.25}
                ]
            }
        }
        boards = market_app.get_eastmoney_industry_boards(5)
        assert boards == [
            {
                "symbol": "BK0001",
                "name": "Test Industry",
                "price": 100.5,
                "change_pct": 2.3,
                "change": 2.25,
            }
        ]
    finally:
        market_app.read_public_json = original_json


def test_intraday_and_index_fallback_parsers() -> None:
    original_json = market_app.read_public_json
    original_text = market_app.read_market_text
    original_eastmoney_intraday = market_app.get_eastmoney_intraday
    original_tencent_intraday = market_app.get_tencent_intraday
    original_eastmoney_indices = market_app.get_eastmoney_indices
    original_tencent_indices = market_app.get_tencent_indices
    original_eastmoney_industry_boards = market_app.get_eastmoney_industry_boards
    original_eastmoney_fund_flow = market_app.get_eastmoney_fund_flow
    original_sina_fund_flow = market_app.get_sina_fund_flow
    original_sina_quote = market_app.get_sina_quote
    original_all_realtime_quotes = market_app.get_all_realtime_quotes
    try:
        market_app.get_eastmoney_intraday = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="blocked")
        )
        market_app.read_public_json = lambda *_: {
            "data": {
                "sh600519": {
                    "data": {
                        "date": "20260710",
                        "data": ["0930 10.00 2 2000", "0931 10.10 5 5030"],
                    },
                    "qt": {"sh600519": ["1", "Test", "600519", "10.10", "9.90"]},
                }
            }
        }
        intraday = market_app.get_intraday_data("600519", 2)
        assert intraday["source"] == "tencent"
        assert intraday["items"][1]["volume"] == 300
        assert intraday["items"][1]["turnover"] == 3030
        assert intraday["items"][1]["volume_unit"] == "share"

        market_app.read_public_json = lambda *_: {
            "data": {"sh600519": {"data": {"date": "20261399", "data": ["0930 10 2 2"]}}}
        }
        try:
            market_app.get_tencent_intraday("600519", 1)
            raise AssertionError("Expected malformed Tencent date to fail.")
        except market_app.HTTPException as exc:
            assert exc.status_code == 502

        market_app.get_eastmoney_intraday = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=404, detail="not found")
        )
        market_app.get_tencent_intraday = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=404, detail="not found")
        )
        try:
            market_app.get_intraday_data("600519", 1)
            raise AssertionError("Expected all-not-found intraday sources to return 404.")
        except market_app.HTTPException as exc:
            assert exc.status_code == 404
        market_app.get_tencent_intraday = original_tencent_intraday

        market_app.get_eastmoney_indices = lambda: []
        market_app.get_eastmoney_industry_boards = lambda _: [
            {"symbol": "BK0001", "name": "Test Industry", "price": 100, "change_pct": 1.5, "change": 1.48}
        ]
        market_app.get_all_realtime_quotes = lambda: market_app.pd.DataFrame()
        fields = [""] * 33
        fields[1:6] = ["Test Index", "000001", "100.0", "99.0", "99.5"]
        fields[30:33] = ["20260710150000", "1.0", "1.01"]
        market_app.read_market_text = lambda *_: f'v_sh000001="{"~".join(fields)}";'
        overview = market_app.get_market_overview_data(3)
        assert overview["index_source"] == "tencent"
        assert overview["indices"][0]["change"] == 1.0
        assert overview["indices"][0]["change_pct"] == 1.01
        assert overview["industry_board_source"] == "eastmoney_industry"
        assert overview["industry_boards"][0]["name"] == "Test Industry"

        market_app.read_market_text = lambda *_: 'v_sh000001="' + "~".join([""] * 32) + '";'
        try:
            market_app.get_tencent_indices()
            raise AssertionError("Expected short Tencent index row to fail.")
        except market_app.HTTPException as exc:
            assert exc.status_code == 502

        market_app.get_tencent_indices = lambda: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="blocked")
        )
        market_app.read_market_text = lambda *_: (
            'var hq_str_s_sh000001="Test Index,100.0,1.0,1.01,0,0";'
        )
        overview = market_app.get_market_overview_data(3)
        assert overview["index_source"] == "sina"
        assert overview["indices"][0]["price"] == 100.0

        market_app.get_eastmoney_fund_flow = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="blocked")
        )
        market_app.read_market_text = lambda *_: (
            '({r0_in:"3000",r0_out:"1000",netamount:"1500",name:"Test",'
            'trade:"10.10",changeratio:"0.01"});'
        )
        market_app.get_sina_quote = lambda *_: {
            "source_updated_at": "2026-07-10T15:00:00+08:00"
        }
        fund_flow = market_app.get_fund_flow_data("600519", 5)
        assert fund_flow["source"] == "sina"
        assert fund_flow["data_status"] == "partial_data"
        assert fund_flow["count"] == 1
        assert fund_flow["items"][0]["main_net_inflow"] == 2000
        assert fund_flow["items"][0]["change_pct"] == 1.0

        market_app.get_eastmoney_fund_flow = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=404, detail="not found")
        )
        market_app.get_sina_fund_flow = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=404, detail="not found")
        )
        try:
            market_app.get_fund_flow_data("600519", 1)
            raise AssertionError("Expected all-not-found fund-flow sources to return 404.")
        except market_app.HTTPException as exc:
            assert exc.status_code == 404
    finally:
        market_app.read_public_json = original_json
        market_app.read_market_text = original_text
        market_app.get_eastmoney_intraday = original_eastmoney_intraday
        market_app.get_tencent_intraday = original_tencent_intraday
        market_app.get_eastmoney_indices = original_eastmoney_indices
        market_app.get_tencent_indices = original_tencent_indices
        market_app.get_eastmoney_industry_boards = original_eastmoney_industry_boards
        market_app.get_eastmoney_fund_flow = original_eastmoney_fund_flow
        market_app.get_sina_fund_flow = original_sina_fund_flow
        market_app.get_sina_quote = original_sina_quote
        market_app.get_all_realtime_quotes = original_all_realtime_quotes


def main() -> None:
    test_kline_source_parsers()
    test_search_source_parser()
    test_etf_market_routing_and_search()
    test_quote_unit_normalization()
    test_quote_timestamp_semantics()
    test_industry_board_parser()
    test_intraday_and_index_fallback_parsers()
    market_app.search_stock_data = fake_search_stock_data
    market_app.get_quote_data = fake_get_quote_data
    market_app.get_kline_data = fake_get_kline_data
    market_app.get_intraday_data = fake_get_intraday_data
    market_app.get_fund_flow_data = fake_get_fund_flow_data
    market_app.get_financial_data = fake_get_financial_data
    market_app.get_news_data = fake_get_news_data
    market_app.get_market_overview_data = fake_get_market_overview_data
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }

    with TestClient(market_app.app, base_url="http://127.0.0.1:8000") as client:
        health = client.get("/health")
        assert health.status_code == 200, health.text

        for legacy_path in (
            "/search?keyword=600000",
            "/quote?symbol=600000",
            "/kline?symbol=600000&period=daily",
        ):
            legacy = client.get(legacy_path)
            assert legacy.status_code == 404, legacy.text

        initialize = client.post(
            "/mcp",
            headers=headers,
            json=rpc_request(
                1,
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "mcp-test", "version": "0.1.0"},
                },
            ),
        )
        assert initialize.status_code == 200, initialize.text

        protocol_version = initialize.json()["result"]["protocolVersion"]
        headers["MCP-Protocol-Version"] = protocol_version

        tools = client.post(
            "/mcp",
            headers=headers,
            json=rpc_request(2, "tools/list"),
        )
        assert tools.status_code == 200, tools.text
        registered_tools = tools.json()["result"]["tools"]
        names = {tool["name"] for tool in registered_tools}
        assert names == {
            "search_a_share",
            "get_a_share_quote",
            "get_a_share_kline",
            "get_a_share_intraday",
            "get_a_share_fund_flow",
            "get_a_share_financials",
            "get_a_share_news",
            "get_a_share_market_overview",
        }
        assert all(tool["annotations"]["readOnlyHint"] is True for tool in registered_tools)

        search = client.post(
            "/mcp",
            headers=headers,
            json=rpc_request(
                3,
                "tools/call",
                {"name": "search_a_share", "arguments": {"keyword": "洛阳钼业"}},
            ),
        )
        assert search.status_code == 200, search.text
        search_result = search.json()["result"]["structuredContent"]
        assert search_result["ok"] is True
        assert search_result["results"][0]["symbol"] == "603993"

        quote = client.post(
            "/mcp",
            headers=headers,
            json=rpc_request(
                4,
                "tools/call",
                {"name": "get_a_share_quote", "arguments": {"symbol": "600519"}},
            ),
        )
        assert quote.status_code == 200, quote.text
        result = quote.json()["result"]["structuredContent"]
        assert result["ok"] is True
        assert result["symbol"] == "600519"
        assert "time" not in result
        assert "market_time" not in result
        assert result["trade_date"] == "2026-07-10"
        assert result["quote_time"] == "2026-07-10T15:00:00+08:00"
        assert result["source_updated_at"] == "2026-07-10T16:14:42+08:00"
        assert result["queried_at"] == "2026-07-10T00:05:00+00:00"
        assert result["volume_unit"] == "share"
        assert result["turnover_unit"] == "CNY"
        assert "pe_dynamic" not in result
        assert "total_market_value" not in result

        for request_id, tool_name, arguments in (
            (5, "get_a_share_kline", {"symbol": "600519"}),
            (6, "get_a_share_intraday", {"symbol": "600519"}),
            (7, "get_a_share_fund_flow", {"symbol": "600519"}),
            (8, "get_a_share_financials", {"symbol": "600519"}),
            (9, "get_a_share_news", {"symbol": "600519"}),
            (10, "get_a_share_market_overview", {}),
        ):
            response = client.post(
                "/mcp",
                headers=headers,
                json=rpc_request(
                    request_id,
                    "tools/call",
                    {"name": tool_name, "arguments": arguments},
                ),
            )
            assert response.status_code == 200, response.text
            assert response.json()["result"]["structuredContent"]["ok"] is True

    assert market_app.format_market_time("20260710150146") == "2026-07-10T15:01:46+08:00"

    print("MCP handshake, tool discovery, and read-only market-tool tests passed.")


if __name__ == "__main__":
    main()
