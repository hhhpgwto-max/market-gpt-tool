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
            "turnover": 123456789,
            "pe_dynamic": 99.9,
            "total_market_value": 999999999,
            "market_time": "2026-07-10T15:01:46+08:00",
        },
        "source": "test",
        "queried_at": "2026-07-10T00:05:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_kline_data(symbol: str, period: str, limit: int) -> dict:
    return {
        "symbol": symbol,
        "period": period,
        "count": limit,
        "items": [{"date": "2026-07-10", "close": 123.45}],
        "source": "tencent",
        "latest_market_time": "2026-07-10",
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
        assert eastmoney["latest_market_time"] == "2026-07-10"
        assert eastmoney["items"][0]["close"] == 123.45

        market_app.read_public_json = lambda *_: {
            "data": {
                "sh600519": {
                    "qfqday": [["2026-07-10", "120.0", "123.45", "124.0", "119.5", "1000"]]
                }
            }
        }
        tencent = market_app.get_tencent_kline("600519", "daily", 1)
        assert tencent["source"] == "tencent"
        assert tencent["latest_market_time"] == "2026-07-10"
        assert tencent["items"][0]["close"] == 123.45

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


def main() -> None:
    test_kline_source_parsers()
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

        quote = client.post(
            "/mcp",
            headers=headers,
            json=rpc_request(
                3,
                "tools/call",
                {"name": "get_a_share_quote", "arguments": {"symbol": "600519"}},
            ),
        )
        assert quote.status_code == 200, quote.text
        result = quote.json()["result"]["structuredContent"]
        assert result["ok"] is True
        assert result["symbol"] == "600519"
        assert "time" not in result
        assert result["market_time"] == "2026-07-10T15:01:46+08:00"
        assert result["queried_at"] == "2026-07-10T00:05:00+00:00"
        assert "pe_dynamic" not in result
        assert "total_market_value" not in result

        for request_id, tool_name, arguments in (
            (4, "get_a_share_kline", {"symbol": "600519"}),
            (5, "get_a_share_intraday", {"symbol": "600519"}),
            (6, "get_a_share_fund_flow", {"symbol": "600519"}),
            (7, "get_a_share_financials", {"symbol": "600519"}),
            (8, "get_a_share_news", {"symbol": "600519"}),
            (9, "get_a_share_market_overview", {}),
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
