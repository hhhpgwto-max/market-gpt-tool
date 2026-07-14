import importlib
import sys
import types
from pathlib import Path
from threading import Event
from time import sleep

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


def fake_get_batch_quote_data(symbols: list[str]) -> dict:
    return {
        "requested_count": len(symbols),
        "count": 1,
        "results": [
            {
                "symbol": symbols[0],
                "name": "Test Security",
                "security_type": "etf",
                "exchange": "SSE",
                "price": 1.23,
                "volume": 100000,
                "volume_unit": "share",
                "turnover": 123000,
                "turnover_unit": "CNY",
                "market_time": "2026-07-10T15:00:00+08:00",
                "source": "test",
            }
        ],
        "errors": [],
        "source": ["test"],
        "source_errors": [],
        "market_time": "2026-07-10T15:00:00+08:00",
        "queried_at": "2026-07-10T00:00:00+00:00",
        "data_status": "full_data",
    }


def fake_get_auction_data(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "opening_price": 123.45,
        "auction_turnover": None,
        "data_status": "partial_data",
        "source": "test",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_filter_a_share_securities_data(**_: object) -> dict:
    return {
        "matched_count": 1,
        "returned_count": 1,
        "conditions": {"security_type": "stock"},
        "results": [{"symbol": "600519", "name": "Test Stock"}],
        "source": ["test"],
        "market_time": "2026-07-10T15:00:00+08:00",
        "queried_at": "2026-07-10T00:00:00+00:00",
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
        "adjustment": "forward_adjusted",
        "adjustment_source_parameter": "test_qfq",
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


def fake_get_announcement_data(symbol: str, days: int, limit: int) -> dict:
    return {
        "symbol": symbol,
        "exchange": "SSE",
        "period": {"start": "2026-07-01", "end": "2026-07-10"},
        "count": 1,
        "items": [
            {
                "title": "Test official announcement",
                "event_tags": ["other"],
                "url": "https://static.sse.com.cn/test.pdf",
            }
        ][:limit],
        "source": ["official_sse_announcements"],
        "source_errors": [],
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_historical_context_data(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "adjustment": "forward_adjusted",
        "source_sessions": 260,
        "windows": {
            str(window): {"requested_sessions": window, "window_complete": True}
            for window in (20, 60, 120, 250)
        },
        "source": "test",
        "source_errors": [],
        "latest_trade_date": "2026-07-10",
    }


def fake_get_security_status_data(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "security_type": "a_share",
        "listing_date": "2001-08-27",
        "price_history_adjustment": {"mode": "forward_adjusted"},
        "source": ["test"],
        "source_errors": [],
    }


def fake_get_decision_context_data(
    symbol: str, benchmark_symbol: str | None
) -> dict:
    return {
        "snapshot_id": f"{symbol}-test",
        "symbol": symbol,
        "benchmark_identifier": benchmark_symbol or "index:000001",
        "available_component_count": 8,
        "requested_component_count": 8,
        "decision_inputs": {
            "quote": {"symbol": symbol},
            "historical_context": fake_get_historical_context_data(symbol),
        },
        "excluded_components": {
            "news": "excluded_pending_relevance_deduplication_and_source_quality_upgrade"
        },
        "source": ["test"],
        "source_errors": [],
        "data_status": "full_data",
    }


def fake_get_relative_strength_data(
    symbol: str,
    benchmark_symbol: str | None,
    peer_symbols: list[str] | None,
) -> dict:
    return {
        "symbol": symbol,
        "benchmark_identifier": benchmark_symbol or "index:000001",
        "peer_count": len(peer_symbols or []),
        "relative_to_benchmark_pct_points": 1.5,
        "relative_status": "outperforming_benchmark",
        "source": ["test"],
        "source_errors": [],
        "market_time": "2026-07-10T15:00:00+08:00",
    }


def fake_scan_intraday_anomalies_data(**parameters: object) -> dict:
    symbols = parameters["symbols"]
    return {
        "requested_count": len(symbols),
        "evaluated_count": len(symbols),
        "triggered_count": 1,
        "results": [{"symbol": symbols[0], "trigger_count": 1}],
        "source": ["test"],
        "source_errors": [],
        "market_time": "2026-07-10T15:00:00+08:00",
    }


def fake_get_market_overview_data(limit: int) -> dict:
    return {
        "indices": [{"name": "Test Index", "price": 100}],
        "industry_boards": [],
        "time": "2026-07-10T00:00:00+00:00",
        "note": "For information only. Not investment advice.",
    }


def fake_get_sector_rankings_data(sector_type: str, level: str, sort_by: str, limit: int) -> dict:
    return {
        "sector_type": sector_type,
        "level": level,
        "sort_by": sort_by,
        "count": 1,
        "items": [{"symbol": "BK0001", "name": "Test Sector", "change_pct": 1.2}],
        "source": ["test"],
        "source_errors": [],
        "queried_at": "2026-07-10T00:00:00+00:00",
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

        eastmoney_started = Event()
        release_eastmoney = Event()

        def blocked_eastmoney(*_: object) -> dict:
            eastmoney_started.set()
            release_eastmoney.wait(1)
            raise market_app.HTTPException(status_code=502, detail="blocked failure")

        market_app.get_eastmoney_kline = blocked_eastmoney
        try:
            with market_app.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    market_app.get_fallback_kline, "600519", "daily", 101, 1
                )
                assert eastmoney_started.wait(0.5)
                fallback = future.result(timeout=0.5)
                assert fallback["source"] == "tencent"
        finally:
            release_eastmoney.set()
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
        calls: list[tuple[str, tuple[object, ...]]] = []

        def read_with_host_fallback(url: str, *_: object) -> dict:
            calls.append((url, _))
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
        assert "push2.eastmoney.com" in calls[0][0]
        assert "push2delay.eastmoney.com" in calls[1][0]
        assert calls[0][1][-2:] == (3, 1)
        assert calls[1][1][-2:] == (3, 1)
        assert boards[0]["symbol"] == "BK0001"
        assert boards[0]["sector_type"] == "industry"
        assert boards[0]["industry_name"] == "Test Industry"
        assert boards[0]["industry_level"] is None
        assert boards[0]["price"] == 100.5
        assert boards[0]["current"] == 100.5
        assert boards[0]["change_pct"] == 2.3
        assert boards[0]["turnover"] is None
        assert boards[0]["top_constituents"] == []

        market_app.read_public_json = lambda *_: {
            "data": {
                "diff": [
                    {"f12": "BK0001", "f14": "Test Industry", "f2": 100.5, "f3": 2.3, "f4": 2.25}
                ]
            }
        }
        boards = market_app.get_eastmoney_industry_boards(5)
        assert boards[0]["name"] == "Test Industry"
        assert boards[0]["momentum_15m"] is None
    finally:
        market_app.read_public_json = original_json


def test_industry_board_deduplication() -> None:
    boards = market_app.deduplicate_industry_boards(
        [
            {"name": "中药Ⅲ", "industry_name": "中药", "industry_level": "Ⅲ", "change_pct": 3.26},
            {"name": "中药Ⅱ", "industry_name": "中药", "industry_level": "Ⅱ", "change_pct": 3.26},
            {"name": "油气开采Ⅲ", "industry_name": "油气开采", "industry_level": "Ⅲ", "change_pct": 2.17},
            {"name": "油气开采Ⅱ", "industry_name": "油气开采", "industry_level": "Ⅱ", "change_pct": 2.17},
            {"name": "国有大型银行Ⅲ", "industry_name": "国有大型银行", "industry_level": "Ⅲ", "change_pct": 2.07},
            {"name": "城商行Ⅲ", "industry_name": "城商行", "industry_level": "Ⅲ", "change_pct": 1.69},
            {"name": "农商行Ⅲ", "industry_name": "农商行", "industry_level": "Ⅲ", "change_pct": 1.69},
        ],
        5,
    )
    assert [board["name"] for board in boards] == [
        "中药Ⅱ",
        "油气开采Ⅱ",
        "国有大型银行Ⅲ",
        "城商行Ⅲ",
        "农商行Ⅲ",
    ]


def test_market_structure_calculations() -> None:
    rows = [
        {"symbol": "600001", "name": "Test Main", "price": 11.0, "change_pct": 10.0, "turnover": 100.0, "high": 11.0, "previous_close": 10.0},
        {"symbol": "300001", "name": "Test Growth", "price": 12.0, "change_pct": 20.0, "turnover": 200.0, "high": 12.0, "previous_close": 10.0},
        {"symbol": "430001", "name": "Test BSE", "price": 7.0, "change_pct": -30.0, "turnover": 300.0, "high": 8.0, "previous_close": 10.0},
        {"symbol": "600002", "name": "*ST Test", "price": 10.5, "change_pct": 5.0, "turnover": 150.0, "high": 10.5, "previous_close": 10.0},
        {"symbol": "600003", "name": "Test Open Board", "price": 10.5, "change_pct": 5.0, "turnover": 50.0, "high": 11.0, "previous_close": 10.0},
    ]
    breadth = market_app.calculate_market_breadth(rows)
    totals = breadth["all_market"]
    assert totals["stock_count"] == 5
    assert totals["rise_count"] == 4
    assert totals["fall_count"] == 1
    assert totals["limit_up_count"] == 3
    assert totals["limit_down_count"] == 1
    assert totals["st_limit_up_count"] == 1
    assert totals["open_board_count"] == 1
    assert breadth["by_exchange"]["BSE"]["limit_down_count"] == 1

    turnover = market_app.market_turnover_summary(rows, "2026-07-10T11:30:00+08:00")
    assert turnover["current"] == 800.0
    assert turnover["estimated_full_day"] == 1600.0
    assert turnover["previous_trade_day_same_time"] is None
    assert turnover["top_turnover_securities"][0]["symbol"] == "430001"

    try:
        market_app.get_sector_rankings_data("industry", "2", "momentum_15m", 20)
        raise AssertionError("Expected unstable minute momentum ranking to be rejected.")
    except market_app.HTTPException as exc:
        assert exc.status_code == 400


def test_batch_quotes_intraday_indicators_and_filtering() -> None:
    original_json = market_app.read_public_json
    original_batch_rows = market_app.get_eastmoney_batch_quote_rows
    original_market_rows = market_app.get_eastmoney_market_quotes
    original_quote_data = market_app.get_quote_data
    try:
        market_app.read_public_json = lambda *_: {
            "data": {
                "diff": [
                    {
                        "f12": "512760",
                        "f13": 1,
                        "f14": "Test ETF",
                        "f2": 1.23,
                        "f3": 2.5,
                        "f4": 0.03,
                        "f5": 1000,
                        "f6": 123000,
                        "f7": 3.2,
                        "f8": 4.5,
                        "f10": 1.2,
                        "f15": 1.25,
                        "f16": 1.2,
                        "f17": 1.21,
                        "f18": 1.2,
                        "f20": 1000000000,
                        "f21": 1000000000,
                        "f124": 1783913737,
                    }
                ]
            }
        }
        batch = market_app.get_batch_quote_data(["512760", "not-a-code"])
        assert batch["count"] == 1
        assert batch["results"][0]["security_type"] == "etf"
        assert batch["results"][0]["volume"] == 100000
        assert batch["results"][0]["volume_unit"] == "share"
        assert batch["errors"][0]["code"] == "invalid_symbol"
        assert market_app.batch_security_metadata("index:000300")["security_type"] == "index"

        market_app.get_eastmoney_batch_quote_rows = lambda _securities: (
            [
                {"f12": "000001", "f13": 0, "f14": "Ping An Bank", "f2": 10},
                {"f12": "000001", "f13": 1, "f14": "SSE Index", "f2": 3900},
            ],
            "test-host",
        )
        collision = market_app.get_batch_quote_data(["000001", "index:000001"])
        assert collision["count"] == 2
        assert collision["results"][0]["name"] == "Ping An Bank"
        assert collision["results"][1]["name"] == "SSE Index"

        base_minute = market_app.datetime(2026, 7, 10, 9, 30)
        items = [
            {
                "time": (base_minute + market_app.timedelta(minutes=minute)).strftime(
                    "%Y-%m-%d %H:%M"
                ),
                "price": 10.0 + minute,
                "high": 10.0 + minute,
                "low": 10.0 + minute,
                "volume": 100,
                "turnover": (10.0 + minute) * 100,
                "average_price": 10.0 + minute / 2,
                "average_price_scope": "test",
            }
            for minute in range(31)
        ]
        indicators = market_app.intraday_mechanical_indicators(items)
        assert indicators["return_5m"] is not None
        assert indicators["return_15m"] is not None
        assert indicators["return_30m"] == 300.0
        assert indicators["at_intraday_high"] is True

        market_app.get_eastmoney_market_quotes = lambda: [
            {
                "symbol": "600001",
                "name": "Eligible Stock",
                "price": 11.0,
                "change_pct": 3.0,
                "volume": 1000.0,
                "turnover": 1000000.0,
                "turnover_rate": 3.0,
                "total_market_value": 10000000000.0,
                "market_time": "2026-07-10T10:00:00+08:00",
            },
            {
                "symbol": "600002",
                "name": "*ST Excluded",
                "price": 12.0,
                "change_pct": 3.0,
                "volume": 1000.0,
                "turnover": 1000000.0,
                "turnover_rate": 3.0,
                "total_market_value": 10000000000.0,
                "market_time": "2026-07-10T10:00:00+08:00",
            },
        ]
        filtered = market_app.filter_a_share_securities_data(
            security_type="stock",
            exclude_st=True,
            change_pct_min=1.0,
            change_pct_max=5.0,
            turnover_min=500000.0,
            turnover_rate_min=2.0,
            above_average_price=True,
            market_cap_max=50000000000.0,
            limit=20,
        )
        assert filtered["matched_count"] == 1
        assert filtered["results"][0]["symbol"] == "600001"

        market_app.get_quote_data = lambda _: fake_get_quote_data("600519")
        auction = market_app.get_auction_data("600519")
        assert auction["auction_price"] == 122.0
        assert auction["auction_turnover"] is None
        assert auction["data_status"] == "partial_data"
    finally:
        market_app.read_public_json = original_json
        market_app.get_eastmoney_batch_quote_rows = original_batch_rows
        market_app.get_eastmoney_market_quotes = original_market_rows
        market_app.get_quote_data = original_quote_data


def test_intraday_session_filter_and_market_time_cap() -> None:
    original_json = market_app.read_public_json
    try:
        market_app.read_public_json = lambda *_: {
            "data": {
                "name": "Test ETF",
                "preClose": 1.0,
                "trends": [
                    "2026-07-10 14:59,1.00,1.01,1.01,1.00,10,1010,1.005",
                    "2026-07-10 15:00,1.01,1.02,1.02,1.01,20,2040,1.010",
                    "2026-07-10 15:01,1.02,1.02,1.02,1.02,0,0,1.010",
                    "2026-07-10 15:11,1.02,1.02,1.02,1.02,0,0,1.010",
                ],
            }
        }
        intraday = market_app.get_eastmoney_intraday("512760", 20)
        assert [item["time"] for item in intraday["items"]] == [
            "2026-07-10 14:59",
            "2026-07-10 15:00",
        ]
        assert intraday["latest_market_time"] == "2026-07-10 15:00"
        assert intraday["filtered_out_of_session_count"] == 2

        market_app.read_public_json = lambda *_: {
            "data": {
                "name": "Opening Test",
                "preClose": 9.8,
                "trends": [
                    "2026-07-10 09:30,10.00,10.00,10.00,10.00,10,1000,10.00",
                    "2026-07-10 09:31,10.10,10.10,10.10,10.10,10,1010,10.05",
                    "2026-07-10 09:32,10.20,10.20,10.20,10.20,10,1020,10.10",
                ],
            }
        }
        truncated = market_app.get_eastmoney_intraday("600519", 1)
        assert truncated["items"][0]["time"] == "2026-07-10 09:32"
        assert truncated["session_open"] == 10.0
        indicators = market_app.intraday_mechanical_indicators(
            truncated["items"],
            opening_price=truncated["session_open"],
            opening_price_scope=truncated["session_open_scope"],
            session_high=truncated["session_high"],
            session_low=truncated["session_low"],
        )
        assert indicators["return_from_open_pct"] == 2.0
        assert indicators["return_from_first_returned_minute_pct"] == 0.0
        assert indicators["opening_price_scope"] == "official_open_from_09_30_exchange_minute"

        indicators = market_app.intraday_mechanical_indicators(
            intraday["items"]
            + [{"time": "2026-07-10 15:11", "price": 999.0, "high": 999.0, "low": 999.0}]
        )
        assert indicators["at_intraday_high"] is True
        assert indicators["distance_from_high_pct"] == 0.0

        assert market_app.market_time_from_source_update(
            "2026-07-10T16:14:42+08:00"
        ) == "2026-07-10T15:00:00+08:00"
    finally:
        market_app.read_public_json = original_json


def test_market_quote_pagination() -> None:
    original_json = market_app.read_public_json
    try:
        requested_pages: list[int] = []

        def paged_market_rows(url: str, *_: object) -> dict:
            match = market_app.re.search(r"(?:\?|&)pn=(\d+)", url)
            assert match is not None
            page = int(match.group(1))
            requested_pages.append(page)
            start = (page - 1) * 100
            end = min(start + 100, 201)
            return {
                "data": {
                    "total": 201,
                    "diff": [
                        {
                            "f12": str(600000 + number),
                            "f14": f"Test {number}",
                            "f2": 10.0,
                            "f3": 1.0,
                            "f5": 100,
                            "f6": 100000,
                        }
                        for number in range(start, end)
                    ],
                }
            }

        market_app.read_public_json = paged_market_rows
        rows = market_app.get_eastmoney_market_quotes()
        assert len(rows) == 201
        assert set(requested_pages) == {1, 2, 3}
    finally:
        market_app.read_public_json = original_json


def test_sina_market_pagination_and_breadth_fallback() -> None:
    original_json = market_app.read_public_json
    original_text = market_app.read_market_text
    original_eastmoney_rows = market_app.get_eastmoney_market_quotes
    original_sina_rows = market_app.get_sina_market_quotes
    try:
        market_app.read_market_text = lambda *_args, **_kwargs: '"201"'

        def sina_page(url: str, *_args: object, **_kwargs: object) -> list[dict]:
            match = market_app.re.search(r"(?:\?|&)page=(\d+)", url)
            assert match is not None
            page = int(match.group(1))
            start = (page - 1) * 100
            end = min(start + 100, 201)
            return [
                {
                    "code": str(600000 + number),
                    "name": f"Test {number}",
                    "trade": "10.0",
                    "changepercent": 1.0,
                    "volume": 100,
                    "amount": 1000,
                    "turnoverratio": 1.0,
                    "high": "10.1",
                    "low": "9.9",
                    "open": "10.0",
                    "settlement": "9.9",
                    "mktcap": 10000,
                }
                for number in range(start, end)
            ]

        market_app.read_public_json = sina_page
        sina = market_app.get_sina_market_quotes()
        assert sina["returned_count"] == 201
        assert sina["coverage_status"] == "complete"

        market_app.get_eastmoney_market_quotes = lambda: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="blocked")
        )
    finally:
        market_app.read_public_json = original_json
        market_app.read_market_text = original_text
        market_app.get_eastmoney_market_quotes = original_eastmoney_rows
        market_app.get_sina_market_quotes = original_sina_rows


def test_fast_market_aggregate() -> None:
    original_json = market_app.read_public_json
    try:
        market_app.read_public_json = lambda *_args, **_kwargs: {
            "data": {
                "diff": [
                    {"f12": "000002", "f104": 10, "f105": 20, "f106": 2, "f6": 1000, "f124": 1783930322},
                    {"f12": "399107", "f104": 30, "f105": 40, "f106": 3, "f6": 2000, "f124": 1783930305},
                    {"f12": "899050", "f104": 5, "f105": 6, "f106": 1, "f6": 300, "f124": 1783928233},
                ]
            }
        }
        component = market_app.get_eastmoney_market_aggregate()
        totals = component["breadth"]["all_market"]
        assert totals["stock_count"] == 117
        assert totals["rise_count"] == 45
        assert totals["fall_count"] == 66
        assert totals["flat_count"] == 6
        assert totals["limit_up_count"] is None
        assert component["turnover"]["current"] == 3300
        assert component["coverage_status"] == "complete_exchange_aggregate"
        assert component["market_time"] == "2026-07-13T15:00:00+08:00"
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
        market_app.read_public_json = lambda *_: {
            "data": {
                "name": "Test Stock",
                "preClose": 9.9,
                "trends": ["2026-07-10 09:30,10.00,10.10,10.20,9.90,2,2000,10.05"],
            }
        }
        eastmoney_intraday = market_app.get_eastmoney_intraday("600519", 1)
        assert eastmoney_intraday["items"][0]["volume"] == 200
        assert eastmoney_intraday["items"][0]["volume_unit"] == "share"
        assert eastmoney_intraday["items"][0]["turnover_unit"] == "CNY"
        assert eastmoney_intraday["session_open"] == 10.0

        market_app.get_eastmoney_intraday = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="blocked")
        )
        tencent_quote = [""] * 35
        tencent_quote[1] = "Test"
        tencent_quote[2] = "600519"
        tencent_quote[3] = "10.10"
        tencent_quote[4] = "9.90"
        tencent_quote[5] = "9.95"
        tencent_quote[33] = "10.20"
        tencent_quote[34] = "9.90"
        market_app.read_public_json = lambda *_: {
            "data": {
                "sh600519": {
                    "data": {
                        "date": "20260710",
                        "data": ["0930 10.00 2 2000", "0931 10.10 5 5030"],
                    },
                    "qt": {"sh600519": tencent_quote},
                }
            }
        }
        intraday = market_app.get_intraday_data("600519", 2)
        assert intraday["source"] == "tencent"
        assert intraday["items"][1]["volume"] == 300
        assert intraday["items"][1]["turnover"] == 3030
        assert intraday["items"][1]["volume_unit"] == "share"
        assert intraday["open"] == 9.95
        assert intraday["high"] == 10.2
        assert intraday["low"] == 9.9
        assert intraday["mechanical_indicators"]["return_from_open_pct"] == 1.5075

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
        fields = [""] * 35
        fields[1:6] = ["Test Index", "000001", "100.0", "99.0", "99.5"]
        fields[30:33] = ["20260710150000", "1.0", "1.01"]
        fields[33:35] = ["101.0", "98.0"]
        market_app.read_market_text = lambda *_: f'v_sh000001="{"~".join(fields)}";'
        market_app.TOOL_CACHE.clear()
        overview = market_app.get_market_overview_data(3)
        assert overview["index_source"] == "tencent"
        assert overview["indices"][0]["change"] == 1.0
        assert overview["indices"][0]["change_pct"] == 1.01
        assert overview["indices"][0]["high"] == 101.0
        assert overview["indices"][0]["low"] == 98.0
        assert overview["industry_board_source"] == "eastmoney_industry"
        assert overview["industry_boards"][0]["name"] == "Test Industry"
        assert overview["response_budget_ms"] == 9000
        assert overview["component_status"]["indices"]["status"] in {
            "live",
            "fresh_cache",
        }

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
        market_app.TOOL_CACHE.clear()
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

        full_history = {
            "source": "eastmoney",
            "count": 5,
            "items": [{"date": "2026-07-10"}],
        }
        partial_day = {
            "source": "sina",
            "data_status": "partial_data",
            "count": 1,
            "items": [{"date": "2026-07-10"}],
        }

        def slightly_slower_full_history(*_: object) -> dict:
            sleep(0.05)
            return full_history

        market_app.get_eastmoney_fund_flow = slightly_slower_full_history
        market_app.get_sina_fund_flow = lambda *_: partial_day
        preferred_flow = market_app.get_fund_flow_data("600519", 5)
        assert preferred_flow["source"] == "eastmoney"
        assert preferred_flow["count"] == 5

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


def test_announcements_relative_strength_and_anomaly_scan() -> None:
    original_json = market_app.read_public_json
    original_post = market_app.read_public_json_post
    original_batch = market_app.get_batch_quote_data
    try:
        market_app.read_public_json = lambda *_args, **_kwargs: {
            "result": [
                [
                    {
                        "ORG_BULLETIN_ID": "sse-1",
                        "ORG_FILE_TYPE": 0,
                        "SECURITY_CODE": "600000",
                        "SECURITY_NAME": "浦发银行",
                        "SSEDATE": "2026-07-10",
                        "TITLE": "2025年年度权益分派实施公告",
                        "BULLETIN_TYPE_DESC": "利润分配",
                        "URL": "/disclosure/test.pdf",
                    },
                    {
                        "ORG_BULLETIN_ID": "sse-1",
                        "ORG_FILE_TYPE": 1,
                        "TITLE": "Legal attachment",
                        "URL": "/disclosure/attachment.pdf",
                    },
                ]
            ]
        }
        sse = market_app.get_sse_announcements(
            "600000", "2026-07-01", "2026-07-14", 10
        )
        assert len(sse) == 1
        assert sse[0]["event_tags"] == ["dividend"]
        assert sse[0]["url"] == "https://static.sse.com.cn/disclosure/test.pdf"

        market_app.read_public_json_post = lambda *_args, **_kwargs: {
            "data": [
                {
                    "annId": 1,
                    "title": "平安银行：董事会决议公告",
                    "publishTime": "2026-07-03 00:00:00",
                    "attachPath": "/disc/test.pdf",
                    "secCode": ["000001"],
                    "secName": ["平安银行"],
                }
            ]
        }
        szse = market_app.get_szse_announcements(
            "000001", "2026-07-01", "2026-07-14", 10
        )
        assert szse[0]["event_tags"] == ["governance"]
        assert szse[0]["official_source"] == "Shenzhen Stock Exchange"

        assert market_app.security_metadata("920068")["exchange"] == "BSE"
        bse = market_app.get_announcement_data("920068", 30, 10)
        assert bse["data_status"] == "unavailable"
        assert bse["source_errors"][0]["error_type"] == "official_source_blocked"

        def batch_snapshot(identifiers: list[str]) -> dict:
            rows = {
                "600519": {
                    "identifier": "600519",
                    "symbol": "600519",
                    "name": "Target",
                    "price": 105,
                    "change_pct": 5.0,
                    "open": 103,
                    "previous_close": 100,
                    "high": 105,
                    "low": 99,
                    "volume_ratio": 3.0,
                    "turnover_rate": 6.0,
                },
                "index:000001": {
                    "identifier": "index:000001",
                    "symbol": "000001",
                    "name": "SSE Index",
                    "price": 3900,
                    "change_pct": 1.0,
                    "high": 3920,
                    "low": 3850,
                },
                "600000": {
                    "identifier": "600000",
                    "symbol": "600000",
                    "name": "Peer",
                    "price": 10,
                    "change_pct": 2.0,
                    "high": 10.1,
                    "low": 9.8,
                },
            }
            results = [rows[item] for item in identifiers if item in rows]
            return {
                "results": results,
                "source": ["test_batch"],
                "source_errors": [],
                "market_time": "2026-07-14T10:00:00+08:00",
            }

        market_app.get_batch_quote_data = batch_snapshot
        relative = market_app.get_relative_strength_data(
            "600519", "index:000001", ["600000"]
        )
        assert relative["relative_to_benchmark_pct_points"] == 4.0
        assert relative["relative_to_peer_average_pct_points"] == 3.0
        assert relative["relative_status"] == "outperforming_benchmark"

        scan = market_app.scan_intraday_anomalies_data(
            symbols=["600519"],
            benchmark_symbol="index:000001",
            change_pct_min=3.0,
            volume_ratio_min=2.0,
            turnover_rate_min=5.0,
            gap_pct_min=2.0,
            near_extreme_pct=0.3,
            relative_strength_min=2.0,
            include_untriggered=False,
        )
        trigger_types = {item["type"] for item in scan["results"][0]["triggers"]}
        assert {
            "large_daily_move",
            "high_daily_volume_ratio",
            "high_turnover_rate",
            "opening_gap",
            "near_intraday_high",
            "benchmark_relative_move",
        } <= trigger_types
    finally:
        market_app.read_public_json = original_json
        market_app.read_public_json_post = original_post
        market_app.get_batch_quote_data = original_batch


def test_reliability_envelope_cache_and_health() -> None:
    market_app.TOOL_CACHE.clear()
    market_app.SOURCE_HEALTH.clear()
    calls = 0

    def loader() -> dict:
        nonlocal calls
        calls += 1
        return {
            "symbol": "600519",
            "source": "eastmoney",
            "market_time": "2026-07-10T15:00:00+08:00",
        }

    key = market_app.cache_key("test", {"symbol": "600519"})
    first, first_cache = market_app.get_cached_tool_data(key, 10, loader)
    second, second_cache = market_app.get_cached_tool_data(key, 10, loader)
    assert calls == 1
    assert first == second
    assert first_cache["cache_hit"] is False
    assert second_cache["cache_hit"] is True

    result = market_app.standardize_tool_success(first, market_app.perf_counter(), second_cache)
    assert result["ok"] is True
    assert result["source"] == ["eastmoney"]
    assert result["cache_hit"] is True
    assert result["data"]["symbol"] == "600519"
    assert "latency_ms" in result

    market_app.TOOL_CACHE[key]["created_at"] = market_app.datetime.now(
        market_app.timezone.utc
    ) - market_app.timedelta(seconds=20)

    def failing_loader() -> dict:
        raise market_app.HTTPException(status_code=502, detail="temporary upstream failure")

    stale = market_app.run_cached_tool(
        "test",
        {"symbol": "600519"},
        10,
        failing_loader,
        "600519",
        max_stale_age_seconds=120,
    )
    assert stale["ok"] is True
    assert stale["is_stale"] is True
    assert stale["stale_reason"] == "live_sources_failed_using_recent_cache"
    assert stale["served_from_stale_cache"] is True
    assert stale["cache_hit"] is True

    market_app.record_source_health("eastmoney", True, 42)
    health = market_app.get_market_data_health_data()
    eastmoney = next(item for item in health["sources"] if item["source"] == "eastmoney")
    assert eastmoney["status"] == "healthy"
    assert health["quote_route"]["status"] == "configured"
    assert health["routing_revision"] == "parallel_fallback_singleflight_stale_cache_v1"

    market_app.TOOL_CACHE.clear()
    concurrent_calls = 0

    def slow_loader() -> dict:
        nonlocal concurrent_calls
        concurrent_calls += 1
        sleep(0.05)
        return {"source": "test", "market_time": "2026-07-10T15:00:00+08:00"}

    with market_app.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(
            lambda _: market_app.get_cached_tool_data("single-flight", 10, slow_loader),
            range(5),
        ))
    assert concurrent_calls == 1
    assert sum(1 for _, cache in results if cache["cache_hit"] is False) == 1
    assert sum(1 for _, cache in results if cache["cache_hit"] is True) == 4

    market_app.TOOL_CACHE.clear()
    failure_calls = 0

    def shared_failing_loader() -> dict:
        nonlocal failure_calls
        failure_calls += 1
        sleep(0.05)
        raise market_app.HTTPException(status_code=502, detail="shared upstream failure")

    def consume_shared_failure(_: int) -> int:
        try:
            market_app.get_cached_tool_data("single-flight-failure", 10, shared_failing_loader)
        except market_app.HTTPException as exc:
            return exc.status_code
        raise AssertionError("Expected the shared loader failure to propagate.")

    with market_app.ThreadPoolExecutor(max_workers=5) as executor:
        failure_statuses = list(executor.map(consume_shared_failure, range(5)))
    assert failure_calls == 1
    assert failure_statuses == [502] * 5
    assert market_app.TOOL_CACHE_INFLIGHT == {}

    market_app.TOOL_CACHE.clear()

    def parent_loader() -> dict:
        child, _ = market_app.get_cached_tool_data(
            "nested-child", 10, lambda: {"value": "child"}
        )
        return {"value": child["value"]}

    parent, _ = market_app.get_cached_tool_data("nested-parent", 10, parent_loader)
    assert parent["value"] == "child"
    assert market_app.TOOL_CACHE_INFLIGHT == {}

    error = market_app.mcp_error(
        "bad", market_app.HTTPException(status_code=400, detail="symbol is required.")
    )
    assert error["error_type"] == "invalid_symbol"
    assert error["source_errors"][0]["error_type"] == "invalid_symbol"


def test_historical_context_and_security_status_facts() -> None:
    original_kline = market_app.get_kline_data
    original_json = market_app.read_public_json
    try:
        base = market_app.datetime(2025, 1, 1)
        items = [
            {
                "date": (base + market_app.timedelta(days=index)).date().isoformat(),
                "open": 10.0 + index / 100,
                "close": 10.0 + index / 100,
                "high": 10.2 + index / 100,
                "low": 9.8 + index / 100,
                "volume": 1000.0 + index,
                "turnover": 10000.0 + index * 10,
                "turnover_rate": 1.0 + index / 1000,
                "amplitude": 2.0 + index / 1000,
            }
            for index in range(260)
        ]
        market_app.get_kline_data = lambda *_: {
            "symbol": "600519",
            "security_type": "a_share",
            "exchange": "SSE",
            "adjustment": "forward_adjusted",
            "adjustment_source_parameter": "test_qfq",
            "items": items,
            "source": "test",
            "source_errors": [],
        }
        historical = market_app.get_historical_context_data("600519")
        assert historical["adjustment"] == "forward_adjusted"
        assert list(historical["windows"]) == ["20", "60", "120", "250"]
        assert all(
            window["window_complete"] for window in historical["windows"].values()
        )
        assert historical["windows"]["250"]["available_sessions"] == 250
        assert (
            historical["windows"]["20"]["turnover"][
                "percentile_rank_in_window"
            ]
            == 100.0
        )

        quote = fake_get_quote_data("600519")
        quote["quote"]["name"] = "*ST Test"
        reference = {
            "name": "*ST Test",
            "listing_date": "2001-08-27",
            "source_security_status_code": 5,
            "source": "test_reference",
        }
        announcements = {
            "items": [
                {
                    "title": "Test dividend",
                    "event_tags": ["dividend"],
                    "event_date": "2026-07-10",
                },
                {"title": "Other", "event_tags": ["other"]},
            ],
            "source": ["official_sse_announcements"],
        }
        status = market_app.build_security_status_data(
            "600519", quote, reference, announcements
        )
        assert status["security_type"] == "a_share"
        assert status["is_st_name_flag"] is True
        assert status["price_limit_reference"]["standard_daily_limit_pct"] == 5.0
        assert status["price_history_adjustment"]["mode"] == "forward_adjusted"
        assert len(status["recent_corporate_action_announcements"]) == 1
        assert status["suspension_status"] == "not_confirmed_by_current_data_contract"

        def reference_hosts(url: str, *_: object) -> dict:
            if url.startswith("https://push2.eastmoney.com/"):
                raise market_app.HTTPException(status_code=502, detail="primary blocked")
            return {
                "data": {
                    "f57": "600519",
                    "f58": "Test Stock",
                    "f189": 20010827,
                    "f292": 5,
                    "f127": "Test Industry",
                }
            }

        market_app.read_public_json = reference_hosts
        reference_result = market_app.get_security_reference_data("600519")
        assert reference_result["listing_date"] == "2001-08-27"
        assert reference_result["source"].startswith("eastmoney_security_reference:")

        reference_key = market_app.cache_key(
            "security_reference_internal", {"symbol": "600519"}
        )
        market_app.TOOL_CACHE.pop(reference_key, None)
        cached_reference = market_app.get_resilient_security_reference_data("600519")
        assert cached_reference["listing_date"] == "2001-08-27"
        market_app.TOOL_CACHE[reference_key]["created_at"] = market_app.datetime.now(
            market_app.timezone.utc
        ) - market_app.timedelta(hours=7)
        market_app.read_public_json = lambda *_: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="all hosts blocked")
        )
        stale_reference = market_app.get_resilient_security_reference_data("600519")
        assert stale_reference["listing_date"] == "2001-08-27"
        assert stale_reference["cache_hit"] is True
        assert (
            stale_reference["reference_stale_reason"]
            == "live_sources_failed_using_slow_changing_reference_cache"
        )
    finally:
        market_app.get_kline_data = original_kline
        market_app.read_public_json = original_json


def main() -> None:
    test_kline_source_parsers()
    test_search_source_parser()
    test_etf_market_routing_and_search()
    test_quote_unit_normalization()
    test_quote_timestamp_semantics()
    test_industry_board_parser()
    test_industry_board_deduplication()
    test_market_structure_calculations()
    test_batch_quotes_intraday_indicators_and_filtering()
    test_intraday_session_filter_and_market_time_cap()
    test_market_quote_pagination()
    test_sina_market_pagination_and_breadth_fallback()
    test_fast_market_aggregate()
    test_intraday_and_index_fallback_parsers()
    test_announcements_relative_strength_and_anomaly_scan()
    test_reliability_envelope_cache_and_health()
    test_historical_context_and_security_status_facts()
    market_app.TOOL_CACHE.clear()
    market_app.search_stock_data = fake_search_stock_data
    market_app.get_quote_data = fake_get_quote_data
    market_app.get_batch_quote_data = fake_get_batch_quote_data
    market_app.get_kline_data = fake_get_kline_data
    market_app.get_intraday_data = fake_get_intraday_data
    market_app.get_auction_data = fake_get_auction_data
    market_app.filter_a_share_securities_data = fake_filter_a_share_securities_data
    market_app.get_fund_flow_data = fake_get_fund_flow_data
    market_app.get_financial_data = fake_get_financial_data
    market_app.get_news_data = fake_get_news_data
    market_app.get_announcement_data = fake_get_announcement_data
    market_app.get_historical_context_data = fake_get_historical_context_data
    market_app.get_security_status_data = fake_get_security_status_data
    market_app.get_decision_context_data = fake_get_decision_context_data
    market_app.get_relative_strength_data = fake_get_relative_strength_data
    market_app.scan_intraday_anomalies_data = fake_scan_intraday_anomalies_data
    market_app.get_market_overview_data = fake_get_market_overview_data
    market_app.get_sector_rankings_data = fake_get_sector_rankings_data
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }

    with TestClient(market_app.app, base_url="http://127.0.0.1:8000") as client:
        health = client.get("/health")
        assert health.status_code == 200, health.text
        assert health.json()["routing_revision"] == "parallel_fallback_singleflight_stale_cache_v1"

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
            "get_a_share_batch_quotes",
            "get_a_share_kline",
            "get_a_share_intraday",
            "get_a_share_auction",
            "filter_a_share_securities",
            "get_a_share_fund_flow",
            "get_a_share_financials",
            "get_a_share_news",
            "get_a_share_announcements",
            "get_a_share_historical_context",
            "get_a_share_security_status",
            "get_a_share_decision_context",
            "get_a_share_relative_strength",
            "scan_a_share_intraday_anomalies",
            "get_a_share_sector_rankings",
            "get_a_share_market_overview",
            "get_market_data_health",
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
        assert result["market_time"] == "2026-07-10T15:00:00+08:00"
        assert result["trade_date"] == "2026-07-10"
        assert result["quote_time"] == "2026-07-10T15:00:00+08:00"
        assert result["source_updated_at"] == "2026-07-10T16:14:42+08:00"
        assert result["queried_at"] != "2026-07-10T00:05:00+00:00"
        assert result["source"] == ["test"]
        assert result["cache_hit"] is False
        assert result["data"]["symbol"] == "600519"
        assert result["volume_unit"] == "share"
        assert result["turnover_unit"] == "CNY"
        assert "pe_dynamic" not in result
        assert "total_market_value" not in result

        for request_id, tool_name, arguments in (
            (5, "get_a_share_kline", {"symbol": "600519"}),
            (6, "get_a_share_intraday", {"symbol": "600519"}),
            (7, "get_a_share_batch_quotes", {"symbols": ["512760", "600519"]}),
            (8, "get_a_share_auction", {"symbol": "600519"}),
            (9, "filter_a_share_securities", {"change_pct_min": 1}),
            (10, "get_a_share_fund_flow", {"symbol": "600519"}),
            (11, "get_a_share_financials", {"symbol": "600519"}),
            (12, "get_a_share_news", {"symbol": "600519"}),
            (13, "get_a_share_sector_rankings", {"sector_type": "industry"}),
            (14, "get_a_share_market_overview", {}),
            (15, "get_market_data_health", {}),
            (16, "get_a_share_announcements", {"symbol": "600519"}),
            (
                17,
                "get_a_share_relative_strength",
                {"symbol": "600519", "peer_symbols": ["600000"]},
            ),
            (
                18,
                "scan_a_share_intraday_anomalies",
                {"symbols": ["600519"], "benchmark_symbol": "index:000001"},
            ),
            (19, "get_a_share_historical_context", {"symbol": "600519"}),
            (20, "get_a_share_security_status", {"symbol": "600519"}),
            (
                21,
                "get_a_share_decision_context",
                {"symbol": "600519", "benchmark_symbol": "index:000001"},
            ),
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
            content = response.json()["result"]["structuredContent"]
            assert content["ok"] is True
            assert "source_errors" in content
            assert "cache_hit" in content
            assert "data" in content

    assert market_app.format_market_time("20260710150146") == "2026-07-10T15:01:46+08:00"

    print("MCP handshake, tool discovery, and read-only market-tool tests passed.")


if __name__ == "__main__":
    main()
