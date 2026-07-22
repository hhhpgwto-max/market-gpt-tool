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
            "total_market_value_unit": "CNY",
            "circulating_market_value": 888888888,
            "circulating_market_value_unit": "CNY",
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


def fake_screen_a_share_research_candidates_data(**_: object) -> dict:
    return {
        "selection_status": "research_candidates_available",
        "no_candidate": False,
        "accepted_count": 1,
        "research_candidates": [
            {
                "symbol": "600519",
                "name": "Test Stock",
                "research_status": "advance_to_deeper_research",
            }
        ],
        "market_gate": {"passed": True},
        "source": ["test"],
        "source_errors": [],
        "missing_fields": [],
        "data_status": "full_data",
        "market_time": "2026-07-10T15:00:00+08:00",
    }


def fake_search_stock_data(keyword: str, limit: int) -> dict:
    return {
        "keyword": keyword,
        "count": 1,
        "results": [{"symbol": "603993", "name": "洛阳钼业", "market": "沪A"}],
        "source": "tencent_search",
        "queried_at": "2026-07-10T00:05:00+00:00",
    }


def fake_get_kline_data(symbol: str, period: str, limit: int, **_: object) -> dict:
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


def fake_get_fund_exposure_data(
    fund_code: str,
    holdings_limit: int,
    detail_level: str,
    look_through_depth: int = 1,
) -> dict:
    return {
        "fund_code": fund_code,
        "fund_name": "Test Fund",
        "top_holdings": [
            {
                "symbol": "600519",
                "name": "Test Stock",
                "weight_pct": 10.0,
                "provider_industry_name": "Manufacturing",
            }
        ][:holdings_limit],
        "industry_distribution": [{"industry_name": "Manufacturing", "weight_pct": 80.0}],
        "asset_allocation": {"stock_pct": 90.0, "bond_pct": 0.0, "cash_pct": 10.0, "other_pct": 0.0, "fund_pct": 0.0},
        "holdings_disclosure_date": "2026-03-31",
        "detail_level": detail_level,
        "source": ["test"],
        "source_errors": [],
        "missing_fields": [],
        "data_status": "full_data",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_portfolio_exposure_data(
    positions: list[dict], normalize_weights: bool, holdings_limit: int, detail_level: str
) -> dict:
    return {
        "input_position_count": len(positions),
        "weights_normalized": normalize_weights,
        "holdings_limit": holdings_limit,
        "detail_level": detail_level,
        "asset_allocation": {"stock_pct": 100.0},
        "source": ["test"],
        "source_errors": [],
        "missing_fields": [],
        "data_status": "full_data",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_ipo_subscription_status_data(
    symbol_or_name: str | None,
    days_ahead: int,
    days_back: int,
    limit: int,
    detail_level: str,
) -> dict:
    return {
        "query": symbol_or_name,
        "schedule_range": {"days_ahead": days_ahead, "days_back": days_back},
        "count": 1,
        "items": [
            {
                "security_code": "301707",
                "subscription_code": "301707",
                "subscription_stage": "subscription_scheduled",
            }
        ][:limit],
        "detail_level": detail_level,
        "source": ["test"],
        "source_errors": [],
        "missing_fields": [],
        "data_status": "full_data",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_a_share_trading_calendar_data(start_date, end_date, detail_level) -> dict:
    return {"start_date": start_date, "end_date": end_date, "items": [], "detail_level": detail_level,
            "source": ["test"], "source_errors": [], "missing_fields": [], "data_status": "full_data"}


def fake_get_a_share_capital_activity_data(symbol, lookback_days, limit, detail_level) -> dict:
    return {"symbol": symbol, "components": {}, "source": ["test"], "source_errors": [],
            "missing_fields": [], "data_status": "full_data", "detail_level": detail_level}


def fake_get_news_data(
    symbol: str,
    limit: int,
    days: int = 30,
    include_industry_context: bool = False,
) -> dict:
    return {
        "symbol": symbol,
        "count": limit,
        "items": [{"title": "Test news", "url": "https://example.com/news"}],
        "source": "test",
        "period_days": days,
        "include_industry_context": include_industry_context,
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


def fake_get_event_timeline_data(symbol: str, days: int, limit: int) -> dict:
    return {
        "symbol": symbol,
        "period_days": days,
        "count": 1,
        "events": [{"event_title": "Test event", "price_feedback": {"status": "available"}}][:limit],
        "source": ["test"],
        "source_errors": [],
        "data_status": "full_data",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_sector_rotation_data(
    sector_type: str, level: str, lookbacks: list[int], limit: int
) -> dict:
    return {
        "sector_type": sector_type,
        "level": level,
        "lookbacks": lookbacks,
        "count": 1,
        "items": [{"symbol": "BK0001", "name": "Test sector"}][:limit],
        "source": ["test"],
        "source_errors": [],
        "data_status": "full_data",
        "queried_at": "2026-07-10T00:00:00+00:00",
    }


def fake_get_overnight_risk_packet_data(detail_level: str) -> dict:
    return {
        "count": 1,
        "items": [{"identifier": "ftse_china_a50_futures", "price": 12000}],
        "missing_fields": [],
        "detail_level": detail_level,
        "source": ["test"],
        "source_errors": [],
        "data_status": "full_data",
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
        "available_component_count": 9,
        "requested_component_count": 9,
        "decision_inputs": {
            "quote": {"symbol": symbol},
            "historical_context": fake_get_historical_context_data(symbol),
        },
        "excluded_components": {},
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


def fake_get_market_snapshot_data(
    symbol: str | None,
    peer_symbols: list[str] | None,
    as_of: str | None,
    sector_limit: int,
    detail_level: str,
) -> dict:
    return {
        "snapshot_id": "market-snapshot-test",
        "requested_as_of": as_of,
        "symbol": symbol,
        "requested_identifiers": [symbol, *(peer_symbols or [])] if symbol else peer_symbols or [],
        "market_time": "2026-07-10T15:00:00+08:00",
        "market_time_range": {
            "earliest": "2026-07-10T15:00:00+08:00",
            "latest": "2026-07-10T15:00:00+08:00",
        },
        "source_time_difference_seconds": 0,
        "source_difference_pct": 0,
        "recommended_source": ["test"],
        "target_quote": {"symbol": symbol, "price": 123.45} if symbol else None,
        "peer_quotes": [],
        "market_overview": fake_get_market_overview_data(sector_limit),
        "component_status": {"market_overview": {"status": "available", "latency_ms": 1}},
        "source": ["test"],
        "source_errors": [],
        "conflicts": [],
        "missing_fields": [],
        "detail_level": detail_level,
        "data_status": "full_data",
    }


def fake_get_limit_activity_data(limit: int) -> dict:
    return {
        "trade_date": "2026-07-10",
        "statistics": {
            "limit_up_count": 2,
            "limit_down_count": 1,
            "open_board_count": 1,
            "consecutive_limit_up_count": 1,
            "max_consecutive_limit_up": 2,
        },
        "limit_up_items": [{"symbol": "600001"}][:limit],
        "open_board_items": [{"symbol": "600002"}][:limit],
        "limit_down_items": [{"symbol": "600003"}][:limit],
        "source": ["test"],
        "source_errors": [],
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
    market_app.PREFERRED_ROUTE_HEALTH.clear()
    try:
        market_app.read_public_json = lambda *_, **__: {
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

        market_app.read_public_json = lambda *_, **__: {
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
                assert future.done() is False
                release_eastmoney.set()
                fallback = future.result(timeout=0.5)
                assert fallback["source"] == "tencent"
        finally:
            release_eastmoney.set()

        market_app.PREFERRED_ROUTE_HEALTH.clear()
        eastmoney_full = {
            **tencent,
            "source": "eastmoney",
            "items": [{**tencent["items"][0], "turnover": 123456.0}],
        }

        def slightly_slower_eastmoney(*_: object) -> dict:
            sleep(0.05)
            return eastmoney_full

        market_app.get_eastmoney_kline = slightly_slower_eastmoney
        preferred = market_app.get_fallback_kline("600519", "daily", 101, 1)
        assert preferred["source"] == "eastmoney"
        assert preferred["items"][0]["turnover"] == 123456.0
    finally:
        market_app.read_public_json = original_reader
        market_app.get_eastmoney_kline = original_eastmoney
        market_app.get_tencent_kline = original_tencent
        market_app.PREFERRED_ROUTE_HEALTH.clear()


def test_kline_range_and_pagination() -> None:
    original_reader = market_app.read_public_json
    requested_urls: list[str] = []
    try:
        def fake_reader(url: str, *_: object, **__: object) -> dict:
            requested_urls.append(url)
            return {
                "data": {
                    "klines": [
                        "2026-07-08,100,101,102,99,10,1000,3,1,1,0.1",
                        "2026-07-09,101,102,103,100,20,2000,3,1,1,0.2",
                        "2026-07-10,102,103,104,101,30,3000,3,1,1,0.3",
                    ]
                }
            }

        market_app.read_public_json = fake_reader
        first = market_app.get_eastmoney_kline(
            "600519", "daily", 101, 2, "2026-07-01", "2026-07-10", "backward"
        )
        assert [item["date"] for item in first["items"]] == ["2026-07-09", "2026-07-10"]
        assert first["has_more"] is True
        assert first["next_page_token"]
        assert first["adjustment"] == "backward_adjusted"
        assert "fqt=2" in requested_urls[0]
        assert "beg=20260701" in requested_urls[0]
        assert "end=20260710" in requested_urls[0]

        second = market_app.get_eastmoney_kline(
            "600519",
            "daily",
            101,
            2,
            "2026-07-01",
            "2026-07-10",
            "backward",
            first["next_page_token"],
        )
        assert [item["date"] for item in second["items"]] == ["2026-07-08"]
        assert second["has_more"] is False

        minute = market_app.get_eastmoney_kline(
            "600519", "1m", 1, 2, "2026-07-01", "2026-07-10"
        )
        assert minute["coverage_status"] == "partial_public_source_history"
        assert "historical_1m_before_available_public_range" in minute["missing_fields"]
    finally:
        market_app.read_public_json = original_reader


def test_tencent_minute_kline_adjustment_fallback() -> None:
    original_reader = market_app.read_public_json
    market_app.TOOL_CACHE.clear()
    requested_urls: list[str] = []
    try:
        def fake_reader(url: str, *_: object, **__: object) -> dict:
            requested_urls.append(url)
            sleep(0.12)
            if "/kline/mkline" in url:
                return {
                    "data": {
                        "sh600519": {
                            "m5": [
                                ["202607100935", "100", "101", "102", "99", "10", "", ""],
                                ["202607100940", "101", "102", "103", "100", "20", "", ""],
                            ]
                        }
                    }
                }
            if "/fqkline/get" in url:
                return {
                    "data": {
                        "sh600519": {
                            "qfqday": [["2026-07-10", "50", "51", "52", "49", "30"]]
                        }
                    }
                }
            return {
                "data": {
                    "sh600519": {
                        "day": [["2026-07-10", "100", "102", "104", "98", "30"]]
                    }
                }
            }

        market_app.read_public_json = fake_reader
        started = market_app.perf_counter()
        result = market_app.get_tencent_kline(
            "600519", "5m", 1, "2026-07-10", "2026-07-10", "forward"
        )
        assert market_app.perf_counter() - started < 0.2
        assert result["source"] == "tencent"
        assert result["adjustment"] == "forward_adjusted"
        assert result["adjustment_source_parameter"] == "tencent_mkline_plus_qfq_daily_factor"
        assert result["items"][0]["open"] == 50.5
        assert result["items"][0]["close"] == 51.0
        assert result["items"][0]["volume"] == 2000.0
        assert result["missing_fields"] == ["turnover", "turnover_rate", "amplitude"]
        assert result["has_more"] is True

        second = market_app.get_tencent_kline(
            "600519",
            "5m",
            1,
            "2026-07-10",
            "2026-07-10",
            "forward",
            result["next_page_token"],
        )
        assert second["items"][0]["date"] == "2026-07-10 09:35"
        assert sum("/kline/mkline" in url for url in requested_urls) == 1
        assert sum("/fqkline/get" in url for url in requested_urls) == 1
        assert sum("/kline/kline" in url for url in requested_urls) == 1
    finally:
        market_app.read_public_json = original_reader
        market_app.TOOL_CACHE.clear()


def test_synchronized_market_snapshot_contract() -> None:
    original_overview = market_app.get_market_overview_data
    original_batch = market_app.get_batch_quote_data
    original_quote = market_app.get_quote_data
    calls = {"overview": 0, "batch": 0, "quote": 0}
    market_app.TOOL_CACHE.clear()
    try:
        def cached_overview(limit: int = 5) -> dict:
            calls["overview"] += 1
            return fake_get_market_overview_data(limit)

        def cached_batch(symbols: list[str]) -> dict:
            calls["batch"] += 1
            return fake_get_batch_quote_data(symbols)

        def cached_quote(symbol: str) -> dict:
            calls["quote"] += 1
            return fake_get_quote_data(symbol)

        market_app.get_market_overview_data = cached_overview
        market_app.get_batch_quote_data = cached_batch
        market_app.get_quote_data = cached_quote
        snapshot = market_app.get_market_snapshot_data(
            "600519", [], None, 5, "summary"
        )
        assert snapshot["snapshot_id"].startswith("market-snapshot-")
        assert snapshot["market_time_range"]["latest"] == "2026-07-10T15:00:00+08:00"
        assert snapshot["source_difference_pct"] > 0
        assert snapshot["conflicts"][0]["type"] == "target_price_difference"
        assert snapshot["recommended_source"] == ["test"]
        assert snapshot["data_status"] == "partial_data"
        repeated = market_app.get_market_snapshot_data(
            "600519", [], None, 5, "summary"
        )
        assert repeated["snapshot_id"] != snapshot["snapshot_id"]
        assert calls == {"overview": 1, "batch": 1, "quote": 1}

        try:
            market_app.normalize_snapshot_as_of("2000-01-01T10:30:00+08:00")
            raise AssertionError("Historical as_of should be rejected")
        except market_app.HTTPException as exc:
            assert exc.status_code == 400
            assert "historical" in str(exc.detail).lower()
    finally:
        market_app.get_market_overview_data = original_overview
        market_app.get_batch_quote_data = original_batch
        market_app.get_quote_data = original_quote
        market_app.TOOL_CACHE.clear()


def test_snapshot_compensates_for_a_late_batch_component() -> None:
    original_collect = market_app.collect_components
    calls = 0
    try:
        def staged_collect(loaders: dict, _budget: float, _executor: object = None):
            nonlocal calls
            calls += 1
            if calls == 1:
                return (
                    {
                        "market_overview": fake_get_market_overview_data(5),
                        "target_quote": fake_get_quote_data("600519"),
                        "batch_quotes": {
                            "requested_count": 1,
                            "count": 0,
                            "results": [],
                            "errors": [],
                            "source": [],
                            "source_errors": [],
                            "data_status": "no_data",
                        },
                    },
                    {
                        "market_overview": {"status": "available", "latency_ms": 10},
                        "target_quote": {"status": "available", "latency_ms": 10},
                        "batch_quotes": {"status": "available", "latency_ms": 10},
                    },
                    [],
                )
            return (
                {"batch_quotes": fake_get_batch_quote_data(["600519"])},
                {"batch_quotes": {"status": "available", "latency_ms": 20}},
                [],
            )

        market_app.collect_components = staged_collect
        snapshot = market_app.get_market_snapshot_data(
            "600519", [], None, 5, "summary"
        )
        assert calls == 2
        assert snapshot["component_status"]["batch_quotes"]["status"] == (
            "recovered_within_compensation_budget"
        )
        assert snapshot["component_status"]["batch_quotes"]["recovery_attempted"] is True
        assert not any(
            error.get("source") == "batch_quotes" for error in snapshot["source_errors"]
        )
        assert snapshot["effective_market_time"] == snapshot["market_time"]
        assert snapshot["source_fetch_time"] == snapshot["tool_queried_at"]
    finally:
        market_app.collect_components = original_collect
        market_app.TOOL_CACHE.clear()


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
        {
            "volume": 2603164,
            "turnover": 458798,
            "total_market_value": 111.61,
            "circulating_market_value": 100.5,
        },
        "tencent",
    )
    assert result["volume"] == 260316400
    assert result["volume_unit"] == "share"
    assert result["turnover"] == 4587980000
    assert result["turnover_unit"] == "CNY"
    assert result["total_market_value"] == 11_161_000_000
    assert result["total_market_value_unit"] == "CNY"
    assert result["circulating_market_value"] == 10_050_000_000
    assert result["circulating_market_value_unit"] == "CNY"


def test_quote_timestamp_semantics() -> None:
    result = market_app.derive_quote_timestamps("2026-07-10T16:14:42+08:00")
    assert result["trade_date"] == "2026-07-10"
    assert result["quote_time"] == "2026-07-10T15:00:00+08:00"
    assert result["source_updated_at"] == "2026-07-10T16:14:42+08:00"

    lunch_time = market_app.datetime(
        2026, 7, 17, 12, 0, tzinfo=market_app.MARKET_TIMEZONE
    )
    assert market_app.is_market_time_stale(
        "2026-07-16T15:00:00+08:00", lunch_time
    )
    assert not market_app.is_market_time_stale(
        "2026-07-17T11:30:00+08:00", lunch_time
    )
    assert market_app.is_market_time_stale(
        "2026-07-17T11:00:00+08:00", lunch_time
    )
    assert (
        market_app.staleness_basis_for(
            "2026-07-16T15:00:00+08:00",
            "2026-07-16",
            "lunch_break",
            True,
        )
        == "current_session_freshness_window"
    )


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
        called_hosts = {url.split("/", 3)[2] for url, _ in calls}
        assert called_hosts == {
            "push2.eastmoney.com",
            "push2delay.eastmoney.com",
            "82.push2.eastmoney.com",
        }
        assert all(arguments[-2:] == (3, 2) for _, arguments in calls)
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


def test_limit_activity_and_index_identity() -> None:
    original_fetch = market_app.fetch_eastmoney_limit_pool
    try:
        rows = {
            "limit_up": [
                {
                    "c": "600001",
                    "n": "Test Leader",
                    "p": 11000,
                    "zdp": 10.0,
                    "amount": 1000,
                    "ltsz": 10000,
                    "hs": 2.0,
                    "lbc": 3,
                    "fbt": 92500,
                    "lbt": 145500,
                    "fund": 500,
                    "zbc": 0,
                    "hybk": "Test Industry",
                    "zttj": {"days": 3, "ct": 3},
                },
                {
                    "c": "000001",
                    "n": "*ST Test",
                    "p": 10500,
                    "zdp": 5.0,
                    "amount": 800,
                    "lbc": 1,
                    "fund": 100,
                    "zttj": {"days": 1, "ct": 1},
                },
            ],
            "open_board": [
                {
                    "c": "300001",
                    "n": "Test Open",
                    "p": 11500,
                    "ztp": 12000,
                    "zdp": 15.0,
                    "amount": 900,
                    "zbc": 2,
                    "zttj": {"days": 0, "ct": 0},
                }
            ],
            "limit_down": [
                {
                    "c": "430001",
                    "n": "Test Down",
                    "p": 7000,
                    "zdp": -30.0,
                    "amount": 700,
                    "days": 2,
                    "lbt": 150000,
                    "oc": 3,
                    "fba": 200,
                }
            ],
        }

        def fake_fetch(pool_type: str, _trade_date: str) -> dict:
            return {
                "pool_type": pool_type,
                "trade_date": "20260714",
                "source_count": len(rows[pool_type]),
                "rows": rows[pool_type],
            }

        market_app.fetch_eastmoney_limit_pool = fake_fetch
        activity = market_app.get_limit_activity_data(10)
        statistics = activity["statistics"]
        assert statistics["limit_up_count"] == 2
        assert statistics["limit_down_count"] == 1
        assert statistics["open_board_count"] == 1
        assert statistics["seal_success_rate_pct"] == 66.67
        assert statistics["consecutive_limit_up_count"] == 1
        assert statistics["max_consecutive_limit_up"] == 3
        assert statistics["st_limit_up_count"] == 1
        assert activity["by_exchange"]["SSE"]["consecutive_limit_up_count"] == 1
        assert activity["limit_up_items"][0]["price"] == 11.0
        assert activity["limit_up_items"][0]["first_seal_time"].endswith("+08:00")

        identity = market_app.enrich_index_identity({"symbol": "932000", "name": None})
        assert identity["identifier"] == "index:932000"
        assert identity["eastmoney_secid"] == "2.932000"
        assert identity["index_role"] == "style"
    finally:
        market_app.fetch_eastmoney_limit_pool = original_fetch


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
        assert batch["source_updated_at"] == batch["results"][0]["source_updated_at"]
        assert batch["source_fetch_time"] == batch["tool_queried_at"]
        assert batch["errors"][0]["code"] == "invalid_symbol"
        assert market_app.batch_security_metadata("index:000300")["security_type"] == "index"

        lunch_update = market_app.datetime(
            2026, 7, 22, 11, 37, tzinfo=market_app.MARKET_TIMEZONE
        )
        lunch_quote = market_app.batch_quote_from_eastmoney_row(
            {"f12": "512760", "f13": 1, "f14": "Test ETF", "f2": 1.23, "f124": lunch_update.timestamp()},
            market_app.batch_security_metadata("512760"),
        )
        assert lunch_quote["source_updated_at"] == "2026-07-22T11:37:00+08:00"
        assert lunch_quote["effective_market_time"] == "2026-07-22T11:30:00+08:00"
        assert lunch_quote["market_time"] == "2026-07-22T11:30:00+08:00"

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
    original_sina_object = market_app.read_sina_object
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
        market_app.get_all_realtime_quotes = lambda: types.SimpleNamespace(empty=True)
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

        flow_started = Event()
        quote_started = Event()
        release_sina = Event()

        def blocked_sina_flow(*_: object) -> dict:
            flow_started.set()
            release_sina.wait(1)
            return {
                "r0_in": "3000",
                "r0_out": "1000",
                "netamount": "1500",
                "name": "Test",
                "trade": "10.10",
                "changeratio": "0.01",
            }

        def blocked_sina_quote(*_: object) -> dict:
            quote_started.set()
            release_sina.wait(1)
            return {"source_updated_at": "2026-07-10T15:00:00+08:00"}

        market_app.read_sina_object = blocked_sina_flow
        market_app.get_sina_quote = blocked_sina_quote
        try:
            with market_app.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(market_app.get_sina_fund_flow, "600519", 5)
                assert flow_started.wait(0.5)
                assert quote_started.wait(0.5)
                release_sina.set()
                parallel_sina = future.result(timeout=0.5)
                assert parallel_sina["source"] == "sina"
        finally:
            release_sina.set()
            market_app.read_sina_object = original_sina_object
            market_app.get_sina_quote = lambda *_: {
                "source_updated_at": "2026-07-10T15:00:00+08:00"
            }

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
        market_app.read_sina_object = original_sina_object
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
    market_app.PREFERRED_ROUTE_HEALTH.clear()
    calls = 0

    def loader() -> dict:
        nonlocal calls
        calls += 1
        return {
            "symbol": "600519",
            "source": "eastmoney",
            "market_time": "2026-07-10T15:00:00+08:00",
            "queried_at": "2026-07-10T15:00:03+08:00",
        }

    key = market_app.cache_key("test", {"symbol": "600519"})
    first, first_cache = market_app.get_cached_tool_data(key, 10, loader)
    second, second_cache = market_app.get_cached_tool_data(key, 10, loader)
    assert calls == 1
    assert first == second
    assert first_cache["cache_hit"] is False
    assert second_cache["cache_hit"] is True

    partial_calls = 0
    partial_key = market_app.cache_key("partial-test", {})

    def partial_loader() -> dict:
        nonlocal partial_calls
        partial_calls += 1
        return {"data_status": "partial_data", "attempt": partial_calls}

    market_app.get_cached_tool_data(
        partial_key, 300, partial_loader, partial_ttl_seconds=15
    )
    market_app.TOOL_CACHE[partial_key]["created_at"] -= market_app.timedelta(seconds=16)
    refreshed, refreshed_cache = market_app.get_cached_tool_data(
        partial_key, 300, partial_loader, partial_ttl_seconds=15
    )
    assert refreshed["attempt"] == 2
    assert refreshed_cache["cache_hit"] is False

    original_market_status_at = market_app.market_status_at
    market_app.market_status_at = lambda *_: "closed"
    try:
        result = market_app.standardize_tool_success(
            first, market_app.perf_counter(), second_cache
        )
    finally:
        market_app.market_status_at = original_market_status_at
    assert result["ok"] is True
    assert result["source"] == ["eastmoney"]
    assert result["cache_hit"] is True
    assert result["data"]["symbol"] == "600519"
    assert "latency_ms" in result
    assert result["effective_market_time"] == "2026-07-10T15:00:00+08:00"
    assert result["source_fetch_time"] == "2026-07-10T15:00:03+08:00"
    assert result["tool_queried_at"] == result["queried_at"]
    assert result["tool_queried_at"] != result["source_fetch_time"]
    assert result["staleness_basis"] == "completed_session_final"
    assert result["data"]["effective_market_time"] == result["effective_market_time"]
    assert result["data"]["source_fetch_time"] == result["source_fetch_time"]
    assert result["data"]["tool_queried_at"] == result["tool_queried_at"]
    assert result["data"]["staleness_basis"] == result["staleness_basis"]

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
    assert stale["staleness_basis"] == "recent_success_cache_after_live_source_failure"
    assert stale["served_from_stale_cache"] is True
    assert stale["cache_hit"] is True

    component_key = market_app.cache_key("component-test", {"scope": "shared"})
    market_app.get_cached_component_with_stale(
        component_key,
        10,
        120,
        lambda: {"value": "fresh", "source_errors": []},
    )
    market_app.TOOL_CACHE[component_key]["created_at"] = market_app.datetime.now(
        market_app.timezone.utc
    ) - market_app.timedelta(seconds=20)
    component_results, component_status, component_errors = market_app.collect_components(
        {
            "shared_component": lambda: market_app.get_cached_component_with_stale(
                component_key, 10, 120, failing_loader
            )
        },
        1,
        market_app.COMPOSITE_TOOL_EXECUTOR,
    )
    assert component_errors == []
    assert component_status["shared_component"]["status"] == "stale_cache"
    assert component_results["shared_component"]["served_from_stale_cache"] is True
    assert component_results["shared_component"]["source_errors"]

    market_app.record_source_health("eastmoney", True, 42)
    health = market_app.get_market_data_health_data()
    eastmoney = next(item for item in health["sources"] if item["source"] == "eastmoney")
    assert eastmoney["status"] == "healthy"
    assert health["quote_route"]["status"] == "configured"
    assert health["quote_route"]["observed_status"] == "operational_on_observed_requests"
    assert health["overall_status"] == "operational_on_observed_requests"
    assert health["observation_coverage"]["is_exhaustive_component_probe"] is False
    assert health["routing_revision"] == "capital_timeline_sector_history_v7"
    assert health["cache"]["max_entries"] == market_app.TOOL_CACHE_MAX_ENTRIES

    market_app.PREFERRED_ROUTE_HEALTH.clear()
    market_app.record_preferred_route_health(
        "kline:eastmoney", False, "temporary failure one"
    )
    market_app.record_preferred_route_health(
        "kline:eastmoney", False, "temporary failure two"
    )
    adaptive_started = market_app.perf_counter()
    adaptive_payload, adaptive_source, adaptive_errors = (
        market_app.prefer_primary_public_source(
            ("eastmoney", lambda: (sleep(0.25), {"source": "eastmoney"})[1]),
            ("tencent", lambda: {"source": "tencent"}),
            1,
            "kline:eastmoney",
        )
    )
    assert market_app.perf_counter() - adaptive_started < 0.15
    assert adaptive_payload["source"] == "tencent"
    assert adaptive_source == "tencent"
    assert any("adaptive fast fallback" in error for error in adaptive_errors)
    adaptive_health = market_app.get_market_data_health_data()
    assert adaptive_health["kline_route"]["eastmoney_circuit"][
        "adaptive_fast_fallback"
    ] is True

    original_cache_limit = market_app.TOOL_CACHE_MAX_ENTRIES
    market_app.TOOL_CACHE.clear()
    inflight_started = Event()
    release_inflight = Event()

    def blocked_cache_loader() -> dict:
        inflight_started.set()
        release_inflight.wait(1)
        return {"value": "owner"}

    with market_app.ThreadPoolExecutor(max_workers=1) as executor:
        owner = executor.submit(
            market_app.get_cached_tool_data,
            "bounded-inflight-wait",
            10,
            blocked_cache_loader,
        )
        assert inflight_started.wait(0.5)
        try:
            market_app.get_cached_tool_data(
                "bounded-inflight-wait",
                10,
                lambda: {"value": "waiter"},
                inflight_wait_timeout_seconds=0.01,
            )
            raise AssertionError("Expected the duplicate in-flight wait to be bounded.")
        except market_app.HTTPException as exc:
            assert exc.status_code == 504
        finally:
            release_inflight.set()
        assert owner.result(timeout=0.5)[0]["value"] == "owner"
    market_app.TOOL_CACHE_MAX_ENTRIES = 3
    try:
        for index in range(5):
            market_app.get_cached_tool_data(
                f"bounded-{index}",
                10,
                lambda index=index: {"value": index},
            )
        assert len(market_app.TOOL_CACHE) == 3
        assert "bounded-0" not in market_app.TOOL_CACHE
        assert "bounded-4" in market_app.TOOL_CACHE
    finally:
        market_app.TOOL_CACHE_MAX_ENTRIES = original_cache_limit

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
    original_tencent_kline = market_app.get_tencent_kline
    original_eastmoney_kline = market_app.get_eastmoney_kline
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
        historical_payload = {
            "symbol": "600519",
            "security_type": "a_share",
            "exchange": "SSE",
            "adjustment": "forward_adjusted",
            "adjustment_source_parameter": "test_qfq",
            "items": items,
            "source": "test",
            "source_errors": [],
        }
        market_app.get_tencent_kline = lambda *_args, **_kwargs: historical_payload
        market_app.get_eastmoney_kline = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            market_app.HTTPException(status_code=502, detail="test primary blocked")
        )
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

        sample_daily = [
            {"date": "2026-07-21", "close": 1.0},
            {"date": "2026-07-22", "close": 2.0},
        ]
        lunch_history, lunch_incomplete = market_app.completed_daily_history(
            sample_daily,
            market_app.datetime(
                2026, 7, 22, 11, 45, tzinfo=market_app.MARKET_TIMEZONE
            ),
        )
        assert lunch_incomplete is True
        assert lunch_history == sample_daily[:-1]
        closed_history, closed_incomplete = market_app.completed_daily_history(
            sample_daily,
            market_app.datetime(
                2026, 7, 22, 15, 5, tzinfo=market_app.MARKET_TIMEZONE
            ),
        )
        assert closed_incomplete is False
        assert closed_history == sample_daily

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
        market_app.get_tencent_kline = original_tencent_kline
        market_app.get_eastmoney_kline = original_eastmoney_kline
        market_app.read_public_json = original_json


def test_candidate_research_screen_evidence_gates() -> None:
    original_overview = market_app.get_market_overview_data
    original_filter = market_app.filter_a_share_securities_data
    original_history = market_app.get_cached_historical_context_data
    history_calls = []
    filter_calls = []
    try:
        def filtered_universe(**_: object) -> dict:
            filter_calls.append(True)
            return {"results": [
                {
                    "symbol": "600001",
                    "name": "Lower Turnover",
                    "turnover": 600_000_000,
                    "change_pct": 5.0,
                },
                {
                    "symbol": "600002",
                    "name": "Higher Turnover",
                    "turnover": 1_200_000_000,
                    "change_pct": 2.0,
                },
                ],
                "source": ["test_filter"],
                "market_time": "2026-07-10T15:00:00+08:00",
            }

        market_app.filter_a_share_securities_data = filtered_universe
        market_app.get_market_overview_data = lambda _limit: {
            "market_activity_facts": {
                "rise_count": 100,
                "fall_count": 500,
                "rise_to_fall_ratio": 0.2,
                "limit_up_count": 10,
                "limit_down_count": 30,
            },
            "market_time": "2026-07-10T15:00:00+08:00",
            "source": ["test_overview"],
        }
        market_app.get_cached_historical_context_data = lambda symbol: history_calls.append(symbol)
        blocked = market_app.screen_a_share_research_candidates_data(
            0.5, 6.0, 500_000_000, 2.0, 100_000_000_000,
            5, 8, 0.5, [20, 60], 0.0, "raw",
        )
        assert blocked["no_candidate"] is True
        assert blocked["selection_status"] == "no_candidate_due_to_market_breadth_gate"
        assert history_calls == []
        assert filter_calls == []
        assert blocked["preselected_count"] == 0

        market_app.get_market_overview_data = lambda _limit: {
            "market_activity_facts": {
                "rise_count": 600,
                "fall_count": 500,
                "rise_to_fall_ratio": 1.2,
            },
            "market_time": "2026-07-10T15:00:00+08:00",
            "source": ["test_overview"],
        }

        def history(symbol: str) -> dict:
            return_pct = 3.0 if symbol == "600002" else -1.0
            return {
                "latest_trade_date": "2026-07-10",
                "source_sessions": 260,
                "windows": {
                    str(window): {
                        "window_complete": True,
                        "return_pct": return_pct,
                        "annualized_volatility_pct": 20.0,
                        "maximum_drawdown_pct": -5.0,
                        "volume": {"latest_vs_prior_average_ratio": 1.1},
                    }
                    for window in (20, 60, 120, 250)
                },
                "source": "test_history",
            }

        market_app.get_cached_historical_context_data = history
        accepted = market_app.screen_a_share_research_candidates_data(
            0.5, 6.0, 500_000_000, 2.0, 100_000_000_000,
            5, 8, 0.5, [20, 60], 0.0, "raw",
        )
        assert accepted["no_candidate"] is False
        assert filter_calls == [True]
        assert accepted["research_candidates"][0]["symbol"] == "600002"
        assert accepted["research_candidates"][0]["evidence_gate_passed"] is True
        assert accepted["rejected_candidates"][0]["symbol"] == "600001"
        assert "history_return_below_minimum:20" in accepted["rejected_candidates"][0]["rejection_reasons"]
    finally:
        market_app.get_market_overview_data = original_overview
        market_app.filter_a_share_securities_data = original_filter
        market_app.get_cached_historical_context_data = original_history


def test_news_relevance_deduplication_and_source_metadata() -> None:
    original_reference = market_app.get_resilient_security_reference_data
    original_eastmoney = market_app.get_eastmoney_news_items
    original_google = market_app.get_google_news_items
    published_at = market_app.now_iso()
    try:
        market_app.get_resilient_security_reference_data = lambda _symbol: {
            "symbol": "600519",
            "name": "贵州茅台",
            "industry": "白酒Ⅱ",
            "source": "test_reference",
            "source_errors": [],
        }

        def item(
            title: str,
            summary: str | None,
            source: str,
            url: str,
            provider: str,
            keyword: str,
            homepage: str | None = None,
        ) -> dict:
            return {
                "published_at": published_at,
                "title": title,
                "summary": summary,
                "source": source,
                "publisher_homepage": homepage,
                "url": url,
                "link_type": "test_link",
                "retrieval_provider": provider,
                "matched_query": keyword,
                "event_date": None,
                "event_date_status": "not_verified_from_test_result",
            }

        def eastmoney_items(keyword: str, _limit: int) -> list[dict]:
            if keyword == "600519":
                return [
                    item(
                        "贵州茅台发布新品",
                        "贵州茅台（600519）发布新品。",
                        "证券时报网",
                        "https://example.com/direct",
                        "eastmoney_news_search",
                        keyword,
                    ),
                    item(
                        "股票行情快报：47股资金流向一览",
                        "表格末尾出现贵州茅台（600519）。",
                        "普通网站",
                        "https://example.com/noise",
                        "eastmoney_news_search",
                        keyword,
                    ),
                ]
            return [
                item(
                    "贵州茅台发布新品！",
                    "同一事件的重复稿件。",
                    "普通网站",
                    "https://example.com/duplicate",
                    "eastmoney_news_search",
                    keyword,
                )
            ]

        def google_items(keyword: str, _limit: int) -> list[dict]:
            if "行业" in keyword:
                return [
                    item(
                        "白酒行业发布新的公开统计数据",
                        None,
                        "新华社",
                        "https://news.google.com/industry",
                        "google_news_rss",
                        keyword,
                        "https://www.news.cn",
                    )
                ]
            return [
                item(
                    "贵州茅台回应市场关注事项",
                    None,
                    "第一财经",
                    "https://news.google.com/company",
                    "google_news_rss",
                    keyword,
                    "https://www.yicai.com",
                ),
                item(
                    "贵州茅台研究论文",
                    None,
                    "arXiv",
                    "https://news.google.com/paper",
                    "google_news_rss",
                    keyword,
                    "https://arxiv.org",
                ),
                item(
                    "中际旭创市值超贵州茅台，资金从消费流向AI",
                    None,
                    "普通网站",
                    "https://news.google.com/comparison",
                    "google_news_rss",
                    keyword,
                    "https://example.com",
                ),
                item(
                    "大跌之下，红利质量ETF出现抢分红行情，登记日在即，贵州茅台逆市红盘",
                    None,
                    "普通网站",
                    "https://news.google.com/roundup",
                    "google_news_rss",
                    keyword,
                    "https://example.com",
                ),
            ]

        market_app.get_eastmoney_news_items = eastmoney_items
        market_app.get_google_news_items = google_items
        result = market_app.get_news_data("600519", 10, 30, True)

        assert result["name"] == "贵州茅台"
        assert result["industry"] == "白酒"
        assert result["duplicate_count"] == 1
        assert result["excluded_count"] == 4
        assert {row["relevance_scope"] for row in result["items"]} == {
            "company",
            "industry_context",
        }
        assert all(row["event_date"] is None for row in result["items"])
        assert all("arxiv" not in str(row["source"]).lower() for row in result["items"])
        assert "google_news_rss" in result["source"]
        assert "eastmoney_news_search" in result["source"]
    finally:
        market_app.get_resilient_security_reference_data = original_reference
        market_app.get_eastmoney_news_items = original_eastmoney
        market_app.get_google_news_items = original_google


def test_rotation_overnight_and_event_helpers() -> None:
    returns = market_app.kline_lookback_returns(
        [{"close": 100}, {"close": 105}, {"close": 110}, {"close": 121}],
        [1, 3, 5],
    )
    assert returns == {"1": 10.0, "3": 21.0, "5": None}

    parsed = market_app.parse_sina_overnight_record(
        "hf_NQ",
        "global_futures",
        [
            "110", "", "109", "110", "112", "108", "10:00:00", "100", "101",
            "0", "1", "1", "2026-07-10", "Nasdaq futures",
        ],
    )
    assert parsed is not None
    assert parsed["change_pct"] == 10.0
    assert parsed["market_time"] == "2026-07-10T10:00:00+08:00"
    assert market_app.OVERNIGHT_INSTRUMENT_METADATA["comex_copper"]["price_unit"] == "US_cent_per_pound"
    assert market_app.OVERNIGHT_INSTRUMENT_METADATA["comex_copper"]["contract_size"] == 25_000

    bars = [
        {"date": "2026-07-10", "close": 100},
        {"date": "2026-07-13", "close": 101},
        {"date": "2026-07-14", "close": 103},
        {"date": "2026-07-15", "close": 106},
        {"date": "2026-07-16", "close": 107},
        {"date": "2026-07-17", "close": 110},
    ]
    feedback = market_app.event_price_feedback("2026-07-10", bars)
    assert feedback["return_after_1_session_pct"] == 1.0
    assert feedback["return_after_3_sessions_pct"] == 6.0
    assert feedback["return_after_5_sessions_pct"] == 10.0
    assert market_app.event_titles_match("公司回购股份方案", "关于公司回购股份方案的公告") is True

    capital_records = market_app.capital_activity_timeline_records(
        {
            "components": {
                "block_trades": {
                    "institution_related_items": [
                        {"trade_date": "2026-07-13", "deal_amount_cny": 500}
                    ]
                },
                "institutional_research": {
                    "items": [
                        {"research_start_date": "2026-07-09", "participant_count": 12}
                    ]
                },
            }
        }
    )
    nearby = market_app.nearby_capital_activity("2026-07-10", capital_records)
    assert len(nearby) == 2
    assert {item["calendar_days_from_event"] for item in nearby} == {-1, 3}
    assert all(item["relationship_status"] == "temporal_proximity_only_not_causation" for item in nearby)

    original_json = market_app.read_public_json
    def sector_json(url: str, *args, **kwargs) -> dict:
        if "7.push2his" not in url:
            raise market_app.HTTPException(status_code=502, detail="test node failure")
        return {"data": {"name": "Test board", "klines": ["2026-07-20,100,101,102,99,10,1000,3,1,1,2"]}}
    market_app.read_public_json = sector_json
    try:
        sector_history = market_app.get_eastmoney_generic_daily_kline("90.BK0001", 30)
    finally:
        market_app.read_public_json = original_json
    assert sector_history["items"][0]["close"] == 101
    assert sector_history["source"] == "7.push2his.eastmoney.com"

    original_sws_json = market_app.read_swsresearch_json

    def sws_json(path: str, parameters: dict) -> dict:
        return {
            "code": "200",
            "data": {
                "results": [
                    {
                        "swindexcode": "801016",
                        "swindexname": "Planting",
                        "bargaindate": parameters["start_date"],
                        "closeindex": 101,
                        "markup": 1,
                        "bargainamount": 10,
                        "turnoverrate": 2,
                    }
                ]
            },
        }

    market_app.read_swsresearch_json = sws_json
    market_app.TOOL_CACHE.pop(
        market_app.cache_key("swsresearch_recent_level2_history", {"limit": 30}), None
    )
    try:
        sws_history = market_app.get_swsresearch_industry_daily_kline("Planting\u2161", 30)
    finally:
        market_app.read_swsresearch_json = original_sws_json
    assert sws_history["provider_identifier"] == "801016"
    assert sws_history["items"][0]["close"] == 101
    assert sws_history["source"] == "swsresearch_official_index_history"


def test_fund_and_portfolio_exposure_calculations() -> None:
    normalized, input_total = market_app.normalized_portfolio_positions(
        [
            {"identifier": "512760", "weight_pct": 60},
            {"identifier": "600519", "weight_pct": 40},
        ],
        False,
    )
    assert input_total == 100
    assert normalized[0]["asset_type"] == "fund"
    assert normalized[1]["asset_type"] == "stock"

    original_component = market_app.get_eastmoney_fund_component
    market_app.get_eastmoney_fund_component = lambda _code, endpoint: (
        {
            "Datas": {
                "fundStocks": [
                    {
                        "GPDM": "00700",
                        "GPJC": "Tencent",
                        "JZBL": "5",
                        "TEXCH": "116",
                    }
                ]
            },
            "Expansion": "2026-03-31",
        }
        if endpoint == "FundMNInverstPosition"
        else {"Datas": [{"GP": "5", "FSRQ": "2026-03-31"}]}
        if endpoint == "FundMNAssetAllocationNew"
        else {"Datas": {}}
        if endpoint == "FundMNNBasicInformation"
        else {"Datas": []}
    )
    try:
        foreign = market_app.get_fund_exposure_data("008888", 10, "raw")
        assert foreign["top_holdings"][0]["identifier"] == "116:00700"
        assert foreign["top_holdings"][0]["symbol"] is None
        assert foreign["top_holdings"][0]["provider_security_code"] == "00700"
    finally:
        market_app.get_eastmoney_fund_component = original_component

    market_app.get_eastmoney_fund_component = lambda code, endpoint: (
        {
            "Datas": {
                "fundStocks": (
                    [{"GPDM": "600050", "GPJC": "China Unicom", "JZBL": "8", "TEXCH": "1"}]
                    if code == "515050"
                    else [{"GPDM": "600519", "GPJC": "Direct holding", "JZBL": "0.2", "TEXCH": "1"}]
                ),
                **(
                    {"ETFCODE": "515050", "ETFSHORTNAME": "Communication ETF"}
                    if code == "008087"
                    else {}
                ),
            },
            "Expansion": "2026-03-31",
        }
        if endpoint == "FundMNInverstPosition"
        else {
            "Datas": [
                {
                    "GP": "2.2" if code == "008087" else "90",
                    "JJ": "92.7" if code == "008087" else "0",
                    "FSRQ": "2026-03-31",
                }
            ]
        }
        if endpoint == "FundMNAssetAllocationNew"
        else {"Datas": {"SHORTNAME": f"Fund {code}"}}
        if endpoint == "FundMNNBasicInformation"
        else {"Datas": []}
    )
    try:
        feeder = market_app.get_fund_exposure_data("008087", 10, "raw")
        assert feeder["underlying_fund_code"] == "515050"
        assert feeder["underlying_fund_weight_pct"] == 92.7
        assert feeder["top_holdings_reported_weight_pct"] == 0.2
        assert feeder["look_through_holdings"][0]["symbol"] == "600050"
        assert feeder["look_through_holdings"][0]["underlying_fund_holding_weight_pct"] == 8
        assert feeder["look_through_holdings"][0]["weight_pct"] == 7.416
    finally:
        market_app.get_eastmoney_fund_component = original_component

    original_fund = market_app.get_fund_exposure_data
    original_reference = market_app.get_fast_portfolio_security_reference
    market_app.get_fund_exposure_data = fake_get_fund_exposure_data
    market_app.get_fast_portfolio_security_reference = lambda symbol: {
        "symbol": symbol,
        "name": "Test Stock",
        "industry": "Manufacturing",
        "source": ["test"],
        "source_errors": [],
    }
    try:
        result = market_app.get_portfolio_exposure_data(
            [
                {"identifier": "512760", "weight_pct": 60},
                {"identifier": "600519", "weight_pct": 40},
            ],
            False,
            10,
            "raw",
        )
        assert result["asset_allocation"]["stock_pct"] == 94.0
        assert result["asset_allocation"]["cash_pct"] == 6.0
        assert result["underlying_exposure"][0]["symbol"] == "600519"
        assert result["underlying_exposure"][0]["exposure_pct"] == 46.0
        assert result["overlapping_underlyings"][0]["source_position_count"] == 2
        assert result["industry_exposure"][0]["exposure_pct"] == 46.0
        assert result["fund_reported_industry_exposure"][0]["exposure_pct"] == 48.0

        def partial_fund(
            code: str, limit: int, level: str, look_through_depth: int = 1
        ) -> dict:
            payload = fake_get_fund_exposure_data(code, limit, level)
            payload["data_status"] = "partial_data"
            payload["missing_fields"] = ["industry_distribution"]
            payload["source_errors"] = [
                {
                    "source": "child_test",
                    "error_type": "upstream_failure",
                    "message": "partial child",
                }
            ]
            return payload

        market_app.get_fund_exposure_data = partial_fund
        partial = market_app.get_portfolio_exposure_data(
            [{"identifier": "512760", "weight_pct": 100}],
            False,
            9,
            "summary",
        )
        assert partial["data_status"] == "partial_data"
        assert partial["partial_child_components"] == ["fund:512760"]
        assert "fund:512760:industry_distribution" in partial["missing_fields"]
        assert partial["source_errors"][0]["source"] == "fund:512760/child_test"
    finally:
        market_app.get_fund_exposure_data = original_fund
        market_app.get_fast_portfolio_security_reference = original_reference


def test_decision_context_follow_up_recommendations() -> None:
    recommendations = market_app.decision_context_follow_up_tools(
        "600519",
        "index:000001",
        {
            "quote": {"status": "available", "latency_ms": 10},
            "intraday": {"status": "stale_cache", "latency_ms": 1},
            "historical_context": {
                "status": "unavailable_within_response_budget",
                "latency_ms": None,
            },
            "official_announcements": {
                "status": "unavailable",
                "latency_ms": None,
            },
            "news": {
                "status": "unavailable_within_response_budget",
                "latency_ms": None,
            },
        },
    )
    assert [item["missing_component"] for item in recommendations] == [
        "historical_context",
        "news",
        "official_announcements",
    ]
    assert recommendations[0] == {
        "missing_component": "historical_context",
        "tool": "get_a_share_historical_context",
        "arguments": {"symbol": "600519"},
        "reason": "unavailable_within_response_budget",
    }
    assert recommendations[1]["arguments"] == {
        "symbol": "600519",
        "limit": 8,
        "days": 30,
        "include_industry_context": False,
    }


def test_market_cross_checks_preserve_coexisting_signals() -> None:
    checks = market_app.build_market_cross_checks(
        [
            {"symbol": "000001", "change_pct": -1.0},
            {"symbol": "399001", "change_pct": -1.4},
        ],
        [
            {"symbol": "BK0475", "name": "银行", "change_pct": -0.2},
            {"symbol": "BK0428", "name": "电力", "change_pct": -0.5},
        ],
        {"all_market": {"rise_count": 500, "fall_count": 4500}},
    )

    assert checks["breadth_counts"]["fall_to_rise_ratio"] == 9.0
    assert checks["primary_index_equal_weight_change_pct"] == -1.2
    assert checks["relationship_status"] == "broad_weakness_and_relative_resilience_coexist"
    assert checks["coexisting_signals"] == [
        "falling_stocks_outnumber_rising_stocks",
        "returned_industry_boards_outperform_primary_index_equal_weight_change",
    ]
    assert [item["name"] for item in checks["relative_resilience_candidates"]] == [
        "银行",
        "电力",
    ]
    assert checks["missing_inputs"] == []

    incomplete = market_app.build_market_cross_checks([], [], None)
    assert incomplete["relationship_status"] == "coexistence_not_observed_in_returned_snapshot"
    assert incomplete["missing_inputs"] == [
        "market_breadth",
        "primary_index_changes",
        "industry_boards",
    ]


def test_ipo_subscription_status_contract() -> None:
    original_json = market_app.read_public_json
    captured: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    fast_cache_key = market_app.cache_key(
        "ipo_recent_calendar_component", {"limit": 100}
    )
    market_app.TOOL_CACHE.pop(fast_cache_key, None)

    normalized_fast_row = market_app.normalize_fast_ipo_row(
        {
            "SECURITY_CODE": "301707",
            "SECURITY_NAME": "Test IPO",
            "APPLY_NUM_UPPER": 5500,
            "SELECT_LISTING_DATE": "2026-08-05 00:00:00",
        }
    )
    assert normalized_fast_row["ONLINE_APPLY_UPPER"] == 5500
    assert normalized_fast_row["LISTING_DATE"] == "2026-08-05 00:00:00"

    def fake_ipo_json(url: str, *args: object, **kwargs: object) -> dict:
        captured.append((url, args, kwargs))
        return {
            "success": True,
            "result": {
                "data": [
                    {
                        "SECURITY_CODE": "301707",
                        "SECURITY_NAME_ABBR": "Test IPO",
                        "APPLY_CODE": "301707",
                        "APPLY_DATE": "2026-07-27 00:00:00",
                        "ASSIGN_DATE": "2026-07-28 00:00:00",
                        "BALLOT_NUM_DATE": "2026-07-29 00:00:00",
                        "BALLOT_PAY_DATE": "2026-07-29 00:00:00",
                        "LISTING_DATE": None,
                        "MARKET": "深交所创业板",
                        "ISSUE_PRICE": None,
                        "ONLINE_APPLY_UPPER": 5500,
                        "ONLINE_ISSUE_NUM": 5756500,
                        "UP_DATE": "2026-07-17 00:00:00",
                    }
                ]
            },
        }

    market_app.read_public_json = fake_ipo_json
    try:
        result = market_app.get_ipo_subscription_status_data(
            "301707", 30, 7, 10, "raw"
        )
        item = result["items"][0]
        assert item["subscription_code"] == "301707"
        assert item["eligibility_rules"]["exchange"] == "SZSE"
        assert item["eligibility_rules"]["required_permission"] == "chinext_trading_permission"
        assert "szse.cn" in item["eligibility_rules"]["official_rule_url"]
        assert item["maximum_subscription_market_value_requirement_cny"] == 55000
        assert item["pending_fields"] == ["issue_price", "listing_date"]
        assert result["data_status"] == "partial_data"
        assert captured[0][0].startswith(market_app.IPO_CALENDAR_FAST_API)
        assert captured[0][2] == {"timeout": 2, "attempts": 1}
        assert result["source"] == ["eastmoney_datapc_ipo_calendar"]

        market_app.read_public_json = lambda *_args, **_kwargs: {
            "success": True,
            "result": ["malformed"],
        }
        try:
            market_app.get_datacenter_ipo_calendar_rows("301707", 10)
        except market_app.HTTPException as exc:
            assert exc.status_code == 502
        else:
            raise AssertionError("Malformed IPO result should be rejected with 502.")

        name_urls: list[str] = []

        def fake_name_json(url: str, *_: object, **__: object) -> dict:
            name_urls.append(url)
            return {
                "success": True,
                "result": {
                    "data": [
                        {
                            "SECURITY_CODE": "301707",
                            "SECURITY_NAME_ABBR": "TestIPO",
                            "APPLY_CODE": "301707",
                            "APPLY_DATE": "2026-07-27 00:00:00",
                            "ISSUE_PRICE": 10,
                            "ONLINE_APPLY_UPPER": 5000,
                            "LISTING_DATE": "2026-08-05 00:00:00",
                        }
                    ]
                },
            }

        market_app.read_public_json = fake_name_json
        market_app.TOOL_CACHE.pop(fast_cache_key, None)
        name_match = market_app.get_ipo_subscription_status_data(
            "TestIPO", 30, 7, 10, "summary"
        )
        assert name_match["items"][0]["security_name"] == "TestIPO"
        assert name_urls[0].startswith(market_app.IPO_CALENDAR_FAST_API)
        assert name_match["source"] == ["eastmoney_datapc_ipo_calendar"]

        today = market_app.datetime.now(market_app.MARKET_TIMEZONE).date()

        def fake_calendar_json(_url: str, *_: object, **__: object) -> dict:
            def row(code: str, apply_date: object) -> dict[str, object]:
                return {
                    "SECURITY_CODE": code,
                    "SECURITY_NAME_ABBR": f"IPO {code}",
                    "APPLY_CODE": code,
                    "APPLY_DATE": str(apply_date),
                    "ISSUE_PRICE": 10,
                    "ONLINE_APPLY_UPPER": 5000,
                    "LISTING_DATE": str(today + market_app.timedelta(days=10)),
                }

            return {
                "success": True,
                "result": {
                    "data": [
                        row("301708", today),
                        row("301709", today + market_app.timedelta(days=5)),
                    ]
                },
            }

        market_app.read_public_json = fake_calendar_json
        market_app.TOOL_CACHE.pop(fast_cache_key, None)
        calendar = market_app.get_ipo_subscription_status_data(
            None, 1, 1, 10, "summary"
        )
        assert calendar["count"] == 1
        assert calendar["items"][0]["security_code"] == "301708"
        assert calendar["schedule_range"] == {
            "start": (today - market_app.timedelta(days=1)).isoformat(),
            "end": (today + market_app.timedelta(days=1)).isoformat(),
        }

        def fake_subscription_code_json(url: str, *_: object, **__: object) -> dict:
            if "APPLY_CODE%3D" not in url:
                return {"success": False, "message": "返回数据为空", "result": None}
            return {
                "success": True,
                "result": {
                    "data": [
                        {
                            "SECURITY_CODE": "603407",
                            "SECURITY_NAME_ABBR": "Subscription Code Match",
                            "APPLY_CODE": "732407",
                            "APPLY_DATE": "2026-04-27 00:00:00",
                            "MARKET": "上交所主板",
                        }
                    ]
                },
            }

        market_app.read_public_json = fake_subscription_code_json
        market_app.TOOL_CACHE.pop(fast_cache_key, None)
        subscription_code_match = market_app.get_ipo_subscription_status_data(
            "732407", 30, 7, 10, "summary"
        )
        assert subscription_code_match["items"][0]["security_code"] == "603407"
        assert subscription_code_match["items"][0]["subscription_code"] == "732407"
        assert subscription_code_match["source"] == [
            "eastmoney_datacenter_ipo_calendar"
        ]
        assert subscription_code_match["source_errors"][0]["source"] == (
            "eastmoney_datapc_ipo_calendar"
        )

        bse = market_app.build_ipo_subscription_item(
            {
                "SECURITY_CODE": "920065",
                "SECURITY_NAME_ABBR": "BSE IPO",
                "APPLY_CODE": "920065",
                "APPLY_DATE": "2026-07-20 00:00:00",
                "ISSUE_PRICE": 24.3,
                "ONLINE_APPLY_UPPER": 787500,
                "IS_BEIJING": 1,
            },
            "summary",
        )
        assert bse["market"] == "北京证券交易所"
        assert bse["subscription_unit_shares"] == 100
        assert bse["eligibility_rules"]["subscription_method"] == "full_cash_subscription"
        assert bse["maximum_subscription_market_value_requirement_cny"] is None
        assert bse["maximum_subscription_cash_cny"] == 19136250.0

        incomplete_limit = market_app.build_ipo_subscription_item(
            {
                "SECURITY_CODE": "603408",
                "APPLY_DATE": "2026-07-20 00:00:00",
                "ISSUE_PRICE": 10,
                "LISTING_DATE": "2026-08-01 00:00:00",
            },
            "summary",
        )
        assert incomplete_limit["pending_fields"] == [
            "online_subscription_limit_shares"
        ]

        sse_rules = market_app.ipo_market_rules(
            {"SECURITY_CODE": "603407", "MARKET": "涓婁氦鎵€涓绘澘"}
        )
        assert "sse.com.cn/lawandrules/sselawsrules2025" in sse_rules["official_rule_url"]

        today = market_app.datetime.now(market_app.MARKET_TIMEZONE).date()
        assert market_app.ipo_subscription_stage(
            {
                "APPLY_DATE": (today - market_app.timedelta(days=2)).isoformat(),
                "BALLOT_PAY_DATE": today.isoformat(),
                "LISTING_DATE": (today + market_app.timedelta(days=5)).isoformat(),
            },
            today.isoformat(),
        ) == "ballot_result_and_payment_today"

        try:
            market_app.normalize_ipo_query('301707")')
        except market_app.HTTPException as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError("Unsafe IPO query characters should be rejected.")
    finally:
        market_app.read_public_json = original_json
        market_app.TOOL_CACHE.pop(fast_cache_key, None)


def test_trading_calendar_and_capital_activity_contracts() -> None:
    calendar = market_app.get_a_share_trading_calendar_data("2026-02-13", "2026-02-25", "raw")
    by_date = {item["date"]: item for item in calendar["items"]}
    assert by_date["2026-02-16"]["session_type"] == "closed_official_holiday"
    assert by_date["2026-02-24"]["is_trading_day"] is True
    assert market_app.market_status_at(market_app.datetime(2026, 2, 16, 10, 0,
        tzinfo=market_app.MARKET_TIMEZONE)) == "closed"
    assert market_app.get_a_share_trading_calendar_data("2027-01-04", "2027-01-04", "summary")["data_status"] == "partial_data"
    original = market_app.get_eastmoney_datacenter_rows
    def rows(report, row_filter, sort, page_size=20):
        if report == "RPT_DAILYBILLBOARD_DETAILSNEW":
            return [{"TRADE_DATE": "2026-07-20", "BILLBOARD_NET_AMT": 20}]
        if report in {"RPT_BILLBOARD_DAILYDETAILSBUY", "RPT_BILLBOARD_DAILYDETAILSSELL"}:
            return [{"OPERATEDEPT_CODE": "0", "OPERATEDEPT_NAME": "机构专用", "BUY": 60, "SELL": 40, "NET": 20}]
        if report == "RPT_DATA_BLOCKTRADE":
            return [{"TRADE_DATE": "2026-07-19", "DEAL_PRICE": 101, "CLOSE_PRICE": 100, "DEAL_AMT": 500,
                     "BUYER_CODE": "0", "BUYER_NAME": "机构专用", "SELLER_CODE": "1"}]
        if report == "RPT_ORG_SURVEYNEW":
            return [{"NOTICE_DATE": "2026-07-18", "SUM": 12}]
        if report == "RPTA_WEB_RZRQ_GGMX":
            return [{"DATE": "2026-07-21", "RZYE": 1000}]
        if report == "RPT_HOLDERNUMLATEST":
            return [{"END_DATE": "2026-06-30", "HOLDER_NUM": 100}]
        raise AssertionError(report)
    market_app.get_eastmoney_datacenter_rows = rows
    try:
        result = market_app.get_a_share_capital_activity_data("600519", 90, 10, "raw")
    finally:
        market_app.get_eastmoney_datacenter_rows = original
    assert result["data_status"] == "full_data"
    assert result["components"]["dragon_tiger"]["latest_institution_net_amount_cny"] == 20
    assert result["components"]["block_trades"]["institution_buy_amount_cny"] == 500
    assert result["components"]["block_trades"]["items"][0]["premium_discount_pct"] == 1.0
    assert result["historical_comparisons"]["block_trades"]["institution_buy_amount_cny"]["recent_total"] == 500
    comparison = market_app.dated_window_totals(
        [
            {"date": market_app.datetime.now(market_app.MARKET_TIMEZONE).date().isoformat(), "amount": 20},
            {"date": (market_app.datetime.now(market_app.MARKET_TIMEZONE).date() - market_app.timedelta(days=31)).isoformat(), "amount": 10},
        ],
        "date",
        ("amount",),
    )
    assert comparison["amount"]["change_pct"] == 100.0


def main() -> None:
    test_kline_source_parsers()
    test_kline_range_and_pagination()
    test_tencent_minute_kline_adjustment_fallback()
    test_synchronized_market_snapshot_contract()
    test_snapshot_compensates_for_a_late_batch_component()
    test_search_source_parser()
    test_etf_market_routing_and_search()
    test_quote_unit_normalization()
    test_quote_timestamp_semantics()
    test_industry_board_parser()
    test_industry_board_deduplication()
    test_market_structure_calculations()
    test_limit_activity_and_index_identity()
    test_batch_quotes_intraday_indicators_and_filtering()
    test_intraday_session_filter_and_market_time_cap()
    test_market_quote_pagination()
    test_sina_market_pagination_and_breadth_fallback()
    test_fast_market_aggregate()
    test_intraday_and_index_fallback_parsers()
    test_announcements_relative_strength_and_anomaly_scan()
    test_news_relevance_deduplication_and_source_metadata()
    test_reliability_envelope_cache_and_health()
    test_historical_context_and_security_status_facts()
    test_candidate_research_screen_evidence_gates()
    test_rotation_overnight_and_event_helpers()
    test_fund_and_portfolio_exposure_calculations()
    test_decision_context_follow_up_recommendations()
    test_market_cross_checks_preserve_coexisting_signals()
    test_ipo_subscription_status_contract()
    test_trading_calendar_and_capital_activity_contracts()
    market_app.TOOL_CACHE.clear()
    market_app.search_stock_data = fake_search_stock_data
    market_app.get_quote_data = fake_get_quote_data
    market_app.get_batch_quote_data = fake_get_batch_quote_data
    market_app.get_kline_data = fake_get_kline_data
    market_app.get_intraday_data = fake_get_intraday_data
    market_app.get_auction_data = fake_get_auction_data
    market_app.filter_a_share_securities_data = fake_filter_a_share_securities_data
    market_app.screen_a_share_research_candidates_data = fake_screen_a_share_research_candidates_data
    market_app.get_fund_flow_data = fake_get_fund_flow_data
    market_app.get_financial_data = fake_get_financial_data
    market_app.get_fund_exposure_data = fake_get_fund_exposure_data
    market_app.get_portfolio_exposure_data = fake_get_portfolio_exposure_data
    market_app.get_news_data = fake_get_news_data
    market_app.get_announcement_data = fake_get_announcement_data
    market_app.get_event_timeline_data = fake_get_event_timeline_data
    market_app.get_historical_context_data = fake_get_historical_context_data
    market_app.get_security_status_data = fake_get_security_status_data
    market_app.get_decision_context_data = fake_get_decision_context_data
    market_app.get_relative_strength_data = fake_get_relative_strength_data
    market_app.scan_intraday_anomalies_data = fake_scan_intraday_anomalies_data
    market_app.get_market_overview_data = fake_get_market_overview_data
    market_app.get_market_snapshot_data = fake_get_market_snapshot_data
    market_app.get_limit_activity_data = fake_get_limit_activity_data
    market_app.get_sector_rankings_data = fake_get_sector_rankings_data
    market_app.get_sector_rotation_data = fake_get_sector_rotation_data
    market_app.get_overnight_risk_packet_data = fake_get_overnight_risk_packet_data
    market_app.get_ipo_subscription_status_data = fake_get_ipo_subscription_status_data
    market_app.get_a_share_trading_calendar_data = fake_get_a_share_trading_calendar_data
    market_app.get_a_share_capital_activity_data = fake_get_a_share_capital_activity_data
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }

    with TestClient(market_app.app, base_url="http://127.0.0.1:8000") as client:
        health = client.get("/health")
        assert health.status_code == 200, health.text
        assert health.json()["routing_revision"] == "capital_timeline_sector_history_v7"

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
            "screen_a_share_research_candidates",
            "get_a_share_fund_flow",
            "get_a_share_financials",
            "get_fund_exposure",
            "get_portfolio_exposure",
            "get_ipo_subscription_status",
            "get_a_share_trading_calendar",
            "get_a_share_capital_activity",
            "get_a_share_news",
            "get_a_share_announcements",
            "get_a_share_event_timeline",
            "get_a_share_historical_context",
            "get_a_share_security_status",
            "get_a_share_decision_context",
            "get_a_share_relative_strength",
            "scan_a_share_intraday_anomalies",
            "get_a_share_sector_rankings",
            "get_a_share_sector_rotation",
            "get_overnight_risk_packet",
            "get_a_share_limit_activity",
            "get_a_share_market_snapshot",
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
        assert result["total_market_value"] == 999999999
        assert result["total_market_value_unit"] == "CNY"
        assert result["circulating_market_value"] == 888888888
        assert result["circulating_market_value_unit"] == "CNY"

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
            (14, "get_a_share_limit_activity", {}),
            (15, "get_a_share_market_snapshot", {"symbol": "600519"}),
            (16, "get_a_share_market_overview", {}),
            (17, "get_market_data_health", {}),
            (18, "get_a_share_announcements", {"symbol": "600519"}),
            (
                19,
                "get_a_share_relative_strength",
                {"symbol": "600519", "peer_symbols": ["600000"]},
            ),
            (
                20,
                "scan_a_share_intraday_anomalies",
                {"symbols": ["600519"], "benchmark_symbol": "index:000001"},
            ),
            (21, "get_a_share_historical_context", {"symbol": "600519"}),
            (22, "get_a_share_security_status", {"symbol": "600519"}),
            (
                23,
                "get_a_share_decision_context",
                {"symbol": "600519", "benchmark_symbol": "index:000001"},
            ),
            (24, "get_a_share_event_timeline", {"symbol": "600519"}),
            (25, "get_a_share_sector_rotation", {"sector_type": "industry"}),
            (26, "get_overnight_risk_packet", {}),
            (27, "get_fund_exposure", {"fund_code": "512760"}),
            (
                28,
                "get_portfolio_exposure",
                {
                    "positions": [
                        {"identifier": "512760", "weight_pct": 60},
                        {"identifier": "600519", "weight_pct": 40},
                    ]
                },
            ),
            (29, "get_ipo_subscription_status", {"symbol_or_name": "301707"}),
            (30, "screen_a_share_research_candidates", {}),
            (31, "get_a_share_trading_calendar", {"start_date": "2026-07-20", "end_date": "2026-07-24"}),
            (32, "get_a_share_capital_activity", {"symbol": "600519"}),
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
