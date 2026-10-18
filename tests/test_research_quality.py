"""Offline regressions for evidence quality; no public-source requests."""

from copy import deepcopy
from datetime import date, timedelta

import pytest

from test_mcp import market_app as app


@pytest.fixture
def packet_sources(monkeypatch):
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()
    payloads = {
        "quote": {"quote": {"price": 10, "name": "Test"}},
        "intraday": {"items": [{"price": 10}]},
        "historical_context": {
            "latest_complete_close": 10,
            "windows": {"20": {"window_complete": True, "return_pct": 1}},
        },
        "security_reference": {"name": "Test", "listing_date": "2000-01-01"},
        "relative_strength": {"relative_to_benchmark_pct_points": 0},
        "market_overview": {"indices": [{"price": 3000}]},
        "news": {"items": [{"title": "Test news"}]},
        "official_announcements": {"items": [{"title": "Test announcement"}]},
        "financials": {"items": [{"report_period": "2020-01-01", "basic_eps": 1.5}]},
    }
    getters = {
        "quote": "get_quote_data",
        "intraday": "get_intraday_data",
        "historical_context": "get_cached_historical_context_data",
        "security_reference": "get_resilient_security_reference_data",
        "relative_strength": "get_relative_strength_data",
        "market_overview": "get_market_overview_data",
        "news": "get_news_data",
        "official_announcements": "get_announcement_data",
        "financials": "get_financial_data",
    }
    for name, getter in getters.items():
        monkeypatch.setattr(
            app, getter, lambda *_args, key=name: deepcopy(payloads[key])
        )
    yield payloads
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()


def test_normal_packet_and_old_disclosure_are_complete(packet_sources):
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "full_data"
    assert result["available_component_count"] == 9
    assert result["complete_fresh_component_count"] == 9
    assert result["recommended_follow_up_tools"] == []
    assert result["component_status"]["financials"]["is_fresh"] is True


@pytest.mark.parametrize(
    "status,available", [("partial_data", 9), ("no_data", 8), ("unavailable", 8)]
)
def test_inner_data_status_is_not_upgraded(packet_sources, status, available):
    packet_sources["market_overview"]["data_status"] = status
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "partial_data"
    assert result["available_component_count"] == available
    assert result["complete_fresh_component_count"] == 8
    assert [
        item["missing_component"] for item in result["recommended_follow_up_tools"]
    ] == ["market_overview"]


@pytest.mark.parametrize("flag", ["served_from_stale_cache", "is_stale"])
def test_stale_component_has_separate_freshness_and_followup(packet_sources, flag):
    packet_sources["historical_context"][flag] = True
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "partial_data"
    assert (
        result["available_component_count"] == result["complete_component_count"] == 9
    )
    assert result["fresh_component_count"] == 8
    assert result["recommended_follow_up_tools"][0]["reason"] == "stale_cache"


@pytest.mark.parametrize(
    "component,payload",
    [
        ("quote", {"quote": {"symbol": "600519", "name": "Test", "price": None}}),
        ("intraday", {"items": [{"time": "2026-01-01", "price": None}]}),
        (
            "historical_context",
            {"windows": {"20": {"requested_sessions": 20, "window_complete": False}}},
        ),
        ("financials", {"items": [{"report_period": "2020-01-01", "basic_eps": None}]}),
        ("market_overview", {"indices": [{"symbol": "000001", "name": "Index"}]}),
        ("news", {"source": "test", "items": []}),
    ],
)
def test_metadata_or_empty_payload_is_not_evidence(packet_sources, component, payload):
    packet_sources[component] = payload
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "partial_data"
    assert result["component_status"][component]["has_data"] is False
    assert [
        item["missing_component"] for item in result["recommended_follow_up_tools"]
    ] == [component]


def test_security_status_needs_quote_and_reference(packet_sources):
    packet_sources["quote"] = {"quote": {"price": None}}
    result = app.get_decision_context_data("600519", None)
    status = result["component_status"]["security_status"]
    assert status["has_data"] is True  # usable reference still exists
    assert status["is_complete"] is False
    assert result["complete_component_count"] == 7
    assert result["decision_inputs"]["security_status"]["data_status"] == "partial_data"


def test_successful_empty_searches_do_not_trigger_refill(packet_sources):
    packet_sources["official_announcements"] = {
        "count": 0,
        "items": [],
        "data_status": "no_data",
        "source_errors": [],
        "source": ["official_sse_announcements"],
        "period": {"start": "2026-01-01", "end": "2026-02-01"},
    }
    packet_sources["news"] = {
        "count": 0,
        "items": [],
        "provider_status": {"test": {"status": "available"}},
        "source_errors": [],
    }
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "full_data"
    assert result["available_component_count"] == 7
    assert result["complete_component_count"] == 9
    assert result["recommended_follow_up_tools"] == []
    assert result["component_status"]["news"]["status"] == "empty_result"
    assert result["component_status"]["official_announcements"]["no_records"] is True


def test_blocked_empty_announcements_are_not_successful_empty(packet_sources):
    packet_sources["official_announcements"] = {
        "count": 0,
        "items": [],
        "data_status": "unavailable",
        "source_errors": [{"error_type": "official_source_blocked"}],
    }
    result = app.get_decision_context_data("600519", None)
    assert result["data_status"] == "partial_data"
    assert (
        result["recommended_follow_up_tools"][0]["missing_component"]
        == "official_announcements"
    )


def test_packet_retains_cache_metadata(packet_sources):
    app.get_decision_context_data("600519", None)
    result = app.get_decision_context_data("600519", None)
    assert result["component_status"]["financials"]["cache_hit"] is True
    assert result["component_status"]["financials"]["cache_age_seconds"] >= 0
    assert result["component_status"]["financials"]["cache_created_at"]


def history_rows(count=21):
    return [
        {
            "date": (date(2026, 1, 1) + timedelta(days=index)).isoformat(),
            "close": 10 + index,
            "high": 11 + index,
            "low": 9 + index,
            "volume": 100 + index,
            "turnover": 1000 + index,
        }
        for index in range(count)
    ]


def test_valid_history_preserves_metrics():
    rows = history_rows()
    result = app.historical_window_metrics(rows, 20)
    assert result["window_complete"] is True
    assert result["return_pct"] == 200
    assert result["annualized_volatility_pct"] == app.annualized_volatility_pct(
        [row["close"] for row in rows[-20:]]
    )
    assert result["range_complete"] is True
    assert result["data_quality"]["reasons"] == []


@pytest.mark.parametrize("value", [None, 0, -1, True, float("inf"), float("nan")])
def test_invalid_middle_close_is_not_bridged(value):
    rows = history_rows()
    rows[10]["close"] = value
    result = app.historical_window_metrics(rows, 20)
    assert result["window_complete"] is False
    assert result["valid_close_observations"] == 19
    assert "missing_or_invalid_close" in result["data_quality"]["reasons"]
    for key in ("return_pct", "annualized_volatility_pct", "maximum_drawdown_pct"):
        assert result[key] is None


@pytest.mark.parametrize("kind", ["duplicate", "unordered", "invalid"])
def test_invalid_dates_cannot_form_complete_window(kind):
    rows = history_rows()
    if kind == "duplicate":
        rows[10]["date"] = rows[9]["date"]
    elif kind == "unordered":
        rows[9], rows[10] = rows[10], rows[9]
    else:
        rows[10]["date"] = "invalid"
    result = app.historical_window_metrics(rows, 20)
    assert result["window_complete"] is False
    assert result["return_pct"] is None
    assert result["annualized_volatility_pct"] is None


def test_short_history_is_explicitly_incomplete():
    result = app.historical_window_metrics(history_rows(10), 20)
    assert result["available_sessions"] == 10
    assert result["window_complete"] is False
    assert "insufficient_sessions" in result["data_quality"]["reasons"]
    assert result["return_pct"] is None


def test_missing_extreme_is_only_an_observed_range():
    rows = history_rows()
    rows[10]["high"] = None
    result = app.historical_window_metrics(rows, 20)
    assert (
        result["window_complete"] is True
    )  # complete close path, not complete high/low coverage
    assert result["range_complete"] is False
    assert result["range_scope"] == "observed_valid_prices_only"
    assert result["data_quality"]["field_observations"]["high"] == 19


def test_missing_latest_volume_does_not_drop_valid_prior_observation():
    rows = history_rows()
    rows[-1]["volume"] = None
    result = app.historical_window_metrics(rows, 20)
    expected = sum(row["volume"] for row in rows[-20:-1]) / 19
    assert result["volume"]["prior_sessions_average"] == round(expected, 6)


def test_boolean_quote_is_not_numeric_evidence():
    status = app.decision_component_quality("quote", {"quote": {"price": True}})
    assert status["has_data"] is False


def test_news_provider_failure_is_partial_but_fallback_attempt_error_is_not():
    payload = {
        "items": [{"title": "Observed report"}],
        "source_errors": ["prior attempt failed"],
    }
    assert app.decision_component_quality("news", payload)["is_complete"] is True
    payload["provider_status"] = {"failed": {"status": "unavailable"}}
    result = app.decision_component_quality("news", payload)
    assert result["has_data"] is True
    assert result["is_complete"] is False


@pytest.mark.parametrize("missing_close", [False, True])
def test_real_historical_builder_quality_reaches_packet(
    packet_sources, monkeypatch, missing_close
):
    rows = history_rows(260)
    if missing_close:
        rows[-10]["close"] = None
    monkeypatch.setattr(
        app,
        "race_public_sources",
        lambda *_args: ({"items": rows, "source": "offline_test"}, "offline_test", []),
    )
    monkeypatch.setattr(
        app, "get_cached_historical_context_data", app.get_historical_context_data
    )
    result = app.get_decision_context_data("600519", None)
    history = result["decision_inputs"]["historical_context"]
    assert history["windows"]["20"]["window_complete"] is not missing_close
    assert result["data_status"] == ("partial_data" if missing_close else "full_data")
    followups = [
        item["missing_component"] for item in result["recommended_follow_up_tools"]
    ]
    assert followups == (["historical_context"] if missing_close else [])


def test_exchange_fund_does_not_require_company_only_components(packet_sources):
    result = app.get_decision_context_data("510300", None)
    assert result["requested_component_count"] == 7
    assert result["complete_fresh_component_count"] == 7
    assert result["data_status"] == "full_data"
    assert result["recommended_follow_up_tools"] == []
