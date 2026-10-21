"""Offline contract for bounded, provenance-preserving recorded evidence packets."""

from copy import deepcopy
import json

import pytest

from test_mcp import market_app as app
from test_research_quality import packet_sources  # noqa: F401


def fixture():
    at = "2026-10-21T15:00:00+08:00"
    metric = {
        "window_complete": True, "range_complete": True, "end_date": "2026-10-21",
        "return_pct": 2, "high": 12, "low": 8, "distance_from_high_pct": -10,
        "distance_from_low_pct": 25, "annualized_volatility_pct": 30, "maximum_drawdown_pct": -12,
    }
    inputs = {
        "quote": {"source": "fixture", "quote": {"price": 10, "previous_close": 9, "high": 11, "low": 8,
                  "change_pct": 11.1, "quote_time": at}},
        "historical_context": {"source": "fixture", "adjustment": "forward_adjusted", "latest_complete_close": 10,
            "latest_complete_trade_date": "2026-10-21",
            "windows": {str(n): deepcopy(metric) for n in app.HISTORICAL_CONTEXT_WINDOWS},
            "path_facts": {"windows": {str(n): deepcopy(metric) for n in app.HISTORICAL_PATH_WINDOWS}}},
        "relative_strength": {"source": "fixture", "target": {"quote_time": at}, "benchmark": {"quote_time": at},
            "target_change_pct": 2, "benchmark_change_pct": 1, "relative_to_benchmark_pct_points": 1,
            "benchmark_identifier": "index:000001"},
        "market_overview": {"market_breadth_source": "fixture", "market_breadth": {"all_market": {"rise_count": 1000, "fall_count": 2000,
            "flat_count": 0, "limit_up_count": 20, "limit_down_count": 10}},
            "current_market_structure": {"temporal_alignment": {"component_market_times": {"market_breadth": at}}},
            "indices": [{"symbol": "000001", "change_pct": 1, "source": "fixture", "market_time": at}]},
    }
    status = {key: {"status": "available", "is_complete": True, "is_fresh": True} for key in inputs}
    return inputs, status


def build(inputs, status):
    return app.build_evidence_snapshot(inputs, status, snapshot_id="synthetic-snapshot", symbol="000001",
        benchmark="index:000001", captured_at="2026-10-22T12:00:00+08:00")


def fact(packet, name):
    item = packet["facts"][name]
    return {**packet["contexts"][item["context_id"]], **item}


def test_projects_existing_facts_with_effective_times_and_basis_without_mutation():
    inputs, status = fixture()
    before = deepcopy(inputs)
    result = build(inputs, status)
    assert result["coverage"]["status"] == "complete"
    assert result["facts"]["quote.price"]["value"] == 10
    assert fact(result, "quote.price")["as_of"] != result["captured_at"]
    assert fact(result, "history.120.high")["comparison_basis"]["window_sessions"] == 120
    assert fact(result, "history.120.high")["adjustment"] == "forward_adjusted"
    assert fact(result, "history.120.high")["effective_at_kind"] == "trading_date"
    assert fact(result, "relative.relative_to_benchmark_pct_points")["comparison_basis"]["benchmark_identifier"] == "index:000001"
    assert inputs == before
    assert "order_plan" not in result
    assert len(result["facts"]) < 65
    assert len(json.dumps(result).encode()) < 20000


@pytest.mark.parametrize("price", [None, 0, -1, float("nan"), float("inf"), True])
def test_invalid_prices_are_null_not_observations(price):
    inputs, status = fixture()
    inputs["quote"]["quote"]["price"] = price
    row = fact(build(inputs, status), "quote.price")
    assert row["value"] is None
    assert row["quality"]["status"] == "no_data"


def test_failure_partial_and_stale_status_do_not_become_complete_data():
    inputs, status = fixture()
    status["quote"]["status"] = "timeout"
    result = build(inputs, status)
    assert result["facts"]["quote.price"]["value"] is None
    status["quote"] = {"status": "stale_cache", "is_complete": True, "is_fresh": False}
    assert fact(build(inputs, status), "quote.price")["quality"]["status"] == "stale_cache"
    inputs["historical_context"]["windows"]["120"]["range_complete"] = False
    row = fact(build(inputs, status), "history.120.high")
    assert row["value"] == 12
    assert row["quality"]["is_complete"] is False
    assert "requested_window_incomplete" in row["quality"]["quality_reasons"]


def test_does_not_substitute_capture_time_or_keep_sensitive_source_strings():
    inputs, status = fixture()
    inputs["quote"]["quote"]["quote_time"] = None
    inputs["quote"]["source"] = "https://example.test/?secret=not-a-real-secret"
    result = build(inputs, status)
    row = fact(result, "quote.price")
    assert row["as_of"] is None
    assert row["source"] == []
    assert row["quality"]["is_complete"] is False
    assert "https://" not in str(result)


def test_unknown_adjustment_and_unaligned_relative_times_remain_quality_gaps():
    inputs, status = fixture()
    inputs["historical_context"].pop("adjustment")
    inputs["relative_strength"]["benchmark"]["quote_time"] = "2026-10-20T15:00:00+08:00"
    result = build(inputs, status)
    assert fact(result, "history.20.return_pct")["quality"]["is_complete"] is False
    assert "relative_times_not_aligned" in fact(result, "relative.relative_to_benchmark_pct_points")["quality"]["quality_reasons"]


def test_malformed_optional_components_do_not_break_the_evidence_packet():
    inputs, status = fixture()
    inputs["market_overview"]["indices"] = {"unexpected": "shape"}
    inputs["market_overview"]["market_breadth"]["all_market"]["rise_count"] = -1
    result = build(inputs, status)
    assert fact(result, "market.breadth.rise_count")["value"] is None
    assert result["coverage"]["status"] == "partial"


def test_integrated_packet_uses_existing_loaders_only(packet_sources, monkeypatch):  # noqa: F811
    def forbidden(*_args, **_kwargs):
        raise AssertionError("new source request from evidence projection")
    monkeypatch.setattr(app, "read_public_json", forbidden)
    result = app.get_decision_context_data("600519", None)
    assert result["evidence_snapshot"]["snapshot_id"] == result["snapshot_id"]
    assert result["evidence_snapshot"]["symbol"] == "600519"
    assert result["evidence_snapshot"]["facts"]["quote.price"]["value"] == 10
    assert fact(result["evidence_snapshot"], "quote.price")["as_of"] is None
    assert result["evidence_snapshot"]["coverage"]["status"] == "partial"
