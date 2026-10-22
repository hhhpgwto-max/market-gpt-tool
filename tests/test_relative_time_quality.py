"""Relative comparisons require aligned observed timestamps, not query time."""
from copy import deepcopy

import pytest

from test_mcp import market_app as app


@pytest.fixture
def batch(monkeypatch):
    at = "2026-07-10T15:00:00+08:00"
    data = {"results": [
        {"identifier": "000001", "price": 10, "change_pct": 5, "market_time": at},
        {"identifier": "index:399001", "price": 3000, "change_pct": 1, "market_time": at},
        {"identifier": "000002", "price": 12, "change_pct": 2, "market_time": at},
    ], "errors": [], "missing_fields": [], "data_status": "full_data", "source": ["fixture"]}
    monkeypatch.setattr(app, "get_batch_quote_data", lambda _: deepcopy(data))
    return data


def compare():
    return app.get_relative_strength_data("000001", "index:399001", ["000002"])


def test_aligned_batch_compares_without_new_source_requests(batch):
    result = compare()
    assert result["data_status"] == "full_data"
    assert result["relative_to_benchmark_pct_points"] == 4
    assert result["relative_to_peer_average_pct_points"] == 3
    assert result["comparable_peer_count"] == 1


@pytest.mark.parametrize("timestamp", [None, "bad", "2026-07-09T15:00:00+08:00", "2026-07-10T14:54:59+08:00"])
def test_unaligned_benchmark_not_computed(batch, timestamp):
    batch["results"][1]["market_time"] = timestamp
    result = compare()
    assert result["relative_to_benchmark_pct_points"] is None
    assert result["benchmark_change_pct"] == 1
    assert result["data_status"] == "partial_data"
    assert "aligned_target_benchmark_change" in result["missing_fields"]


def test_unaligned_peer_is_not_in_average_or_silently_omitted(batch):
    batch["results"][2]["market_time"] = "2026-07-09T15:00:00+08:00"
    result = compare()
    assert result["peer_count"] == 1 and result["comparable_peer_count"] == 0
    assert result["relative_to_peer_average_pct_points"] is None
    assert result["data_status"] == "partial_data"
    assert "aligned_peer_changes" in result["missing_fields"]


def test_missing_peer_and_upstream_partial_stay_partial(batch):
    batch["results"].pop()
    batch["errors"] = [{"identifier": "000002", "code": "no_data"}]
    result = compare()
    assert result["relative_to_benchmark_pct_points"] == 4
    assert result["errors"] == batch["errors"]
    assert result["data_status"] == "partial_data"


def test_nonfinite_change_never_compares(batch):
    batch["results"][0]["change_pct"] = float("nan")
    result = compare()
    assert result["target_change_pct"] is None
    assert result["relative_to_benchmark_pct_points"] is None
    assert result["data_status"] == "partial_data"
