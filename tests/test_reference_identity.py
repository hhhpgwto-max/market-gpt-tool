"""Reference identity is checked before race selection, cache entry and reuse."""
from datetime import datetime, timedelta, timezone

import pytest

from test_mcp import market_app as app


@pytest.fixture(autouse=True)
def clean_cache():
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()
    yield
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()


@pytest.mark.parametrize("payload", [None, [], {}, {"data": []}, {"data": {"f57": "000002"}},
    {"data": {"f57": 1}}, {"data": {"f57": None}}])
def test_bad_identity_or_shape_never_wins_source_race(monkeypatch, payload):
    monkeypatch.setattr(app, "read_public_json", lambda *_: payload)
    with pytest.raises(app.HTTPException, match="identity/shape mismatch"):
        app.get_security_reference_data("000001")


def test_valid_matching_source_is_accepted(monkeypatch):
    monkeypatch.setattr(app, "read_public_json", lambda *_: {"data": {"f57": "000001", "f58": "Synthetic"}})
    result = app.get_security_reference_data("000001")
    assert result["symbol"] == "000001" and result["name"] == "Synthetic"


def test_wrong_live_identity_is_not_cached(monkeypatch):
    monkeypatch.setattr(app, "get_security_reference_data", lambda _: {"symbol": "000002", "name": "Wrong"})
    with pytest.raises(app.HTTPException, match="not cached"):
        app.get_resilient_security_reference_data("000001")
    assert app.cache_key("security_reference_internal", {"symbol": "000001"}) not in app.TOOL_CACHE


@pytest.mark.parametrize("age", [0, 86400])
def test_wrong_fresh_or_stale_cache_is_retired_and_refreshed(monkeypatch, age):
    key = app.cache_key("security_reference_internal", {"symbol": "000001"})
    app.TOOL_CACHE[key] = {"created_at": datetime.now(timezone.utc) - timedelta(seconds=age),
                          "data": {"symbol": "000002", "name": "Wrong"}}
    monkeypatch.setattr(app, "get_security_reference_data", lambda _: {"symbol": "000001", "name": "Right"})
    result = app.get_resilient_security_reference_data("000001")
    assert result["name"] == "Right"


def test_verified_stale_cache_retains_stale_flag(monkeypatch):
    key = app.cache_key("security_reference_internal", {"symbol": "000001"})
    app.TOOL_CACHE[key] = {"created_at": datetime.now(timezone.utc) - timedelta(days=1),
                          "data": {"symbol": "000001", "name": "Synthetic"}}
    def fail(_):
        raise app.HTTPException(502, "synthetic timeout")
    monkeypatch.setattr(app, "get_security_reference_data", fail)
    result = app.get_resilient_security_reference_data("000001")
    assert result["symbol"] == "000001" and result["reference_stale_reason"]
    assert app.decision_component_quality("security_reference", result)["status"] == "stale_cache"
