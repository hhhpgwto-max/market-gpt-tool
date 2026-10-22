"""Synthetic provider failures cannot discard healthy evidence or invent dates."""
from datetime import datetime, timedelta, timezone
from threading import Event
from time import perf_counter

import pytest

from test_mcp import market_app as app


def item(published=None):
    return {"title": "Synthetic Company 000001 contract announcement", "url": "https://example.invalid/news",
            "published_at": published or datetime.now(timezone.utc).isoformat(),
            "source": "Synthetic", "retrieval_provider": "synthetic"}


@pytest.fixture(autouse=True)
def reference(monkeypatch):
    monkeypatch.setattr(app, "get_resilient_security_reference_data", lambda _: {"symbol": "000001", "name": "Synthetic Company"})
    monkeypatch.setattr(app, "get_google_news_items", lambda *_: [item()])


@pytest.mark.parametrize("error", [TypeError("bad shape"), AttributeError("bad attribute"), app.HTTPException(502, "network failure")])
def test_one_bad_provider_does_not_remove_healthy_news(monkeypatch, error):
    def fail(*_):
        raise error
    monkeypatch.setattr(app, "get_eastmoney_news_items", fail)
    result = app.get_news_data("000001", 5, 1)
    assert result["count"] == 1 and result["data_status"] == "partial_data"
    assert any(status["status"] == "unavailable" for status in result["provider_status"].values())


@pytest.mark.parametrize("rows", [None, {}, [None], [{"title": "missing url"}]])
def test_malformed_rows_are_isolated(monkeypatch, rows):
    monkeypatch.setattr(app, "get_eastmoney_news_items", lambda *_: rows)
    result = app.get_news_data("000001", 5, 1)
    assert result["count"] == 1 and result["data_status"] == "partial_data"


@pytest.mark.parametrize("published,reason", [(None, "missing_or_invalid"), ("bad-date", "missing_or_invalid"),
    ("2026-02-30", "missing_or_invalid"), ((datetime.now(timezone.utc) + timedelta(days=1)).isoformat(), "future")])
def test_unknown_and_future_news_never_enters_recent_window(monkeypatch, published, reason):
    row = item()
    row["published_at"] = published
    monkeypatch.setattr(app, "get_eastmoney_news_items", lambda *_: [row])
    monkeypatch.setattr(app, "get_google_news_items", lambda *_: [])
    result = app.get_news_data("000001", 5, 1)
    assert result["items"] == [] and result["data_status"] == "partial_data"
    assert result["publication_time_quality"]["rejected_counts"][reason] == 2
    assert "news_publication_time_unverified" in result["missing_fields"]
    assert app.decision_component_quality("news", result)["is_complete"] is False


def test_old_news_is_normal_empty_search_not_provider_failure(monkeypatch):
    old = item((datetime.now(timezone.utc) - timedelta(days=10)).isoformat())
    monkeypatch.setattr(app, "get_eastmoney_news_items", lambda *_: [old])
    monkeypatch.setattr(app, "get_google_news_items", lambda *_: [])
    result = app.get_news_data("000001", 5, 1)
    assert result["count"] == 0 and result["data_status"] == "no_data"
    assert result["source_errors"] == []


def test_timeout_returns_healthy_source_without_waiting_for_slow_thread(monkeypatch):
    release = Event()
    def slow(*_):
        release.wait(2)
        return []
    monkeypatch.setattr(app, "NEWS_SOURCE_BUDGET_SECONDS", .03)
    monkeypatch.setattr(app, "get_eastmoney_news_items", slow)
    started = perf_counter()
    try:
        result = app.get_news_data("000001", 5, 1)
        assert perf_counter() - started < .5
        assert result["count"] == 1 and result["data_status"] == "partial_data"
        assert any(value.get("error_type") == "timeout" for value in result["provider_status"].values())
    finally:
        release.set()


def test_all_provider_failures_are_unavailable_not_no_news(monkeypatch):
    def fail(*_):
        raise TypeError("synthetic malformed provider")
    monkeypatch.setattr(app, "get_eastmoney_news_items", fail)
    monkeypatch.setattr(app, "get_google_news_items", fail)
    result = app.get_news_data("000001", 5, 1)
    assert result["count"] == 0 and result["data_status"] == "unavailable"
    assert app.decision_component_quality("news", result)["status"] == "unavailable"
    assert {error["error_type"] for error in app.normalize_source_errors(result["source_errors"])} == {"source_contract_error"}


@pytest.mark.parametrize("payload", [{}, {"result": {}}, {"result": {"cmsArticleWebOld": None}}, {"result": {"cmsArticleWebOld": [None]}}])
def test_eastmoney_invalid_payload_is_not_empty_success(monkeypatch, payload):
    monkeypatch.setattr(app, "read_public_jsonp", lambda *_: payload)
    with pytest.raises(app.HTTPException):
        app.get_eastmoney_news_items("000001", 5)


@pytest.mark.parametrize("status", ["unavailable", "partial_data", "no_data"])
def test_short_failure_cache_allows_next_call_to_recover(status):
    key = f"synthetic-news-cache-{status}"
    app.TOOL_CACHE[key] = {"created_at": datetime.now(timezone.utc) - timedelta(seconds=21),
                          "data": {"data_status": status, "items": []}}
    try:
        data, cache = app.get_cached_tool_data(key, 300, lambda: {"data_status": "full_data", "items": [item()]}, partial_ttl_seconds=20)
        assert data["data_status"] == "full_data" and cache["cache_hit"] is False
    finally:
        app.TOOL_CACHE.pop(key, None)
