"""Offline boundary regressions; synthetic data only, no external providers."""

import asyncio
import errno
import io
import json
import logging
import socket
import ssl
from threading import Event
from urllib.error import HTTPError, URLError

import pytest

from test_mcp import market_app as app


@pytest.fixture(autouse=True)
def isolated_cache():
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()
    yield
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()


@pytest.mark.parametrize("price", [None, 0, -1, float("nan"), float("inf"), True])
def test_missing_or_invalid_quote_never_becomes_full_data(monkeypatch, price):
    monkeypatch.setattr(app, "get_quote_data", lambda **_: {
        "quote": {"symbol": "600000", "price": price}, "source": "synthetic",
    })
    result = app.get_a_share_quote("600000")
    assert result["ok"] is False
    assert result["data_status"] == "unavailable"
    assert result["error_type"] == "missing_data"
    assert result["missing_fields"] == ["price"]
    assert result["data"] == {}
    assert "price" not in result
    assert not app.TOOL_CACHE


def test_partial_quote_keeps_unknown_fields_null_and_explicit(monkeypatch):
    monkeypatch.setattr(app, "get_quote_data", lambda **_: {
        "quote": {"symbol": "600000", "price": 10.0}, "source": "synthetic",
        "note": "synthetic",
    })
    result = app.get_a_share_quote("600000")
    assert result["price"] == 10
    assert result["data_status"] == "partial_data"
    assert result["volume"] is None
    assert "volume" in result["missing_fields"]
    assert "quote_time" in result["missing_fields"]


def test_quote_race_does_not_accept_missing_price_from_fast_source(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_quote", lambda _: {"price": None})
    monkeypatch.setattr(app, "get_tencent_quote", lambda _: {"price": 10.0})
    monkeypatch.setattr(app, "get_sina_quote", lambda _: {"price": float("nan")})
    quote, source, _ = app.get_fastest_public_quote("600000")
    assert quote["price"] == 10
    assert source == "tencent"


def test_all_quote_sources_timeout_returns_no_fabricated_quote(monkeypatch):
    def timed_out(_):
        raise app.HTTPException(status_code=504, detail="synthetic provider timed out")

    for name in ("get_eastmoney_quote", "get_tencent_quote", "get_sina_quote"):
        monkeypatch.setattr(app, name, timed_out)
    result = app.get_a_share_quote("600000")
    assert result["ok"] is False
    assert result["error_type"] == "timeout"
    assert result["data"] == {}
    assert result["trade_date"] is None
    assert result["recovery"]["max_additional_attempts"] == 1
    assert result["recovery"]["automatic_retry_performed"] is False


@pytest.mark.parametrize(
    ("status", "message", "expected"),
    [
        (503, "Research capacity busy; retry in smaller batch.", "service_capacity"),
        (504, "request deadline", "timeout"),
        (429, "Too many requests", "rate_limited"),
        (502, "Remote end closed connection without response", "network_error"),
    ],
)
def test_error_categories_are_not_conflated(status, message, expected):
    result = app.mcp_error(None, app.HTTPException(status_code=status, detail=message))
    assert result["error_type"] == expected
    assert result["retryable"] is True


def test_mcp_deadline_preserves_busy_slot_until_real_work_finishes(monkeypatch):
    release = Event()
    finished = Event()
    pool = app.BoundedResearchExecutor(max_workers=1, max_pending=1)
    monkeypatch.setattr(app, "MCP_REQUEST_EXECUTOR", pool)
    monkeypatch.setattr(app, "MCP_RESPONSE_BUDGET_SECONDS", 0.03)

    def blocked(*_):
        release.wait(2)
        finished.set()
        return {"source": "synthetic"}

    monkeypatch.setattr(app, "get_decision_context_data", blocked)

    async def exercise():
        _, timed_out = await app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": "600000"}
        )
        assert timed_out["failure_layer"] == "mcp_request_deadline"
        assert timed_out["error_type"] == "timeout"
        assert timed_out["data"] == {}
        assert not finished.is_set()
        _, busy = await app.mcp.call_tool("get_market_data_health", {})
        assert busy["error_type"] == "service_capacity"
        assert busy["failure_layer"] == "service_capacity"
        release.set()
        assert await asyncio.to_thread(finished.wait, 1)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)


def test_slow_provider_does_not_block_other_mcp_calls(monkeypatch):
    release = Event()
    entered = Event()
    pool = app.BoundedResearchExecutor(max_workers=2, max_pending=2)
    monkeypatch.setattr(app, "MCP_REQUEST_EXECUTOR", pool)
    monkeypatch.setattr(app, "MCP_RESPONSE_BUDGET_SECONDS", 0.1)

    def blocked(*_):
        entered.set()
        release.wait(2)
        return {"source": "synthetic"}

    monkeypatch.setattr(app, "get_decision_context_data", blocked)

    async def exercise():
        slow = asyncio.create_task(app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": "600000"}
        ))
        assert await asyncio.to_thread(entered.wait, 1)
        _, healthy = await app.mcp.call_tool("get_market_data_health", {})
        assert healthy["ok"] is True
        assert not slow.done()
        _, failed = await slow
        assert failed["error_type"] == "timeout"

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)


def test_boundary_logs_only_safe_fields_and_hides_unexpected_exception(monkeypatch, caplog):
    def failed(*_):
        raise RuntimeError("https://example.com/?token=SECRET and private arguments")

    monkeypatch.setattr(app, "get_decision_context_data", failed)
    with caplog.at_level(logging.INFO, logger="market_sentinel.requests"):
        _, result = asyncio.run(app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": "600000"}
        ))
    assert result["ok"] is False
    assert result["error_type"] == "unexpected_error"
    assert "SECRET" not in json.dumps(result)
    logs = [
        json.loads(record.message) for record in caplog.records
        if record.name == "market_sentinel.requests"
    ]
    assert logs
    assert set(logs[-1]) == {
        "request_id", "tool", "stage", "error_type", "elapsed_ms",
    }
    assert logs[-1]["request_id"] == result["request_id"]
    assert "SECRET" not in caplog.text
    assert "600000" not in caplog.text


def test_provider_timeout_is_not_mislabeled_as_mcp_response_deadline(monkeypatch):
    def failed(*_):
        raise TimeoutError("synthetic provider timeout with private query")

    monkeypatch.setattr(app, "get_decision_context_data", failed)
    _, result = asyncio.run(app.mcp.call_tool(
        "get_a_share_decision_context", {"symbol": "600000"}
    ))
    assert result["error_type"] == "timeout"
    assert result["failure_layer"] == "tool_execution"
    assert result["recovery"]["running_work_may_still_complete"] is False
    assert "private query" not in json.dumps(result)


@pytest.mark.parametrize(
    ("requested_attempts", "expected_attempts"),
    [(-10, 1), (0, 1), (1, 1), (2, 2), (999, 2)],
)
def test_provider_retry_budget_is_clamped(monkeypatch, requested_attempts, expected_attempts):
    calls = []

    def failed(*_, **__):
        calls.append(1)
        raise TimeoutError("synthetic timeout")

    monkeypatch.setattr(app, "urlopen", failed)
    with pytest.raises(app.HTTPException):
        app.read_public_json(
            "https://example.com", "https://example.com", attempts=requested_attempts,
        )
    assert len(calls) == expected_attempts


@pytest.mark.parametrize("failure", [
    TimeoutError("synthetic timeout"),
    ConnectionResetError("synthetic connection reset"),
    URLError(TimeoutError("synthetic timeout")),
    OSError(errno.ETIMEDOUT, "synthetic timeout"),
    URLError(socket.gaierror(socket.EAI_AGAIN, "synthetic temporary DNS failure")),
    HTTPError("https://example.com", 502, "Bad Gateway", {}, None),
    HTTPError("https://example.com", 503, "Service Unavailable", {}, None),
    HTTPError("https://example.com", 504, "Gateway Timeout", {}, None),
])
def test_transient_read_retry_can_recover_and_records_both_attempts(monkeypatch, failure):
    calls = []
    health = []

    def read(*_, **__):
        calls.append(1)
        if len(calls) == 1:
            raise failure
        return io.BytesIO(b'{"synthetic":true}')

    monkeypatch.setattr(app, "urlopen", read)
    monkeypatch.setattr(app, "record_source_health", lambda *args: health.append(args))
    result = app.read_public_json(
        "https://example.com", "https://example.com", attempts=999,
    )
    assert result == {"synthetic": True}
    assert len(calls) == 2
    assert [item[1] for item in health] == [False, True]


@pytest.mark.parametrize("failure", [
    *[
        HTTPError("https://example.com", code, "synthetic failure", {}, None)
        for code in (400, 401, 403, 404, 429, 500)
    ],
    ssl.SSLCertVerificationError("synthetic untrusted certificate"),
    URLError(ssl.SSLCertVerificationError("synthetic hostname mismatch")),
    PermissionError("synthetic non-network failure"),
    URLError(socket.gaierror(socket.EAI_NONAME, "synthetic unknown host")),
])
def test_terminal_transport_failures_do_not_retry(monkeypatch, failure):
    calls = []
    health = []

    def failed(*_, **__):
        calls.append(1)
        raise failure

    monkeypatch.setattr(app, "urlopen", failed)
    monkeypatch.setattr(app, "record_source_health", lambda *args: health.append(args))
    with pytest.raises(app.HTTPException):
        app.read_public_json("https://example.com", "https://example.com", attempts=999)
    assert len(calls) == 1
    assert [item[1] for item in health] == [False]


@pytest.mark.parametrize("body", [b"not JSON", b"\xff"])
def test_malformed_json_or_encoding_does_not_retry(monkeypatch, body):
    calls = []
    health = []

    def read(*_, **__):
        calls.append(1)
        return io.BytesIO(body)

    monkeypatch.setattr(app, "urlopen", read)
    monkeypatch.setattr(app, "record_source_health", lambda *args: health.append(args))
    with pytest.raises(app.HTTPException):
        app.read_public_json("https://example.com", "https://example.com", attempts=2)
    assert len(calls) == 1
    assert [item[1] for item in health] == [False]


def test_partial_result_logs_source_failure_category_without_raw_error(monkeypatch, caplog):
    monkeypatch.setattr(app, "get_decision_context_data", lambda *_: {
        "source": "synthetic",
        "data_status": "partial_data",
        "source_errors": [{
            "source": "synthetic", "error_type": "timeout",
            "message": "https://example.com/?token=SECRET timed out",
        }],
    })
    with caplog.at_level(logging.INFO, logger="market_sentinel.requests"):
        _, result = asyncio.run(app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": "600000"}
        ))
    assert result["data_status"] == "partial_data"
    record = next(
        json.loads(item.message) for item in caplog.records
        if item.name == "market_sentinel.requests"
    )
    assert record["stage"] == "completed_partial"
    assert record["error_type"] == "timeout"
    assert "SECRET" not in caplog.text


def test_quote_sources_use_https_without_insecure_fallback(monkeypatch):
    urls = []
    parts = [""] * 47
    parts[1], parts[2], parts[3] = "Synthetic", "600000", "10.0"

    def reader(url, *_):
        urls.append(url)
        if "qt.gtimg.cn" in url:
            return 'v_sh600000="' + "~".join(parts) + '";'
        return 'var hq_str_sh600000="Synthetic,10,10,10,10,10,0,0,100,1000,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-10-20,15:00:00";'

    monkeypatch.setattr(app, "read_market_text", reader)
    assert app.get_tencent_quote("600000")["price"] == 10
    assert app.get_sina_quote("600000")["price"] == 10
    assert urls == [
        "https://qt.gtimg.cn/q=sh600000",
        "https://hq.sinajs.cn/list=sh600000",
    ]
