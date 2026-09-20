"""Offline concurrency regressions: no public providers or real trading data."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
import asyncio

import pytest

from test_mcp import market_app as app


@pytest.fixture(autouse=True)
def isolated_tool_cache():
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()
    yield
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()


def test_bounded_pool_rejects_overload_and_recovers_after_cancellation():
    release = Event()
    started = Event()
    pool = app.BoundedResearchExecutor(max_workers=1, max_pending=2)
    try:
        running = pool.submit(lambda: (started.set(), release.wait(2)))
        assert started.wait(1)
        queued = pool.submit(lambda: "queued")
        rejected = pool.submit(lambda: "must not run")
        with pytest.raises(app.HTTPException) as failure:
            rejected.result()
        assert failure.value.status_code == 503
        assert queued.cancel()
        replacement = pool.submit(lambda: "replacement")
        release.set()
        running.result(timeout=1)
        assert replacement.result(timeout=1) == "replacement"
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)


def test_source_race_cancels_unused_queued_work(monkeypatch):
    pool = app.BoundedResearchExecutor(max_workers=1, max_pending=3)
    monkeypatch.setattr(app, "PUBLIC_SOURCE_EXECUTOR", pool)
    # Controlled futures avoid a scheduling race where the loser starts first.
    first = app.Future()
    first.set_result({"source": "winner"})
    second = app.Future()
    futures = iter([first, second])
    monkeypatch.setattr(pool, "submit", lambda *_args: next(futures))
    try:
        result, source, _ = app.race_public_sources(
            (("winner", lambda: None), ("unused", lambda: None)), 1
        )
        assert result == {"source": "winner"}
        assert source == "winner"
        assert second.cancelled()
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def test_packet_components_share_cache_and_exact_followups(monkeypatch):
    app.TOOL_CACHE.clear()
    app.TOOL_CACHE_INFLIGHT.clear()
    lock = Lock()
    counts = {"market": 0, "financials": 0}

    def market(_limit):
        with lock:
            counts["market"] += 1
        return {"source": "test", "effective_market_time": "2026-09-18T15:00:00+08:00"}

    def financials(_symbol, _limit):
        with lock:
            counts["financials"] += 1
        return {"source": "test", "reports": []}

    for name in (
        "get_quote_data",
        "get_intraday_data",
        "get_cached_historical_context_data",
        "get_resilient_security_reference_data",
        "get_relative_strength_data",
        "get_news_data",
        "get_announcement_data",
    ):
        monkeypatch.setattr(app, name, lambda *_args: {"source": "test"})
    monkeypatch.setattr(app, "get_market_overview_data", market)
    monkeypatch.setattr(app, "get_financial_data", financials)
    monkeypatch.setattr(app, "build_security_status_data", lambda *_args: {})
    monkeypatch.setattr(app, "compact_intraday_context", lambda data: data)
    with ThreadPoolExecutor(max_workers=2) as callers:
        results = list(
            callers.map(
                lambda s: app.get_decision_context_data(s, None), ["600519", "600036"]
            )
        )
    assert all(r["data_status"] == "full_data" for r in results)
    assert counts == {"market": 1, "financials": 2}
    followup = app.get_a_share_financials("600519", 4)
    assert followup["ok"] is True
    assert followup["cache_hit"] is True
    assert counts["financials"] == 2
    assert (
        results[0]["decision_inputs"]["market_overview"]["effective_market_time"]
        == "2026-09-18T15:00:00+08:00"
    )


def test_saturated_packet_returns_explicit_missing_components(monkeypatch):
    pool = app.BoundedResearchExecutor(max_workers=1, max_pending=1)
    release = Event()
    started = Event()
    monkeypatch.setattr(app, "DECISION_COMPONENT_EXECUTOR", pool)
    monkeypatch.setattr(app, "build_security_status_data", lambda *_args: None)
    try:
        blocked = pool.submit(lambda: (started.set(), release.wait(2)))
        assert started.wait(1)
        result = app.get_decision_context_data("600519", None)
        assert result["data_status"] == "partial_data"
        assert len(result["recommended_follow_up_tools"]) == 9
        assert all(
            s["status"] == "unavailable" for s in result["component_status"].values()
        )
        assert all("capacity busy" in e["message"] for e in result["source_errors"])
        release.set()
        blocked.result(timeout=1)
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)


def test_registered_mcp_requests_overlap_without_blocking_event_loop(monkeypatch):
    pool = app.BoundedResearchExecutor(max_workers=2, max_pending=2)
    monkeypatch.setattr(app, "MCP_REQUEST_EXECUTOR", pool)
    both_started = Event()
    release = Event()
    lock = Lock()
    started = []

    def packet(symbol, _benchmark):
        with lock:
            started.append(symbol)
            if len(started) == 2:
                both_started.set()
        assert release.wait(2), "protocol event loop was blocked by synchronous work"
        return {"symbol": symbol, "source": "test"}

    monkeypatch.setattr(app, "get_decision_context_data", packet)

    async def exercise():
        tasks = [asyncio.create_task(app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": symbol}
        )) for symbol in ("600176", "600519")]
        try:
            # This await must run while both provider calls are still blocked.
            assert await asyncio.to_thread(both_started.wait, 1)
            assert not any(task.done() for task in tasks)
        finally:
            release.set()
        await asyncio.gather(*tasks)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)


def test_registered_mcp_overload_is_explicit_and_recoverable(monkeypatch):
    pool = app.BoundedResearchExecutor(max_workers=1, max_pending=1)
    monkeypatch.setattr(app, "MCP_REQUEST_EXECUTOR", pool)
    release = Event()
    blocked = pool.submit(release.wait, 2)
    try:
        result = asyncio.run(app.mcp.call_tool(
            "get_a_share_decision_context", {"symbol": "600176"}
        ))
        # SDK may return content plus structured output; inspect either representation.
        assert "Research capacity busy" in str(result)
        release.set()
        blocked.result(timeout=1)
        assert pool.submit(lambda: "recovered").result(timeout=1) == "recovered"
    finally:
        release.set()
        pool.shutdown(wait=True, cancel_futures=True)
