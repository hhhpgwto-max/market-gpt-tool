"""Read-only provider failover preserves real adjusted history and bounded work."""
from copy import deepcopy

import pytest

from test_mcp import market_app as app


def payload():
    return {"data": {"sz000001": {"qfqday": [
        ["2026-07-09", "10", "11", "12", "9", "100"],
        ["2026-07-10", "11", "12", "13", "10", "120"],
    ]}}}


def test_healthy_primary_does_not_issue_redundant_request(monkeypatch):
    calls = []
    monkeypatch.setattr(app, "read_public_json", lambda url, *_: calls.append(url) or payload())
    result = app.get_tencent_kline("000001", "daily", 2)
    assert len(calls) == 1
    assert result["source_endpoint"] == "web.ifzq.gtimg.cn"
    assert result["adjustment"] == "forward_adjusted"
    assert result["source_errors"] == []
    assert result["items"][-1]["close"] == 12


@pytest.mark.parametrize("failure", ["HTTP Error 501: Not Implemented", "connection reset", "request timeout"])
def test_primary_failure_uses_second_https_endpoint_once(monkeypatch, failure):
    calls = []

    def read(url, referer, timeout):
        calls.append((url, timeout))
        if "web.ifzq" in url:
            raise app.HTTPException(status_code=502, detail=failure)
        return payload()

    monkeypatch.setattr(app, "read_public_json", read)
    result = app.get_tencent_kline("000001", "daily", 2)
    assert len(calls) == 2 and all(timeout == 2 for _, timeout in calls)
    assert all(url.startswith("https://") for url, _ in calls)
    assert result["source_endpoint"] == "ifzq.gtimg.cn"
    assert failure in result["source_errors"][0]
    assert result["count"] == 2


def test_both_failures_remain_failure_without_empty_success(monkeypatch):
    calls = []

    def fail(url, *_):
        calls.append(url)
        raise app.HTTPException(status_code=502, detail="synthetic timeout")

    monkeypatch.setattr(app, "read_public_json", fail)
    with pytest.raises(app.HTTPException, match="synthetic timeout"):
        app.get_tencent_kline("000001", "daily", 2)
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ["raw_only", "empty", "bad_price", "duplicate", "bad_ohlc", "bad_date"])
def test_malformed_or_wrong_adjustment_cannot_win_failover(monkeypatch, kind):
    bad = deepcopy(payload())
    data = bad["data"]["sz000001"]
    if kind == "raw_only":
        data["day"] = data.pop("qfqday")
    elif kind == "empty":
        data["qfqday"] = []
    elif kind == "bad_price":
        data["qfqday"][0][2] = "NaN"
    elif kind == "duplicate":
        data["qfqday"][1][0] = data["qfqday"][0][0]
    elif kind == "bad_ohlc":
        data["qfqday"][0][3] = "1"
    elif kind == "bad_date":
        data["qfqday"][0][0] = "2026-02-30"
    calls = []
    monkeypatch.setattr(app, "read_public_json", lambda url, *_: calls.append(url) or (bad if "web.ifzq" in url else payload()))
    result = app.get_tencent_kline("000001", "daily", 2)
    assert len(calls) == 2 and result["source_endpoint"] == "ifzq.gtimg.cn"
    assert result["items"][0]["close"] == 11
    assert result["source_errors"]


@pytest.mark.parametrize("volume", ["NaN", "inf", True, -1, None])
def test_invalid_volume_cannot_enter_history_or_json(monkeypatch, volume):
    bad = payload()
    bad["data"]["sz000001"]["qfqday"][0][5] = volume
    monkeypatch.setattr(app, "read_public_json", lambda url, *_: bad if "web.ifzq" in url else payload())
    result = app.get_tencent_kline("000001", "daily", 2)
    assert result["source_endpoint"] == "ifzq.gtimg.cn"
    assert result["items"][0]["volume"] == 10000
