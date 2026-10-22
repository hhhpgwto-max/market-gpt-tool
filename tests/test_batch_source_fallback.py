"""Synthetic regressions for the independent, whole-batch quote fallback."""

import pytest

from test_mcp import market_app as app


def tencent_row(code, price=10, timestamp="20261022103000", symbol=None):
    fields = [""] * 47
    values = {
        1: "Synthetic", 2: symbol or code[2:], 3: price, 4: 9, 5: 9.5,
        30: timestamp, 31: 1, 32: 11.11, 33: 11, 34: 9,
        36: 100, 37: 50, 38: 1.5, 43: 2, 44: 3, 45: 2,
    }
    for index, value in values.items():
        fields[index] = str(value)
    return f'v_{code}="' + "~".join(fields) + '";'


def eastmoney_row(code="600000", price=10, market=1):
    return {
        "f12": code, "f13": market, "f2": price, "f3": 1,
        "f14": "Synthetic EM", "f124": 1792636200,
    }


def unavailable(*_args, **_kwargs):
    raise app.HTTPException(status_code=502, detail="synthetic connection timed out")


def test_complete_primary_never_issues_fallback(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", lambda _: ([eastmoney_row()], "test-host"))
    monkeypatch.setattr(app, "read_market_text", lambda *_: pytest.fail("fallback was unnecessary"))
    result = app.get_batch_quote_data(["600000"])
    assert result["count"] == 1
    assert result["source"] == ["eastmoney_batch:test-host"]
    assert result["data_status"] == "full_data"


def test_source_outage_recovers_whole_batch_with_one_request(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    calls = []

    def response(*args):
        calls.append(args)
        return tencent_row("sz000001") + tencent_row("sh000001", 3000)

    monkeypatch.setattr(app, "read_market_text", response)
    result = app.get_batch_quote_data(["000001", "index:000001"])
    assert len(calls) == 1
    assert calls[0][0] == "https://qt.gtimg.cn/q=sz000001,sh000001"
    assert calls[0][2] == 3
    assert result["count"] == 2
    assert [row["identifier"] for row in result["results"]] == ["000001", "index:000001"]
    assert result["results"][0]["exchange"] == "SZSE"
    assert result["results"][1]["exchange"] == "SSE"
    assert result["results"][0]["volume"] == 10000
    assert result["results"][0]["turnover"] == 500000
    assert result["results"][0]["total_market_value"] == 300000000
    assert result["data_status"] == "full_data"
    assert result["source_errors"]
    assert result["source"] == ["tencent_batch"]


@pytest.mark.parametrize("price", [None, 0, -1, float("nan"), float("inf"), True])
def test_invalid_primary_price_never_becomes_success(monkeypatch, price):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", lambda _: ([eastmoney_row(price=price)], "test-host"))
    monkeypatch.setattr(app, "read_market_text", lambda *_: tencent_row("sh600000", 12))
    result = app.get_batch_quote_data(["600000"])
    assert result["source"] == ["tencent_batch"]
    assert result["results"][0]["price"] == 12


def test_primary_missing_items_fallback_is_not_merged(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", lambda _: ([eastmoney_row(price=10)], "test-host"))
    monkeypatch.setattr(app, "read_market_text", lambda *_: tencent_row("sh600000", 12) + tencent_row("sz000001", 14))
    result = app.get_batch_quote_data(["600000", "000001"])
    assert [row["price"] for row in result["results"]] == [12, 14]
    assert all(row["source"] == "tencent_batch" for row in result["results"])


def test_less_complete_fallback_retains_original_without_merging(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", lambda _: ([eastmoney_row(), eastmoney_row("600001")], "test-host"))
    monkeypatch.setattr(app, "read_market_text", lambda *_: tencent_row("sz000001", 14))
    result = app.get_batch_quote_data(["600000", "600001", "000001"])
    assert result["count"] == 2
    assert result["source"] == ["eastmoney_batch:test-host"]
    assert result["data_status"] == "partial_data"
    assert "000001" not in [row["symbol"] for row in result["results"]]


@pytest.mark.parametrize("body", ["", 'v_sh600000="";', tencent_row("sh600000", symbol="600001"), tencent_row("sh600000", 0), tencent_row("sh600000", "nan")])
def test_missing_invalid_or_mismatched_rows_are_not_quotes(monkeypatch, body):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    monkeypatch.setattr(app, "read_market_text", lambda *_: body)
    result = app.get_batch_quote_data(["600000"])
    assert result["count"] == 0
    assert result["data_status"] == "no_data"
    assert result["results"] == []
    assert result["market_time"] is None


def test_both_sources_fail_retains_explicit_no_data(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    monkeypatch.setattr(app, "read_market_text", unavailable)
    result = app.get_batch_quote_data(["600000", "000001"])
    assert result["count"] == 0
    assert result["data_status"] == "no_data"
    assert len(result["errors"]) == 2
    assert any("tencent_batch" in error for error in result["source_errors"])


@pytest.mark.parametrize("timestamp", ["", "not-a-time", "20261021103000"])
def test_missing_or_mixed_batch_times_are_partial(monkeypatch, timestamp):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    monkeypatch.setattr(app, "read_market_text", lambda *_: tencent_row("sh600000") + tencent_row("sz000001", timestamp=timestamp))
    result = app.get_batch_quote_data(["600000", "000001"])
    assert result["count"] == 2
    assert result["data_status"] == "partial_data"
    assert result["time_alignment"]["status"] == "quality_hold"
    assert "aligned_batch_market_times" in result["missing_fields"]


def test_invalid_input_preserved_and_duplicate_identifiers_deduplicated(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    monkeypatch.setattr(app, "read_market_text", lambda *_: tencent_row("sh600000"))
    result = app.get_batch_quote_data(["600000", "600000", "invalid"])
    assert result["count"] == 1
    assert result["requested_count"] == 3
    assert result["data_status"] == "partial_data"
    assert result["errors"][0]["code"] == "invalid_symbol"


def test_invalid_only_input_never_fetches(monkeypatch):
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", lambda *_: pytest.fail("no valid symbols"))
    result = app.get_batch_quote_data(["invalid", None])
    assert result["count"] == 0
    assert len(result["errors"]) == 2


def test_twenty_symbols_still_one_fallback_request(monkeypatch):
    symbols = [f"600{number:03d}" for number in range(20)]
    monkeypatch.setattr(app, "get_eastmoney_batch_quote_rows", unavailable)
    calls = []

    def response(*args):
        calls.append(args)
        return "".join(tencent_row("sh" + symbol) for symbol in symbols)

    monkeypatch.setattr(app, "read_market_text", response)
    result = app.get_batch_quote_data(symbols)
    assert result["count"] == 20
    assert len(calls) == 1
