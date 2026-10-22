"""Official disclosure fallback never invents empty success or issuer identity."""
from datetime import datetime

import pytest

from test_mcp import market_app as app


def szse_row():
    return {"annId": "synthetic", "secCode": ["000001"], "secName": ["Test"],
            "title": "Test announcement", "publishTime": "2026-07-10 00:00:00",
            "attachPath": "/disc/test.pdf"}


def cninfo_payload():
    return {"totalAnnouncement": 1, "announcements": [{
        "secCode": "000001", "secName": "Test", "orgId": "syntheticIssuer",
        "announcementId": "synthetic", "announcementTitle": "Test announcement",
        "announcementTime": int(datetime(2026, 7, 10, tzinfo=app.MARKET_TIMEZONE).timestamp() * 1000),
        "adjunctUrl": "finalpage/2026-07-10/test.PDF",
    }]}


def mock_cninfo(monkeypatch, payload):
    monkeypatch.setattr(app, "get_cached_tool_data", lambda _k, _t, loader, **_kw: (loader(), {}))
    monkeypatch.setattr(app, "read_public_json", lambda *_a, **_k: {
        "stockList": [{"code": "000001", "orgId": "syntheticIssuer"}]
    })
    calls = []
    monkeypatch.setattr(app, "read_cninfo_announcement_query", lambda query: calls.append(query) or payload)
    return calls


@pytest.mark.parametrize("payload", [{}, [], {"data": None}, {"data": {}},
                                       {"code": 500, "data": []}, {"success": False, "data": []}])
def test_szse_failure_payload_is_not_empty_success(monkeypatch, payload):
    monkeypatch.setattr(app, "read_public_json_post", lambda *_a, **_k: payload)
    with pytest.raises(app.HTTPException):
        app.get_szse_announcements("000001", "2026-07-01", "2026-07-20", 10)


@pytest.mark.parametrize("payload", [{}, {"result": None}, {"result": {}},
                                       {"result": [None]}, {"success": False, "result": []}])
def test_sse_failure_payload_is_not_empty_success(monkeypatch, payload):
    monkeypatch.setattr(app, "read_public_json", lambda *_a, **_k: payload)
    with pytest.raises(app.HTTPException):
        app.get_sse_announcements("600001", "2026-07-01", "2026-07-20", 10)


@pytest.mark.parametrize("key,value", [("secCode", ["000002"]), ("secCode", "000001"),
    ("title", None), ("publishTime", None), ("publishTime", "2026-08-01"),
    ("attachPath", "https://untrusted.invalid/file.pdf")])
def test_szse_row_must_match_security_date_and_document_host(monkeypatch, key, value):
    row = szse_row()
    row[key] = value
    monkeypatch.setattr(app, "read_public_json_post", lambda *_a, **_k: {"data": [row]})
    with pytest.raises(app.HTTPException):
        app.get_szse_announcements("000001", "2026-07-01", "2026-07-20", 10)


def test_valid_empty_primary_does_not_trigger_fallback(monkeypatch):
    monkeypatch.setattr(app, "get_szse_announcements", lambda *_: [])
    monkeypatch.setattr(app, "get_cninfo_announcements", lambda *_: pytest.fail("unnecessary fallback"))
    result = app.get_announcement_data("000001", 30, 3)
    assert result["data_status"] == "no_data"
    assert result["source_errors"] == []


def test_primary_failure_returns_verified_fallback_and_failure_context(monkeypatch):
    def fail(*_):
        raise app.HTTPException(502, "synthetic connection reset")
    monkeypatch.setattr(app, "get_szse_announcements", fail)
    monkeypatch.setattr(app, "get_cninfo_announcements", lambda *_: [szse_row()])
    result = app.get_announcement_data("000001", 30, 3)
    assert result["count"] == 1 and result["data_status"] == "partial_data"
    assert result["source"] == ["official_cninfo_announcements"]
    assert "connection reset" in result["source_errors"][0]["message"]
    assert result["recovery_status"] == "official_fallback_used"


def test_both_routes_fail_without_no_data_success(monkeypatch):
    def fail(*_):
        raise app.HTTPException(502, "synthetic failure")
    monkeypatch.setattr(app, "get_szse_announcements", fail)
    monkeypatch.setattr(app, "get_cninfo_announcements", fail)
    with pytest.raises(app.HTTPException, match="routes unavailable"):
        app.get_announcement_data("000001", 30, 3)


def test_cninfo_query_uses_exact_issuer_and_original_pdf(monkeypatch):
    calls = mock_cninfo(monkeypatch, cninfo_payload())
    rows = app.get_cninfo_announcements("000001", "2026-07-01", "2026-07-20", 3)
    assert len(calls) == 1 and calls[0]["stock"] == "000001,syntheticIssuer"
    assert calls[0]["seDate"] == "2026-07-01~2026-07-20"
    assert rows[0]["url"] == "https://static.cninfo.com.cn/finalpage/2026-07-10/test.PDF"
    assert "CNINFO" in rows[0]["official_source"]
    assert rows[0]["published_at"].startswith("2026-07-10")


@pytest.mark.parametrize("key,value", [("secCode", "000002"), ("orgId", "wrongIssuer"),
    ("announcementTime", None), ("announcementTime", True), ("announcementTime", float("nan")),
    ("announcementTime", 10**30), ("announcementTitle", ""), ("adjunctUrl", "//evil.invalid/a.pdf")])
def test_cninfo_rejects_invalid_records(monkeypatch, key, value):
    payload = cninfo_payload()
    payload["announcements"][0][key] = value
    mock_cninfo(monkeypatch, payload)
    with pytest.raises(app.HTTPException):
        app.get_cninfo_announcements("000001", "2026-07-01", "2026-07-20", 3)


@pytest.mark.parametrize("payload", [{}, {"announcements": []},
    {"totalAnnouncement": 1, "announcements": []}, {"totalAnnouncement": True, "announcements": []}])
def test_cninfo_missing_contract_is_not_no_announcements(monkeypatch, payload):
    mock_cninfo(monkeypatch, payload)
    with pytest.raises(app.HTTPException):
        app.get_cninfo_announcements("000001", "2026-07-01", "2026-07-20", 3)


def test_cninfo_explicit_empty_is_valid(monkeypatch):
    mock_cninfo(monkeypatch, {"totalAnnouncement": 0, "announcements": None})
    assert app.get_cninfo_announcements("000001", "2026-07-01", "2026-07-20", 3) == []


def test_missing_cninfo_identity_never_runs_broad_search(monkeypatch):
    calls = mock_cninfo(monkeypatch, cninfo_payload())
    with pytest.raises(app.HTTPException, match="issuer map"):
        app.get_cninfo_announcements("000002", "2026-07-01", "2026-07-20", 3)
    assert calls == []


def test_szse_positive_count_empty_list_is_failure(monkeypatch):
    monkeypatch.setattr(app, "read_public_json_post", lambda *_a, **_k: {"announceCount": 2, "data": []})
    with pytest.raises(app.HTTPException, match="inconsistency"):
        app.get_szse_announcements("000001", "2026-07-01", "2026-07-20", 3)


def test_sse_contradictory_list_fields_do_not_drop_disclosures(monkeypatch):
    monkeypatch.setattr(app, "read_public_json", lambda *_a, **_k: {"result": [], "pageHelp": {"total": 2, "data": [{"TITLE": "present"}]}})
    with pytest.raises(app.HTTPException, match="contradiction"):
        app.get_sse_announcements("600001", "2026-07-01", "2026-07-20", 3)


def test_cninfo_truncated_page_is_failure(monkeypatch):
    payload = cninfo_payload()
    payload["totalAnnouncement"] = 5
    mock_cninfo(monkeypatch, payload)
    with pytest.raises(app.HTTPException, match="inconsistency"):
        app.get_cninfo_announcements("000001", "2026-07-01", "2026-07-20", 3)
