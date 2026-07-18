import os
import base64
import json
import re
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from html import unescape
from math import ceil, sqrt
from threading import Event, Lock
from time import perf_counter
from typing import Any
from urllib.parse import urlencode
from urllib.error import URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree  # nosec B405

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations


APP_NAME = os.getenv("MARKET_TOOL_NAME", "market-gpt-tool")
ROUTING_REVISION = "candidate_evidence_gate_history_cache_v1"

MCP_INSTRUCTIONS = (
    "Use these read-only tools for current A-share stock and exchange-traded fund market data, intraday prices, news, "
    "official announcements, fund flows, financial metrics, batch quotes, auction facts, market overview, mechanical "
    "anomaly scans, relative strength, historical context, security status, multi-source market evidence packets, "
    "sector rankings and rotation history, overnight cross-market observations, company event timelines, fund "
    "look-through holdings, portfolio exposure aggregation, and "
    "public IPO subscription schedules and eligibility rules, and "
    "operational data-route health. "
    "Use search_a_share when the stock code is unclear. "
    "Use get_a_share_quote for the latest quote and get_a_share_kline for date-ranged or paginated price history. "
    "Use get_a_share_market_snapshot when market overview, a target security, and peers must be captured in one "
    "bounded request with explicit timestamp differences and source conflicts. Historical as-of reconstruction is "
    "not available unless the tool explicitly says so. "
    "When get_a_share_decision_context returns recommended_follow_up_tools, use those exact tool names and "
    "arguments to refill relevant components that missed the packet response budget before answering. "
    "For stock-screening or stock-selection requests, use screen_a_share_research_candidates before naming any "
    "security. Treat its results as research candidates, not recommendations. If it returns no_candidate=true, "
    "do not force a ticker; report the failed market, history, or data-completeness gate instead. "
    "Use get_ipo_subscription_status for current IPO schedules, subscription codes, dates, issuer limits, and "
    "exchange rules. Never infer the user's account market value, board permissions, application, or winning status. "
    "For current company news, use get_a_share_news before broad web search; use get_a_share_announcements for "
    "official exchange filings. Do not use Wikipedia, encyclopedia pages, or academic-paper indexes as evidence "
    "for current company events. News results are evidence with provenance, not positive/negative judgements. "
    "Use get_a_share_limit_activity for public limit-up, limit-down, open-board, and consecutive-board facts. "
    "Do not convert those mechanical counts into sentiment or trading labels. Never place trades, access brokerage "
    "accounts, issue buy/sell recommendations, or make automated financial decisions. All data is informational only "
    "and is not investment advice."
)

READ_ONLY_TOOL = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

mcp = FastMCP(
    "Market Sentinel",
    instructions=MCP_INSTRUCTIONS,
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        allowed_hosts=[
            "market-gpt-tool.onrender.com",
            "127.0.0.1:8000",
        ],
    ),
)
mcp_http_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(
    title="Market GPT Tool",
    version="0.12.0",
    description="A read-only A-share market data MCP service for ChatGPT.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "Mcp-Session-Id", "Last-Event-ID", "Authorization"],
)


QUOTE_COLUMNS = {
    "股票代码": "symbol",
    "股票名称": "name",
    "最新价": "price",
    "涨跌幅": "change_pct",
    "涨跌额": "change",
    "成交量": "volume",
    "成交额": "turnover",
    "振幅": "amplitude",
    "最高": "high",
    "最低": "low",
    "今开": "open",
    "昨收": "previous_close",
    "换手率": "turnover_rate",
    "市盈率-动态": "pe_dynamic",
    "市净率": "pb",
    "总市值": "total_market_value",
    "流通市值": "circulating_market_value",
}

KLINE_COLUMNS = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "turnover",
    "振幅": "amplitude",
    "涨跌幅": "change_pct",
    "涨跌额": "change",
    "换手率": "turnover_rate",
}

KLINE_PERIODS = {
    "daily": 101,
    "weekly": 102,
    "monthly": 103,
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
}

KLINE_ADJUSTMENTS = {
    "none": {"eastmoney_fqt": 0, "label": "unadjusted"},
    "forward": {"eastmoney_fqt": 1, "label": "forward_adjusted"},
    "backward": {"eastmoney_fqt": 2, "label": "backward_adjusted"},
}

EASTMONEY_FIELDS = ",".join(
    [
        "f57",
        "f58",
        "f43",
        "f169",
        "f170",
        "f46",
        "f44",
        "f45",
        "f47",
        "f48",
        "f60",
        "f168",
        "f162",
        "f167",
        "f116",
        "f117",
        "f124",
    ]
)

QUOTE_RESPONSE_FIELDS = (
    "symbol",
    "name",
    "price",
    "change",
    "change_pct",
    "open",
    "high",
    "low",
    "previous_close",
    "volume",
    "volume_unit",
    "turnover",
    "turnover_unit",
    "total_market_value",
    "total_market_value_unit",
    "circulating_market_value",
    "circulating_market_value_unit",
)

KLINE_RESPONSE_FIELDS = (
    "date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "volume_unit",
    "turnover",
    "turnover_unit",
    "change_pct",
)

KLINE_RAW_RESPONSE_FIELDS = (
    *KLINE_RESPONSE_FIELDS,
    "amplitude",
    "change",
    "turnover_rate",
)

FINANCIAL_RESPONSE_FIELDS = {
    "REPORT_DATE": "report_period",
    "REPORT_TYPE": "report_type",
    "NOTICE_DATE": "notice_date",
    "TOTALOPERATEREVE": "revenue",
    "TOTALOPERATEREVETZ": "revenue_yoy_pct",
    "PARENTNETPROFIT": "net_profit",
    "PARENTNETPROFITTZ": "net_profit_yoy_pct",
    "EPSJB": "basic_eps",
    "BPS": "net_assets_per_share",
    "ROEJQ": "roe_weighted_pct",
    "XSMLL": "gross_margin_pct",
    "ZCFZL": "debt_asset_ratio_pct",
    "MGJYXJJE": "operating_cash_flow_per_share",
}

INDEX_SECIDS = ",".join(
    (
        "1.000001",  # 上证指数
        "0.399001",  # 深证成指
        "0.399006",  # 创业板指
        "1.000688",  # 科创 50
        "1.000300",  # 沪深 300
        "1.000905",  # 中证 500
        "0.399852",  # 中证 1000
        "2.932000",  # 中证 2000
        "1.000016",  # 上证 50
        "1.000922",  # 中证红利
    )
)
PRIMARY_INDEX_SYMBOLS = {"000001", "399001", "399006"}
OVERVIEW_INDEX_SYMBOLS = {
    "000001",
    "399001",
    "399006",
    "000688",
    "000300",
    "000905",
    "399852",
    "932000",
    "000016",
    "000922",
}
TENCENT_INDEX_CODES = ",".join(
    (
        "sh000001",
        "sz399001",
        "sz399006",
        "sh000688",
        "sh000300",
        "sh000905",
        "sz399852",
        "sh932000",
        "sh000016",
        "sh000922",
    )
)
MARKET_QUOTE_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
MARKET_QUOTE_FIELDS = "f12,f14,f2,f3,f5,f6,f8,f15,f16,f17,f18,f20,f124"
BATCH_QUOTE_FIELDS = "f12,f13,f14,f2,f3,f4,f5,f6,f7,f8,f10,f15,f16,f17,f18,f20,f21,f124"
INDEX_SECID_BY_SYMBOL = {
    "000001": "1.000001",
    "399001": "0.399001",
    "399006": "0.399006",
    "000688": "1.000688",
    "000300": "1.000300",
    "000905": "1.000905",
    "399852": "0.399852",
    "932000": "2.932000",
    "000016": "1.000016",
    "000922": "1.000922",
    "899050": "0.899050",
}
INDEX_IDENTITY = {
    "000001": {"expected_name": "SSE Composite", "exchange": "SSE", "index_role": "primary"},
    "399001": {"expected_name": "Shenzhen Component", "exchange": "SZSE", "index_role": "primary"},
    "399006": {"expected_name": "ChiNext", "exchange": "SZSE", "index_role": "primary"},
    "000688": {"expected_name": "STAR 50", "exchange": "SSE", "index_role": "style"},
    "000300": {"expected_name": "CSI 300", "exchange": "SSE", "index_role": "style"},
    "000905": {"expected_name": "CSI 500", "exchange": "SSE", "index_role": "style"},
    "399852": {"expected_name": "CSI 1000", "exchange": "SZSE", "index_role": "style"},
    "932000": {"expected_name": "CSI 2000", "exchange": "SSE", "index_role": "style"},
    "000016": {"expected_name": "SSE 50", "exchange": "SSE", "index_role": "style"},
    "000922": {"expected_name": "CSI Dividend", "exchange": "SSE", "index_role": "style"},
}
SECTOR_TYPE_CONFIG = {
    "industry": "m:90+t:2",
    "concept": "m:90+t:3",
}
MARKET_TIMEZONE = timezone(timedelta(hours=8))

SYMBOL_PATTERN = re.compile(r"^\d{6}$")
INDUSTRY_LEVEL_PATTERN = re.compile(r"^(?P<industry_name>.+?)(?P<industry_level>[ⅠⅡⅢ])$")
INDUSTRY_LEVEL_RANK = {"Ⅰ": 1, "Ⅱ": 2, "Ⅲ": 3}

# These codes are listed on the Shanghai and Shenzhen exchanges rather than being
# ordinary company shares.  Keeping the prefixes here prevents a Shanghai ETF such
# as 512760 from being sent to public sources as the incorrect sz512760 / 0.512760.
ETF_PREFIXES = (
    "510",
    "511",
    "512",
    "513",
    "515",
    "516",
    "517",
    "518",
    "560",
    "561",
    "562",
    "563",
    "588",
    "159",
)
LOF_PREFIXES = ("501", "502", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169")

TOOL_CACHE: dict[str, dict[str, Any]] = {}
TOOL_CACHE_LOCK = Lock()
TOOL_CACHE_INFLIGHT: dict[str, dict[str, Any]] = {}
TOOL_CACHE_MAX_ENTRIES = 512
SOURCE_HEALTH: dict[str, dict[str, Any]] = {}
SOURCE_HEALTH_LOCK = Lock()
PREFERRED_ROUTE_HEALTH: dict[str, dict[str, Any]] = {}
PREFERRED_ROUTE_HEALTH_LOCK = Lock()
PUBLIC_SOURCE_EXECUTOR = ThreadPoolExecutor(max_workers=16)
SINA_AUX_EXECUTOR = ThreadPoolExecutor(max_workers=4)
TENCENT_KLINE_EXECUTOR = ThreadPoolExecutor(max_workers=8)
# Composite tools can fan out into several slow public routes. Keep those pools
# bounded and reusable so concurrent MCP calls do not create an unbounded tree
# of short-lived threads.
COMPOSITE_TOOL_EXECUTOR = ThreadPoolExecutor(max_workers=16)
FUND_COMPONENT_EXECUTOR = ThreadPoolExecutor(max_workers=12)
SECURITY_REFERENCE_EXECUTOR = ThreadPoolExecutor(max_workers=6)


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must be a six-digit mainland stock or exchange-listed fund code.",
        )
    return normalized


def security_metadata(symbol: str) -> dict[str, str]:
    """Classify a listed security and centralize source-specific market prefixes."""
    symbol = normalize_symbol(symbol)
    if symbol.startswith(("920", "4", "8")):
        exchange = "BSE"
        exchange_name = "Beijing Stock Exchange"
        market_prefix = "bj"
        eastmoney_market = "0"
        eastmoney_suffix = "BJ"
    elif symbol.startswith(("5", "6", "9")):
        exchange = "SSE"
        exchange_name = "Shanghai Stock Exchange"
        market_prefix = "sh"
        eastmoney_market = "1"
        eastmoney_suffix = "SH"
    elif symbol.startswith(("0", "1", "2", "3")):
        exchange = "SZSE"
        exchange_name = "Shenzhen Stock Exchange"
        market_prefix = "sz"
        eastmoney_market = "0"
        eastmoney_suffix = "SZ"
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported mainland exchange code: {symbol}",
        )

    if symbol.startswith(ETF_PREFIXES):
        security_type = "etf"
    elif symbol.startswith(LOF_PREFIXES):
        security_type = "lof"
    else:
        security_type = "a_share"

    return {
        "symbol": symbol,
        "security_type": security_type,
        "exchange": exchange,
        "exchange_name": exchange_name,
        "market_prefix": market_prefix,
        "eastmoney_market": eastmoney_market,
        "eastmoney_suffix": eastmoney_suffix,
    }


def batch_security_metadata(identifier: Any) -> dict[str, str]:
    """Classify a batch item, allowing an explicit index: prefix for ambiguous codes."""
    raw_identifier = str(identifier).strip()
    if raw_identifier.lower().startswith("index:"):
        symbol = normalize_symbol(raw_identifier.split(":", 1)[1])
        secid = INDEX_SECID_BY_SYMBOL.get(symbol)
        if secid is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unsupported index code. Use index: with one of the public indices returned by "
                    "get_a_share_market_overview."
                ),
            )
        exchange = "BSE" if symbol == "899050" else "SZSE" if secid.startswith("0.") else "SSE"
        return {
            "identifier": raw_identifier,
            "symbol": symbol,
            "security_type": "index",
            "exchange": exchange,
            "exchange_name": "Index quote",
            "eastmoney_secid": secid,
        }

    security = security_metadata(raw_identifier)
    return {"identifier": raw_identifier, **security, "eastmoney_secid": eastmoney_secid(raw_identifier)}


def enrich_index_identity(index: dict[str, Any]) -> dict[str, Any]:
    symbol = str(index.get("symbol") or "")
    identity = INDEX_IDENTITY.get(symbol, {})
    return {
        **index,
        "identifier": f"index:{symbol}" if symbol else None,
        "eastmoney_secid": INDEX_SECID_BY_SYMBOL.get(symbol),
        "exchange": identity.get("exchange"),
        "index_role": identity.get("index_role"),
        "expected_name": identity.get("expected_name"),
    }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def cache_key(tool_name: str, parameters: Any) -> str:
    return f"{tool_name}:{json.dumps(parameters, ensure_ascii=False, sort_keys=True, default=str)}"


def prune_tool_cache_locked(incoming_key: str) -> None:
    """Keep the in-process success cache bounded without touching in-flight work."""
    if incoming_key in TOOL_CACHE:
        return
    while len(TOOL_CACHE) >= TOOL_CACHE_MAX_ENTRIES:
        oldest_key = min(
            TOOL_CACHE,
            key=lambda key: TOOL_CACHE[key]["created_at"],
        )
        TOOL_CACHE.pop(oldest_key, None)


def get_cached_tool_data(
    key: str, ttl_seconds: int, loader: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
    while True:
        now = datetime.now(timezone.utc)
        with TOOL_CACHE_LOCK:
            cached = TOOL_CACHE.get(key)
            if cached is not None:
                age_seconds = (now - cached["created_at"]).total_seconds()
                if age_seconds <= ttl_seconds:
                    return (
                        deepcopy(cached["data"]),
                        {
                            "cache_hit": True,
                            "cache_created_at": cached["created_at"].isoformat(),
                            "cache_age_seconds": round(age_seconds, 3),
                        },
                    )
            entry = TOOL_CACHE_INFLIGHT.get(key)
            if entry is None:
                entry = {"event": Event(), "waiters": 0, "error": None}
                TOOL_CACHE_INFLIGHT[key] = entry
                break
            entry["waiters"] += 1
        # The owner runs its loader without holding a cache lock. Exact-key
        # duplicates wait, while nested loaders for other keys stay independent.
        entry["event"].wait()
        with TOOL_CACHE_LOCK:
            error = entry["error"]
            entry["waiters"] -= 1
            if entry["waiters"] == 0 and TOOL_CACHE_INFLIGHT.get(key) is entry:
                TOOL_CACHE_INFLIGHT.pop(key, None)
        if error is not None:
            error_kind, status_code, detail = error
            if error_kind == "http":
                raise HTTPException(status_code=status_code, detail=detail)
            raise RuntimeError(detail)

    try:
        data = loader()
        created_at = datetime.now(timezone.utc)
        with TOOL_CACHE_LOCK:
            prune_tool_cache_locked(key)
            TOOL_CACHE[key] = {"created_at": created_at, "data": deepcopy(data)}
        return (
            data,
            {
                "cache_hit": False,
                "cache_created_at": created_at.isoformat(),
                "cache_age_seconds": 0.0,
            },
        )
    except HTTPException as exc:
        with TOOL_CACHE_LOCK:
            entry["error"] = ("http", exc.status_code, str(exc.detail))
        raise
    except Exception as exc:
        with TOOL_CACHE_LOCK:
            entry["error"] = ("exception", 502, str(exc))
        raise
    finally:
        with TOOL_CACHE_LOCK:
            entry["event"].set()
            if entry["waiters"] == 0 and TOOL_CACHE_INFLIGHT.get(key) is entry:
                TOOL_CACHE_INFLIGHT.pop(key, None)


def get_cached_tool_snapshot(
    key: str, max_age_seconds: int
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    now = datetime.now(timezone.utc)
    with TOOL_CACHE_LOCK:
        cached = TOOL_CACHE.get(key)
        if cached is None:
            return None
        age_seconds = (now - cached["created_at"]).total_seconds()
        if age_seconds > max_age_seconds:
            return None
        return (
            deepcopy(cached["data"]),
            {
                "cache_hit": True,
                "cache_created_at": cached["created_at"].isoformat(),
                "cache_age_seconds": round(age_seconds, 3),
            },
        )


def get_cached_component_with_stale(
    key: str,
    ttl_seconds: int,
    max_stale_age_seconds: int,
    loader: Any,
) -> dict[str, Any]:
    """Share component refreshes and preserve a recent honest fallback on transient failure."""
    try:
        data, _ = get_cached_tool_data(key, ttl_seconds, loader)
        return data
    except HTTPException as exc:
        stale = get_cached_tool_snapshot(key, max_stale_age_seconds)
        if stale is None:
            raise
        data, cache = stale
        data["served_from_stale_cache"] = True
        data["stale_cache_age_seconds"] = cache["cache_age_seconds"]
        data.setdefault("source_errors", []).append(
            f"live_refresh: {exc.detail}; using recent component cache"
        )
        return data


def source_name_from_url(url: str) -> str:
    text = url.lower()
    if "eastmoney" in text:
        return "eastmoney"
    if "gtimg" in text or "qq.com" in text:
        return "tencent"
    if "sina" in text:
        return "sina"
    return "public_market_source"


def record_source_health(source: str, success: bool, latency_ms: int, error: str | None = None) -> None:
    now = now_iso()
    with SOURCE_HEALTH_LOCK:
        state = SOURCE_HEALTH.setdefault(
            source,
            {
                "attempt_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "average_latency_ms": 0.0,
                "last_success_at": None,
                "last_error_at": None,
                "last_error": None,
                "consecutive_failures": 0,
            },
        )
        state["attempt_count"] += 1
        previous_average = state["average_latency_ms"]
        state["average_latency_ms"] = round(
            (previous_average * (state["attempt_count"] - 1) + latency_ms)
            / state["attempt_count"],
            2,
        )
        if success:
            state["success_count"] += 1
            state["last_success_at"] = now
            state["consecutive_failures"] = 0
        else:
            state["failure_count"] += 1
            state["last_error_at"] = now
            state["last_error"] = error
            state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1


def source_is_temporarily_degraded(
    source: str, min_consecutive_failures: int = 2, cooldown_seconds: int = 90
) -> bool:
    """Use recent observed failures as a short-lived circuit-breaker signal."""
    with SOURCE_HEALTH_LOCK:
        state = deepcopy(SOURCE_HEALTH.get(source))
    if not state or state.get("consecutive_failures", 0) < min_consecutive_failures:
        return False
    last_error_at = state.get("last_error_at")
    if not last_error_at:
        return False
    try:
        error_time = datetime.fromisoformat(str(last_error_at))
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - error_time).total_seconds() <= cooldown_seconds


def record_preferred_route_health(
    route: str, success: bool, error: str | None = None
) -> None:
    with PREFERRED_ROUTE_HEALTH_LOCK:
        state = PREFERRED_ROUTE_HEALTH.setdefault(
            route,
            {
                "attempt_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "consecutive_failures": 0,
                "last_success_at": None,
                "last_error_at": None,
                "last_error": None,
            },
        )
        state["attempt_count"] += 1
        if success:
            state["success_count"] += 1
            state["consecutive_failures"] = 0
            state["last_success_at"] = now_iso()
        else:
            state["failure_count"] += 1
            state["consecutive_failures"] += 1
            state["last_error_at"] = now_iso()
            state["last_error"] = error


def preferred_route_is_temporarily_degraded(
    route: str, min_consecutive_failures: int = 2, cooldown_seconds: int = 90
) -> bool:
    with PREFERRED_ROUTE_HEALTH_LOCK:
        state = deepcopy(PREFERRED_ROUTE_HEALTH.get(route))
    if not state or state.get("consecutive_failures", 0) < min_consecutive_failures:
        return False
    last_error_at = state.get("last_error_at")
    if not last_error_at:
        return False
    try:
        error_time = datetime.fromisoformat(str(last_error_at))
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - error_time).total_seconds() <= cooldown_seconds


def classify_error_type(message: Any, status_code: int | None = None) -> str:
    text = str(message).lower()
    if status_code == 404 or "not found" in text or "not_found" in text:
        return "not_found"
    if status_code == 400 or "must be" in text or "invalid" in text or "required" in text:
        return "invalid_symbol"
    if "timeout" in text or "timed out" in text or "exceeded" in text:
        return "timeout"
    if "429" in text or "rate limit" in text or "rate_limited" in text:
        return "rate_limited"
    if "json" in text or "unexpected" in text or "format" in text or "parse" in text:
        return "parse_error"
    if "502" in text or "503" in text or "504" in text or "upstream" in text:
        return "upstream_5xx"
    return "network_error"


def race_public_sources(
    source_getters: tuple[tuple[str, Any], ...], timeout_seconds: float
) -> tuple[Any, str, list[str]]:
    """Return the first successful independent source without serial failure delay."""
    futures = {
        PUBLIC_SOURCE_EXECUTOR.submit(getter): source for source, getter in source_getters
    }
    pending = set(futures)
    errors: list[str] = []
    status_codes: list[int] = []
    source_order = {source: index for index, (source, _) in enumerate(source_getters)}
    deadline = perf_counter() + timeout_seconds
    while pending:
        remaining = deadline - perf_counter()
        if remaining <= 0:
            break
        completed, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
        if not completed:
            break
        for future in sorted(completed, key=lambda item: source_order[futures[item]]):
            source = futures[future]
            try:
                return future.result(), source, errors
            except HTTPException as exc:
                errors.append(f"{source}: {exc.detail}")
                status_codes.append(exc.status_code)
            except Exception as exc:  # pragma: no cover - defensive source boundary
                errors.append(f"{source}: {exc}")
                status_codes.append(502)
    for future in pending:
        future.cancel()
        errors.append(f"{futures[future]}: request exceeded the {timeout_seconds:g} second budget")
        status_codes.append(502)
    status_code = 404 if status_codes and all(code == 404 for code in status_codes) else 502
    raise HTTPException(
        status_code=status_code,
        detail="; ".join(errors) or "All public market sources are unavailable.",
    )


def prefer_primary_public_source(
    primary: tuple[str, Any],
    fallback: tuple[str, Any],
    timeout_seconds: float,
    degradation_route: str | None = None,
) -> tuple[Any, str, list[str]]:
    """Prefetch a fallback concurrently without discarding a healthy richer primary."""
    primary_source, primary_getter = primary
    fallback_source, fallback_getter = fallback
    futures = {
        PUBLIC_SOURCE_EXECUTOR.submit(primary_getter): primary_source,
        PUBLIC_SOURCE_EXECUTOR.submit(fallback_getter): fallback_source,
    }
    pending = set(futures)
    errors: list[str] = []
    status_codes: list[int] = []
    fallback_payload: Any = None
    deadline = perf_counter() + timeout_seconds
    while pending:
        remaining = deadline - perf_counter()
        if remaining <= 0:
            break
        completed, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
        if not completed:
            break
        for future in sorted(completed, key=lambda item: futures[item] != primary_source):
            source = futures[future]
            try:
                payload = future.result()
                if source == primary_source:
                    return payload, source, errors
                fallback_payload = payload
            except HTTPException as exc:
                errors.append(f"{source}: {exc.detail}")
                status_codes.append(exc.status_code)
            except Exception as exc:  # pragma: no cover - defensive source boundary
                errors.append(f"{source}: {exc}")
                status_codes.append(502)
        primary_pending = any(futures[future] == primary_source for future in pending)
        route_degraded = (
            preferred_route_is_temporarily_degraded(degradation_route)
            if degradation_route
            else source_is_temporarily_degraded(primary_source)
        )
        if (
            fallback_payload is not None
            and primary_pending
            and route_degraded
        ):
            for future in pending:
                if futures[future] == primary_source:
                    future.cancel()
            errors.append(
                f"{primary_source}: adaptive fast fallback used after repeated recent source failures"
            )
            return fallback_payload, fallback_source, errors
        if fallback_payload is not None and not primary_pending:
            return fallback_payload, fallback_source, errors
    for future in pending:
        future.cancel()
        errors.append(f"{futures[future]}: request exceeded the {timeout_seconds:g} second budget")
        status_codes.append(502)
    if fallback_payload is not None:
        return fallback_payload, fallback_source, errors
    status_code = 404 if status_codes and all(code == 404 for code in status_codes) else 502
    raise HTTPException(
        status_code=status_code,
        detail="; ".join(errors) or "Both preferred and fallback sources are unavailable.",
    )


def normalize_sources(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def normalize_source_errors(value: Any) -> list[dict[str, str]]:
    if not value:
        return []
    raw_errors = value if isinstance(value, list) else [value]
    normalized = []
    for item in raw_errors:
        if isinstance(item, dict):
            message = str(item.get("message") or item.get("error") or item)
            normalized.append(
                {
                    "source": str(item.get("source") or "public_market_source"),
                    "error_type": str(item.get("error_type") or classify_error_type(message)),
                    "message": message,
                }
            )
            continue
        message = str(item)
        source = message.split(":", 1)[0] if ":" in message else "public_market_source"
        normalized.append(
            {
                "source": source,
                "error_type": classify_error_type(message),
                "message": message,
            }
        )
    return normalized


def infer_trade_date(data: dict[str, Any], market_time: str | None) -> str | None:
    trade_date = data.get("trade_date") or data.get("latest_trade_date")
    if trade_date:
        return str(trade_date)[:10]
    return market_time[:10] if market_time and len(market_time) >= 10 else None


def staleness_basis_for(
    market_time: str | None,
    trade_date: str | None,
    market_status: str,
    is_stale: bool,
) -> str:
    """Explain which market-time state the stale flag is evaluated against."""
    parsed = parse_market_datetime(market_time)
    now = datetime.now(MARKET_TIMEZONE)
    if is_stale:
        return "current_session_freshness_window"
    if parsed is not None:
        if market_status not in {"open", "lunch_break"} and parsed.hour == 15 and parsed.minute == 0:
            return "completed_session_final"
        if market_status in {"open", "lunch_break"} and parsed.date() == now.date():
            return "current_session_observation"
        return "latest_available_market_observation"
    if trade_date:
        return "trade_date_only"
    return "unavailable"


def standardize_tool_success(
    data: dict[str, Any], started_at: float, cache: dict[str, Any] | None = None
) -> dict[str, Any]:
    tool_queried_at = now_iso()
    content = deepcopy(data)
    content.setdefault(
        "snapshot_id",
        f"snapshot-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
    )
    content.setdefault("source_updated_at", None)
    content.setdefault("missing_fields", [])
    content.setdefault("conflicts", [])
    content.setdefault("data_status", "full_data")
    content.setdefault("detail_level", "summary")
    market_time = (
        content.get("market_time")
        or content.get("quote_time")
        or content.get("latest_market_time")
    )
    trade_date = infer_trade_date(content, market_time)
    market_status = market_status_at()
    source_time = parse_market_datetime(market_time)
    source_age_seconds = (
        max(0.0, (datetime.now(MARKET_TIMEZONE) - source_time).total_seconds())
        if source_time is not None
        else None
    )
    is_stale = False
    stale_reason = None
    if market_time:
        is_stale = is_market_time_stale(market_time)
        if is_stale:
            stale_reason = "market_time_lags_current_trading_session"
    elif (
        trade_date
        and market_status in {"open", "lunch_break"}
        and trade_date < datetime.now(MARKET_TIMEZONE).date().isoformat()
    ):
        is_stale = True
        stale_reason = "previous_trade_day_data_during_open_market"

    cache = cache or {
        "cache_hit": False,
        "cache_created_at": tool_queried_at,
        "cache_age_seconds": 0.0,
    }
    source_fetch_time = (
        content.get("source_fetch_time")
        or content.get("queried_at")
        or cache.get("cache_created_at")
        or tool_queried_at
    )
    staleness_basis = staleness_basis_for(
        market_time,
        trade_date,
        market_status,
        is_stale,
    )
    content.update(
        {
            "effective_market_time": market_time,
            "source_fetch_time": source_fetch_time,
            "tool_queried_at": tool_queried_at,
            "staleness_basis": staleness_basis,
            # Backward-compatible alias. New callers should use tool_queried_at.
            "queried_at": tool_queried_at,
        }
    )
    result = {"ok": True, **content}
    result.update(
        {
            "market_status": market_status,
            "trade_date": trade_date,
            "effective_market_time": market_time,
            "market_time": market_time,
            "source_fetch_time": source_fetch_time,
            "tool_queried_at": tool_queried_at,
            "queried_at": tool_queried_at,
            "source": normalize_sources(content.get("source")),
            "source_errors": normalize_source_errors(content.get("source_errors")),
            "is_stale": is_stale,
            "stale_reason": stale_reason,
            "staleness_basis": staleness_basis,
            "data_age_seconds": round(source_age_seconds, 3)
            if source_age_seconds is not None
            else cache["cache_age_seconds"],
            "latency_ms": int((perf_counter() - started_at) * 1000),
            **cache,
            "data": content,
        }
    )
    return result


def format_market_time(value: Any) -> str | None:
    if value in (None, "", "-"):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{14}", text):
        return datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=MARKET_TIMEZONE).isoformat()
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").replace(tzinfo=MARKET_TIMEZONE).isoformat()
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=MARKET_TIMEZONE).isoformat()
        except ValueError:
            pass
    return text


def format_unix_market_time(value: Any) -> str | None:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).astimezone(MARKET_TIMEZONE).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def derive_quote_timestamps(source_updated_at: str | None) -> dict[str, str | None]:
    result = {
        "trade_date": None,
        "quote_time": None,
        "source_updated_at": source_updated_at,
    }
    if not source_updated_at:
        return result

    try:
        updated = datetime.fromisoformat(source_updated_at)
    except ValueError:
        return result

    result["trade_date"] = updated.date().isoformat()
    minutes = updated.hour * 60 + updated.minute
    if 9 * 60 + 30 <= minutes <= 11 * 60 + 30:
        quote_time = updated
    elif 11 * 60 + 30 < minutes < 13 * 60:
        quote_time = updated.replace(hour=11, minute=30, second=0, microsecond=0)
    elif 13 * 60 <= minutes <= 15 * 60:
        quote_time = updated
    elif minutes > 15 * 60:
        quote_time = updated.replace(hour=15, minute=0, second=0, microsecond=0)
    else:
        quote_time = None

    result["quote_time"] = quote_time.isoformat() if quote_time else None
    return result


def market_time_from_source_update(source_updated_at: str | None) -> str | None:
    return derive_quote_timestamps(source_updated_at)["quote_time"]


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if type(value).__name__ in {"NAType", "NaTType"}:
        return None
    try:
        if bool(value != value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_dict(row: Any, columns: dict[str, str]) -> dict[str, Any]:
    available_columns = getattr(row, "index", row)
    return {
        output_key: clean_value(row.get(input_key))
        for input_key, output_key in columns.items()
        if input_key in available_columns
    }


def scaled(value: Any, divisor: float = 100) -> Any:
    if value in (None, "-", ""):
        return None
    return clean_value(value / divisor)


def eastmoney_secid(symbol: str) -> str:
    security = security_metadata(symbol)
    return f"{security['eastmoney_market']}.{security['symbol']}"


def market_symbol(symbol: str) -> str:
    security = security_metadata(symbol)
    return f"{security['market_prefix']}{security['symbol']}"


def to_number(value: Any) -> float | None:
    if value in (None, "-", ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_market_text(url: str, referer: str, timeout: int = 3) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
        },
    )
    started_at = perf_counter()
    source = source_name_from_url(url)
    try:
        # Callers construct URLs from a fixed quote-provider host list.
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            text = response.read().decode("gbk", errors="replace")
        record_source_health(source, True, int((perf_counter() - started_at) * 1000))
        return text
    except OSError as exc:
        record_source_health(source, False, int((perf_counter() - started_at) * 1000), str(exc))
        raise


def read_public_json(
    url: str,
    referer: str,
    timeout: int = 3,
    attempts: int = 1,
) -> Any:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Accept": "application/json, text/plain, */*",
        },
    )
    errors = []
    source = source_name_from_url(url)
    for _ in range(attempts):
        started_at = perf_counter()
        try:
            with urlopen(request, timeout=timeout) as response:  # nosec B310
                payload = json.loads(response.read().decode("utf-8"))
            record_source_health(source, True, int((perf_counter() - started_at) * 1000))
            return payload
        except (OSError, URLError, json.JSONDecodeError) as exc:
            errors.append(str(exc))
            record_source_health(source, False, int((perf_counter() - started_at) * 1000), str(exc))
    raise HTTPException(status_code=502, detail=f"Failed to fetch public market data: {'; '.join(errors)}")


def read_public_json_post(
    url: str,
    referer: str,
    payload: dict[str, Any],
    timeout: int = 5,
) -> Any:
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Origin": "https://www.szse.cn",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "X-Request-Type": "ajax",
            "X-Requested-With": "XMLHttpRequest",
        },
        method="POST",
    )
    started_at = perf_counter()
    source = source_name_from_url(url)
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310
            result = json.loads(response.read().decode("utf-8"))
        record_source_health(source, True, int((perf_counter() - started_at) * 1000))
        return result
    except (OSError, URLError, json.JSONDecodeError) as exc:
        record_source_health(
            source,
            False,
            int((perf_counter() - started_at) * 1000),
            str(exc),
        )
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch official disclosure data: {exc}",
        ) from exc


def read_public_jsonp(url: str, referer: str) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Accept": "*/*",
        },
    )
    started_at = perf_counter()
    source = source_name_from_url(url)
    try:
        with urlopen(request, timeout=3) as response:  # nosec B310
            text = response.read().decode("utf-8")
        start = text.find("(")
        end = text.rfind(")")
        if start < 0 or end <= start:
            raise ValueError("Unexpected JSONP response.")
        payload = json.loads(text[start + 1 : end])
        record_source_health(source, True, int((perf_counter() - started_at) * 1000))
        return payload
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        record_source_health(source, False, int((perf_counter() - started_at) * 1000), str(exc))
        raise HTTPException(status_code=502, detail=f"Failed to fetch public market news: {exc}") from exc


def read_sina_object(url: str, referer: str) -> dict[str, Any]:
    try:
        text = read_market_text(url, referer).strip().rstrip(";").strip()
        if text.startswith("(") and text.endswith(")"):
            text = text[1:-1].strip()
        text = re.sub(r'([,{]\s*)([A-Za-z_]\w*)\s*:', r'\1"\2":', text)
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Sina object response.")
        return payload
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Sina market data: {exc}") from exc


def get_tencent_quote(symbol: str) -> dict[str, Any]:
    url = f"http://qt.gtimg.cn/q={market_symbol(symbol)}"
    try:
        text = read_market_text(url, "https://stockapp.finance.qq.com/")
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Tencent quote: {exc}") from exc

    if '"' not in text:
        raise HTTPException(status_code=502, detail="Unexpected Tencent quote format.")
    parts = text.split('"', 2)[1].split("~")
    if len(parts) < 47 or not parts[1]:
        raise HTTPException(status_code=404, detail=f"Stock not found from Tencent: {symbol}")

    return {
        "symbol": clean_value(parts[2]),
        "name": clean_value(parts[1]),
        "price": to_number(parts[3]),
        "change_pct": to_number(parts[32]),
        "change": to_number(parts[31]),
        "volume": to_number(parts[36]),
        "turnover": to_number(parts[37]),
        "amplitude": to_number(parts[43]),
        "high": to_number(parts[33]),
        "low": to_number(parts[34]),
        "open": to_number(parts[5]),
        "previous_close": to_number(parts[4]),
        "turnover_rate": to_number(parts[38]),
        "pe_dynamic": to_number(parts[39]),
        "pb": to_number(parts[46]),
        "total_market_value": to_number(parts[44]),
        "circulating_market_value": to_number(parts[45]),
        "source_updated_at": format_market_time(parts[30] if len(parts) > 30 else None),
    }


def get_sina_quote(symbol: str) -> dict[str, Any]:
    url = f"http://hq.sinajs.cn/list={market_symbol(symbol)}"
    try:
        text = read_market_text(url, "https://finance.sina.com.cn/")
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Sina quote: {exc}") from exc

    if '"' not in text:
        raise HTTPException(status_code=502, detail="Unexpected Sina quote format.")
    parts = text.split('"', 2)[1].split(",")
    if len(parts) < 32 or not parts[0]:
        raise HTTPException(status_code=404, detail=f"Stock not found from Sina: {symbol}")

    price = to_number(parts[3])
    previous_close = to_number(parts[2])
    change = None if price is None or previous_close is None else price - previous_close
    change_pct = None
    if change is not None and previous_close not in (None, 0):
        change_pct = change / previous_close * 100

    return {
        "symbol": symbol,
        "name": clean_value(parts[0]),
        "price": price,
        "change_pct": change_pct,
        "change": change,
        "volume": to_number(parts[8]),
        "turnover": to_number(parts[9]),
        "high": to_number(parts[4]),
        "low": to_number(parts[5]),
        "open": to_number(parts[1]),
        "previous_close": previous_close,
        "source_updated_at": format_market_time(
            f"{parts[30]} {parts[31]}" if len(parts) > 31 else None
        ),
    }


def get_eastmoney_quote(symbol: str) -> dict[str, Any]:
    url = (
        "https://push2.eastmoney.com/api/qt/stock/get"
        f"?secid={eastmoney_secid(symbol)}&fields={EASTMONEY_FIELDS}"
    )
    payload = read_public_json(url, "https://quote.eastmoney.com/")

    data = payload.get("data")
    if not data:
        raise HTTPException(status_code=404, detail=f"Stock not found from Eastmoney: {symbol}")

    return {
        "symbol": clean_value(data.get("f57")),
        "name": clean_value(data.get("f58")),
        "price": scaled(data.get("f43")),
        "change_pct": scaled(data.get("f170")),
        "change": scaled(data.get("f169")),
        "volume": clean_value(data.get("f47")),
        "turnover": clean_value(data.get("f48")),
        "high": scaled(data.get("f44")),
        "low": scaled(data.get("f45")),
        "open": scaled(data.get("f46")),
        "previous_close": scaled(data.get("f60")),
        "turnover_rate": scaled(data.get("f168")),
        "pe_dynamic": scaled(data.get("f162")),
        "pb": scaled(data.get("f167")),
        "total_market_value": clean_value(data.get("f116")),
        "circulating_market_value": clean_value(data.get("f117")),
        "source_updated_at": format_unix_market_time(data.get("f124")),
    }


def get_fallback_quote(symbol: str) -> tuple[dict[str, Any], str]:
    errors = []
    for source, getter in (
        ("tencent", get_tencent_quote),
        ("sina", get_sina_quote),
        ("eastmoney", get_eastmoney_quote),
    ):
        try:
            return getter(symbol), source
        except HTTPException as exc:
            errors.append(f"{source}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def normalize_quote_units(quote: dict[str, Any], source: str) -> dict[str, Any]:
    volume = to_number(quote.get("volume"))
    turnover = to_number(quote.get("turnover"))
    total_market_value = to_number(quote.get("total_market_value"))
    circulating_market_value = to_number(quote.get("circulating_market_value"))

    if volume is not None and source in {"efinance", "eastmoney", "tencent"}:
        volume *= 100
    if turnover is not None and source == "tencent":
        turnover *= 10_000
    # Tencent publishes both market-cap fields in 100 million CNY, while the
    # Eastmoney route already publishes raw CNY. Normalize the public contract
    # so callers never have to infer a unit from the selected fallback source.
    if source == "tencent":
        if total_market_value is not None:
            total_market_value *= 100_000_000
        if circulating_market_value is not None:
            circulating_market_value *= 100_000_000

    quote["volume"] = volume
    quote["volume_unit"] = "share"
    quote["turnover"] = turnover
    quote["turnover_unit"] = "CNY"
    quote["total_market_value"] = total_market_value
    quote["total_market_value_unit"] = "CNY"
    quote["circulating_market_value"] = circulating_market_value
    quote["circulating_market_value_unit"] = "CNY"
    return quote


def get_all_realtime_quotes() -> Any:
    try:
        # efinance imports pandas and numpy. Keep that heavy import off the
        # process-start path so /health and MCP discovery can answer sooner on
        # a sleeping free Render instance. This fallback loads it on first use.
        import efinance as ef

        data = ef.stock.get_realtime_quotes()
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch realtime market data: {exc}",
        ) from exc

    if data is None or data.empty:
        raise HTTPException(status_code=502, detail="Realtime market data is empty.")
    return data


def search_tencent_stock(keyword: str, limit: int) -> list[dict[str, Any]]:
    url = "https://smartbox.gtimg.cn/s3/?" + urlencode({"q": keyword, "t": "all"})
    try:
        text = read_market_text(url, "https://stockapp.finance.qq.com/")
    except OSError as exc:
        raise HTTPException(status_code=502, detail="Tencent stock search is unavailable.") from exc

    if '"' not in text:
        raise HTTPException(status_code=502, detail="Unexpected Tencent search format.")

    results = []
    for candidate in text.split('"', 2)[1].split("^"):
        parts = candidate.split("~")
        if len(parts) < 5 or not SYMBOL_PATTERN.fullmatch(parts[1]):
            continue
        try:
            security = security_metadata(parts[1])
        except HTTPException:
            continue
        if parts[0].lower() != security["market_prefix"]:
            continue
        try:
            name = json.loads(f'"{parts[2]}"')
        except json.JSONDecodeError:
            name = parts[2]
        results.append(
            {
                "symbol": parts[1],
                "name": name,
                "market": security["exchange_name"],
                "security_type": security["security_type"],
            }
        )
        if len(results) >= limit:
            break
    return results


def search_sina_stock(keyword: str, limit: int) -> list[dict[str, Any]]:
    url = "https://suggest3.sinajs.cn/suggest/" + urlencode(
        {"type": "11,12,13,14,15", "key": keyword, "name": "suggestdata"}
    )
    try:
        text = read_market_text(url, "https://finance.sina.com.cn/")
    except OSError as exc:
        raise HTTPException(status_code=502, detail="Sina stock search is unavailable.") from exc

    if '"' not in text:
        raise HTTPException(status_code=502, detail="Unexpected Sina search format.")

    results = []
    for candidate in text.split('"', 2)[1].split(";"):
        parts = candidate.split(",")
        if len(parts) < 4 or not SYMBOL_PATTERN.fullmatch(parts[2]):
            continue
        market_prefix = parts[3][:2].lower()
        try:
            security = security_metadata(parts[2])
        except HTTPException:
            continue
        if market_prefix != security["market_prefix"]:
            continue
        results.append(
            {
                "symbol": parts[2],
                "name": parts[0],
                "market": security["exchange_name"],
                "security_type": security["security_type"],
            }
        )
        if len(results) >= limit:
            break
    return results


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "name": APP_NAME,
        "routing_revision": ROUTING_REVISION,
        "time": now_iso(),
        "data_health": get_market_data_health_data(),
    }


def search_stock_data(keyword: str, limit: int) -> dict[str, Any]:
    keyword = keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required.")

    results = search_tencent_stock(keyword, limit)
    source = "tencent_search"
    if not results:
        results = search_sina_stock(keyword, limit)
        source = "sina_search"

    return {
        "keyword": keyword,
        "count": len(results),
        "results": results,
        "source": source,
        "queried_at": now_iso(),
    }


def get_quote_data(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    quote, source, source_errors = get_fastest_public_quote(symbol)

    quote = normalize_quote_units(quote, source)
    quote.update(derive_quote_timestamps(quote.get("source_updated_at")))
    quote.update(
        {
            "security_type": security["security_type"],
            "exchange": security["exchange"],
        }
    )

    return {
        "quote": quote,
        "source": source,
        "source_errors": source_errors,
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def normalize_batch_symbols(symbols: Any) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    if not isinstance(symbols, list):
        raise HTTPException(status_code=400, detail="symbols must be a JSON array of up to 20 codes.")
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols must contain at least one code.")
    if len(symbols) > 20:
        raise HTTPException(status_code=400, detail="symbols supports at most 20 codes per request.")

    securities: list[dict[str, str]] = []
    errors: list[dict[str, Any]] = []
    seen_identifiers = set()
    for item in symbols:
        if not isinstance(item, str):
            errors.append(
                {
                    "identifier": item,
                    "code": "invalid_symbol",
                    "error": "Each symbol must be a string.",
                }
            )
            continue
        identifier = item.strip()
        if identifier in seen_identifiers:
            continue
        seen_identifiers.add(identifier)
        try:
            securities.append(batch_security_metadata(identifier))
        except HTTPException as exc:
            errors.append(
                {
                    "identifier": identifier,
                    "code": "invalid_symbol",
                    "error": str(exc.detail),
                }
            )
    return securities, errors


def get_eastmoney_batch_quote_rows(securities: list[dict[str, str]]) -> tuple[list[dict[str, Any]], str]:
    query = urlencode(
        {
            "fltt": 2,
            "invt": 2,
            "fields": BATCH_QUOTE_FIELDS,
            "secids": ",".join(security["eastmoney_secid"] for security in securities),
        }
    )
    hosts = (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    )

    def load_host(host: str) -> list[dict[str, Any]]:
        payload = read_public_json(
            f"https://{host}/api/qt/ulist.np/get?{query}",
            "https://quote.eastmoney.com/",
            3,
            1,
        )
        rows = ((payload.get("data") or {}).get("diff")) or []
        if not rows:
            raise HTTPException(status_code=502, detail="no quote rows")
        return rows

    rows, host, _ = race_public_sources(
        tuple((host, lambda host=host: load_host(host)) for host in hosts), 4
    )
    return rows, host


def get_fastest_public_quote(symbol: str) -> tuple[dict[str, Any], str, list[str]]:
    """Race independent public quote sources and retain failures for partial diagnostics."""
    source_getters = (
        ("eastmoney", get_eastmoney_quote),
        ("tencent", get_tencent_quote),
        ("sina", get_sina_quote),
    )
    errors: list[str] = []
    executor = ThreadPoolExecutor(max_workers=len(source_getters))
    futures = {executor.submit(getter, symbol): source for source, getter in source_getters}
    pending = set(futures)
    try:
        deadline = perf_counter() + 6
        while pending:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                break
            for future in completed:
                source = futures[future]
                try:
                    return future.result(), source, errors
                except HTTPException as exc:
                    errors.append(f"{source}: {exc.detail}")
                except Exception as exc:  # pragma: no cover - defensive source boundary
                    errors.append(f"{source}: {exc}")
        for future in pending:
            future.cancel()
            errors.append(f"{futures[future]}: request timeout after 6 seconds")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    raise HTTPException(
        status_code=502,
        detail="; ".join(errors) or "All public quote sources are unavailable.",
    )


def batch_quote_from_eastmoney_row(row: dict[str, Any], security: dict[str, str]) -> dict[str, Any]:
    volume = to_number(row.get("f5"))
    market_time = format_unix_market_time(row.get("f124"))
    return {
        "identifier": security["identifier"],
        "symbol": security["symbol"],
        "name": clean_value(row.get("f14")),
        "security_type": security["security_type"],
        "exchange": security["exchange"],
        "price": to_number(row.get("f2")),
        "change": to_number(row.get("f4")),
        "change_pct": to_number(row.get("f3")),
        "open": to_number(row.get("f17")),
        "high": to_number(row.get("f15")),
        "low": to_number(row.get("f16")),
        "previous_close": to_number(row.get("f18")),
        "volume": None if volume is None else volume * 100,
        "volume_unit": "share",
        "turnover": to_number(row.get("f6")),
        "turnover_unit": "CNY",
        "amplitude": to_number(row.get("f7")),
        "turnover_rate": to_number(row.get("f8")),
        "volume_ratio": to_number(row.get("f10")),
        "total_market_value": to_number(row.get("f20")),
        "total_market_value_unit": "CNY",
        "circulating_market_value": to_number(row.get("f21")),
        "circulating_market_value_unit": "CNY",
        "market_time": market_time,
        "source": "eastmoney_batch",
    }


def get_batch_quote_data(symbols: Any) -> dict[str, Any]:
    securities, errors = normalize_batch_symbols(symbols)
    results: list[dict[str, Any]] = []
    source_errors: list[str] = []
    source = []
    if securities:
        try:
            rows, host = get_eastmoney_batch_quote_rows(securities)
            source.append(f"eastmoney_batch:{host}")
            rows_by_secid = {
                f"{row.get('f13')}.{str(row.get('f12') or '').zfill(6)}": row
                for row in rows
            }
            for security in securities:
                row = rows_by_secid.get(security["eastmoney_secid"])
                if row is None:
                    errors.append(
                        {
                            "identifier": security["identifier"],
                            "symbol": security["symbol"],
                            "code": "no_data",
                            "error": "The public batch source did not return this security.",
                        }
                    )
                    continue
                results.append(batch_quote_from_eastmoney_row(row, security))
        except HTTPException as exc:
            source_errors.append(str(exc.detail))
            for security in securities:
                errors.append(
                    {
                        "identifier": security["identifier"],
                        "symbol": security["symbol"],
                        "code": "upstream_error",
                        "error": "The public batch quote source is temporarily unavailable.",
                    }
                )

    market_times = sorted(item["market_time"] for item in results if item.get("market_time"))
    return {
        "requested_count": len(symbols),
        "count": len(results),
        "results": results,
        "errors": errors,
        "source": source,
        "source_errors": source_errors,
        "market_time": market_times[-1] if market_times else None,
        "market_time_range": (
            {"earliest": market_times[0], "latest": market_times[-1]} if market_times else None
        ),
        "queried_at": now_iso(),
        "data_status": "full_data" if results and not errors else "partial_data" if results else "no_data",
        "note": "All successful quotes are requested from one public batch snapshot; no investment judgement is generated.",
    }


def parse_iso_date_parameter(value: str | None, field_name: str) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"{field_name} must use YYYY-MM-DD."
        ) from exc


def encode_kline_page_token(before: str) -> str:
    payload = json.dumps({"before": before}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_kline_page_token(page_token: str | None) -> str | None:
    if not page_token:
        return None
    try:
        padded = page_token + "=" * (-len(page_token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        before = str(payload["before"])
        datetime.fromisoformat(before)
        return before
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid K-line page_token.") from exc


def filter_and_page_kline_items(
    items: list[dict[str, Any]],
    limit: int,
    start_date: str | None,
    end_date: str | None,
    page_token: str | None,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    before = decode_kline_page_token(page_token)
    filtered = []
    for item in items:
        item_time = str(item.get("date") or "")
        item_day = item_time[:10]
        if not item_time:
            continue
        if start_date and item_day < start_date:
            continue
        if end_date and item_day > end_date:
            continue
        if before and item_time >= before:
            continue
        filtered.append(item)
    filtered.sort(key=lambda item: str(item.get("date") or ""))
    has_more = len(filtered) > limit
    page = filtered[-limit:]
    next_page_token = (
        encode_kline_page_token(str(page[0]["date"])) if has_more and page else None
    )
    return page, next_page_token, has_more


def kline_coverage_status(
    period: str, start_date: str | None, items: list[dict[str, Any]]
) -> tuple[str, list[str]]:
    if period == "1m" and start_date:
        first_day = str(items[0].get("date") or "")[:10] if items else None
        if not first_day or first_day > start_date:
            return (
                "partial_public_source_history",
                ["historical_1m_before_available_public_range"],
            )
    return "available_public_range", []


def get_kline_data(
    symbol: str,
    period: str,
    limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
    adjust: str = "forward",
    page_token: str | None = None,
) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    klt = KLINE_PERIODS.get(period)
    if klt is None:
        raise HTTPException(status_code=400, detail=f"Unsupported period: {period}")
    start_date = parse_iso_date_parameter(start_date, "start_date")
    end_date = parse_iso_date_parameter(end_date, "end_date")
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must not be after end_date.")
    if adjust not in KLINE_ADJUSTMENTS:
        raise HTTPException(
            status_code=400, detail="adjust must be none, forward, or backward."
        )
    decode_kline_page_token(page_token)

    payload = get_fallback_kline(
        symbol,
        period,
        klt,
        limit,
        start_date,
        end_date,
        adjust,
        page_token,
    )
    security = security_metadata(symbol)
    payload.update(
        {
            "security_type": security["security_type"],
            "exchange": security["exchange"],
        }
    )
    return payload


def get_eastmoney_kline(
    symbol: str,
    period: str,
    klt: int,
    limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
    adjust: str = "forward",
    page_token: str | None = None,
) -> dict[str, Any]:
    before = decode_kline_page_token(page_token)
    source_end_date = end_date
    if before and not (start_date or end_date):
        before_day = before[:10]
        source_end_date = before_day
    requested_source_limit = 5000 if start_date or end_date or page_token else limit + 1
    adjustment = KLINE_ADJUSTMENTS[adjust]
    query = urlencode(
        {
            "secid": eastmoney_secid(symbol),
            "klt": klt,
            "fqt": adjustment["eastmoney_fqt"],
            "lmt": requested_source_limit,
            "beg": (start_date or "0").replace("-", ""),
            "end": (source_end_date or "2050-01-01").replace("-", ""),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
    )
    def load_source_payload() -> dict[str, Any]:
        return read_public_json(
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{query}",
            "https://quote.eastmoney.com/",
            attempts=2,
        )

    if start_date or end_date:
        source_cache_key = cache_key(
            "eastmoney_kline_source_range",
            {
                "symbol": symbol,
                "period": period,
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
            },
        )
        payload, _ = get_cached_tool_data(
            source_cache_key,
            300 if period in {"daily", "weekly", "monthly"} else 30,
            load_source_payload,
        )
    else:
        payload = load_source_payload()
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        raise HTTPException(status_code=404, detail=f"Kline data not found from Eastmoney: {symbol}")

    items = []
    for kline in klines:
        values = kline.split(",")
        if len(values) < 11:
            continue
        volume = to_number(values[5])
        items.append(
            {
                "date": values[0],
                "open": to_number(values[1]),
                "close": to_number(values[2]),
                "high": to_number(values[3]),
                "low": to_number(values[4]),
                "volume": None if volume is None else volume * 100,
                "volume_unit": "share",
                "turnover": to_number(values[6]),
                "turnover_unit": "CNY",
                "amplitude": to_number(values[7]),
                "change_pct": to_number(values[8]),
                "change": to_number(values[9]),
                "turnover_rate": to_number(values[10]),
            }
        )
    if not items:
        raise HTTPException(status_code=502, detail=f"Unexpected Eastmoney Kline format: {symbol}")

    items, next_page_token, has_more = filter_and_page_kline_items(
        items, limit, start_date, end_date, page_token
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"Kline data not found in requested range: {symbol}")
    coverage_status, missing_fields = kline_coverage_status(period, start_date, items)

    return {
        "symbol": symbol,
        "period": period,
        "adjustment": adjustment["label"],
        "adjustment_source_parameter": f"eastmoney_fqt_{adjustment['eastmoney_fqt']}",
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "available_start": items[0]["date"],
        "available_end": items[-1]["date"],
        "coverage_status": coverage_status,
        "missing_fields": missing_fields,
        "has_more": has_more,
        "next_page_token": next_page_token,
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "latest_trade_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_tencent_minute_adjustment_factors(
    symbol: str, adjust: str
) -> dict[str, float]:
    if adjust == "none":
        return {}
    adjustment_code = "qfq" if adjust == "forward" else "hfq"
    market_code = market_symbol(symbol)
    adjusted_url = (
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
        + urlencode({"param": f"{market_code},day,,,640,{adjustment_code}"})
    )
    raw_url = (
        "https://ifzq.gtimg.cn/appstock/app/kline/kline?"
        + urlencode({"param": f"{market_code},day,,,640"})
    )
    def load_factors() -> dict[str, Any]:
        with ThreadPoolExecutor(max_workers=2) as executor:
            adjusted_future = executor.submit(
                read_public_json, adjusted_url, "https://gu.qq.com/", 4, 2
            )
            raw_future = executor.submit(
                read_public_json, raw_url, "https://gu.qq.com/", 4, 2
            )
            adjusted_payload = adjusted_future.result()
            raw_payload = raw_future.result()
        adjusted_rows = (
            (adjusted_payload.get("data") or {}).get(market_code) or {}
        ).get(f"{adjustment_code}day") or []
        raw_rows = ((raw_payload.get("data") or {}).get(market_code) or {}).get("day") or []
        raw_close_by_date = {
            str(row[0]): to_number(row[2])
            for row in raw_rows
            if len(row) >= 3 and to_number(row[2]) not in (None, 0)
        }
        factors = {}
        for row in adjusted_rows:
            if len(row) < 3:
                continue
            trade_date = str(row[0])
            adjusted_close = to_number(row[2])
            raw_close = raw_close_by_date.get(trade_date)
            if adjusted_close is not None and raw_close not in (None, 0):
                factors[trade_date] = adjusted_close / raw_close
        if not factors:
            raise HTTPException(
                status_code=502,
                detail=f"Tencent {adjustment_code} factors were unavailable for minute K-line fallback.",
            )
        return {"factors": factors}

    cached, _ = get_cached_tool_data(
        cache_key(
            "tencent_minute_adjustment_factors",
            {"symbol": symbol, "adjust": adjust},
        ),
        300,
        load_factors,
    )
    return cached["factors"]


def get_tencent_minute_kline(
    symbol: str,
    period: str,
    limit: int,
    start_date: str | None,
    end_date: str | None,
    adjust: str,
    page_token: str | None,
) -> dict[str, Any]:
    tencent_period = {
        "1m": "m1",
        "5m": "m5",
        "15m": "m15",
        "30m": "m30",
        "60m": "m60",
    }[period]
    market_code = market_symbol(symbol)
    query = urlencode({"param": f"{market_code},{tencent_period},,640"})
    source_future = TENCENT_KLINE_EXECUTOR.submit(
        get_cached_tool_data,
        cache_key("tencent_minute_kline_source", {"symbol": symbol, "period": period}),
        15,
        lambda: read_public_json(
            f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?{query}",
            "https://gu.qq.com/",
            timeout=4,
            attempts=2,
        ),
    )
    factors_future = TENCENT_KLINE_EXECUTOR.submit(
        get_tencent_minute_adjustment_factors, symbol, adjust
    )
    payload, _ = source_future.result()
    factors = factors_future.result()
    rows = ((payload.get("data") or {}).get(market_code) or {}).get(tencent_period) or []
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"Minute Kline data not found from Tencent: {symbol}"
        )
    items = []
    previous_close = None
    missing_factor_dates = set()
    for row in rows:
        if len(row) < 6:
            continue
        try:
            item_time = datetime.strptime(str(row[0]), "%Y%m%d%H%M").strftime(
                "%Y-%m-%d %H:%M"
            )
        except ValueError:
            continue
        trade_date = item_time[:10]
        factor = 1.0 if adjust == "none" else factors.get(trade_date)
        if factor is None:
            missing_factor_dates.add(trade_date)
            continue
        open_price = to_number(row[1])
        close = to_number(row[2])
        high = to_number(row[3])
        low = to_number(row[4])
        volume = to_number(row[5])
        adjusted_close = close * factor if close is not None else None
        change_pct = None
        if adjusted_close is not None and previous_close not in (None, 0):
            change_pct = round((adjusted_close - previous_close) / previous_close * 100, 4)
        items.append(
            {
                "date": item_time,
                "open": open_price * factor if open_price is not None else None,
                "close": adjusted_close,
                "high": high * factor if high is not None else None,
                "low": low * factor if low is not None else None,
                "volume": volume * 100 if volume is not None else None,
                "volume_unit": "share",
                "turnover": None,
                "turnover_unit": "CNY",
                "change_pct": change_pct,
            }
        )
        previous_close = adjusted_close
    if not items:
        raise HTTPException(
            status_code=502, detail=f"Unexpected Tencent minute Kline format: {symbol}"
        )
    source_first_day = str(items[0]["date"])[:10]
    requested_range_is_truncated = bool(
        start_date and len(rows) >= 640 and source_first_day > start_date
    )
    items, next_page_token, has_more = filter_and_page_kline_items(
        items, limit, start_date, end_date, page_token
    )
    if not items:
        raise HTTPException(
            status_code=404, detail=f"Kline data not found in requested range: {symbol}"
        )
    missing_fields = ["turnover", "turnover_rate", "amplitude"]
    if requested_range_is_truncated:
        missing_fields.append("history_before_tencent_640_bar_public_limit")
    if missing_factor_dates:
        missing_fields.append(
            "adjustment_factors_for:" + ",".join(sorted(missing_factor_dates))
        )
    adjustment = KLINE_ADJUSTMENTS[adjust]
    return {
        "symbol": symbol,
        "period": period,
        "adjustment": adjustment["label"],
        "adjustment_source_parameter": (
            "tencent_unadjusted_mkline"
            if adjust == "none"
            else f"tencent_mkline_plus_{'qfq' if adjust == 'forward' else 'hfq'}_daily_factor"
        ),
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "available_start": items[0]["date"],
        "available_end": items[-1]["date"],
        "coverage_status": (
            "partial_public_source_limit_640"
            if requested_range_is_truncated or missing_factor_dates
            else "available_public_range"
        ),
        "missing_fields": missing_fields,
        "has_more": has_more,
        "next_page_token": next_page_token,
        "count": len(items),
        "items": items,
        "source": "tencent",
        "latest_trade_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "Tencent public minute bars with explicit adjustment-factor provenance; no investment judgement.",
    }


def get_tencent_kline(
    symbol: str,
    period: str,
    limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
    adjust: str = "forward",
    page_token: str | None = None,
) -> dict[str, Any]:
    if period in {"1m", "5m", "15m", "30m", "60m"}:
        return get_tencent_minute_kline(
            symbol, period, limit, start_date, end_date, adjust, page_token
        )
    tencent_periods = {
        "daily": "day",
        "weekly": "week",
        "monthly": "month",
    }
    tencent_period = tencent_periods.get(period)
    if tencent_period is None:
        raise HTTPException(status_code=502, detail=f"Tencent Kline fallback is unavailable for {period}.")
    if adjust != "forward":
        raise HTTPException(
            status_code=502,
            detail="Tencent Kline fallback only provides the forward-adjusted contract.",
        )

    requested_source_limit = 5000 if start_date or end_date or page_token else limit + 1
    query = urlencode(
        {
            "param": f"{market_symbol(symbol)},{tencent_period},,,{requested_source_limit},qfq",
        }
    )
    payload = read_public_json(
        f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?{query}",
        "https://gu.qq.com/",
    )
    data = (payload.get("data") or {}).get(market_symbol(symbol)) or {}
    rows = data.get(f"qfq{tencent_period}") or data.get(tencent_period) or []
    if not rows:
        raise HTTPException(status_code=404, detail=f"Kline data not found from Tencent: {symbol}")

    items = []
    previous_close = None
    for row in rows:
        if len(row) < 6:
            continue
        close = to_number(row[2])
        volume = to_number(row[5])
        change_pct = None
        if close is not None and previous_close not in (None, 0):
            change_pct = round((close - previous_close) / previous_close * 100, 4)
        items.append(
            {
                "date": clean_value(row[0]),
                "open": to_number(row[1]),
                "close": close,
                "high": to_number(row[3]),
                "low": to_number(row[4]),
                "volume": None if volume is None else volume * 100,
                "volume_unit": "share",
                "turnover": None,
                "turnover_unit": "CNY",
                "change_pct": change_pct,
            }
        )
        previous_close = close
    if not items:
        raise HTTPException(status_code=502, detail=f"Unexpected Tencent Kline format: {symbol}")

    items, next_page_token, has_more = filter_and_page_kline_items(
        items, limit, start_date, end_date, page_token
    )
    if not items:
        raise HTTPException(status_code=404, detail=f"Kline data not found in requested range: {symbol}")

    return {
        "symbol": symbol,
        "period": period,
        "adjustment": "forward_adjusted",
        "adjustment_source_parameter": "tencent_qfq",
        "requested_start_date": start_date,
        "requested_end_date": end_date,
        "available_start": items[0]["date"],
        "available_end": items[-1]["date"],
        "coverage_status": "available_public_range",
        "missing_fields": ["turnover", "turnover_rate", "amplitude"],
        "has_more": has_more,
        "next_page_token": next_page_token,
        "count": len(items),
        "items": items,
        "source": "tencent",
        "latest_trade_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_fallback_kline(
    symbol: str,
    period: str,
    klt: int,
    limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
    adjust: str = "forward",
    page_token: str | None = None,
) -> dict[str, Any]:
    if period in {"daily", "weekly", "monthly"} and adjust != "forward":
        payload = get_eastmoney_kline(
            symbol, period, klt, limit, start_date, end_date, adjust, page_token
        )
        payload["source_errors"] = []
        return payload
    def eastmoney_loader() -> dict[str, Any]:
        try:
            payload = get_eastmoney_kline(
                symbol, period, klt, limit, start_date, end_date, adjust, page_token
            )
        except Exception as exc:
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            record_preferred_route_health("kline:eastmoney", False, str(detail))
            raise
        record_preferred_route_health("kline:eastmoney", True)
        return payload

    def tencent_loader() -> dict[str, Any]:
        return get_tencent_kline(
            symbol, period, limit, start_date, end_date, adjust, page_token
        )

    primary = (
        ("tencent", tencent_loader)
        if period == "1m"
        else ("eastmoney", eastmoney_loader)
    )
    secondary = (
        ("eastmoney", eastmoney_loader)
        if period == "1m"
        else ("tencent", tencent_loader)
    )
    payload, _, errors = prefer_primary_public_source(
        primary,
        secondary,
        7.0 if period in {"1m", "5m", "15m", "30m", "60m"} else 3.5,
        "kline:eastmoney",
    )
    payload["source_errors"] = errors
    return payload


def parse_intraday_minute(value: Any) -> datetime | None:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M").replace(
            tzinfo=MARKET_TIMEZONE
        )
    except ValueError:
        return None


def intraday_item_is_trading_minute(item: dict[str, Any]) -> bool:
    timestamp = parse_intraday_minute(item.get("time"))
    if timestamp is None or timestamp.weekday() >= 5:
        return False
    minute = timestamp.hour * 60 + timestamp.minute
    return (
        9 * 60 + 30 <= minute <= 11 * 60 + 30
        or 13 * 60 <= minute <= 15 * 60
    )


def filter_intraday_trading_items(
    items: list[dict[str, Any]], limit: int | None = None
) -> list[dict[str, Any]]:
    filtered = [item for item in items if intraday_item_is_trading_minute(item)]
    return filtered[-limit:] if limit is not None else filtered


def get_eastmoney_intraday(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    query = urlencode(
        {
            "fields1": "f1,f2,f3,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f17",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ndays": 1,
            "iscr": 0,
            "secid": eastmoney_secid(symbol),
        }
    )
    payload = read_public_json(
        f"https://push2.eastmoney.com/api/qt/stock/trends2/get?{query}",
        "https://quote.eastmoney.com/",
    )
    data = payload.get("data") or {}
    trends = data.get("trends") or []
    if not trends:
        raise HTTPException(status_code=404, detail=f"Intraday data not found: {symbol}")

    parsed_items = []
    for trend in trends:
        values = trend.split(",")
        if len(values) < 8:
            continue
        volume = to_number(values[5])
        parsed_items.append(
            {
                "time": values[0],
                "open": to_number(values[1]),
                "close": to_number(values[2]),
                "price": to_number(values[2]),
                "high": to_number(values[3]),
                "low": to_number(values[4]),
                "volume": None if volume is None else volume * 100,
                "volume_unit": "share",
                "turnover": to_number(values[6]),
                "turnover_unit": "CNY",
                "average_price": to_number(values[7]),
                "average_price_scope": "source_reported_cumulative_day_average",
            }
        )
    valid_items = filter_intraday_trading_items(parsed_items)
    items = valid_items[-limit:]
    if not items:
        raise HTTPException(
            status_code=404,
            detail=f"Intraday source returned no valid A-share trading-session minutes: {symbol}",
        )

    session_prices = [to_number(item.get("price")) for item in valid_items]
    session_highs = [to_number(item.get("high")) for item in valid_items]
    session_lows = [to_number(item.get("low")) for item in valid_items]
    opening_price = to_number(valid_items[0].get("open")) or to_number(
        valid_items[0].get("price")
    )
    return {
        "symbol": symbol,
        "name": clean_value(data.get("name")),
        "previous_close": clean_value(data.get("preClose")),
        "session_open": opening_price,
        "session_open_scope": (
            "official_open_from_09_30_exchange_minute"
            if str(valid_items[0]["time"]).endswith("09:30")
            else "first_available_trading_minute_open"
        ),
        "session_first_minute_time": valid_items[0]["time"],
        "session_high": max(value for value in session_highs if value is not None),
        "session_low": min(value for value in session_lows if value is not None),
        "session_last_price": next(
            (value for value in reversed(session_prices) if value is not None),
            None,
        ),
        "count": len(items),
        "items": items,
        "raw_count": len(parsed_items),
        "filtered_out_of_session_count": len(parsed_items) - len(valid_items),
        "session_filter": "09:30-11:30 and 13:00-15:00 Asia/Shanghai",
        "source": "eastmoney",
        "latest_market_time": items[-1]["time"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_tencent_intraday(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    code = market_symbol(symbol)
    payload = read_public_json(
        f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={code}",
        "https://stockapp.finance.qq.com/",
    )
    stock = (payload.get("data") or {}).get(code) or {}
    data = stock.get("data") or {}
    rows = data.get("data") or []
    trade_date = clean_value(data.get("date"))
    if trade_date and re.fullmatch(r"\d{8}", str(trade_date)):
        try:
            trade_date = datetime.strptime(str(trade_date), "%Y%m%d").date().isoformat()
        except ValueError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Unexpected Tencent intraday date for {symbol}: {trade_date}",
            ) from exc
    if not rows or not trade_date:
        raise HTTPException(status_code=404, detail=f"Intraday data not found: {symbol}")

    parsed = []
    raw_count = 0
    previous_volume = 0.0
    previous_turnover = 0.0
    for row in rows:
        values = row.split()
        if len(values) < 4:
            continue
        cumulative_volume = to_number(values[2])
        cumulative_turnover = to_number(values[3])
        if cumulative_volume is None or cumulative_turnover is None:
            continue
        raw_count += 1
        item = {
            "time": f"{trade_date} {values[0][:2]}:{values[0][2:]}",
            "open": None,
            "close": to_number(values[1]),
            "price": to_number(values[1]),
            "high": None,
            "low": None,
            "volume": max(0.0, cumulative_volume - previous_volume) * 100,
            "turnover": max(0.0, cumulative_turnover - previous_turnover),
            "volume_unit": "share",
            "turnover_unit": "CNY",
            "average_price": None,
            "average_price_scope": "unavailable_from_tencent_minute_source",
        }
        if not intraday_item_is_trading_minute(item):
            continue
        parsed.append(item)
        previous_volume = cumulative_volume
        previous_turnover = cumulative_turnover
    items = parsed[-limit:]
    if not items:
        raise HTTPException(status_code=404, detail=f"Intraday data not found: {symbol}")
    quote = (stock.get("qt") or {}).get(code) or []
    quote_open = to_number(quote[5]) if len(quote) > 5 else None
    opening_price = quote_open or to_number(parsed[0].get("price"))
    parsed_prices = [to_number(item.get("price")) for item in parsed]
    quote_high = to_number(quote[33]) if len(quote) > 33 else None
    quote_low = to_number(quote[34]) if len(quote) > 34 else None
    return {
        "symbol": symbol,
        "name": clean_value(quote[1]) if len(quote) > 1 else None,
        "previous_close": to_number(quote[4]) if len(quote) > 4 else None,
        "session_open": opening_price,
        "session_open_scope": (
            "official_open_from_tencent_quote"
            if quote_open is not None
            else "first_trading_minute_price_fallback"
        ),
        "session_first_minute_time": parsed[0]["time"],
        "session_high": quote_high
        if quote_high is not None
        else max(value for value in parsed_prices if value is not None),
        "session_low": quote_low
        if quote_low is not None
        else min(value for value in parsed_prices if value is not None),
        "session_last_price": next(
            (value for value in reversed(parsed_prices) if value is not None),
            None,
        ),
        "count": len(items),
        "items": items,
        "raw_count": raw_count,
        "filtered_out_of_session_count": raw_count - len(parsed),
        "session_filter": "09:30-11:30 and 13:00-15:00 Asia/Shanghai",
        "source": "tencent",
        "latest_market_time": items[-1]["time"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def intraday_item_is_unfinished(item_time: Any) -> bool:
    timestamp = parse_intraday_minute(item_time)
    if timestamp is None:
        return False
    now = datetime.now(MARKET_TIMEZONE)
    return (
        timestamp.date() == now.date()
        and timestamp.hour == now.hour
        and timestamp.minute == now.minute
        and market_status_at(now) == "open"
    )


def add_intraday_completion_flags(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for item in items:
        item["is_current_minute_unfinished"] = intraday_item_is_unfinished(item.get("time"))
    return items


def percentage_change(current: float | None, reference: float | None) -> float | None:
    if current is None or reference in (None, 0):
        return None
    return round((current - reference) / reference * 100, 4)


def intraday_mechanical_indicators(
    items: list[dict[str, Any]],
    opening_price: float | None = None,
    opening_price_scope: str | None = None,
    session_high: float | None = None,
    session_low: float | None = None,
) -> dict[str, Any]:
    items = filter_intraday_trading_items(items)
    if not items:
        return {"status": "unavailable_without_trading_session_minutes"}
    prices = [to_number(item.get("price") or item.get("close")) for item in items]
    valid_prices = [price for price in prices if price is not None]
    if not valid_prices:
        return {"status": "unavailable_without_minute_prices"}

    current = valid_prices[-1]

    def window_return(minutes: int) -> float | None:
        if len(valid_prices) < minutes + 1:
            return None
        return percentage_change(current, valid_prices[-(minutes + 1)])

    highs = [to_number(item.get("high")) or to_number(item.get("price")) for item in items]
    lows = [to_number(item.get("low")) or to_number(item.get("price")) for item in items]
    returned_window_high = max(value for value in highs if value is not None)
    returned_window_low = min(value for value in lows if value is not None)
    day_high = session_high if session_high is not None else returned_window_high
    day_low = session_low if session_low is not None else returned_window_low
    first_returned_price = to_number(items[0].get("price")) or to_number(
        items[0].get("close")
    )
    source_average = to_number(items[-1].get("average_price"))
    total_turnover = sum(to_number(item.get("turnover")) or 0 for item in items)
    total_volume = sum(to_number(item.get("volume")) or 0 for item in items)
    calculated_average = total_turnover / total_volume if total_volume else None
    average_price = source_average or calculated_average
    average_scope = (
        items[-1].get("average_price_scope")
        if source_average is not None
        else "returned_minutes_vwap_not_full_day"
    )
    recent_turnover = sum(to_number(item.get("turnover")) or 0 for item in items[-5:])
    prior_turnover = sum(to_number(item.get("turnover")) or 0 for item in items[-10:-5])

    return {
        "status": "available",
        "return_5m": window_return(5),
        "return_15m": window_return(15),
        "return_30m": window_return(30),
        "distance_from_high_pct": percentage_change(current, day_high),
        "distance_from_low_pct": percentage_change(current, day_low),
        "price_above_average_pct": percentage_change(current, average_price),
        "average_price": average_price,
        "average_price_scope": average_scope,
        "opening_price": opening_price,
        "opening_price_scope": opening_price_scope or "unavailable",
        "return_from_open_pct": percentage_change(current, opening_price),
        "first_returned_minute_time": items[0].get("time"),
        "first_returned_minute_price": first_returned_price,
        "return_from_first_returned_minute_pct": percentage_change(
            current, first_returned_price
        ),
        "turnover_last_5_reported_minutes": recent_turnover,
        "turnover_previous_5_reported_minutes": prior_turnover if len(items) >= 10 else None,
        "turnover_speed_5m_vs_previous_5m_pct": (
            percentage_change(recent_turnover, prior_turnover) if len(items) >= 10 else None
        ),
        "at_intraday_high": current >= max(valid_prices),
        "at_intraday_low": current <= min(valid_prices),
        "definitions": {
            "returns": "Current price compared with the price N reported trading minutes earlier.",
            "return_from_open": "Current price compared with the official session opening price, even when the returned minute window starts later than 09:30.",
            "return_from_first_returned_minute": "Current price compared with the first minute included in this response; this is not labeled as the opening return.",
            "turnover_speed": "Most recent five reported trading minutes compared with the preceding five; lunch-break minutes are not generated.",
            "average_price": "Source cumulative-day average when available; otherwise VWAP of the returned minute window only.",
        },
    }


def get_intraday_data(symbol: str, limit: int) -> dict[str, Any]:
    source_getters = (
        ("eastmoney", get_eastmoney_intraday),
        ("tencent", get_tencent_intraday),
    )

    def finalize_payload(
        payload: dict[str, Any], source: str, errors: list[str]
    ) -> dict[str, Any]:
        original_count = len(payload["items"])
        payload["items"] = filter_intraday_trading_items(payload["items"], limit)
        if not payload["items"]:
            raise HTTPException(
                status_code=404,
                detail=f"{source} returned no valid trading-session minutes.",
            )
        payload["items"] = add_intraday_completion_flags(payload["items"])
        payload["count"] = len(payload["items"])
        payload["filtered_out_of_session_count"] = (
            payload.get("filtered_out_of_session_count", 0)
            + original_count
            - len(payload["items"])
        )
        payload["latest_market_time"] = payload["items"][-1]["time"]
        payload["market_time"] = format_market_time(payload["latest_market_time"])
        payload["open"] = payload.get("session_open")
        payload["high"] = payload.get("session_high")
        payload["low"] = payload.get("session_low")
        payload["mechanical_indicators"] = intraday_mechanical_indicators(
            payload["items"],
            opening_price=to_number(payload.get("session_open")),
            opening_price_scope=payload.get("session_open_scope"),
            session_high=to_number(payload.get("session_high")),
            session_low=to_number(payload.get("session_low")),
        )
        payload["security_type"] = security_metadata(symbol)["security_type"]
        payload["exchange"] = security_metadata(symbol)["exchange"]
        payload["source_errors"] = errors
        payload["note"] = "Minute facts and mechanical indicators only; no trading or investment judgement is generated."
        return payload

    executor = ThreadPoolExecutor(max_workers=len(source_getters))
    futures = {
        executor.submit(getter, symbol, limit): source
        for source, getter in source_getters
    }
    pending = set(futures)
    errors: list[str] = []
    status_codes: list[int] = []
    deadline = perf_counter() + 9
    try:
        while pending:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                break
            for future in completed:
                source = futures[future]
                try:
                    return finalize_payload(future.result(), source, errors)
                except HTTPException as exc:
                    errors.append(f"{source}: {exc.detail}")
                    status_codes.append(exc.status_code)
                except Exception as exc:  # pragma: no cover - defensive source boundary
                    errors.append(f"{source}: {exc}")
                    status_codes.append(502)
        for future in pending:
            future.cancel()
            errors.append(f"{futures[future]}: request exceeded the 9 second budget")
            status_codes.append(502)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    status_code = 404 if status_codes and all(code == 404 for code in status_codes) else 502
    raise HTTPException(status_code=status_code, detail="; ".join(errors))


def get_auction_data(symbol: str) -> dict[str, Any]:
    payload = get_quote_data(symbol)
    quote = payload["quote"]
    opening_price = to_number(quote.get("open"))
    previous_close = to_number(quote.get("previous_close"))
    opening_change = (
        opening_price - previous_close
        if opening_price is not None and previous_close is not None
        else None
    )
    return {
        "symbol": quote.get("symbol"),
        "name": quote.get("name"),
        "security_type": quote.get("security_type"),
        "exchange": quote.get("exchange"),
        "trade_date": quote.get("trade_date"),
        "auction_period": "09:15-09:25 Asia/Shanghai",
        "auction_price": opening_price,
        "auction_price_status": "opening_price_from_public_quote",
        "auction_change": opening_change,
        "auction_change_pct": percentage_change(opening_price, previous_close),
        "auction_turnover": None,
        "final_auction_volume": None,
        "unmatched_bid_volume": None,
        "unmatched_ask_volume": None,
        "cancellation_change": None,
        "open": opening_price,
        "previous_close": previous_close,
        "auction_market_time": None,
        "source_updated_at": quote.get("source_updated_at"),
        "source": payload["source"],
        "queried_at": payload["queried_at"],
        "data_status": "partial_data" if opening_price is not None else "no_data",
        "unavailable_fields": [
            "auction_price_path_09_15_to_09_25",
            "auction_turnover",
            "final_auction_volume",
            "unmatched_bid_volume",
            "unmatched_ask_volume",
            "cancellation_change",
            "auction_market_time",
        ],
        "note": (
            "The public quote provides the opening price only. It does not provide a verifiable full call-auction "
            "process, unmatched orders, or cancellation changes."
        ),
    }


def filter_a_share_securities_data(
    security_type: str,
    exclude_st: bool,
    change_pct_min: float | None,
    change_pct_max: float | None,
    turnover_min: float | None,
    turnover_rate_min: float | None,
    above_average_price: bool | None,
    market_cap_max: float | None,
    limit: int,
) -> dict[str, Any]:
    normalized_type = security_type.strip().lower()
    if normalized_type not in {"stock", "a_share"}:
        raise HTTPException(
            status_code=400,
            detail="This public all-market filter currently supports ordinary A-share stocks only.",
        )
    if (
        change_pct_min is not None
        and change_pct_max is not None
        and change_pct_min > change_pct_max
    ):
        raise HTTPException(status_code=400, detail="change_pct_min cannot be greater than change_pct_max.")

    rows = get_eastmoney_market_quotes()
    matched = []
    for row in rows:
        name = str(row.get("name") or "")
        change_pct = to_number(row.get("change_pct"))
        turnover = to_number(row.get("turnover"))
        turnover_rate = to_number(row.get("turnover_rate"))
        market_cap = to_number(row.get("total_market_value"))
        volume = to_number(row.get("volume"))
        price = to_number(row.get("price"))
        average_price = turnover / (volume * 100) if turnover and volume else None
        if "退" in name or price is None:
            continue
        if exclude_st and is_st_security(name):
            continue
        if change_pct_min is not None and (change_pct is None or change_pct < change_pct_min):
            continue
        if change_pct_max is not None and (change_pct is None or change_pct > change_pct_max):
            continue
        if turnover_min is not None and (turnover is None or turnover < turnover_min):
            continue
        if turnover_rate_min is not None and (turnover_rate is None or turnover_rate < turnover_rate_min):
            continue
        if market_cap_max is not None and (market_cap is None or market_cap > market_cap_max):
            continue
        if above_average_price is True and (average_price is None or price <= average_price):
            continue
        matched.append(
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "security_type": "a_share",
                "exchange": exchange_for_symbol(str(row["symbol"])),
                "price": price,
                "change_pct": change_pct,
                "turnover": turnover,
                "turnover_unit": "CNY",
                "turnover_rate": turnover_rate,
                "total_market_value": market_cap,
                "total_market_value_unit": "CNY",
                "average_price": average_price,
                "price_above_average_pct": percentage_change(price, average_price),
                "market_time": row.get("market_time"),
            }
        )

    market_times = sorted(item["market_time"] for item in matched if item.get("market_time"))
    conditions = {
        "security_type": "stock",
        "exclude_st": exclude_st,
        "change_pct_min": change_pct_min,
        "change_pct_max": change_pct_max,
        "turnover_min": turnover_min,
        "turnover_rate_min": turnover_rate_min,
        "above_average_price": above_average_price,
        "market_cap_max": market_cap_max,
    }
    return {
        "matched_count": len(matched),
        "returned_count": min(len(matched), limit),
        "conditions": conditions,
        "results": matched[:limit],
        "sort_order": "public_source_change_pct_desc",
        "source": ["eastmoney_all_a_share_snapshot"],
        "market_time": market_times[-1] if market_times else None,
        "queried_at": now_iso(),
        "scope": "Mechanical conditions only; ordinary A shares only, excluding ETFs, funds, B shares, and delisting-arrangement securities.",
        "note": "No hidden weights, scores, recommendations, or trading conclusions are applied.",
    }


def candidate_history_gate(
    context: dict[str, Any] | None,
    required_windows: list[int],
    minimum_return_pct: float,
) -> tuple[dict[str, Any], list[str]]:
    if not context:
        return {}, ["historical_context_unavailable"]
    windows = context.get("windows") or {}
    selected: dict[str, Any] = {}
    rejections = []
    for window in required_windows:
        metrics = windows.get(str(window)) or {}
        selected[str(window)] = {
            key: metrics.get(key)
            for key in (
                "window_complete",
                "start_date",
                "end_date",
                "return_pct",
                "annualized_volatility_pct",
                "maximum_drawdown_pct",
                "distance_from_high_pct",
                "distance_from_low_pct",
            )
        }
        if not metrics.get("window_complete"):
            rejections.append(f"history_window_incomplete:{window}")
            continue
        return_pct = to_number(metrics.get("return_pct"))
        if return_pct is None:
            rejections.append(f"history_return_unavailable:{window}")
        elif return_pct < minimum_return_pct:
            rejections.append(f"history_return_below_minimum:{window}")
    return {
        "latest_trade_date": context.get("latest_trade_date"),
        "source_sessions": context.get("source_sessions"),
        "windows": selected,
        "latest_volume_vs_20_session_average_ratio": (
            ((windows.get("20") or {}).get("volume") or {}).get(
                "latest_vs_prior_average_ratio"
            )
        ),
        "source": context.get("source"),
        "served_from_stale_cache": context.get("served_from_stale_cache", False),
        "stale_cache_age_seconds": context.get("stale_cache_age_seconds"),
    }, rejections


def screen_a_share_research_candidates_data(
    change_pct_min: float,
    change_pct_max: float,
    turnover_min: float,
    turnover_rate_min: float,
    market_cap_max: float,
    candidate_limit: int,
    history_pool_limit: int,
    minimum_market_rise_to_fall_ratio: float,
    required_positive_history_windows: list[int],
    minimum_history_return_pct: float,
    detail_level: str,
) -> dict[str, Any]:
    started_at = perf_counter()
    base_loaders = {
        "market_overview": lambda: get_market_overview_data(10),
        "all_market_filter": lambda: filter_a_share_securities_data(
            security_type="stock",
            exclude_st=True,
            change_pct_min=change_pct_min,
            change_pct_max=change_pct_max,
            turnover_min=turnover_min,
            turnover_rate_min=turnover_rate_min,
            above_average_price=True,
            market_cap_max=market_cap_max,
            limit=200,
        ),
    }
    base_results, base_status, base_errors = collect_components(
        base_loaders, 12, COMPOSITE_TOOL_EXECUTOR
    )
    overview = base_results.get("market_overview") or {}
    filtered = base_results.get("all_market_filter") or {}
    activity = overview.get("market_activity_facts") or {}
    rise_to_fall_ratio = to_number(activity.get("rise_to_fall_ratio"))
    market_gate = {
        "minimum_rise_to_fall_ratio": minimum_market_rise_to_fall_ratio,
        "observed_rise_to_fall_ratio": rise_to_fall_ratio,
        "rise_count": activity.get("rise_count"),
        "fall_count": activity.get("fall_count"),
        "limit_up_count": activity.get("limit_up_count"),
        "limit_down_count": activity.get("limit_down_count"),
        "passed": (
            rise_to_fall_ratio is not None
            and rise_to_fall_ratio >= minimum_market_rise_to_fall_ratio
        ),
    }
    preselected = sorted(
        filtered.get("results") or [],
        key=lambda item: (
            -(to_number(item.get("turnover")) or 0),
            str(item.get("symbol") or ""),
        ),
    )[:history_pool_limit]
    screen_conditions = {
        "exclude_st": True,
        "change_pct_min": change_pct_min,
        "change_pct_max": change_pct_max,
        "turnover_min_cny": turnover_min,
        "turnover_rate_min_pct": turnover_rate_min,
        "above_average_price": True,
        "market_cap_max_cny": market_cap_max,
        "preselection_sort": "turnover_desc_then_symbol",
        "minimum_market_rise_to_fall_ratio": minimum_market_rise_to_fall_ratio,
        "required_positive_history_windows": required_positive_history_windows,
        "minimum_history_return_pct": minimum_history_return_pct,
    }
    if not overview or not filtered:
        return {
            "selection_status": "no_candidate_due_to_incomplete_base_evidence",
            "no_candidate": True,
            "research_candidates": [],
            "preselected_candidates": preselected if detail_level == "raw" else [],
            "preselected_count": len(preselected),
            "accepted_count": 0,
            "market_gate": market_gate,
            "screen_conditions": screen_conditions,
            "component_status": base_status,
            "source_errors": base_errors,
            "missing_fields": [
                name for name in base_loaders if name not in base_results
            ],
            "data_status": "partial_data",
            "queried_at": now_iso(),
            "latency_ms": int((perf_counter() - started_at) * 1000),
            "note": "No ticker is forced when the market snapshot or candidate universe is unavailable.",
        }
    if not market_gate["passed"]:
        return {
            "selection_status": "no_candidate_due_to_market_breadth_gate",
            "no_candidate": True,
            "research_candidates": [],
            "preselected_candidates": preselected if detail_level == "raw" else [],
            "preselected_count": len(preselected),
            "accepted_count": 0,
            "market_gate": market_gate,
            "screen_conditions": screen_conditions,
            "component_status": base_status,
            "source": sorted(
                {
                    *normalize_sources(overview.get("source")),
                    *normalize_sources(filtered.get("source")),
                }
            ),
            "source_errors": base_errors,
            "missing_fields": [],
            "data_status": "full_data" if not base_errors else "partial_data",
            "market_time": overview.get("market_time") or filtered.get("market_time"),
            "queried_at": now_iso(),
            "latency_ms": int((perf_counter() - started_at) * 1000),
            "note": "The transparent caller-configurable market-breadth gate failed, so no ticker is promoted. This is a mechanical research screen, not a buy/sell decision.",
        }

    history_loaders = {
        f"history:{item['symbol']}": lambda item=item: get_cached_historical_context_data(
            str(item["symbol"])
        )
        for item in preselected
    }
    histories, history_status, history_errors = (
        collect_components(history_loaders, 15, COMPOSITE_TOOL_EXECUTOR)
        if history_loaders
        else ({}, {}, [])
    )
    accepted = []
    rejected = []
    for candidate in preselected:
        symbol = str(candidate["symbol"])
        context = histories.get(f"history:{symbol}")
        persistence, rejection_reasons = candidate_history_gate(
            context,
            required_positive_history_windows,
            minimum_history_return_pct,
        )
        item = {
            **candidate,
            "history_persistence": persistence,
            "evidence_gate_passed": not rejection_reasons,
            "rejection_reasons": rejection_reasons,
            "research_status": (
                "advance_to_deeper_research"
                if not rejection_reasons
                else "screen_rejected_or_incomplete"
            ),
        }
        (accepted if not rejection_reasons else rejected).append(item)
    accepted = accepted[:candidate_limit]
    unavailable_history = any(
        "historical_context_unavailable" in item["rejection_reasons"]
        for item in rejected
    )
    selection_status = (
        "research_candidates_available"
        if accepted
        else (
            "no_candidate_due_to_incomplete_history_evidence"
            if unavailable_history
            else "no_candidate_due_to_history_gate"
        )
    )
    all_errors = [*base_errors, *history_errors]
    return {
        "selection_status": selection_status,
        "no_candidate": not accepted,
        "research_candidates": accepted,
        "rejected_candidates": rejected if detail_level == "raw" else rejected[:5],
        "preselected_count": len(preselected),
        "accepted_count": len(accepted),
        "market_gate": market_gate,
        "screen_conditions": screen_conditions,
        "component_status": {**base_status, **history_status},
        "source": sorted(
            {
                *normalize_sources(overview.get("source")),
                *normalize_sources(filtered.get("source")),
                *[
                    source
                    for context in histories.values()
                    for source in normalize_sources(context.get("source"))
                ],
            }
        ),
        "source_errors": all_errors,
        "missing_fields": [
            key
            for key, status in history_status.items()
            if status.get("status") in {"unavailable", "unavailable_within_response_budget"}
        ],
        "data_status": "full_data" if not all_errors else "partial_data",
        "market_time": overview.get("market_time") or filtered.get("market_time"),
        "queried_at": now_iso(),
        "latency_ms": int((perf_counter() - started_at) * 1000),
        "note": "Candidates passed transparent market, liquidity, and multi-window history gates for deeper research only. They are not recommendations or instructions to trade.",
    }


def get_eastmoney_fund_flow(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    query = urlencode(
        {
            "lmt": limit,
            "klt": 101,
            "secid": eastmoney_secid(symbol),
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
        }
    )
    payload = read_public_json(
        f"https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?{query}",
        "https://quote.eastmoney.com/",
    )
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        raise HTTPException(status_code=404, detail=f"Fund-flow data not found: {symbol}")

    items = []
    for kline in klines[-limit:]:
        values = kline.split(",")
        if len(values) < 13:
            continue
        items.append(
            {
                "date": values[0],
                "main_net_inflow": to_number(values[1]),
                "small_net_inflow": to_number(values[2]),
                "medium_net_inflow": to_number(values[3]),
                "large_net_inflow": to_number(values[4]),
                "super_large_net_inflow": to_number(values[5]),
                "main_net_inflow_pct": to_number(values[6]),
                "close": to_number(values[11]),
                "change_pct": to_number(values[12]),
            }
        )

    return {
        "symbol": symbol,
        "name": clean_value(data.get("name")),
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "latest_market_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "Fund-flow figures are public-market estimates, for information only.",
    }


def get_sina_fund_flow(symbol: str, _: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    code = market_symbol(symbol)
    payload_future = SINA_AUX_EXECUTOR.submit(
        read_sina_object,
        (
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            f"MoneyFlow.ssi_ssfx_flzjtj?daima={code}"
        ),
        "https://finance.sina.com.cn/",
    )
    quote_future = SINA_AUX_EXECUTOR.submit(get_sina_quote, symbol)
    try:
        payload = payload_future.result()
        quote = quote_future.result()
    finally:
        payload_future.cancel()
        quote_future.cancel()
    trade_date = derive_quote_timestamps(quote.get("source_updated_at"))["trade_date"]
    if not payload or not trade_date:
        raise HTTPException(status_code=404, detail=f"Fund-flow data not found: {symbol}")
    large_in = to_number(payload.get("r0_in"))
    large_out = to_number(payload.get("r0_out"))
    main_net = (
        large_in - large_out if large_in is not None and large_out is not None else None
    )
    return {
        "symbol": symbol,
        "name": clean_value(payload.get("name")),
        "count": 1,
        "items": [
            {
                "date": trade_date,
                "main_net_inflow": main_net,
                "total_net_inflow": to_number(payload.get("netamount")),
                "close": to_number(payload.get("trade")),
                "change_pct": (
                    to_number(payload.get("changeratio")) * 100
                    if to_number(payload.get("changeratio")) is not None
                    else None
                ),
                "currency_unit": "CNY",
            }
        ],
        "source": "sina",
        "data_status": "partial_data",
        "latest_market_date": trade_date,
        "queried_at": now_iso(),
        "note": "Sina Level-1 fund-flow estimate; fallback provides current-day summary only.",
    }


def get_fund_flow_data(symbol: str, limit: int) -> dict[str, Any]:
    def eastmoney_loader() -> dict[str, Any]:
        try:
            payload = get_eastmoney_fund_flow(symbol, limit)
        except Exception as exc:
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            record_preferred_route_health("fund_flow:eastmoney", False, str(detail))
            raise
        record_preferred_route_health("fund_flow:eastmoney", True)
        return payload

    payload, _, errors = prefer_primary_public_source(
        ("eastmoney", eastmoney_loader),
        ("sina", lambda: get_sina_fund_flow(symbol, limit)),
        5.5,
        "fund_flow:eastmoney",
    )
    payload["source_errors"] = errors
    return payload


def get_financial_data(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    if security["security_type"] in {"etf", "lof"}:
        raise HTTPException(
            status_code=400,
            detail="Financial statements are not applicable to exchange-listed funds.",
        )
    secucode = f"{symbol}.{security['eastmoney_suffix']}"
    query = urlencode(
        {
            "reportName": "RPT_F10_FINANCE_MAINFINADATA",
            "columns": "ALL",
            "filter": f'(SECUCODE="{secucode}")',
            "pageNumber": 1,
            "pageSize": limit,
        }
    )
    payload = read_public_json(
        f"https://datacenter.eastmoney.com/securities/api/data/v1/get?{query}",
        "https://data.eastmoney.com/",
    )
    result = payload.get("result") or {}
    rows = result.get("data") or []
    if not rows:
        raise HTTPException(status_code=404, detail=f"Financial data not found: {symbol}")

    return {
        "symbol": symbol,
        "security_type": security["security_type"],
        "exchange": security["exchange"],
        "name": clean_value(rows[0].get("SECURITY_NAME_ABBR")),
        "count": len(rows),
        "items": [row_to_dict(row, FINANCIAL_RESPONSE_FIELDS) for row in rows],
        "source": "eastmoney",
        "latest_report_period": clean_value(rows[0].get("REPORT_DATE")),
        "queried_at": now_iso(),
        "note": "Financial figures follow the latest available public report, not real-time data.",
    }


def strip_html(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return unescape(re.sub(r"<[^>]+>", "", str(value))).replace("\r", " ").replace("\n", " ").strip()


ESTABLISHED_NEWS_SOURCE_MARKERS = (
    "新华社",
    "央视新闻",
    "中国证券报",
    "上海证券报",
    "证券时报",
    "财联社",
    "第一财经",
    "每日经济新闻",
    "澎湃新闻",
    "界面新闻",
    "中国基金报",
    "经济观察报",
    "21世纪经济报道",
)

OFFICIAL_NEWS_SOURCE_MARKERS = (
    "国务院",
    "中国证监会",
    "国家统计局",
    "上海证券交易所",
    "深圳证券交易所",
    "北京证券交易所",
)

EXCLUDED_CURRENT_NEWS_SOURCE_MARKERS = (
    "wikipedia",
    "维基百科",
    "百度百科",
    "互动百科",
    "arxiv",
)

ROUTINE_MARKET_TABLE_TITLE_MARKERS = (
    "股票行情快报",
    "资金流出榜",
    "资金流入榜",
    "主力动向",
    "特大单净流",
    "融资客青睐",
    "大宗交易超",
)


def get_eastmoney_news_items(keyword: str, limit: int) -> list[dict[str, Any]]:
    callback = "marketNewsCallback"
    parameters = {
        "uid": "",
        "keyword": keyword,
        "type": ["cmsArticleWebOld"],
        "client": "web",
        "clientType": "web",
        "clientVersion": "curr",
        "param": {
            "cmsArticleWebOld": {
                "searchScope": "default",
                "sort": "default",
                "pageIndex": 1,
                "pageSize": min(max(limit, 10), 30),
                "preTag": "<em>",
                "postTag": "</em>",
            }
        },
    }
    query = urlencode(
        {
            "cb": callback,
            "param": json.dumps(parameters, ensure_ascii=False, separators=(",", ":")),
            "_": "1",
        }
    )
    payload = read_public_jsonp(
        f"https://search-api-web.eastmoney.com/search/jsonp?{query}",
        f"https://so.eastmoney.com/news/s?{urlencode({'keyword': keyword})}",
    )
    articles = (payload.get("result") or {}).get("cmsArticleWebOld") or []
    return [
        {
            "published_at": format_market_time(article.get("date")),
            "title": strip_html(article.get("title")),
            "summary": strip_html(article.get("content")),
            "source": clean_value(article.get("mediaName")),
            "publisher_homepage": None,
            "url": clean_value(article.get("url")),
            "link_type": "eastmoney_reprint_or_hosted_copy",
            "retrieval_provider": "eastmoney_news_search",
            "matched_query": keyword,
            "event_date": None,
            "event_date_status": "not_verified_from_search_snippet",
        }
        for article in articles[:limit]
        if article.get("title") and article.get("url")
    ]


def get_google_news_items(keyword: str, limit: int) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "q": keyword,
            "hl": "zh-CN",
            "gl": "CN",
            "ceid": "CN:zh-Hans",
        }
    )
    url = f"https://news.google.com/rss/search?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
    )
    started_at = perf_counter()
    try:
        # The URL host is fixed and is not derived from user input.
        with urlopen(request, timeout=5) as response:  # nosec B310
            payload = response.read()
        unsafe_xml_declaration = (
            b"<!DOCTYPE" in payload.upper() or b"<!ENTITY" in payload.upper()
        )
        if len(payload) > 2_000_000 or unsafe_xml_declaration:
            raise ValueError("Google News RSS XML exceeded safety limits.")
        # DTD/entity declarations are rejected above before parsing the fixed-host RSS.
        root = ElementTree.fromstring(payload)  # nosec B314
        record_source_health("google_news", True, int((perf_counter() - started_at) * 1000))
    except (OSError, URLError, ValueError, ElementTree.ParseError) as exc:
        record_source_health(
            "google_news", False, int((perf_counter() - started_at) * 1000), str(exc)
        )
        raise HTTPException(status_code=502, detail=f"Google News RSS unavailable: {exc}") from exc

    items = []
    for item in root.findall(".//item")[:limit]:
        source_element = item.find("source")
        publisher = clean_value(source_element.text if source_element is not None else None)
        title = strip_html(item.findtext("title"))
        if title and publisher and title.endswith(f" - {publisher}"):
            title = title[: -(len(str(publisher)) + 3)].strip()
        published_at = clean_value(item.findtext("pubDate"))
        try:
            published_at = (
                parsedate_to_datetime(str(published_at))
                .astimezone(MARKET_TIMEZONE)
                .isoformat()
            )
        except (TypeError, ValueError, OverflowError):
            pass
        url_value = clean_value(item.findtext("link"))
        if not title or not url_value:
            continue
        items.append(
            {
                "published_at": published_at,
                "title": title,
                "summary": None,
                "source": publisher,
                "publisher_homepage": clean_value(
                    source_element.get("url") if source_element is not None else None
                ),
                "url": url_value,
                "link_type": "google_news_redirect_to_publisher",
                "retrieval_provider": "google_news_rss",
                "matched_query": keyword,
                "event_date": None,
                "event_date_status": "not_verified_from_rss_title",
            }
        )
    return items


def normalized_industry_news_term(value: Any) -> str | None:
    text = str(value or "").strip()
    text = re.sub(r"[ⅠⅡⅢⅣIV]+$", "", text).strip()
    return text or None


def news_source_tier(item: dict[str, Any]) -> str:
    source_text = " ".join(
        str(value or "")
        for value in (item.get("source"), item.get("publisher_homepage"))
    ).lower()
    if any(marker.lower() in source_text for marker in OFFICIAL_NEWS_SOURCE_MARKERS):
        return "official_or_regulatory"
    if any(marker.lower() in source_text for marker in ESTABLISHED_NEWS_SOURCE_MARKERS):
        return "established_financial_media"
    return "general_news_source"


def news_relevance(
    item: dict[str, Any],
    symbol: str,
    company_name: str | None,
    industry: str | None,
    include_industry_context: bool,
) -> tuple[str | None, int, list[str]]:
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    title_lower = title.lower()
    combined_lower = f"{title} {summary}".lower()
    source_text = f"{item.get('source') or ''} {item.get('publisher_homepage') or ''}".lower()
    if any(marker.lower() in source_text for marker in EXCLUDED_CURRENT_NEWS_SOURCE_MARKERS):
        return None, 0, ["excluded_reference_or_academic_source"]
    if any(marker in title for marker in ROUTINE_MARKET_TABLE_TITLE_MARKERS):
        return None, 0, ["excluded_routine_market_snapshot_or_table"]

    reasons = []
    score = 0
    name_lower = str(company_name or "").lower()
    if name_lower and re.search(
        rf"(?:\d+(?:\.\d+)?\s*个|相当于|堪比|市值.{{0,8}}(?:超|超过|追平)).{{0,8}}{re.escape(name_lower)}",
        title_lower,
    ):
        return None, 0, ["excluded_company_used_only_as_comparison"]
    name_position = title_lower.find(name_lower) if name_lower else -1
    late_roundup_mention = (
        "etf" in title_lower and name_position > 20
    ) or (
        len(title_lower) > 50 and name_position > 30
    )
    if late_roundup_mention:
        return None, 0, ["excluded_company_mentioned_late_in_roundup_title"]
    if name_lower and name_lower in title_lower:
        score += 10
        reasons.append("company_name_in_title")
    elif name_lower and name_lower in combined_lower:
        score += 6
        reasons.append("company_name_in_summary")
    if symbol in title:
        score += 8
        reasons.append("stock_code_in_title")
    elif symbol in summary:
        score += 2
        reasons.append("stock_code_in_summary")

    if (name_lower and name_lower in combined_lower) or symbol in title:
        return "company", score, reasons

    industry_lower = str(industry or "").lower()
    if include_industry_context and industry_lower and industry_lower in combined_lower:
        reasons.append("industry_term_present")
        return "industry_context", max(score, 3), reasons
    return None, score, reasons


def normalized_news_title(value: Any) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(value or "").lower())


def news_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=MARKET_TIMEZONE)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def get_news_data(
    symbol: str,
    limit: int,
    days: int = 30,
    include_industry_context: bool = False,
) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    source_errors = []
    try:
        reference = get_resilient_security_reference_data(symbol)
    except HTTPException as exc:
        source_errors.append(f"security_reference: {exc.detail}")
        reference = {}
    company_name = clean_value(reference.get("name"))
    industry = normalized_industry_news_term(reference.get("industry"))
    if not company_name:
        try:
            quote, quote_source, quote_errors = get_fastest_public_quote(symbol)
            company_name = clean_value(quote.get("name"))
            source_errors.extend(quote_errors)
            reference["source"] = quote_source
        except HTTPException as exc:
            source_errors.append(f"company_name_fallback: {exc.detail}")

    jobs: list[tuple[str, Any]] = [(symbol, get_eastmoney_news_items)]
    if company_name:
        jobs.extend(
            [
                (str(company_name), get_eastmoney_news_items),
                (f'"{company_name}" 股票', get_google_news_items),
            ]
        )
    if include_industry_context and industry:
        jobs.append((f'"{industry}" 行业', get_google_news_items))

    candidates: list[dict[str, Any]] = []
    provider_status: dict[str, dict[str, Any]] = {}
    per_query_limit = min(max(limit * 3, 12), 30)
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {
            executor.submit(loader, keyword, per_query_limit): (keyword, loader.__name__)
            for keyword, loader in jobs
        }
        for future in as_completed(futures):
            keyword, loader_name = futures[future]
            status_key = f"{loader_name}:{keyword}"
            try:
                rows = future.result()
                candidates.extend(rows)
                provider_status[status_key] = {"status": "available", "count": len(rows)}
            except HTTPException as exc:
                provider_status[status_key] = {"status": "unavailable", "count": 0}
                source_errors.append(f"{status_key}: {exc.detail}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    relevant = []
    excluded_count = 0
    for item in candidates:
        published = news_datetime(item.get("published_at"))
        if published is not None and published < cutoff:
            excluded_count += 1
            continue
        scope, score, reasons = news_relevance(
            item, symbol, str(company_name or "") or None, industry, include_industry_context
        )
        if scope is None:
            excluded_count += 1
            continue
        item["relevance_scope"] = scope
        item["relevance_score"] = score
        item["relevance_reasons"] = reasons
        item["source_tier"] = news_source_tier(item)
        relevant.append(item)

    tier_rank = {
        "official_or_regulatory": 3,
        "established_financial_media": 2,
        "general_news_source": 1,
    }
    relevant.sort(
        key=lambda item: (
            item.get("relevance_score", 0),
            tier_rank.get(str(item.get("source_tier")), 0),
            (news_datetime(item.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc)).timestamp(),
        ),
        reverse=True,
    )

    deduplicated = []
    title_keys: list[str] = []
    duplicate_count = 0
    for item in relevant:
        title_key = normalized_news_title(item.get("title"))
        if not title_key:
            excluded_count += 1
            continue
        if any(SequenceMatcher(None, title_key, existing).ratio() >= 0.86 for existing in title_keys):
            duplicate_count += 1
            continue
        title_keys.append(title_key)
        deduplicated.append(item)

    if include_industry_context:
        company_items = [
            item for item in deduplicated if item["relevance_scope"] == "company"
        ]
        industry_items = [
            item for item in deduplicated if item["relevance_scope"] == "industry_context"
        ]
        industry_slots = min(len(industry_items), max(1, min(2, limit // 4)))
        selected_items = [
            *company_items[: max(0, limit - industry_slots)],
            *industry_items[:industry_slots],
        ]
    else:
        selected_items = deduplicated[:limit]

    providers = sorted(
        {
            str(item.get("retrieval_provider"))
            for item in selected_items
            if item.get("retrieval_provider")
        }
    )
    return {
        "symbol": symbol,
        "name": company_name,
        "industry": industry,
        "security_type": security["security_type"],
        "exchange": security["exchange"],
        "period_days": days,
        "include_industry_context": include_industry_context,
        "count": len(selected_items),
        "items": selected_items,
        "candidate_count": len(candidates),
        "excluded_count": excluded_count,
        "duplicate_count": duplicate_count,
        "provider_status": provider_status,
        "source": providers,
        "source_errors": source_errors,
        "queried_at": now_iso(),
        "selection_policy": (
            "Company results require the company name in title/summary or the stock code in the title. "
            "Industry-only results are returned separately only when include_industry_context is true. "
            "Near-duplicate titles and encyclopedia/academic-reference sources are excluded mechanically."
        ),
        "time_scope": (
            "published_at is the article publication time. event_date remains null unless it can be "
            "verified separately; publication time must not be treated as event time."
        ),
        "note": "Evidence retrieval only. Source tier and relevance fields are mechanical metadata, not truth, importance, sentiment, or trading judgements.",
    }


ANNOUNCEMENT_EVENT_KEYWORDS = {
    "financial_results": ("年报", "半年报", "季报", "业绩预告", "业绩快报"),
    "dividend": ("分红", "派息", "权益分派", "利润分配"),
    "buyback": ("回购",),
    "shareholder_change": ("增持", "减持", "持股变动", "股东变更"),
    "unlock": ("解禁", "限售股上市流通"),
    "suspension_resume": ("停牌", "复牌"),
    "risk_warning": ("风险警示", "退市风险", "可能被终止上市"),
    "regulatory": ("问询函", "监管函", "处罚", "立案"),
    "major_transaction": ("重大资产重组", "收购", "出售资产", "重大合同"),
    "financing": ("定向增发", "非公开发行", "可转换公司债", "配股"),
    "governance": ("董事会", "监事会", "股东会", "高管变动"),
}


def announcement_event_tags(title: Any) -> list[str]:
    text = str(title or "")
    return [
        event_type
        for event_type, keywords in ANNOUNCEMENT_EVENT_KEYWORDS.items()
        if any(keyword in text for keyword in keywords)
    ] or ["other"]


def get_sse_announcements(
    symbol: str, start_date: str, end_date: str, limit: int
) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "isPagination": "true",
            "pageHelp.pageSize": limit,
            "pageHelp.pageNo": 1,
            "pageHelp.beginPage": 1,
            "pageHelp.cacheSize": 1,
            "pageHelp.endPage": 1,
            "START_DATE": start_date,
            "END_DATE": end_date,
            "SECURITY_CODE": symbol,
            "TITLE": "",
            "BULLETIN_TYPE": "",
            "stockType": "",
        }
    )
    payload = read_public_json(
        f"https://query.sse.com.cn/security/stock/queryCompanyBulletinNew.do?{query}",
        "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
        timeout=5,
        attempts=1,
    )
    groups = payload.get("result") or ((payload.get("pageHelp") or {}).get("data")) or []
    items: list[dict[str, Any]] = []
    for group in groups:
        rows = group if isinstance(group, list) else [group]
        main_rows = [row for row in rows if row.get("ORG_FILE_TYPE") in (0, "0", None)]
        for row in (main_rows or rows)[:1]:
            title = clean_value(row.get("TITLE"))
            path = clean_value(row.get("URL"))
            items.append(
                {
                    "announcement_id": clean_value(row.get("ORG_BULLETIN_ID")),
                    "symbol": clean_value(row.get("SECURITY_CODE")) or symbol,
                    "name": clean_value(row.get("SECURITY_NAME")),
                    "published_at": clean_value(row.get("SSEDATE")),
                    "title": title,
                    "category": clean_value(row.get("BULLETIN_TYPE_DESC")),
                    "event_tags": announcement_event_tags(title),
                    "event_date": clean_value(row.get("SSEDATE")),
                    "event_date_type": "announcement_publication_date",
                    "url": f"https://static.sse.com.cn{path}" if path else None,
                    "official_source": "Shanghai Stock Exchange",
                }
            )
    return items[:limit]


def get_szse_announcements(
    symbol: str, start_date: str, end_date: str, limit: int
) -> list[dict[str, Any]]:
    payload = read_public_json_post(
        "https://www.szse.cn/api/disc/announcement/annList",
        "https://www.szse.cn/disclosure/listed/notice/index.html",
        {
            "seDate": [start_date, end_date],
            "stock": [symbol],
            "channelCode": ["listedNotice_disc"],
            "pageSize": limit,
            "pageNum": 1,
        },
    )
    items = []
    for row in (payload.get("data") or [])[:limit]:
        title = clean_value(row.get("title"))
        path = clean_value(row.get("attachPath"))
        codes = row.get("secCode") or []
        names = row.get("secName") or []
        published_at = clean_value(row.get("publishTime"))
        items.append(
            {
                "announcement_id": clean_value(row.get("annId") or row.get("id")),
                "symbol": str(codes[0]) if codes else symbol,
                "name": clean_value(names[0]) if names else None,
                "published_at": published_at,
                "title": title,
                "category": None,
                "event_tags": announcement_event_tags(title),
                "event_date": published_at.split(" ", 1)[0] if published_at else None,
                "event_date_type": "announcement_publication_date",
                "url": f"https://disc.static.szse.cn{path}" if path else None,
                "official_source": "Shenzhen Stock Exchange",
            }
        )
    return items


def get_announcement_data(symbol: str, days: int, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    if security["security_type"] in {"etf", "lof"}:
        raise HTTPException(
            status_code=400,
            detail="Company announcements are only available for ordinary listed shares.",
        )
    end = datetime.now(MARKET_TIMEZONE).date()
    start = end - timedelta(days=days)
    if security["exchange"] == "BSE":
        return {
            "symbol": symbol,
            "exchange": "BSE",
            "period": {"start": start.isoformat(), "end": end.isoformat()},
            "count": 0,
            "items": [],
            "data_status": "unavailable",
            "source": [],
            "source_errors": [
                {
                    "source": "Beijing Stock Exchange",
                    "error_type": "official_source_blocked",
                    "message": "The BSE public announcement page currently returns HTTP 403 to the service environment.",
                }
            ],
            "queried_at": now_iso(),
            "note": "No third-party announcement source is substituted for the blocked official BSE route.",
        }
    loader = get_sse_announcements if security["exchange"] == "SSE" else get_szse_announcements
    items = loader(symbol, start.isoformat(), end.isoformat(), limit)
    return {
        "symbol": symbol,
        "exchange": security["exchange"],
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "count": len(items),
        "items": items,
        "data_status": "full_data" if items else "no_data",
        "source": [f"official_{security['exchange'].lower()}_announcements"],
        "source_errors": [],
        "queried_at": now_iso(),
        "note": "Event tags are mechanical title matches. Event dates are publication dates unless explicitly stated otherwise.",
    }


def event_timeline_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = news_datetime(text)
    if parsed is not None:
        return parsed.astimezone(MARKET_TIMEZONE).date().isoformat()
    match = re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(0).replace("/", "-"), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def event_timeline_title_key(value: Any) -> str:
    text = normalized_news_title(value)
    for marker in ("关于", "公告", "公司", "股份有限公司", "有限责任公司"):
        text = text.replace(marker, "")
    return text


def event_titles_match(left: str, right: str) -> bool:
    if not left or not right:
        return False
    shorter, longer = sorted((left, right), key=len)
    if len(shorter) >= 8 and shorter in longer:
        return True
    return SequenceMatcher(None, left, right).ratio() >= 0.72


def event_price_feedback(
    disclosure_date: str | None,
    bars: list[dict[str, Any]],
) -> dict[str, Any]:
    result = {
        "anchor_trade_date": None,
        "anchor_close": None,
        "return_after_1_session_pct": None,
        "return_after_3_sessions_pct": None,
        "return_after_5_sessions_pct": None,
        "status": "unavailable",
    }
    if not disclosure_date or not bars:
        return result
    dated = [bar for bar in bars if clean_value(bar.get("date")) and to_number(bar.get("close")) is not None]
    anchor_index = next(
        (index for index, bar in enumerate(dated) if str(bar.get("date")) >= disclosure_date),
        None,
    )
    if anchor_index is None:
        result["status"] = "pending_no_trading_session_yet"
        return result
    anchor = dated[anchor_index]
    anchor_close = to_number(anchor.get("close"))
    result["anchor_trade_date"] = clean_value(anchor.get("date"))
    result["anchor_close"] = anchor_close
    available = 0
    for sessions in (1, 3, 5):
        target_index = anchor_index + sessions
        if target_index >= len(dated) or anchor_close in (None, 0):
            continue
        target_close = to_number(dated[target_index].get("close"))
        if target_close is None:
            continue
        result[f"return_after_{sessions}_session{'s' if sessions > 1 else ''}_pct"] = round(
            (target_close / anchor_close - 1) * 100, 4
        )
        available += 1
    result["status"] = "available" if available == 3 else "partial_or_pending"
    return result


def get_event_timeline_data(symbol: str, days: int, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    # Use one stable fetch size so different timeline display limits can share
    # the same official-source cache entry.
    announcement_limit = 25
    news_limit = min(limit * 3, 10)
    kline_limit = min(max(days + 20, 40), 160)
    loaders = {
        "official_announcements": lambda: get_cached_component_with_stale(
            cache_key(
                "get_a_share_announcements",
                {"symbol": symbol, "days": days, "limit": announcement_limit},
            ),
            300,
            3600,
            lambda: get_announcement_data(symbol, days, announcement_limit),
        ),
        "news": lambda: get_cached_component_with_stale(
            cache_key(
                "get_a_share_news",
                {
                    "symbol": symbol,
                    "limit": news_limit,
                    "days": days,
                    "include_industry_context": False,
                },
            ),
            300,
            3600,
            lambda: get_news_data(symbol, news_limit, days, False),
        ),
        "daily_kline": lambda: get_cached_component_with_stale(
            cache_key(
                "event_timeline_daily_kline",
                {"symbol": symbol, "limit": kline_limit},
            ),
            300,
            86400,
            lambda: get_kline_data(symbol, "daily", kline_limit),
        ),
    }
    results, component_status, source_errors = collect_components(
        loaders, 7, COMPOSITE_TOOL_EXECUTOR
    )
    if not results.get("official_announcements") and not results.get("news"):
        raise HTTPException(status_code=502, detail="Both official announcements and news timeline sources failed.")

    candidates: list[dict[str, Any]] = []
    for item in (results.get("official_announcements") or {}).get("items", []):
        candidates.append(
            {
                "record_type": "official_announcement",
                "title": clean_value(item.get("title")),
                "published_at": clean_value(item.get("published_at")),
                "event_date": event_timeline_date(item.get("event_date") or item.get("published_at")),
                "event_date_type": item.get("event_date_type") or "announcement_publication_date",
                "source": clean_value(item.get("official_source")) or "official_exchange_announcement",
                "source_independence": "official_primary_source",
                "event_tags": item.get("event_tags") or ["other"],
                "url": clean_value(item.get("url")),
            }
        )
    for item in (results.get("news") or {}).get("items", []):
        link_type = str(item.get("link_type") or "")
        candidates.append(
            {
                "record_type": "media_report",
                "title": clean_value(item.get("title")),
                "published_at": clean_value(item.get("published_at")),
                "event_date": event_timeline_date(item.get("event_date") or item.get("published_at")),
                "event_date_type": (
                    item.get("event_date_status") or "media_publication_date_not_verified_event_time"
                ),
                "source": clean_value(item.get("source")) or "publisher_not_disclosed",
                "source_independence": (
                    "publisher_attributed_via_news_index"
                    if "redirect_to_publisher" in link_type
                    else "hosted_or_reprint_independence_unclear"
                ),
                "event_tags": announcement_event_tags(item.get("title")),
                "url": clean_value(item.get("url")),
            }
        )

    candidates.sort(key=lambda item: (str(item.get("event_date") or ""), str(item.get("published_at") or "")))
    clusters: list[dict[str, Any]] = []
    for item in candidates:
        key = event_timeline_title_key(item.get("title"))
        cluster = next(
            (candidate for candidate in clusters if event_titles_match(key, candidate["title_key"])),
            None,
        )
        if cluster is None:
            clusters.append({"title_key": key, "records": [item]})
        else:
            cluster["records"].append(item)

    bars = (results.get("daily_kline") or {}).get("items", [])
    events = []
    for cluster in clusters:
        records = sorted(
            cluster["records"],
            key=lambda item: (
                0 if item["record_type"] == "official_announcement" else 1,
                str(item.get("published_at") or ""),
            ),
        )
        primary = records[0]
        dates = [str(item["event_date"]) for item in records if item.get("event_date")]
        source_names = sorted({str(item.get("source")) for item in records if item.get("source")})
        events.append(
            {
                "event_title": primary.get("title"),
                "event_date": min(dates) if dates else None,
                "event_date_status": (
                    "official_disclosure_date_available"
                    if any(item["record_type"] == "official_announcement" for item in records)
                    else "media_publication_date_only"
                ),
                "event_tags": sorted({tag for item in records for tag in item.get("event_tags", [])}),
                "record_count": len(records),
                "source_count": len(source_names),
                "has_official_primary_source": any(
                    item["record_type"] == "official_announcement" for item in records
                ),
                "independence_status": (
                    "official_primary_plus_multiple_publishers"
                    if any(item["record_type"] == "official_announcement" for item in records)
                    and len(source_names) > 1
                    else "official_primary_only"
                    if any(item["record_type"] == "official_announcement" for item in records)
                    else "multiple_attributed_publishers"
                    if len(source_names) > 1
                    else records[0]["source_independence"]
                ),
                "sources": source_names,
                "records": records,
                "price_feedback": event_price_feedback(min(dates) if dates else None, bars),
            }
        )
    events.sort(key=lambda item: str(item.get("event_date") or ""), reverse=True)
    events = events[:limit]
    nested_errors = [
        error
        for payload in results.values()
        if isinstance(payload, dict)
        for error in normalize_source_errors(payload.get("source_errors"))
    ]
    return {
        "symbol": symbol,
        "period_days": days,
        "count": len(events),
        "events": events,
        "price_feedback_method": "Forward-adjusted close-to-close return from the first trading session on or after the disclosed/publication date.",
        "component_status": component_status,
        "source": sorted(
            {
                source
                for payload in results.values()
                if isinstance(payload, dict)
                for source in normalize_sources(payload.get("source"))
            }
        ),
        "source_errors": [*source_errors, *nested_errors],
        "data_status": (
            "full_data"
            if len(results) == 3 and not source_errors and not nested_errors
            else "partial_data"
        ),
        "queried_at": now_iso(),
        "note": "Records are clustered mechanically by title similarity. Publication time is not silently relabelled as the real-world event time, and price feedback is not a good/bad or trading judgement.",
    }


HISTORICAL_CONTEXT_WINDOWS = (20, 60, 120, 250)
CORPORATE_ACTION_EVENT_TAGS = {
    "dividend",
    "buyback",
    "shareholder_change",
    "unlock",
    "suspension_resume",
    "major_transaction",
    "financing",
}


def mean_or_none(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def percentile_rank(value: float | None, values: list[float]) -> float | None:
    if value is None or not values:
        return None
    return round(sum(item <= value for item in values) / len(values) * 100, 2)


def annualized_volatility_pct(closes: list[float]) -> float | None:
    returns = [
        (current - previous) / previous
        for previous, current in zip(closes, closes[1:])
        if previous != 0
    ]
    if len(returns) < 2:
        return None
    average = sum(returns) / len(returns)
    variance = sum((value - average) ** 2 for value in returns) / (len(returns) - 1)
    return round(sqrt(variance) * sqrt(252) * 100, 4)


def maximum_drawdown_pct(closes: list[float]) -> float | None:
    if not closes:
        return None
    peak = closes[0]
    drawdown = 0.0
    for close in closes:
        peak = max(peak, close)
        if peak:
            drawdown = min(drawdown, (close - peak) / peak * 100)
    return round(drawdown, 4)


def historical_window_metrics(
    items: list[dict[str, Any]], window: int
) -> dict[str, Any]:
    window_items = items[-window:]
    closes = [
        value
        for item in window_items
        if (value := to_number(item.get("close"))) is not None
    ]
    highs = [
        value
        for item in window_items
        if (value := to_number(item.get("high"))) is not None
    ]
    lows = [
        value
        for item in window_items
        if (value := to_number(item.get("low"))) is not None
    ]
    latest = window_items[-1] if window_items else {}
    latest_close = to_number(latest.get("close"))
    comparison_close = (
        to_number(items[-(window + 1)].get("close"))
        if len(items) >= window + 1
        else None
    )
    metrics: dict[str, Any] = {
        "requested_sessions": window,
        "available_sessions": len(window_items),
        "window_complete": len(items) >= window + 1,
        "start_date": clean_value(window_items[0].get("date")) if window_items else None,
        "end_date": clean_value(latest.get("date")),
        "return_pct": percentage_change(latest_close, comparison_close),
        "annualized_volatility_pct": annualized_volatility_pct(closes),
        "maximum_drawdown_pct": maximum_drawdown_pct(closes),
        "high": max(highs) if highs else None,
        "low": min(lows) if lows else None,
        "distance_from_high_pct": percentage_change(
            latest_close, max(highs) if highs else None
        ),
        "distance_from_low_pct": percentage_change(
            latest_close, min(lows) if lows else None
        ),
    }
    for field in ("turnover", "volume", "turnover_rate", "amplitude"):
        values = [
            value
            for item in window_items
            if (value := to_number(item.get(field))) is not None
        ]
        latest_value = to_number(latest.get(field))
        prior_values = values[:-1] if len(values) > 1 else []
        prior_average = mean_or_none(prior_values)
        metrics[field] = {
            "latest": latest_value,
            "prior_sessions_average": prior_average,
            "latest_vs_prior_average_ratio": (
                round(latest_value / prior_average, 4)
                if latest_value is not None and prior_average not in (None, 0)
                else None
            ),
            "percentile_rank_in_window": percentile_rank(latest_value, values),
            "available_observations": len(values),
        }
    return metrics


def get_historical_context_data(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    payload, _, source_errors = race_public_sources(
        (
            (
                "tencent",
                lambda: get_tencent_kline(symbol, "daily", 260),
            ),
            (
                "eastmoney",
                lambda: get_eastmoney_kline(
                    symbol,
                    "daily",
                    KLINE_PERIODS["daily"],
                    260,
                ),
            ),
        ),
        6,
    )
    payload["source_errors"] = [
        *normalize_source_errors(payload.get("source_errors")),
        *normalize_source_errors(source_errors),
    ]
    security = security_metadata(symbol)
    payload.update(
        {
            "security_type": security["security_type"],
            "exchange": security["exchange"],
        }
    )
    items = payload["items"]
    if not items:
        raise HTTPException(status_code=404, detail=f"Historical context not found: {symbol}")
    latest = items[-1]
    latest_date = clean_value(latest.get("date"))
    is_incomplete_session = (
        latest_date == datetime.now(MARKET_TIMEZONE).date().isoformat()
        and market_status_at() == "open"
    )
    windows = {
        str(window): historical_window_metrics(items, window)
        for window in HISTORICAL_CONTEXT_WINDOWS
    }
    return {
        "symbol": symbol,
        "security_type": payload.get("security_type"),
        "exchange": payload.get("exchange"),
        "period": "daily",
        "adjustment": payload.get("adjustment", "forward_adjusted"),
        "adjustment_source_parameter": payload.get("adjustment_source_parameter"),
        "latest_trade_date": latest_date,
        "latest_close": to_number(latest.get("close")),
        "latest_session_may_be_incomplete": is_incomplete_session,
        "source_sessions": len(items),
        "windows": windows,
        "source": payload.get("source"),
        "source_errors": payload.get("source_errors", []),
        "queried_at": now_iso(),
        "data_status": (
            "full_data"
            if all(item["window_complete"] for item in windows.values())
            else "partial_data"
        ),
        "note": "Historical values are mechanical forward-adjusted facts. Percentiles describe location within each window and are not scores or recommendations.",
    }


def get_cached_historical_context_data(symbol: str) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    return get_cached_component_with_stale(
        cache_key(
            "historical_context_internal",
            {"symbol": normalized_symbol, "adjust": "forward", "sessions": 260},
        ),
        300,
        604800,
        lambda: get_historical_context_data(normalized_symbol),
    )


def parse_yyyymmdd(value: Any) -> str | None:
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{8}", text):
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def consume_background_future(future: Any) -> None:
    try:
        future.exception()
    except Exception:
        return


def get_security_reference_data(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    fields = "f57,f58,f43,f47,f48,f60,f124,f189,f292,f84,f85,f127,f128"
    query = urlencode({"secid": eastmoney_secid(symbol), "fields": fields})
    hosts = (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    )
    executor = ThreadPoolExecutor(max_workers=len(hosts))
    futures = {
        executor.submit(
            read_public_json,
            f"https://{host}/api/qt/stock/get?{query}",
            "https://quote.eastmoney.com/",
            3,
            2,
        ): host
        for host in hosts
    }
    pending = set(futures)
    data: dict[str, Any] = {}
    selected_host: str | None = None
    errors: list[str] = []
    deadline = perf_counter() + 6
    try:
        while pending and not data:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                break
            for future in completed:
                host = futures[future]
                try:
                    candidate = future.result().get("data") or {}
                    if not candidate.get("f57"):
                        raise HTTPException(
                            status_code=502,
                            detail="security reference response contained no symbol",
                        )
                    data = candidate
                    selected_host = host
                    break
                except HTTPException as exc:
                    errors.append(f"{host}: {exc.detail}")
                except Exception as exc:  # pragma: no cover - defensive source boundary
                    errors.append(f"{host}: {exc}")
        for future in pending:
            errors.append(
                f"{futures[future]}: security reference request exceeded the 6 second budget"
            )
            future.add_done_callback(consume_background_future)
            future.cancel()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    if not data or not selected_host:
        raise HTTPException(
            status_code=502,
            detail="Security reference unavailable from all public hosts: "
            + "; ".join(errors),
        )
    return {
        "symbol": clean_value(data.get("f57")) or symbol,
        "name": clean_value(data.get("f58")),
        "listing_date": parse_yyyymmdd(data.get("f189")),
        "source_security_status_code": clean_value(data.get("f292")),
        "industry": clean_value(data.get("f127")),
        "region": clean_value(data.get("f128")),
        "total_shares": to_number(data.get("f84")),
        "circulating_shares": to_number(data.get("f85")),
        "source": f"eastmoney_security_reference:{selected_host}",
        "source_errors": errors,
        "queried_at": now_iso(),
    }


def get_resilient_security_reference_data(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    key = cache_key("security_reference_internal", {"symbol": symbol})
    try:
        data, _ = get_cached_tool_data(
            key,
            21600,
            lambda: get_security_reference_data(symbol),
        )
        return data
    except HTTPException as exc:
        stale = get_cached_tool_snapshot(key, 604800)
        if stale is None:
            raise
        data, cache = stale
        data.setdefault("source_errors", []).append(
            f"live_refresh: {exc.detail}; using cached slow-changing security reference"
        )
        data["cache_hit"] = True
        data["cache_age_seconds"] = cache["cache_age_seconds"]
        data["reference_stale_reason"] = (
            "live_sources_failed_using_slow_changing_reference_cache"
        )
        return data


def collect_components(
    loaders: dict[str, Any],
    response_budget_seconds: float,
    executor: ThreadPoolExecutor | None = None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    def timed_load(loader: Any) -> tuple[Any, int]:
        started = perf_counter()
        return loader(), int((perf_counter() - started) * 1000)

    owns_executor = executor is None
    selected_executor = executor or ThreadPoolExecutor(max_workers=max(1, len(loaders)))
    futures = {
        selected_executor.submit(timed_load, loader): name
        for name, loader in loaders.items()
    }
    done, pending = wait(futures, timeout=response_budget_seconds)
    results: dict[str, Any] = {}
    statuses: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    for future in done:
        name = futures[future]
        try:
            result, latency_ms = future.result()
            results[name] = result
            statuses[name] = {
                "status": (
                    "stale_cache"
                    if isinstance(result, dict)
                    and result.get("served_from_stale_cache")
                    else "available"
                ),
                "latency_ms": latency_ms,
            }
        except HTTPException as exc:
            statuses[name] = {"status": "unavailable", "latency_ms": None}
            errors.append(
                {
                    "source": name,
                    "error_type": classify_error_type(exc.detail, exc.status_code),
                    "message": str(exc.detail),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive component boundary
            statuses[name] = {"status": "unavailable", "latency_ms": None}
            errors.append(
                {"source": name, "error_type": "unexpected_error", "message": str(exc)}
            )
    for future in pending:
        name = futures[future]
        future.cancel()
        statuses[name] = {
            "status": "unavailable_within_response_budget",
            "latency_ms": None,
        }
        errors.append(
            {
                "source": name,
                "error_type": "timeout",
                "message": f"Component exceeded the {response_budget_seconds:g} second response budget.",
            }
        )
    if owns_executor:
        selected_executor.shutdown(wait=False, cancel_futures=True)
    return results, statuses, errors


def build_security_status_data(
    symbol: str,
    quote_payload: dict[str, Any] | None,
    reference: dict[str, Any] | None,
    announcements: dict[str, Any] | None,
    component_status: dict[str, Any] | None = None,
    source_errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    security = security_metadata(symbol)
    quote = (quote_payload or {}).get("quote") or {}
    reference = reference or {}
    name = quote.get("name") or reference.get("name")
    listing_date = reference.get("listing_date")
    listing_age_days = None
    if listing_date:
        listing_age_days = (
            datetime.now(MARKET_TIMEZONE).date()
            - datetime.fromisoformat(listing_date).date()
        ).days
    announcement_items = (announcements or {}).get("items") or []
    corporate_actions = [
        item
        for item in announcement_items
        if has_corporate_action_tag(item.get("event_tags"))
    ]
    is_st = is_st_security(name)
    standard_limit = price_limit_pct(symbol, is_st)
    return {
        "symbol": symbol,
        "name": name,
        "security_type": security["security_type"],
        "exchange": security["exchange"],
        "industry": reference.get("industry"),
        "region": reference.get("region"),
        "listing_date": listing_date,
        "listing_age_calendar_days": listing_age_days,
        "is_st_name_flag": is_st,
        "is_delisting_arrangement_name_flag": "退" in str(name or ""),
        "current_quote_observation": {
            "status": "quote_available" if quote.get("price") is not None else "unavailable",
            "price": quote.get("price"),
            "trade_date": quote.get("trade_date"),
            "quote_time": quote.get("quote_time"),
            "volume": quote.get("volume"),
            "source_updated_at": quote.get("source_updated_at"),
        },
        "suspension_status": "not_confirmed_by_current_data_contract",
        "recent_suspension_resume_announcements": [
            item
            for item in announcement_items
            if "suspension_resume" in (item.get("event_tags") or [])
        ],
        "source_security_status_code": reference.get("source_security_status_code"),
        "source_security_status_code_interpretation": "raw_source_code_not_mapped_to_an_official_exchange_status",
        "price_limit_reference": {
            "standard_daily_limit_pct": standard_limit,
            "scope": "mechanical_standard_rule_from_security_code_and_ST_name_flag",
            "exceptions": "IPO initial no-limit sessions, relisting, resumed trading, and product-specific rules require separate official confirmation.",
        },
        "price_history_adjustment": {
            "mode": "forward_adjusted",
            "eastmoney_parameter": "fqt=1",
            "tencent_parameter": "qfq",
            "intraday_adjustment": "not_applicable_to_same-day_minutes",
        },
        "recent_corporate_action_announcements": corporate_actions,
        "corporate_action_date_scope": "announcement_publication_dates_only_unless_the_title_explicitly_states_an_effective_date",
        "component_status": component_status or {},
        "source": [
            source
            for source in (
                (quote_payload or {}).get("source"),
                reference.get("source"),
                *((announcements or {}).get("source") or []),
            )
            if source
        ],
        "source_errors": [*(source_errors or []), *(reference.get("source_errors") or [])],
        "queried_at": now_iso(),
        "data_status": "full_data" if quote_payload and reference else "partial_data",
        "note": "This tool reports observable status facts and standard rule references. It does not infer whether a security should be traded.",
    }


def has_corporate_action_tag(tags: Any) -> bool:
    return bool(CORPORATE_ACTION_EVENT_TAGS & set(tags or []))


def get_security_status_data(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    loaders: dict[str, Any] = {
        "quote": lambda: get_quote_data(symbol),
        "security_reference": lambda: get_resilient_security_reference_data(symbol),
    }
    if security["security_type"] not in {"etf", "lof"}:
        loaders["official_announcements"] = lambda: get_announcement_data(symbol, 180, 20)
    results, statuses, errors = collect_components(loaders, 8)
    if not results.get("quote") and not results.get("security_reference"):
        raise HTTPException(status_code=502, detail="Security status sources were unavailable.")
    return build_security_status_data(
        symbol,
        results.get("quote"),
        results.get("security_reference"),
        results.get("official_announcements"),
        statuses,
        errors,
    )


def compact_intraday_context(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "symbol",
            "name",
            "security_type",
            "exchange",
            "previous_close",
            "open",
            "high",
            "low",
            "count",
            "latest_market_time",
            "market_time",
            "mechanical_indicators",
            "source",
            "source_errors",
        )
    } | {"latest_minutes": (payload.get("items") or [])[-10:]}


def decision_context_follow_up_tools(
    symbol: str,
    benchmark: str,
    component_status: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    specifications = (
        ("quote", "get_a_share_quote", {"symbol": symbol}),
        ("intraday", "get_a_share_intraday", {"symbol": symbol, "limit": 60}),
        (
            "historical_context",
            "get_a_share_historical_context",
            {"symbol": symbol},
        ),
        (
            "security_reference",
            "get_a_share_security_status",
            {"symbol": symbol},
        ),
        (
            "relative_strength",
            "get_a_share_relative_strength",
            {"symbol": symbol, "benchmark_symbol": benchmark},
        ),
        ("market_overview", "get_a_share_market_overview", {"limit": 5}),
        (
            "news",
            "get_a_share_news",
            {
                "symbol": symbol,
                "limit": 8,
                "days": 30,
                "include_industry_context": False,
            },
        ),
        (
            "official_announcements",
            "get_a_share_announcements",
            {"symbol": symbol, "days": 180, "limit": 10},
        ),
        ("financials", "get_a_share_financials", {"symbol": symbol, "limit": 4}),
    )
    recommendations = []
    for component, tool, arguments in specifications:
        status = str((component_status.get(component) or {}).get("status") or "")
        if status not in {"unavailable", "unavailable_within_response_budget"}:
            continue
        recommendations.append(
            {
                "missing_component": component,
                "tool": tool,
                "arguments": arguments,
                "reason": status,
            }
        )
    return recommendations


def get_decision_context_data(symbol: str, benchmark_symbol: str | None) -> dict[str, Any]:
    started_at = perf_counter()
    started_iso = now_iso()
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    benchmark = benchmark_symbol or default_benchmark_identifier(symbol)
    loaders: dict[str, Any] = {
        "quote": lambda: get_quote_data(symbol),
        "intraday": lambda: get_intraday_data(symbol, 60),
        "historical_context": lambda: get_cached_historical_context_data(symbol),
        "security_reference": lambda: get_resilient_security_reference_data(symbol),
        "relative_strength": lambda: get_relative_strength_data(symbol, benchmark, None),
        "market_overview": lambda: get_market_overview_data(5),
        "news": lambda: get_news_data(symbol, 8, 30, False),
    }
    if security["security_type"] not in {"etf", "lof"}:
        loaders["official_announcements"] = lambda: get_announcement_data(symbol, 180, 10)
        loaders["financials"] = lambda: get_financial_data(symbol, 4)
    results, statuses, errors = collect_components(loaders, 12)
    if security["security_type"] in {"etf", "lof"}:
        statuses["official_announcements"] = {
            "status": "not_applicable_to_exchange_listed_fund",
            "latency_ms": None,
        }
        statuses["financials"] = {
            "status": "not_applicable_to_exchange_listed_fund",
            "latency_ms": None,
        }
    quote_payload = results.get("quote")
    security_status = build_security_status_data(
        symbol,
        quote_payload,
        results.get("security_reference"),
        results.get("official_announcements"),
        {
            key: statuses.get(key)
            for key in ("quote", "security_reference", "official_announcements")
            if key in statuses
        },
        [error for error in errors if error["source"] in {"quote", "security_reference", "official_announcements"}],
    )
    completed_iso = now_iso()
    snapshot_id = (
        f"{symbol}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    )
    decision_inputs = {
        "quote": quote_payload,
        "intraday": (
            compact_intraday_context(results["intraday"])
            if "intraday" in results
            else None
        ),
        "historical_context": results.get("historical_context"),
        "security_status": security_status,
        "relative_strength": results.get("relative_strength"),
        "official_announcements": results.get("official_announcements"),
        "financials": results.get("financials"),
        "market_overview": results.get("market_overview"),
        "news": results.get("news"),
    }
    applicable_component_names = set(decision_inputs)
    if security["security_type"] in {"etf", "lof"}:
        applicable_component_names -= {"official_announcements", "financials"}
    available_count = sum(
        decision_inputs[name] is not None for name in applicable_component_names
    )
    requested_count = len(applicable_component_names)
    recommended_follow_ups = decision_context_follow_up_tools(
        symbol, benchmark, statuses
    )
    return {
        "snapshot_id": snapshot_id,
        "symbol": symbol,
        "benchmark_identifier": benchmark,
        "snapshot_started_at": started_iso,
        "snapshot_completed_at": completed_iso,
        "snapshot_span_ms": int((perf_counter() - started_at) * 1000),
        "component_status": statuses,
        "available_component_count": available_count,
        "requested_component_count": requested_count,
        "applicable_components": sorted(applicable_component_names),
        "recommended_follow_up_tools": recommended_follow_ups,
        "follow_up_note": (
            "Call only the listed read-only tools to refill components that were unavailable in this packet. "
            "An unavailable packet component means not returned within this request, not that the underlying data does not exist."
            if recommended_follow_ups
            else None
        ),
        "decision_inputs": decision_inputs,
        "excluded_components": {},
        "source": sorted(
            {
                str(source)
                for value in results.values()
                if isinstance(value, dict)
                for source in normalize_sources(value.get("source"))
            }
        ),
        "source_errors": errors,
        "queried_at": completed_iso,
        "data_status": (
            "full_data"
            if available_count == requested_count and not errors
            else "partial_data"
        ),
        "note": "Evidence packet only. It contains facts, comparisons, timestamps, provenance, and missing-data reasons; GPT remains responsible for interpretation and judgement.",
    }


def default_benchmark_identifier(symbol: str) -> str:
    security = security_metadata(symbol)
    if security["security_type"] in {"etf", "lof"}:
        return "index:000300"
    return {
        "SSE": "index:000001",
        "SZSE": "index:399001",
        "BSE": "index:899050",
    }[security["exchange"]]


def day_range_position_pct(quote: dict[str, Any]) -> float | None:
    price = to_number(quote.get("price"))
    high = to_number(quote.get("high"))
    low = to_number(quote.get("low"))
    if price is None or high is None or low is None or high <= low:
        return None
    return round((price - low) / (high - low) * 100, 2)


def get_relative_strength_data(
    symbol: str,
    benchmark_symbol: str | None,
    peer_symbols: list[str] | None,
) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    benchmark = (benchmark_symbol or default_benchmark_identifier(symbol)).strip()
    peers = peer_symbols or []
    requested = list(dict.fromkeys([symbol, benchmark, *peers]))
    if len(requested) > 20:
        raise HTTPException(
            status_code=400,
            detail="Target, benchmark, and peers support at most 20 identifiers.",
        )
    payload = get_batch_quote_data(requested)
    quotes = {
        item.get("identifier", item.get("symbol")): item for item in payload["results"]
    }
    target = quotes.get(symbol)
    if target is None:
        raise HTTPException(
            status_code=502,
            detail="The target quote was unavailable in the batch snapshot.",
        )
    benchmark_quote = quotes.get(benchmark)
    target_change = to_number(target.get("change_pct"))
    benchmark_change = (
        to_number(benchmark_quote.get("change_pct")) if benchmark_quote else None
    )
    peer_quotes = [quotes[peer] for peer in peers if peer in quotes]
    peer_changes = [
        value
        for item in peer_quotes
        if (value := to_number(item.get("change_pct"))) is not None
    ]
    peer_average = round(sum(peer_changes) / len(peer_changes), 3) if peer_changes else None
    relative_to_benchmark = (
        round(target_change - benchmark_change, 3)
        if target_change is not None and benchmark_change is not None
        else None
    )
    relative_to_peers = (
        round(target_change - peer_average, 3)
        if target_change is not None and peer_average is not None
        else None
    )
    return {
        "symbol": symbol,
        "target": target,
        "benchmark_identifier": benchmark,
        "benchmark": benchmark_quote,
        "peer_count": len(peer_quotes),
        "peers": peer_quotes,
        "target_change_pct": target_change,
        "benchmark_change_pct": benchmark_change,
        "relative_to_benchmark_pct_points": relative_to_benchmark,
        "peer_average_change_pct": peer_average,
        "relative_to_peer_average_pct_points": relative_to_peers,
        "day_range_position_pct": day_range_position_pct(target),
        "relative_status": (
            "outperforming_benchmark"
            if relative_to_benchmark is not None and relative_to_benchmark > 0
            else "underperforming_benchmark"
            if relative_to_benchmark is not None and relative_to_benchmark < 0
            else "matching_or_unavailable"
        ),
        "market_time": payload.get("market_time"),
        "source": payload.get("source", []),
        "source_errors": payload.get("source_errors", []),
        "queried_at": now_iso(),
        "note": "Relative strength is the current percentage-point difference, not a prediction or trading recommendation.",
    }


def scan_intraday_anomalies_data(
    symbols: list[str],
    benchmark_symbol: str | None,
    change_pct_min: float,
    volume_ratio_min: float,
    turnover_rate_min: float,
    gap_pct_min: float,
    near_extreme_pct: float,
    relative_strength_min: float,
    include_untriggered: bool,
) -> dict[str, Any]:
    benchmark = benchmark_symbol.strip() if benchmark_symbol else None
    requested = list(dict.fromkeys([*symbols, *([benchmark] if benchmark else [])]))
    if not requested or len(requested) > 20:
        raise HTTPException(
            status_code=400,
            detail="symbols plus benchmark must contain 1 to 20 identifiers.",
        )
    thresholds = (
        change_pct_min,
        volume_ratio_min,
        turnover_rate_min,
        gap_pct_min,
        near_extreme_pct,
        relative_strength_min,
    )
    if any(value < 0 for value in thresholds):
        raise HTTPException(
            status_code=400,
            detail="Anomaly thresholds must be non-negative.",
        )
    payload = get_batch_quote_data(requested)
    quotes = {
        item.get("identifier", item.get("symbol")): item for item in payload["results"]
    }
    benchmark_quote = quotes.get(benchmark) if benchmark else None
    benchmark_change = (
        to_number(benchmark_quote.get("change_pct")) if benchmark_quote else None
    )
    results = []
    for identifier in symbols:
        quote = quotes.get(identifier)
        if quote is None:
            continue
        change_pct = to_number(quote.get("change_pct"))
        volume_ratio = to_number(quote.get("volume_ratio"))
        turnover_rate = to_number(quote.get("turnover_rate"))
        gap_pct = percentage_change(
            to_number(quote.get("open")),
            to_number(quote.get("previous_close")),
        )
        relative = (
            round(change_pct - benchmark_change, 3)
            if change_pct is not None and benchmark_change is not None
            else None
        )
        price = to_number(quote.get("price"))
        high = to_number(quote.get("high"))
        low = to_number(quote.get("low"))
        distance_to_high = (
            round((high - price) / high * 100, 3)
            if price is not None and high not in (None, 0)
            else None
        )
        distance_to_low = (
            round((price - low) / low * 100, 3)
            if price is not None and low not in (None, 0)
            else None
        )
        triggers: list[dict[str, Any]] = []

        def add_trigger(
            condition: bool,
            trigger_type: str,
            value: Any,
            threshold: Any,
        ) -> None:
            if condition:
                triggers.append(
                    {"type": trigger_type, "value": value, "threshold": threshold}
                )

        add_trigger(
            change_pct is not None and abs(change_pct) >= change_pct_min,
            "large_daily_move",
            change_pct,
            change_pct_min,
        )
        add_trigger(
            volume_ratio is not None and volume_ratio >= volume_ratio_min,
            "high_daily_volume_ratio",
            volume_ratio,
            volume_ratio_min,
        )
        add_trigger(
            turnover_rate is not None and turnover_rate >= turnover_rate_min,
            "high_turnover_rate",
            turnover_rate,
            turnover_rate_min,
        )
        add_trigger(
            gap_pct is not None and abs(gap_pct) >= gap_pct_min,
            "opening_gap",
            gap_pct,
            gap_pct_min,
        )
        add_trigger(
            distance_to_high is not None and distance_to_high <= near_extreme_pct,
            "near_intraday_high",
            distance_to_high,
            near_extreme_pct,
        )
        add_trigger(
            distance_to_low is not None and distance_to_low <= near_extreme_pct,
            "near_intraday_low",
            distance_to_low,
            near_extreme_pct,
        )
        add_trigger(
            relative is not None and abs(relative) >= relative_strength_min,
            "benchmark_relative_move",
            relative,
            relative_strength_min,
        )
        if triggers or include_untriggered:
            results.append(
                {
                    "identifier": identifier,
                    "symbol": quote.get("symbol"),
                    "name": quote.get("name"),
                    "change_pct": change_pct,
                    "volume_ratio": volume_ratio,
                    "turnover_rate": turnover_rate,
                    "opening_gap_pct": gap_pct,
                    "relative_to_benchmark_pct_points": relative,
                    "day_range_position_pct": day_range_position_pct(quote),
                    "trigger_count": len(triggers),
                    "triggers": triggers,
                }
            )
    results.sort(
        key=lambda item: (
            item["trigger_count"],
            abs(item.get("change_pct") or 0),
        ),
        reverse=True,
    )
    return {
        "requested_count": len(symbols),
        "evaluated_count": len([symbol for symbol in symbols if symbol in quotes]),
        "triggered_count": len(
            [item for item in results if item["trigger_count"] > 0]
        ),
        "benchmark_identifier": benchmark,
        "benchmark": benchmark_quote,
        "thresholds": {
            "change_pct_min": change_pct_min,
            "volume_ratio_min": volume_ratio_min,
            "turnover_rate_min": turnover_rate_min,
            "gap_pct_min": gap_pct_min,
            "near_extreme_pct": near_extreme_pct,
            "relative_strength_min": relative_strength_min,
        },
        "results": results,
        "market_time": payload.get("market_time"),
        "source": payload.get("source", []),
        "source_errors": payload.get("source_errors", []),
        "queried_at": now_iso(),
        "note": "Triggers are caller-controlled mechanical conditions. Daily volume ratio is not a five-minute volume surge signal.",
    }


def get_eastmoney_indices() -> list[dict[str, Any]]:
    index_query = urlencode(
        {
            "fltt": 2,
            "invt": 2,
            "fields": "f12,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18,f124",
            "secids": INDEX_SECIDS,
        }
    )
    index_payload = read_public_json(
        f"https://push2.eastmoney.com/api/qt/ulist.np/get?{index_query}",
        "https://quote.eastmoney.com/",
    )
    index_rows = ((index_payload.get("data") or {}).get("diff")) or []
    if not index_rows:
        raise HTTPException(status_code=502, detail="Eastmoney returned no major-index rows.")
    return [
        enrich_index_identity({
            "symbol": clean_value(row.get("f12")),
            "name": clean_value(row.get("f14")),
            "price": to_number(row.get("f2")),
            "change_pct": to_number(row.get("f3")),
            "change": to_number(row.get("f4")),
            "open": to_number(row.get("f17")),
            "high": to_number(row.get("f15")),
            "low": to_number(row.get("f16")),
            "previous_close": to_number(row.get("f18")),
            "turnover": to_number(row.get("f6")),
            "source_updated_at": format_unix_market_time(row.get("f124")),
            "market_time": market_time_from_source_update(
                format_unix_market_time(row.get("f124"))
            ),
        })
        for row in index_rows
    ]


def get_tencent_indices() -> list[dict[str, Any]]:
    try:
        text = read_market_text(
            f"https://qt.gtimg.cn/q={TENCENT_INDEX_CODES}",
            "https://stockapp.finance.qq.com/",
        )
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Tencent indices: {exc}") from exc
    indices = []
    for match in re.finditer(r'v_\w+="([^"]*)"', text):
        values = match.group(1).split("~")
        if len(values) < 33:
            continue
        indices.append(
            enrich_index_identity({
                "symbol": clean_value(values[2]),
                "name": clean_value(values[1]),
                "price": to_number(values[3]),
                "change_pct": to_number(values[32]),
                "change": to_number(values[31]),
                "open": to_number(values[5]),
                "high": to_number(values[33]) if len(values) > 33 else None,
                "low": to_number(values[34]) if len(values) > 34 else None,
                "previous_close": to_number(values[4]),
                "source_updated_at": format_market_time(values[30]),
                "market_time": market_time_from_source_update(
                    format_market_time(values[30])
                ),
            })
        )
    if not indices:
        raise HTTPException(status_code=502, detail="Unexpected Tencent index response.")
    return indices


def get_sina_indices() -> list[dict[str, Any]]:
    try:
        text = read_market_text(
            "https://hq.sinajs.cn/list=s_sh000001,s_sz399001,s_sz399006",
            "https://finance.sina.com.cn/",
        )
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Sina indices: {exc}") from exc
    indices = []
    for match in re.finditer(r'var hq_str_s_(?:sh|sz)(\d+)="([^"]*)";', text):
        values = match.group(2).split(",")
        if len(values) < 4 or not values[0]:
            continue
        indices.append(
            enrich_index_identity({
                "symbol": match.group(1),
                "name": clean_value(values[0]),
                "price": to_number(values[1]),
                "change": to_number(values[2]),
                "change_pct": to_number(values[3]),
                "open": None,
                "high": None,
                "low": None,
                "previous_close": None,
                "market_time": None,
            })
        )
    if not indices:
        raise HTTPException(status_code=502, detail="Unexpected Sina index response.")
    return indices


def _to_int(value: Any) -> int | None:
    number = to_number(value)
    return int(number) if number is not None else None


def get_eastmoney_sector_boards(sector_type: str, candidate_limit: int = 500) -> list[dict[str, Any]]:
    if sector_type not in SECTOR_TYPE_CONFIG:
        raise HTTPException(status_code=400, detail="sector_type must be industry or concept.")
    query = urlencode(
        {
            "pn": 1,
            "pz": max(30, min(candidate_limit, 500)),
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": SECTOR_TYPE_CONFIG[sector_type],
            "fields": "f12,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18,f22,f104,f105,f128,f136,f140",
        }
    )
    errors = []
    futures = {
        PUBLIC_SOURCE_EXECUTOR.submit(
            read_public_json,
            f"https://{host}/api/qt/clist/get?{query}",
            "https://quote.eastmoney.com/",
            3,
            2,
        ): host
        for host in (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
        )
    }
    for future in as_completed(futures):
        host = futures[future]
        try:
            payload = future.result()
            rows = ((payload.get("data") or {}).get("diff")) or []
            if not rows:
                raise HTTPException(
                    status_code=502,
                    detail=f"{host} returned no industry-board rows.",
                )
            boards = []
            for row in rows:
                name = clean_value(row.get("f14"))
                if not name:
                    continue
                metadata = industry_name_metadata(name) if sector_type == "industry" else {}
                rise_count = _to_int(row.get("f104"))
                fall_count = _to_int(row.get("f105"))
                leader_symbol = clean_value(row.get("f140"))
                leader_name = clean_value(row.get("f128"))
                top_constituents = []
                if leader_symbol not in (None, "-") and leader_name not in (None, "-"):
                    top_constituents.append(
                        {
                            "symbol": str(leader_symbol).zfill(6),
                            "name": leader_name,
                            "change_pct": to_number(row.get("f136")),
                            "criterion": "highest_change_pct_reported_by_source",
                        }
                    )
                boards.append(
                    {
                        "symbol": clean_value(row.get("f12")),
                        "name": name,
                        "sector_type": sector_type,
                        "level": INDUSTRY_LEVEL_RANK.get(metadata.get("industry_level")),
                        **metadata,
                        "price": to_number(row.get("f2")),
                        "current": to_number(row.get("f2")),
                        "change_pct": to_number(row.get("f3")),
                        "change": to_number(row.get("f4")),
                        "turnover": to_number(row.get("f6")),
                        "rise_count": rise_count,
                        "fall_count": fall_count,
                        "flat_count": None,
                        "rise_ratio": (
                            round(rise_count / (rise_count + fall_count), 4)
                            if rise_count is not None and fall_count is not None and rise_count + fall_count
                            else None
                        ),
                        "rise_ratio_scope": "rising_vs_falling_constituents_only; flat count unavailable",
                        "limit_up_count": None,
                        "momentum_5m": None,
                        "momentum_15m": None,
                        "momentum_30m": None,
                        "momentum_status": "unavailable_from_current_public_snapshot",
                        "source_speed": to_number(row.get("f22")),
                        "high": to_number(row.get("f15")),
                        "low": to_number(row.get("f16")),
                        "open": to_number(row.get("f17")),
                        "previous_close": to_number(row.get("f18")),
                        "top_constituents": top_constituents,
                    }
                )
            if boards:
                for pending in futures:
                    if pending is future:
                        continue
                    pending.add_done_callback(consume_background_future)
                    pending.cancel()
                return boards
            errors.append(f"{host}: no usable industry-board rows")
        except HTTPException as exc:
            errors.append(f"{host}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def get_eastmoney_industry_boards(limit: int) -> list[dict[str, Any]]:
    boards = get_eastmoney_sector_boards("industry", max(limit * 4, 100))
    return deduplicate_industry_boards(boards, limit)


def industry_name_metadata(name: Any) -> dict[str, str | None]:
    text = str(name).strip() if name else ""
    match = INDUSTRY_LEVEL_PATTERN.fullmatch(text)
    if not match:
        return {"industry_name": text or None, "industry_level": None}
    return {
        "industry_name": match.group("industry_name"),
        "industry_level": match.group("industry_level"),
    }


def deduplicate_industry_boards(
    boards: list[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for board in boards:
        industry_name = str(board.get("industry_name") or board.get("name") or "")
        existing = selected.get(industry_name)
        if existing is None:
            selected[industry_name] = board
            continue

        current_rank = INDUSTRY_LEVEL_RANK.get(board.get("industry_level"), 99)
        existing_rank = INDUSTRY_LEVEL_RANK.get(existing.get("industry_level"), 99)
        if current_rank < existing_rank:
            selected[industry_name] = board
        elif current_rank == existing_rank:
            current_change = board.get("change_pct")
            existing_change = existing.get("change_pct")
            if (current_change if current_change is not None else float("-inf")) > (
                existing_change if existing_change is not None else float("-inf")
            ):
                selected[industry_name] = board

    return sorted(
        selected.values(),
        key=lambda item: item.get("change_pct") if item.get("change_pct") is not None else float("-inf"),
        reverse=True,
    )[:limit]


def get_calculated_industry_boards(limit: int) -> list[dict[str, Any]]:
    data = get_all_realtime_quotes()
    industry_column = next(
        (column for column in ("所属行业", "所处行业", "行业") if column in data.columns),
        None,
    )
    if not industry_column or "涨跌幅" not in data.columns:
        raise HTTPException(status_code=502, detail="Realtime quotes did not include industry data.")

    grouped: dict[str, list[float]] = {}
    for _, row in data.iterrows():
        industry = clean_value(row.get(industry_column))
        change_pct = to_number(row.get("涨跌幅"))
        if industry and change_pct is not None:
            grouped.setdefault(str(industry), []).append(change_pct)
    boards = [
        {
            "name": name,
            "sector_type": "industry",
            "level": None,
            "industry_name": name,
            "industry_level": None,
            "change_pct": round(sum(changes) / len(changes), 2),
            "average_change_pct": round(sum(changes) / len(changes), 2),
            "stock_count": len(changes),
        }
        for name, changes in grouped.items()
    ]
    boards.sort(key=lambda item: item["change_pct"], reverse=True)
    if not boards:
        raise HTTPException(status_code=502, detail="Realtime quotes produced no industry-board data.")
    return boards[:limit]


def get_eastmoney_market_quotes() -> list[dict[str, Any]]:
    page_size = 100

    def fetch_page(host: str, page: int) -> tuple[int, dict[str, Any]]:
        query = urlencode(
            {
                "pn": page,
                "pz": page_size,
                "po": 1,
                "np": 1,
                "fltt": 2,
                "invt": 2,
                "fid": "f3",
                "fs": MARKET_QUOTE_FS,
                "fields": MARKET_QUOTE_FIELDS,
            }
        )
        return (
            page,
            read_public_json(
                f"https://{host}/api/qt/clist/get?{query}",
                "https://quote.eastmoney.com/",
                3,
                1,
            ),
        )

    errors = []
    for host in (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    ):
        try:
            _, first_payload = fetch_page(host, 1)
            first_data = first_payload.get("data") or {}
            first_rows = first_data.get("diff") or []
            if not first_rows:
                raise HTTPException(status_code=502, detail=f"{host} returned no stock rows.")
            total = _to_int(first_data.get("total")) or len(first_rows)
            page_count = ceil(total / page_size)
            pages: dict[int, list[dict[str, Any]]] = {1: first_rows}
            if page_count > 1:
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = [
                        executor.submit(fetch_page, host, page)
                        for page in range(2, page_count + 1)
                    ]
                    for future in as_completed(futures):
                        page, payload = future.result()
                        rows = ((payload.get("data") or {}).get("diff")) or []
                        if not rows:
                            raise HTTPException(
                                status_code=502,
                                detail=f"{host} returned no stock rows for page {page}.",
                            )
                        pages[page] = rows

            raw_rows = [row for page in range(1, page_count + 1) for row in pages[page]]
            if len(raw_rows) < total:
                raise HTTPException(
                    status_code=502,
                    detail=f"{host} returned {len(raw_rows)} of {total} expected stock rows.",
                )
            return [
                {
                    "symbol": str(row.get("f12") or "").zfill(6),
                    "name": clean_value(row.get("f14")),
                    "price": to_number(row.get("f2")),
                    "change_pct": to_number(row.get("f3")),
                    "volume": to_number(row.get("f5")),
                    "turnover": to_number(row.get("f6")),
                    "turnover_rate": to_number(row.get("f8")),
                    "high": to_number(row.get("f15")),
                    "low": to_number(row.get("f16")),
                    "open": to_number(row.get("f17")),
                    "previous_close": to_number(row.get("f18")),
                    "total_market_value": to_number(row.get("f20")),
                    "source_updated_at": format_unix_market_time(row.get("f124")),
                    "market_time": market_time_from_source_update(
                        format_unix_market_time(row.get("f124"))
                    ),
                }
                for row in raw_rows
                if row.get("f12") and row.get("f14")
            ]
        except HTTPException as exc:
            errors.append(f"{host}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def get_sina_market_quotes() -> dict[str, Any]:
    count_url = (
        "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
        "Market_Center.getHQNodeStockCount?node=hs_a"
    )
    try:
        count_text = read_market_text(
            count_url,
            "https://vip.stock.finance.sina.com.cn/",
            timeout=3,
        )
    except OSError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch Sina market count: {exc}",
        ) from exc
    match = re.search(r"\d+", count_text)
    if match is None:
        raise HTTPException(status_code=502, detail="Unexpected Sina market-count response.")

    expected_count = int(match.group(0))
    page_size = 100
    page_count = ceil(expected_count / page_size)

    def fetch_page(page: int) -> tuple[int, list[dict[str, Any]]]:
        query = urlencode(
            {
                "page": page,
                "num": page_size,
                "sort": "changepercent",
                "asc": 0,
                "node": "hs_a",
                "symbol": "",
                "_s_r_a": "page",
            }
        )
        payload = read_public_json(
            "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            f"Market_Center.getHQNodeData?{query}",
            "https://vip.stock.finance.sina.com.cn/",
            timeout=3,
            attempts=1,
        )
        if not isinstance(payload, list):
            raise HTTPException(
                status_code=502,
                detail=f"Unexpected Sina market page {page} response.",
            )
        return page, payload

    executor = ThreadPoolExecutor(max_workers=20)
    futures = {executor.submit(fetch_page, page): page for page in range(1, page_count + 1)}
    done, pending = wait(futures, timeout=5)
    pages: dict[int, list[dict[str, Any]]] = {}
    errors: list[str] = []
    for future in done:
        page = futures[future]
        try:
            page_number, rows = future.result()
            pages[page_number] = rows
        except (HTTPException, OSError, ValueError, TypeError) as exc:
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            errors.append(f"sina page {page}: {detail}")
    for future in pending:
        future.cancel()
        errors.append(f"sina page {futures[future]}: request exceeded the 5 second budget")
    executor.shutdown(wait=False, cancel_futures=True)

    raw_rows = [row for page in sorted(pages) for row in pages[page]]
    minimum_acceptable_rows = ceil(expected_count * 0.8)
    if len(raw_rows) < minimum_acceptable_rows:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Sina returned only {len(raw_rows)} of about {expected_count} market rows; "
                + "; ".join(errors)
            ),
        )

    rows = [
        {
            "symbol": str(row.get("code") or "").zfill(6),
            "name": clean_value(row.get("name")),
            "price": to_number(row.get("trade")),
            "change_pct": to_number(row.get("changepercent")),
            "volume": to_number(row.get("volume")),
            "turnover": to_number(row.get("amount")),
            "turnover_rate": to_number(row.get("turnoverratio")),
            "high": to_number(row.get("high")),
            "low": to_number(row.get("low")),
            "open": to_number(row.get("open")),
            "previous_close": to_number(row.get("settlement")),
            "total_market_value": (
                to_number(row.get("mktcap")) * 10_000
                if to_number(row.get("mktcap")) is not None
                else None
            ),
            "market_time": None,
        }
        for row in raw_rows
        if row.get("code") and row.get("name")
    ]
    return {
        "rows": rows,
        "expected_count": expected_count,
        "returned_count": len(rows),
        "coverage_status": "complete" if not errors and len(raw_rows) >= expected_count else "partial",
        "source_errors": errors,
    }


def exchange_for_symbol(symbol: str) -> str | None:
    if symbol.startswith("6"):
        return "SSE"
    if symbol.startswith(("0", "2", "3")):
        return "SZSE"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return None


def is_st_security(name: Any) -> bool:
    normalized = str(name or "").strip().upper().lstrip("*")
    return normalized.startswith("ST")


def price_limit_pct(symbol: str, is_st: bool) -> float:
    if is_st:
        return 5.0
    if symbol.startswith(("300", "301", "688", "689")):
        return 20.0
    if exchange_for_symbol(symbol) == "BSE":
        return 30.0
    return 10.0


def empty_breadth_counts() -> dict[str, Any]:
    return {
        "stock_count": 0,
        "rise_count": 0,
        "fall_count": 0,
        "flat_count": 0,
        "rise_over_3_count": 0,
        "rise_over_5_count": 0,
        "rise_over_7_count": 0,
        "fall_over_3_count": 0,
        "fall_over_5_count": 0,
        "fall_over_7_count": 0,
        "limit_up_count": 0,
        "limit_down_count": 0,
        "open_board_count": 0,
        "consecutive_limit_up_count": None,
        "st_limit_up_count": 0,
        "st_limit_down_count": 0,
    }


def add_breadth_row(counts: dict[str, Any], row: dict[str, Any]) -> None:
    change_pct = row.get("change_pct")
    if change_pct is None:
        return
    counts["stock_count"] += 1
    if change_pct > 0:
        counts["rise_count"] += 1
    elif change_pct < 0:
        counts["fall_count"] += 1
    else:
        counts["flat_count"] += 1
    for threshold in (3, 5, 7):
        if change_pct > threshold:
            counts[f"rise_over_{threshold}_count"] += 1
        elif change_pct < -threshold:
            counts[f"fall_over_{threshold}_count"] += 1

    symbol = str(row["symbol"])
    is_st = is_st_security(row.get("name"))
    limit_pct = price_limit_pct(symbol, is_st)
    at_limit_up = change_pct >= limit_pct - 0.15
    at_limit_down = change_pct <= -limit_pct + 0.15
    if at_limit_up:
        counts["limit_up_count"] += 1
        if is_st:
            counts["st_limit_up_count"] += 1
    if at_limit_down:
        counts["limit_down_count"] += 1
        if is_st:
            counts["st_limit_down_count"] += 1

    previous_close = row.get("previous_close")
    high = row.get("high")
    if previous_close and high:
        high_change_pct = (high - previous_close) / previous_close * 100
        if high_change_pct >= limit_pct - 0.15 and not at_limit_up:
            counts["open_board_count"] += 1


def calculate_market_breadth(rows: list[dict[str, Any]]) -> dict[str, Any]:
    all_market = empty_breadth_counts()
    by_exchange = {exchange: empty_breadth_counts() for exchange in ("SSE", "SZSE", "BSE")}
    for row in rows:
        exchange = exchange_for_symbol(str(row.get("symbol") or ""))
        name = str(row.get("name") or "")
        if not exchange or "退" in name or row.get("price") is None:
            continue
        add_breadth_row(all_market, row)
        add_breadth_row(by_exchange[exchange], row)
    return {
        "scope": "Ordinary A shares only; excludes ETFs, funds, B shares, delisting-arrangement securities, and rows without a current price.",
        "all_market": all_market,
        "by_exchange": by_exchange,
        "consecutive_limit_up_status": "unavailable_without_a_historical_limit-up_pool",
    }


def parse_market_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return result.replace(tzinfo=MARKET_TIMEZONE) if result.tzinfo is None else result.astimezone(MARKET_TIMEZONE)


def market_status_at(now: datetime | None = None) -> str:
    now = now or datetime.now(MARKET_TIMEZONE)
    if now.weekday() >= 5:
        return "closed"
    minute = now.hour * 60 + now.minute
    if minute < 9 * 60 + 30:
        return "pre_open"
    if 9 * 60 + 30 <= minute < 11 * 60 + 30 or 13 * 60 <= minute < 15 * 60:
        return "open"
    if 11 * 60 + 30 <= minute < 13 * 60:
        return "lunch_break"
    return "closed"


def trading_minutes_elapsed(market_time: str | None) -> int | None:
    timestamp = parse_market_datetime(market_time)
    if timestamp is None:
        return None
    minute = timestamp.hour * 60 + timestamp.minute
    if minute < 9 * 60 + 30:
        return 0
    if minute <= 11 * 60 + 30:
        return minute - (9 * 60 + 30)
    if minute < 13 * 60:
        return 120
    if minute <= 15 * 60:
        return 120 + minute - (13 * 60)
    return 240


def market_turnover_summary(rows: list[dict[str, Any]], market_time: str | None) -> dict[str, Any]:
    eligible_rows = [
        row
        for row in rows
        if exchange_for_symbol(str(row.get("symbol") or ""))
        and "退" not in str(row.get("name") or "")
        and row.get("turnover") is not None
    ]
    by_exchange = {
        exchange: sum(
            row["turnover"]
            for row in eligible_rows
            if exchange_for_symbol(str(row["symbol"])) == exchange
        )
        for exchange in ("SSE", "SZSE", "BSE")
    }
    current = sum(row["turnover"] for row in eligible_rows)
    elapsed = trading_minutes_elapsed(market_time)
    estimated = round(current / elapsed * 240, 2) if elapsed and 0 < elapsed < 240 else current if elapsed == 240 else None
    top_rows = sorted(eligible_rows, key=lambda item: item["turnover"], reverse=True)[:10]
    return {
        "current": current,
        "unit": "CNY",
        "previous_trade_day_same_time": None,
        "change": None,
        "change_pct": None,
        "comparison_status": "unavailable_without_a_reliable_prior-day_market-wide_intraday_series",
        "estimated_full_day": estimated,
        "estimated_full_day_status": "mechanical_elapsed-time_extrapolation" if estimated is not None else "unavailable_before_open",
        "by_exchange": by_exchange,
        "top_turnover_securities": [
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "price": row["price"],
                "change_pct": row["change_pct"],
                "turnover": row["turnover"],
                "exchange": exchange_for_symbol(str(row["symbol"])),
            }
            for row in top_rows
        ],
        "scope": "Ordinary A shares only; this public all-market source does not include exchange-traded funds in the ranking.",
    }


LIMIT_POOL_CONFIG = {
    "limit_up": ("getTopicZTPool", "fbt:asc"),
    "open_board": ("getTopicZBPool", "fbt:asc"),
    "limit_down": ("getTopicDTPool", "fund:asc"),
}


def eastmoney_pool_price(value: Any) -> float | None:
    number = to_number(value)
    return round(number / 1000, 3) if number is not None else None


def format_pool_time(trade_date: str, value: Any) -> str | None:
    if value in (None, "", 0, "0"):
        return None
    digits = re.sub(r"\D", "", str(value)).zfill(6)[-6:]
    if not re.fullmatch(r"\d{6}", digits):
        return None
    try:
        timestamp = datetime.strptime(f"{trade_date}{digits}", "%Y%m%d%H%M%S").replace(
            tzinfo=MARKET_TIMEZONE
        )
    except ValueError:
        return None
    return timestamp.isoformat()


def fetch_eastmoney_limit_pool(pool_type: str, trade_date: str) -> dict[str, Any]:
    endpoint, sort = LIMIT_POOL_CONFIG[pool_type]
    query = urlencode(
        {
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "dpt": "wz.ztzt",
            "Pageindex": 0,
            "pagesize": 200,
            "sort": sort,
            "date": trade_date,
        }
    )
    payload = read_public_json(
        f"https://push2ex.eastmoney.com/{endpoint}?{query}",
        "https://quote.eastmoney.com/ztb/",
        timeout=5,
        attempts=1,
    )
    data = payload.get("data") or {}
    qdate = str(data.get("qdate") or "")
    if not re.fullmatch(r"\d{8}", qdate):
        raise HTTPException(status_code=404, detail=f"No {pool_type} pool for {trade_date}.")
    return {
        "pool_type": pool_type,
        "trade_date": qdate,
        "source_count": _to_int(data.get("tc")),
        "rows": data.get("pool") or [],
    }


def parse_limit_pool_item(
    pool_type: str, row: dict[str, Any], trade_date: str
) -> dict[str, Any] | None:
    symbol = str(row.get("c") or "").zfill(6)
    if not SYMBOL_PATTERN.fullmatch(symbol):
        return None
    common = {
        "symbol": symbol,
        "name": clean_value(row.get("n")),
        "exchange": exchange_for_symbol(symbol),
        "pool_type": pool_type,
        "price": eastmoney_pool_price(row.get("p")),
        "change_pct": to_number(row.get("zdp")),
        "turnover": to_number(row.get("amount")),
        "turnover_unit": "CNY",
        "circulating_market_value": to_number(row.get("ltsz")),
        "turnover_rate": to_number(row.get("hs")),
        "industry": clean_value(row.get("hybk")),
    }
    if pool_type == "limit_up":
        recent_stats = row.get("zttj") if isinstance(row.get("zttj"), dict) else {}
        return {
            **common,
            "consecutive_limit_up": _to_int(row.get("lbc")),
            "recent_limit_up_days": _to_int(recent_stats.get("days")),
            "recent_limit_up_count": _to_int(recent_stats.get("ct")),
            "first_seal_time": format_pool_time(trade_date, row.get("fbt")),
            "last_seal_time": format_pool_time(trade_date, row.get("lbt")),
            "seal_fund": to_number(row.get("fund")),
            "open_board_count": _to_int(row.get("zbc")),
        }
    if pool_type == "open_board":
        return {
            **common,
            "limit_up_price": eastmoney_pool_price(row.get("ztp")),
            "first_seal_time": format_pool_time(trade_date, row.get("fbt")),
            "open_board_count": _to_int(row.get("zbc")),
            "amplitude_pct": to_number(row.get("zf")),
            "consecutive_limit_up": _to_int((row.get("zttj") or {}).get("ct")),
        }
    return {
        **common,
        "consecutive_limit_down_days": _to_int(row.get("days")),
        "last_limit_down_time": format_pool_time(trade_date, row.get("lbt")),
        "open_count": _to_int(row.get("oc")),
        "sealed_order_amount": to_number(row.get("fba") or row.get("fund")),
    }


def limit_activity_statistics(
    limit_up_items: list[dict[str, Any]],
    open_board_items: list[dict[str, Any]],
    limit_down_items: list[dict[str, Any]],
) -> dict[str, Any]:
    limit_up_count = len(limit_up_items)
    open_board_count = len(open_board_items)
    limit_down_count = len(limit_down_items)
    attempts = limit_up_count + open_board_count
    consecutive = [
        item for item in limit_up_items if (item.get("consecutive_limit_up") or 0) >= 2
    ]
    board_distribution: dict[str, int] = {}
    for item in limit_up_items:
        boards = item.get("consecutive_limit_up") or 1
        key = "4_or_more" if boards >= 4 else str(boards)
        board_distribution[key] = board_distribution.get(key, 0) + 1
    return {
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "open_board_count": open_board_count,
        "seal_success_rate_pct": round(limit_up_count / attempts * 100, 2)
        if attempts
        else None,
        "consecutive_limit_up_count": len(consecutive),
        "max_consecutive_limit_up": max(
            (item.get("consecutive_limit_up") or 1 for item in limit_up_items),
            default=None,
        ),
        "limit_up_board_distribution": board_distribution,
        "st_limit_up_count": sum(
            is_st_security(item.get("name")) for item in limit_up_items
        ),
        "st_limit_down_count": sum(
            is_st_security(item.get("name")) for item in limit_down_items
        ),
        "total_seal_fund": sum(
            to_number(item.get("seal_fund")) or 0 for item in limit_up_items
        ),
        "currency_unit": "CNY",
    }


def limit_activity_by_exchange(
    limit_up_items: list[dict[str, Any]],
    open_board_items: list[dict[str, Any]],
    limit_down_items: list[dict[str, Any]],
) -> dict[str, dict[str, int]]:
    result = {
        exchange: {
            "limit_up_count": 0,
            "limit_down_count": 0,
            "open_board_count": 0,
            "consecutive_limit_up_count": 0,
            "st_limit_up_count": 0,
            "st_limit_down_count": 0,
        }
        for exchange in ("SSE", "SZSE", "BSE")
    }
    for key, items in (
        ("limit_up_count", limit_up_items),
        ("open_board_count", open_board_items),
        ("limit_down_count", limit_down_items),
    ):
        for item in items:
            exchange = item.get("exchange")
            if exchange in result:
                result[exchange][key] += 1
    for item in limit_up_items:
        exchange = item.get("exchange")
        if exchange in result and (item.get("consecutive_limit_up") or 0) >= 2:
            result[exchange]["consecutive_limit_up_count"] += 1
        if exchange in result and is_st_security(item.get("name")):
            result[exchange]["st_limit_up_count"] += 1
    for item in limit_down_items:
        exchange = item.get("exchange")
        if exchange in result and is_st_security(item.get("name")):
            result[exchange]["st_limit_down_count"] += 1
    return result


def get_limit_activity_data(limit: int) -> dict[str, Any]:
    last_errors: list[str] = []
    pool_results: dict[str, dict[str, Any]] = {}
    requested_date = None
    for offset in range(8):
        candidate = datetime.now(MARKET_TIMEZONE).date() - timedelta(days=offset)
        if candidate.weekday() >= 5:
            continue
        requested_date = candidate.strftime("%Y%m%d")
        executor = ThreadPoolExecutor(max_workers=3)
        futures = {
            executor.submit(fetch_eastmoney_limit_pool, pool_type, requested_date): pool_type
            for pool_type in LIMIT_POOL_CONFIG
        }
        pool_results = {}
        errors = []
        for future in as_completed(futures):
            pool_type = futures[future]
            try:
                pool_results[pool_type] = future.result()
            except HTTPException as exc:
                errors.append(f"{pool_type}: {exc.detail}")
            except Exception as exc:  # pragma: no cover - defensive source boundary
                errors.append(f"{pool_type}: {exc}")
        executor.shutdown(wait=False, cancel_futures=True)
        qdates = {result["trade_date"] for result in pool_results.values()}
        if qdates:
            last_errors = errors
            break
        last_errors.extend(errors)
    if not pool_results:
        raise HTTPException(
            status_code=502,
            detail="Eastmoney limit-activity pools unavailable: " + "; ".join(last_errors),
        )

    trade_date = max(result["trade_date"] for result in pool_results.values())
    parsed: dict[str, list[dict[str, Any]]] = {}
    source_counts: dict[str, int | None] = {}
    for pool_type in LIMIT_POOL_CONFIG:
        result = pool_results.get(pool_type) or {}
        source_counts[pool_type] = result.get("source_count")
        parsed[pool_type] = [
            item
            for row in result.get("rows", [])
            if (item := parse_limit_pool_item(pool_type, row, trade_date)) is not None
        ]

    parsed["limit_up"].sort(
        key=lambda item: (item.get("consecutive_limit_up") or 0, item.get("seal_fund") or 0),
        reverse=True,
    )
    parsed["open_board"].sort(
        key=lambda item: (item.get("open_board_count") or 0, item.get("turnover") or 0),
        reverse=True,
    )
    parsed["limit_down"].sort(
        key=lambda item: (
            item.get("consecutive_limit_down_days") or 0,
            item.get("sealed_order_amount") or 0,
        ),
        reverse=True,
    )
    statistics = limit_activity_statistics(
        parsed["limit_up"], parsed["open_board"], parsed["limit_down"]
    )
    return {
        "trade_date": datetime.strptime(trade_date, "%Y%m%d").date().isoformat(),
        "requested_date": requested_date,
        "statistics": statistics,
        "by_exchange": limit_activity_by_exchange(
            parsed["limit_up"], parsed["open_board"], parsed["limit_down"]
        ),
        "source_counts": source_counts,
        "limit_up_items": parsed["limit_up"][:limit],
        "open_board_items": parsed["open_board"][:limit],
        "limit_down_items": parsed["limit_down"][:limit],
        "source": ["eastmoney_limit_up_pool", "eastmoney_open_board_pool", "eastmoney_limit_down_pool"],
        "source_errors": last_errors,
        "data_status": "full_data" if len(pool_results) == 3 and not last_errors else "partial_data",
        "queried_at": now_iso(),
        "note": "Mechanical public limit-pool facts only. Counts, seal rate, and board height are not sentiment labels or trading signals.",
    }


def latest_market_time(indices: list[dict[str, Any]]) -> str | None:
    timestamps = [
        timestamp
        for index in indices
        if (timestamp := index.get("market_time") or index.get("source_updated_at"))
    ]
    return max(timestamps) if timestamps else None


def is_market_time_stale(
    market_time: str | None, now: datetime | None = None
) -> bool:
    now = now or datetime.now(MARKET_TIMEZONE)
    status = market_status_at(now)
    if status not in {"open", "lunch_break"}:
        return False
    source_time = parse_market_datetime(market_time)
    if source_time is None:
        return True
    if source_time.date() != now.date():
        return True
    freshness_reference = (
        now.replace(hour=11, minute=30, second=0, microsecond=0)
        if status == "lunch_break"
        else now
    )
    return freshness_reference - source_time > timedelta(minutes=10)


def get_cached_sector_board_component(sector_type: str) -> dict[str, Any]:
    return get_cached_component_with_stale(
        cache_key("sector_board_snapshot", {"sector_type": sector_type}),
        30,
        300,
        lambda: {
            "items": get_eastmoney_sector_boards(sector_type),
            "source_errors": [],
        },
    )


def get_sector_rankings_data(
    sector_type: str, level: str, sort_by: str, limit: int
) -> dict[str, Any]:
    normalized_type = sector_type.strip().lower()
    if normalized_type not in SECTOR_TYPE_CONFIG:
        raise HTTPException(status_code=400, detail="sector_type must be industry or concept.")
    normalized_level = str(level).strip().lower()
    if normalized_level not in {"1", "2", "3", "all"}:
        raise HTTPException(status_code=400, detail="level must be 1, 2, 3, or all.")
    if sort_by not in {"change_pct", "turnover", "momentum_5m", "momentum_15m", "momentum_30m"}:
        raise HTTPException(
            status_code=400,
            detail="sort_by must be change_pct, turnover, momentum_5m, momentum_15m, or momentum_30m.",
        )
    if sort_by.startswith("momentum_"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Minute momentum is not returned because the public board K-line source is not stable enough "
                "to calculate a complete ranking without gaps. Use change_pct or turnover."
            ),
        )

    board_component = get_cached_sector_board_component(normalized_type)
    boards = board_component.get("items") or []
    if normalized_type == "industry":
        boards = deduplicate_industry_boards(boards, len(boards))
        if normalized_level != "all":
            boards = [board for board in boards if board.get("level") == int(normalized_level)]
    boards.sort(
        key=lambda item: item.get(sort_by) if item.get(sort_by) is not None else float("-inf"),
        reverse=True,
    )
    items = boards[:limit]
    return {
        "sector_type": normalized_type,
        "level": normalized_level if normalized_type == "industry" else None,
        "sort_by": sort_by,
        "count": len(items),
        "items": items,
        "source": ["eastmoney_sector_snapshot"],
        "source_errors": normalize_source_errors(board_component.get("source_errors")),
        "queried_at": now_iso(),
        "note": "Mechanical ranking of public sector quotes only; no theme, trading, or investment judgement is generated.",
    }


def get_eastmoney_generic_daily_kline(secid: str, limit: int) -> dict[str, Any]:
    query = urlencode(
        {
            "secid": secid,
            "klt": 101,
            "fqt": 1,
            "lmt": limit,
            "beg": 0,
            "end": "20500101",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
    )
    errors = []
    # The delayed quote host identifies boards but currently returns an empty K-line
    # array. Avoid a second serial wait after the dedicated history route fails.
    for host in ("push2his.eastmoney.com",):
        try:
            payload = read_public_json(
                f"https://{host}/api/qt/stock/kline/get?{query}",
                "https://quote.eastmoney.com/",
                timeout=3,
                attempts=1,
            )
            data = payload.get("data") or {}
            rows = []
            for raw in data.get("klines") or []:
                values = str(raw).split(",")
                if len(values) < 11:
                    continue
                rows.append(
                    {
                        "date": clean_value(values[0]),
                        "open": to_number(values[1]),
                        "close": to_number(values[2]),
                        "high": to_number(values[3]),
                        "low": to_number(values[4]),
                        "volume": to_number(values[5]),
                        "turnover": to_number(values[6]),
                        "change_pct": to_number(values[8]),
                        "turnover_rate": to_number(values[10]),
                    }
                )
            if rows:
                return {"name": clean_value(data.get("name")), "items": rows, "source": host}
            errors.append(f"{host}: no daily rows")
        except HTTPException as exc:
            errors.append(f"{host}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def sector_history_cache_key(secid: str, limit: int) -> str:
    return cache_key("sector_daily_history", {"secid": secid, "limit": limit})


def get_cached_sector_daily_kline(secid: str, limit: int) -> dict[str, Any]:
    return get_cached_component_with_stale(
        sector_history_cache_key(secid, limit),
        300,
        3600,
        lambda: get_eastmoney_generic_daily_kline(secid, limit),
    )


def get_recent_sector_history_snapshot(
    secid: str, limit: int
) -> dict[str, Any] | None:
    snapshot = get_cached_tool_snapshot(sector_history_cache_key(secid, limit), 3600)
    if snapshot is None:
        return None
    data, cache = snapshot
    data["served_from_stale_cache"] = True
    data["stale_cache_age_seconds"] = cache["cache_age_seconds"]
    return data


def kline_lookback_returns(items: list[dict[str, Any]], lookbacks: list[int]) -> dict[str, float | None]:
    closes = [to_number(item.get("close")) for item in items]
    valid = [value for value in closes if value is not None]
    latest = valid[-1] if valid else None
    return {
        str(window): (
            round((latest / valid[-window - 1] - 1) * 100, 4)
            if latest is not None and len(valid) > window and valid[-window - 1] not in (None, 0)
            else None
        )
        for window in lookbacks
    }


def get_sector_rotation_data(
    sector_type: str,
    level: str,
    lookbacks: list[int],
    limit: int,
) -> dict[str, Any]:
    normalized_type = sector_type.strip().lower()
    if normalized_type not in SECTOR_TYPE_CONFIG:
        raise HTTPException(status_code=400, detail="sector_type must be industry or concept.")
    normalized_level = str(level).strip().lower()
    if normalized_level not in {"1", "2", "3", "all"}:
        raise HTTPException(status_code=400, detail="level must be 1, 2, 3, or all.")
    normalized_lookbacks = sorted({int(value) for value in lookbacks})
    if not normalized_lookbacks or any(value < 1 or value > 20 for value in normalized_lookbacks):
        raise HTTPException(status_code=400, detail="lookbacks must contain integers from 1 through 20.")

    board_component = get_cached_sector_board_component(normalized_type)
    boards = board_component.get("items") or []
    if normalized_type == "industry":
        boards = deduplicate_industry_boards(boards, len(boards))
        if normalized_level != "all":
            boards = [board for board in boards if board.get("level") == int(normalized_level)]
    candidate_count = min(max(limit * 2, 8), 16)
    by_change = sorted(
        boards,
        key=lambda item: (
            item.get("change_pct")
            if item.get("change_pct") is not None
            else float("-inf")
        ),
        reverse=True,
    )
    by_turnover = sorted(boards, key=lambda item: item.get("turnover") or 0, reverse=True)
    candidates = []
    seen_board_symbols = set()
    for board in [*by_change[: candidate_count // 2], *by_turnover[: candidate_count // 2]]:
        board_symbol = str(board.get("symbol") or "")
        if not board_symbol or board_symbol in seen_board_symbols:
            continue
        seen_board_symbols.add(board_symbol)
        candidates.append(board)
    history_results: dict[str, dict[str, Any]] = {}
    source_errors: list[str | dict[str, Any]] = normalize_source_errors(
        board_component.get("source_errors")
    )
    history_limit = max(normalized_lookbacks) + 6
    history_route = "eastmoney_sector_daily_history"
    route_degraded = preferred_route_is_temporarily_degraded(
        history_route, min_consecutive_failures=1, cooldown_seconds=600
    )
    first_board_symbol = (
        str(candidates[0].get("symbol")) if candidates else None
    )
    initial_loaders: dict[str, Any] = {
        "market_turnover": lambda: get_cached_component_with_stale(
            cache_key("sector_rotation_market_turnover", {}),
            15,
            300,
            get_overview_breadth_component,
        )
    }
    if not route_degraded and first_board_symbol:
        initial_loaders.update(
            {
                "benchmark_history": lambda: get_cached_sector_daily_kline(
                    "1.000300", history_limit
                ),
                f"sector_history:{first_board_symbol}": lambda: get_cached_sector_daily_kline(
                    f"90.{first_board_symbol}", history_limit
                ),
            }
        )
    initial_results, _, initial_errors = collect_components(
        initial_loaders, 4, COMPOSITE_TOOL_EXECUTOR
    )
    source_errors.extend(
        f"{error['source']}: {error['message']}" for error in initial_errors
    )
    market_component = initial_results.get("market_turnover") or {}
    total_market_turnover = to_number(
        (market_component.get("turnover") or {}).get("current")
    )
    benchmark_history = initial_results.get("benchmark_history") or {
        "items": [],
        "source": None,
    }
    if first_board_symbol and initial_results.get(f"sector_history:{first_board_symbol}"):
        history_results[first_board_symbol] = initial_results[
            f"sector_history:{first_board_symbol}"
        ]

    board_probe = history_results.get(first_board_symbol or "", {})
    live_probe_succeeded = bool(
        board_probe.get("items") and not board_probe.get("served_from_stale_cache")
    )
    if first_board_symbol and not route_degraded:
        record_preferred_route_health(
            history_route,
            live_probe_succeeded,
            None if live_probe_succeeded else "Representative board history route failed.",
        )
    if live_probe_succeeded:
        remaining_loaders = {
            f"sector_history:{board['symbol']}": lambda board=board: get_cached_sector_daily_kline(
                f"90.{board['symbol']}", history_limit
            )
            for board in candidates[1:]
            if board.get("symbol")
        }
        remaining_results, _, remaining_errors = collect_components(
            remaining_loaders, 4, COMPOSITE_TOOL_EXECUTOR
        )
        source_errors.extend(
            f"{error['source']}: {error['message']}" for error in remaining_errors
        )
        for key, payload in remaining_results.items():
            history_results[key.split(":", 1)[1]] = payload
    elif first_board_symbol:
        route_degraded = True
        source_errors.append(
            "sector_history: public board-history route is temporarily bypassed after a failed representative probe"
        )

    if route_degraded:
        benchmark_history = (
            get_recent_sector_history_snapshot("1.000300", history_limit)
            or benchmark_history
        )
        for board in candidates:
            board_symbol = str(board.get("symbol") or "")
            if not board_symbol or board_symbol in history_results:
                continue
            snapshot = get_recent_sector_history_snapshot(
                f"90.{board_symbol}", history_limit
            )
            if snapshot:
                history_results[board_symbol] = snapshot

    benchmark_returns = kline_lookback_returns(
        benchmark_history.get("items", []), normalized_lookbacks
    )
    items = []
    for board in candidates:
        history = history_results.get(str(board.get("symbol")), {"items": []})
        rows = history.get("items", [])
        returns = kline_lookback_returns(rows, normalized_lookbacks)
        relative_returns = {
            window: (
                round(value - benchmark_returns[window], 4)
                if value is not None and benchmark_returns.get(window) is not None
                else None
            )
            for window, value in returns.items()
        }
        recent_changes = [to_number(row.get("change_pct")) for row in rows[-5:]]
        recent_changes = [value for value in recent_changes if value is not None]
        recent_closes = [to_number(row.get("close")) for row in rows[-20:]]
        recent_closes = [value for value in recent_closes if value is not None]
        item = deepcopy(board)
        item.update(
            {
                "returns_pct": returns,
                "benchmark_returns_pct": benchmark_returns,
                "relative_to_csi300_pct": relative_returns,
                "positive_sessions_last_5": sum(value > 0 for value in recent_changes) if recent_changes else None,
                "available_sessions_last_5": len(recent_changes),
                "at_20_session_closing_high": (
                    recent_closes[-1] == max(recent_closes) if recent_closes else None
                ),
                "history_status": "available" if rows else "unavailable",
                "history_source": history.get("source"),
                "history_is_stale": bool(history.get("served_from_stale_cache")),
                "history_cache_age_seconds": history.get("stale_cache_age_seconds"),
                "turnover_share_of_a_share_market_pct": (
                    round((to_number(board.get("turnover")) or 0) / total_market_turnover * 100, 6)
                    if total_market_turnover not in (None, 0) and to_number(board.get("turnover")) is not None
                    else None
                ),
                "leader_continuity": None,
                "leader_continuity_status": "unavailable_without_reliable_daily_constituent-leader_history",
            }
        )
        items.append(item)

    ranking_window = "5" if "5" in {str(value) for value in normalized_lookbacks} else str(normalized_lookbacks[0])
    items.sort(
        key=lambda item: (
            item["relative_to_csi300_pct"].get(ranking_window)
            if item["relative_to_csi300_pct"].get(ranking_window) is not None
            else item.get("change_pct")
            if item.get("change_pct") is not None
            else float("-inf")
        ),
        reverse=True,
    )
    items = items[:limit]
    histories_available = sum(item["history_status"] == "available" for item in items)
    return {
        "sector_type": normalized_type,
        "level": normalized_level if normalized_type == "industry" else None,
        "lookbacks": normalized_lookbacks,
        "benchmark": {"identifier": "index:000300", "name": "CSI 300", "returns_pct": benchmark_returns},
        "a_share_market_turnover": total_market_turnover,
        "a_share_market_turnover_unit": "CNY" if total_market_turnover is not None else None,
        "ranking_basis": f"{ranking_window}-session relative return when available, otherwise current change_pct",
        "count": len(items),
        "history_available_count": histories_available,
        "items": items,
        "source": sorted(
            {
                "eastmoney_sector_snapshot",
                *[str(item.get("history_source")) for item in items if item.get("history_source")],
                *([str(benchmark_history.get("source"))] if benchmark_history.get("source") else []),
                *([str(market_component.get("source"))] if market_component.get("source") else []),
            }
        ),
        "source_errors": source_errors,
        "data_status": (
            "full_data"
            if histories_available == len(items)
            and all(value is not None for value in benchmark_returns.values())
            else "partial_data"
        ),
        "queried_at": now_iso(),
        "note": "Rotation fields are mechanical multi-session returns, breadth, turnover, and persistence facts. Missing board history or leader continuity remains explicit and is not inferred.",
    }


OVERNIGHT_SINA_INSTRUMENTS = {
    "shanghai_copper_continuous": ("nf_CU0", "domestic_futures"),
    "lme_copper": ("hf_CAD", "global_futures"),
    "comex_copper": ("hf_HG", "global_futures"),
    "usd_cny_onshore": ("fx_susdcny", "fx"),
    "nasdaq_100_futures": ("hf_NQ", "global_futures"),
    "ftse_china_a50_futures": ("hf_CHA50CFD", "global_futures"),
    "hang_seng_index_futures": ("hf_HSI", "global_futures"),
    "nasdaq_composite": ("gb_ixic", "global_index"),
}

OVERNIGHT_INSTRUMENT_METADATA = {
    "shanghai_copper_continuous": {
        "venue": "SHFE",
        "instrument_type": "futures_continuous_quote",
        "quote_currency": "CNY",
        "price_unit": "CNY_per_metric_tonne",
        "contract_size": 5,
        "contract_size_unit": "metric_tonne",
        "contract_spec_status": "standard_contract_spec_continuous_provider_quote",
        "contract_spec_source": "https://www.shfe.com.cn/regulation/exchangerules/productrules/202512/t20251231_829954.html",
    },
    "lme_copper": {
        "venue": "LME",
        "instrument_type": "futures_provider_quote",
        "quote_currency": "USD",
        "price_unit": "USD_per_metric_tonne",
        "contract_size": 25,
        "contract_size_unit": "metric_tonne",
        "contract_spec_status": "standard_contract_spec_provider_code_has_no_expiry",
        "contract_spec_source": "https://www.lme.com/en/metals/non-ferrous/lme-copper",
    },
    "comex_copper": {
        "venue": "COMEX",
        "instrument_type": "futures_provider_quote",
        "quote_currency": "USD",
        "price_unit": "US_cent_per_pound",
        "contract_size": 25_000,
        "contract_size_unit": "pound",
        "contract_spec_status": "standard_HG_contract_spec_provider_code_has_no_expiry",
        "contract_spec_source": "https://www.cmegroup.com/trading/metals/files/copper-futures-and-options.pdf",
    },
    "usd_cny_onshore": {
        "venue": "onshore_fx",
        "instrument_type": "foreign_exchange",
        "base_currency": "USD",
        "quote_currency": "CNY",
        "price_unit": "CNY_per_USD",
        "contract_spec_status": "not_applicable",
    },
    "nasdaq_100_futures": {
        "venue": "provider_global_futures_feed",
        "instrument_type": "index_futures_provider_quote",
        "quote_currency": "USD",
        "price_unit": "index_points",
        "contract_spec_status": "provider_code_does_not_identify_expiry_or_multiplier",
    },
    "ftse_china_a50_futures": {
        "venue": "provider_global_futures_feed",
        "instrument_type": "index_futures_provider_quote",
        "quote_currency": "USD",
        "price_unit": "index_points",
        "contract_spec_status": "provider_code_does_not_identify_expiry_or_multiplier",
    },
    "hang_seng_index_futures": {
        "venue": "provider_global_futures_feed",
        "instrument_type": "index_futures_provider_quote",
        "quote_currency": "HKD",
        "price_unit": "index_points",
        "contract_spec_status": "provider_code_does_not_identify_expiry_or_multiplier",
    },
    "nasdaq_composite": {
        "venue": "NASDAQ",
        "instrument_type": "cash_index",
        "quote_currency": None,
        "price_unit": "index_points",
        "contract_spec_status": "not_applicable",
    },
}


def parse_sina_overnight_record(identifier: str, category: str, values: list[str]) -> dict[str, Any] | None:
    if not values or not any(str(value).strip() for value in values):
        return None
    if category == "domestic_futures" and len(values) >= 18:
        current = to_number(values[8])
        previous = to_number(values[10])
        clock = values[1]
        if re.fullmatch(r"\d{6}", clock):
            clock = f"{clock[:2]}:{clock[2:4]}:{clock[4:]}"
        return {
            "name": clean_value(values[0]),
            "price": current,
            "change": round(current - previous, 6) if current is not None and previous is not None else None,
            "change_pct": round((current / previous - 1) * 100, 4) if current is not None and previous not in (None, 0) else None,
            "market_time": format_market_time(f"{values[17]} {clock}") if values[17] and clock else None,
            "previous_reference": previous,
            "reference_type": "previous_settlement",
        }
    if category == "global_futures" and len(values) >= 14:
        current = to_number(values[0])
        previous = to_number(values[7])
        return {
            "name": clean_value(values[13]),
            "price": current,
            "change": round(current - previous, 6) if current is not None and previous is not None else None,
            "change_pct": round((current / previous - 1) * 100, 4) if current is not None and previous not in (None, 0) else None,
            "market_time": format_market_time(f"{values[12]} {values[6]}") if values[12] and values[6] else None,
            "previous_reference": previous,
            "reference_type": "previous_settlement",
        }
    if category == "fx" and len(values) >= 18:
        return {
            "name": clean_value(values[9]),
            "price": to_number(values[1]),
            "change": to_number(values[11]),
            "change_pct": to_number(values[10]),
            "market_time": format_market_time(f"{values[17]} {values[0]}") if values[17] and values[0] else None,
            "previous_reference": to_number(values[8]),
            "reference_type": "provider_reference",
        }
    if category == "global_index" and len(values) >= 5:
        return {
            "name": clean_value(values[0]),
            "price": to_number(values[1]),
            "change": to_number(values[4]),
            "change_pct": to_number(values[2]),
            "market_time": format_market_time(values[3]),
            "previous_reference": to_number(values[25]) if len(values) > 25 else None,
            "reference_type": "previous_close",
        }
    return None


def get_sina_overnight_observations() -> dict[str, dict[str, Any]]:
    codes = ",".join(code for code, _ in OVERNIGHT_SINA_INSTRUMENTS.values())
    try:
        text = read_market_text(
            f"https://hq.sinajs.cn/list={codes}",
            "https://finance.sina.com.cn/",
            timeout=5,
        )
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Sina overnight quote route unavailable: {exc}") from exc
    raw_by_code = {
        match.group(1): match.group(2).split(",")
        for match in re.finditer(r'var hq_str_([^=]+)="([^"]*)";', text)
    }
    observations = {}
    for name, (code, category) in OVERNIGHT_SINA_INSTRUMENTS.items():
        parsed = parse_sina_overnight_record(name, category, raw_by_code.get(code, []))
        if parsed:
            observations[name] = {
                "identifier": name,
                "provider_code": code,
                "market_group": category,
                **OVERNIGHT_INSTRUMENT_METADATA.get(name, {}),
                **parsed,
                "source": "sina_public_quote",
            }
    if not observations:
        raise HTTPException(status_code=502, detail="Sina returned no usable overnight observations.")
    return observations


def get_overnight_risk_packet_data(detail_level: str) -> dict[str, Any]:
    observation_component = get_cached_component_with_stale(
        cache_key("overnight_observations", {}),
        15,
        1800,
        lambda: {
            "observations": get_sina_overnight_observations(),
            "source_errors": [],
        },
    )
    observations = observation_component.get("observations") or {}
    required = [
        *OVERNIGHT_SINA_INSTRUMENTS.keys(),
        "us_dollar_index",
        "us_10_year_treasury_yield",
    ]
    missing = [identifier for identifier in required if identifier not in observations]
    latest_times = [
        parsed
        for item in observations.values()
        if (parsed := parse_market_datetime(item.get("market_time"))) is not None
    ]
    latest_time = max(latest_times).isoformat() if latest_times else None
    items = list(observations.values())
    if detail_level == "summary":
        items = [
            {
                key: item.get(key)
                for key in (
                    "identifier",
                    "name",
                    "provider_code",
                    "venue",
                    "instrument_type",
                    "price",
                    "price_unit",
                    "quote_currency",
                    "contract_size",
                    "contract_size_unit",
                    "contract_spec_status",
                    "contract_spec_source",
                    "change",
                    "change_pct",
                    "market_time",
                    "reference_type",
                    "source",
                )
            }
            for item in items
        ]
    return {
        "market_time": latest_time,
        "count": len(items),
        "items": items,
        "missing_fields": missing,
        "coverage": {
            "requested_count": len(required),
            "available_count": len(observations),
            "unavailable_count": len(missing),
        },
        "source": ["sina_public_quote"],
        "source_errors": [
            {
                "source": "current_free_public_routes",
                "error_type": "instrument_unavailable",
                "message": f"No currently validated free quote route for {identifier}.",
            }
            for identifier in missing
        ]
        + normalize_source_errors(observation_component.get("source_errors")),
        "data_status": "full_data" if not missing else "partial_data",
        "detail_level": detail_level,
        "queried_at": now_iso(),
        "note": "Latest cross-market observations only. Different venues have different sessions and timestamps; missing dollar-index or Treasury-yield data is not estimated, and no next-day A-share direction is inferred.",
    }


IPO_CALENDAR_API = "https://datacenter-web.eastmoney.com/api/data/v1/get"
IPO_CALENDAR_FAST_API = "https://datapc.eastmoney.com/da/purchase/list2"
IPO_RULE_SOURCES = {
    "sse": "https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/issue/c/c_20250515_10778982.shtml",
    "szse": "https://www.szse.cn/ipo/guide/rule/P020190228664910292959.pdf",
    "bse": "https://www.bse.cn/fxrz_list/200010917.html",
}
IPO_CALENDAR_COLUMNS = (
    "SECURITY_CODE,SECURITY_NAME,SECURITY_NAME_ABBR,APPLY_CODE,APPLY_DATE,"
    "LISTING_DATE,TRADE_MARKET,MARKET,MARKET_TYPE_NEW,BALLOT_PAY_DATE,"
    "ASSIGN_DATE,BALLOT_NUM_DATE,RESULT_NOTICE_DATE,ISSUE_PRICE,"
    "ONLINE_APPLY_PRICE,ONLINE_APPLY_UPPER,ONLINE_ISSUE_NUM,EACHBALLOT_SHARES,"
    "ISSUE_WAY,MARKET_CAP_CONFIRMDATE,INFO_CODE,MAIN_BUSINESS,ISSUE_STATE,"
    "IS_BEIJING,UP_DATE"
)


def normalize_ipo_query(symbol_or_name: str | None) -> str | None:
    if symbol_or_name is None:
        return None
    normalized = str(symbol_or_name).strip()
    if not normalized:
        return None
    if len(normalized) > 30 or not re.fullmatch(r"[0-9A-Za-z\u4e00-\u9fff·()（）-]+", normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol_or_name must be an exact six-digit IPO code or a short company name.",
        )
    return normalized


def ipo_calendar_date(value: Any) -> str | None:
    text = str(value or "").strip()[:10]
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def normalize_fast_ipo_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    security_code = str(row.get("SECURITY_CODE") or "")
    trade_market = str(row.get("TRADE_MARKET") or "")
    if security_code.startswith("688"):
        board = "科创板"
    elif security_code.startswith(("300", "301")):
        board = "创业板"
    elif int(to_number(row.get("IS_BEIJING")) or 0) == 1:
        board = "北交所"
    else:
        board = ""
    normalized["SECURITY_NAME_ABBR"] = row.get("SECURITY_NAME_ABBR") or row.get(
        "SECURITY_NAME"
    )
    normalized["MARKET"] = " ".join(value for value in (trade_market, board) if value)
    normalized["LISTING_DATE"] = row.get("LISTING_DATE") or row.get(
        "SELECT_LISTING_DATE"
    )
    normalized["ONLINE_APPLY_UPPER"] = row.get("ONLINE_APPLY_UPPER") or row.get(
        "APPLY_NUM_UPPER"
    )
    return normalized


def ipo_row_matches_query(row: dict[str, Any], query: str) -> bool:
    return query in {
        str(row.get("SECURITY_CODE") or "").strip(),
        str(row.get("APPLY_CODE") or "").strip(),
        str(row.get("SECURITY_NAME_ABBR") or "").strip(),
        str(row.get("SECURITY_NAME") or "").strip(),
    }


def get_fast_ipo_calendar_rows(fetch_limit: int) -> list[dict[str, Any]]:
    normalized_limit = max(20, min(fetch_limit, 100))
    key = cache_key("ipo_recent_calendar_component", {"limit": normalized_limit})

    def load() -> dict[str, Any]:
        payload = read_public_json(
            f"{IPO_CALENDAR_FAST_API}?{urlencode({'stat': 0, 'st': 'APPLY_DATE', 'sr': -1, 'p': 1, 'ps': normalized_limit})}",
            "https://datapc.eastmoney.com/da/Purchase/Index?color=w",
            timeout=2,
            attempts=1,
        )
        if not isinstance(payload, dict) or payload.get("success") is False:
            raise HTTPException(status_code=502, detail="Unexpected fast IPO calendar response.")
        result = payload.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("data"), list):
            raise HTTPException(status_code=502, detail="Unexpected fast IPO calendar response.")
        return {
            "rows": [
                normalize_fast_ipo_row(row)
                for row in result["data"]
                if isinstance(row, dict)
            ]
        }

    component, _ = get_cached_tool_data(key, 60, load)
    return component["rows"]


def get_datacenter_ipo_calendar_rows(
    symbol_or_name: str | None, fetch_limit: int
) -> list[dict[str, Any]]:
    query = normalize_ipo_query(symbol_or_name)
    parameters: dict[str, Any] = {
        "reportName": "RPTA_APP_IPOAPPLY",
        "columns": IPO_CALENDAR_COLUMNS,
        "sortColumns": "APPLY_DATE",
        "sortTypes": -1,
        "pageSize": max(20, min(fetch_limit, 100)),
        "pageNumber": 1,
    }
    if query:
        field = "SECURITY_CODE" if SYMBOL_PATTERN.fullmatch(query) else "SECURITY_NAME_ABBR"
        parameters["filter"] = f'({field}="{query}")'

    def load_rows() -> list[Any]:
        payload = read_public_json(
            f"{IPO_CALENDAR_API}?{urlencode(parameters)}",
            "https://data.eastmoney.com/xg/xg/default.html",
            timeout=3,
            attempts=1,
        )
        if not isinstance(payload, dict):
            raise HTTPException(status_code=502, detail="Unexpected public IPO calendar response.")
        if payload.get("success") is False:
            if "数据为空" in str(payload.get("message") or ""):
                return []
            raise HTTPException(status_code=502, detail="Unexpected public IPO calendar response.")
        result = payload.get("result")
        if result is None:
            return []
        if not isinstance(result, dict):
            raise HTTPException(status_code=502, detail="Unexpected public IPO calendar response.")
        return result.get("data") or []

    rows = load_rows()
    if query and SYMBOL_PATTERN.fullmatch(query) and not rows:
        parameters["filter"] = f'(APPLY_CODE="{query}")'
        rows = load_rows()
    if not isinstance(rows, list):
        raise HTTPException(status_code=502, detail="IPO calendar rows had an unexpected format.")
    return [row for row in rows if isinstance(row, dict)]


def get_ipo_calendar_route(
    symbol_or_name: str | None, fetch_limit: int
) -> dict[str, Any]:
    query = normalize_ipo_query(symbol_or_name)
    source_errors: list[str] = []
    try:
        recent_rows = get_fast_ipo_calendar_rows(100)
        matched_rows = (
            [row for row in recent_rows if ipo_row_matches_query(row, query)]
            if query
            else recent_rows[:fetch_limit]
        )
        if matched_rows or query is None:
            return {
                "rows": matched_rows[:fetch_limit],
                "source": "eastmoney_datapc_ipo_calendar",
                "source_errors": [],
            }
    except HTTPException as exc:
        source_errors.append(f"eastmoney_datapc_ipo_calendar: {exc.detail}")

    rows = get_datacenter_ipo_calendar_rows(query, fetch_limit)
    return {
        "rows": rows,
        "source": "eastmoney_datacenter_ipo_calendar",
        "source_errors": source_errors,
    }


def get_eastmoney_ipo_calendar_rows(
    symbol_or_name: str | None, fetch_limit: int
) -> list[dict[str, Any]]:
    return get_ipo_calendar_route(symbol_or_name, fetch_limit)["rows"]


def ipo_subscription_stage(row: dict[str, Any], today: str) -> str:
    apply_date = ipo_calendar_date(row.get("APPLY_DATE"))
    assign_date = ipo_calendar_date(row.get("ASSIGN_DATE"))
    ballot_date = ipo_calendar_date(row.get("BALLOT_NUM_DATE"))
    payment_date = ipo_calendar_date(row.get("BALLOT_PAY_DATE") or row.get("ONLINE_PAY_DATE"))
    listing_date = ipo_calendar_date(row.get("LISTING_DATE"))
    if listing_date and listing_date <= today:
        return "listed"
    if payment_date == today:
        return "ballot_result_and_payment_today"
    if ballot_date == today:
        return "ballot_result_scheduled_today"
    if assign_date == today:
        return "allocation_numbering_today"
    if apply_date == today:
        return "subscription_open_today"
    if apply_date and apply_date > today:
        return "subscription_scheduled"
    if listing_date and listing_date > today:
        return "listing_scheduled"
    if payment_date and payment_date < today:
        return "listing_date_pending"
    if apply_date and apply_date < today:
        return "post_subscription_schedule_pending"
    return "schedule_pending"


def ipo_market_rules(row: dict[str, Any]) -> dict[str, Any]:
    market = str(row.get("MARKET") or row.get("TRADE_MARKET") or "")
    is_beijing = "北交所" in market or int(to_number(row.get("IS_BEIJING")) or 0) == 1
    if is_beijing:
        return {
            "exchange": "BSE",
            "required_permission": "bse_qualified_investor_permission",
            "subscription_method": "full_cash_subscription",
            "minimum_same_market_average_value_cny": None,
            "market_value_per_subscription_unit_cny": None,
            "market_value_calculation": None,
            "eligible_securities_scope": None,
            "subscription_unit_shares": int(to_number(row.get("EACHBALLOT_SHARES")) or 100),
            "funding_timing": "full_subscription_cash_required_on_subscription_day",
            "official_rule_url": IPO_RULE_SOURCES["bse"],
        }
    exchange = "SSE" if "上交所" in market or str(row.get("SECURITY_CODE") or "").startswith("6") else "SZSE"
    required_permission = "corresponding_a_share_market_account"
    if "科创板" in market:
        required_permission = "star_market_trading_permission"
    elif "创业板" in market:
        required_permission = "chinext_trading_permission"
    return {
        "exchange": exchange,
        "required_permission": required_permission,
        "subscription_method": "same_market_value_subscription",
        "minimum_same_market_average_value_cny": 10000,
        "market_value_per_subscription_unit_cny": 5000,
        "market_value_calculation": "average eligible same-market holdings over the 20 trading days through T-2",
        "eligible_securities_scope": "same-market non-restricted A shares and non-restricted depositary receipts; ETFs, funds, bonds, B shares and other products are excluded",
        "subscription_unit_shares": 500,
        "funding_timing": "no_upfront_subscription_cash; winning_allocation_payment_on_published_payment_date",
        "official_rule_url": IPO_RULE_SOURCES["sse" if exchange == "SSE" else "szse"],
    }


def build_ipo_subscription_item(row: dict[str, Any], detail_level: str) -> dict[str, Any]:
    today = datetime.now(MARKET_TIMEZONE).date().isoformat()
    rules = ipo_market_rules(row)
    issue_price = to_number(row.get("ISSUE_PRICE") or row.get("ONLINE_APPLY_PRICE"))
    online_apply_upper = to_number(row.get("ONLINE_APPLY_UPPER"))
    unit_shares = int(rules["subscription_unit_shares"])
    pending_fields = [
        field
        for field, value in (
            ("issue_price", issue_price),
            ("listing_date", ipo_calendar_date(row.get("LISTING_DATE"))),
            ("online_subscription_limit_shares", online_apply_upper),
        )
        if value is None
    ]
    item = {
        "security_code": clean_value(row.get("SECURITY_CODE")),
        "security_name": clean_value(row.get("SECURITY_NAME_ABBR") or row.get("SECURITY_NAME")),
        "subscription_code": clean_value(row.get("APPLY_CODE")),
        "market": clean_value(row.get("MARKET") or row.get("TRADE_MARKET"))
        or ({"SSE": "上海证券交易所", "SZSE": "深圳证券交易所", "BSE": "北京证券交易所"}[rules["exchange"]]),
        "subscription_stage": ipo_subscription_stage(row, today),
        "subscription_date": ipo_calendar_date(row.get("APPLY_DATE")),
        "allocation_number_date": ipo_calendar_date(row.get("ASSIGN_DATE")),
        "ballot_result_date": ipo_calendar_date(row.get("BALLOT_NUM_DATE") or row.get("RESULT_NOTICE_DATE")),
        "payment_date": ipo_calendar_date(row.get("BALLOT_PAY_DATE") or row.get("ONLINE_PAY_DATE")),
        "listing_date": ipo_calendar_date(row.get("LISTING_DATE")),
        "issue_price_cny": issue_price,
        "online_subscription_limit_shares": int(online_apply_upper) if online_apply_upper is not None else None,
        "subscription_unit_shares": unit_shares,
        "maximum_subscription_cash_cny": (
            round(online_apply_upper * issue_price, 2)
            if online_apply_upper is not None and issue_price is not None
            else None
        ),
        "maximum_subscription_market_value_requirement_cny": (
            int(online_apply_upper / unit_shares * rules["market_value_per_subscription_unit_cny"])
            if online_apply_upper is not None
            and rules["market_value_per_subscription_unit_cny"] is not None
            else None
        ),
        "eligibility_rules": rules,
        "personal_eligibility_status": "not_evaluated_without_brokerage_account_and_permissions",
        "pending_fields": pending_fields,
        "source_updated_at": ipo_calendar_date(row.get("UP_DATE")),
    }
    if detail_level == "raw":
        item.update(
            {
                "online_issue_shares": int(to_number(row.get("ONLINE_ISSUE_NUM")) or 0) or None,
                "issue_method": clean_value(row.get("ISSUE_WAY")),
                "main_business": clean_value(row.get("MAIN_BUSINESS")),
                "provider_information_code": clean_value(row.get("INFO_CODE")),
                "provider_issue_state": clean_value(row.get("ISSUE_STATE")),
            }
        )
    return item


def get_ipo_subscription_status_data(
    symbol_or_name: str | None,
    days_ahead: int,
    days_back: int,
    limit: int,
    detail_level: str,
) -> dict[str, Any]:
    query = normalize_ipo_query(symbol_or_name)
    route = get_ipo_calendar_route(query, 100 if query is None else limit)
    rows = route["rows"]
    today = datetime.now(MARKET_TIMEZONE).date()
    range_start = (today - timedelta(days=days_back)).isoformat()
    range_end = (today + timedelta(days=days_ahead)).isoformat()
    if query:
        selected = rows[:limit]
    else:
        selected = [
            row
            for row in rows
            if (apply_date := ipo_calendar_date(row.get("APPLY_DATE")))
            and range_start <= apply_date <= range_end
        ][:limit]
    if query and not selected:
        raise HTTPException(status_code=404, detail=f"No public IPO calendar record found for {query}.")
    items = [build_ipo_subscription_item(row, detail_level) for row in selected]
    pending_fields = sorted(
        {
            f"{item.get('security_code') or item.get('security_name')}:{field}"
            for item in items
            for field in item.get("pending_fields") or []
        }
    )
    source_update_dates = sorted(
        str(item["source_updated_at"])
        for item in items
        if item.get("source_updated_at")
    )
    return {
        "query": query,
        "schedule_range": {"start": range_start, "end": range_end} if query is None else None,
        "count": len(items),
        "items": items,
        "personalized_eligibility": "not_available_without_brokerage_account_market_value_and_board_permissions",
        "rule_source_urls": sorted({item["eligibility_rules"]["official_rule_url"] for item in items}),
        "source": [route["source"]],
        "source_updated_at": source_update_dates[-1] if source_update_dates else None,
        "source_errors": normalize_source_errors(route.get("source_errors")),
        "missing_fields": pending_fields,
        "detail_level": detail_level,
        "data_status": "partial_data" if pending_fields else "full_data",
        "queried_at": now_iso(),
        "note": "Public schedule and rule facts only. Issue price, listing date, and issuer limits can be pending or revised; confirm the latest issuer announcement and brokerage screen before submitting. The tool cannot see account market value, permissions, prior applications, or winning allocations.",
    }


FUND_API_BASE = "https://fundmobapi.eastmoney.com/FundMNewApi"
FUND_API_DEVICE_ID = "00000000-0000-0000-0000-000000000000"


def normalize_fund_code(fund_code: str) -> str:
    normalized = str(fund_code or "").strip()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise HTTPException(status_code=400, detail="fund_code must be a six-digit public fund code.")
    return normalized


def get_eastmoney_fund_component(fund_code: str, endpoint: str) -> dict[str, Any]:
    parameters = {
        "FCODE": fund_code,
        "appType": "ttjj",
        "deviceid": FUND_API_DEVICE_ID,
        "plat": "Iphone",
        "product": "EFund",
        "serverVersion": "6.3.8",
        "version": "6.3.8",
    }
    payload = read_public_json(
        f"{FUND_API_BASE}/{endpoint}?{urlencode(parameters)}",
        "https://fund.eastmoney.com/",
        timeout=3,
        attempts=2,
    )
    if not isinstance(payload, dict) or payload.get("Success") is False:
        raise HTTPException(status_code=502, detail=f"Unexpected fund response from {endpoint}.")
    return payload


def get_cached_fund_component(fund_code: str, endpoint: str) -> dict[str, Any]:
    return get_cached_component_with_stale(
        cache_key(
            "eastmoney_fund_component",
            {"fund_code": fund_code, "endpoint": endpoint},
        ),
        1800,
        86400,
        lambda: get_eastmoney_fund_component(fund_code, endpoint),
    )


def fund_component_date(payload: dict[str, Any], rows: list[dict[str, Any]], field: str) -> str | None:
    expansion = clean_value(payload.get("Expansion"))
    if expansion and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(expansion)):
        return str(expansion)
    return next((str(row[field]) for row in rows if clean_value(row.get(field))), None)


def get_fund_exposure_data(
    fund_code: str,
    holdings_limit: int,
    detail_level: str,
    look_through_depth: int = 1,
) -> dict[str, Any]:
    fund_code = normalize_fund_code(fund_code)
    loaders = {
        "basic_information": lambda: get_cached_fund_component(
            fund_code, "FundMNNBasicInformation"
        ),
        "holdings": lambda: get_cached_fund_component(
            fund_code, "FundMNInverstPosition"
        ),
        "industry_distribution": lambda: get_cached_fund_component(
            fund_code, "FundMNSectorAllocation"
        ),
        "asset_allocation": lambda: get_cached_fund_component(
            fund_code, "FundMNAssetAllocationNew"
        ),
    }
    results, component_status, source_errors = collect_components(
        loaders, 7, FUND_COMPONENT_EXECUTOR
    )
    if not results:
        raise HTTPException(status_code=502, detail=f"All public fund exposure sources failed: {fund_code}")

    basic = (results.get("basic_information") or {}).get("Datas") or {}
    raw_holdings_container = (results.get("holdings") or {}).get("Datas") or {}
    raw_holdings = (
        raw_holdings_container.get("fundStocks") or []
        if isinstance(raw_holdings_container, dict)
        else []
    )
    holdings = []
    for row in raw_holdings:
        weight = to_number(row.get("JZBL"))
        raw_security_code = clean_value(row.get("GPDM"))
        provider_market_code = clean_value(row.get("TEXCH"))
        if not raw_security_code or weight is None:
            continue
        provider_identifier = (
            f"{provider_market_code}:{raw_security_code}"
            if provider_market_code
            else str(raw_security_code)
        )
        mainland_symbol = (
            str(raw_security_code)
            if provider_market_code in {"1", "2"}
            and SYMBOL_PATTERN.fullmatch(str(raw_security_code))
            else None
        )
        holdings.append(
            {
                "identifier": mainland_symbol or provider_identifier,
                "symbol": mainland_symbol,
                "provider_security_code": str(raw_security_code),
                "provider_market_code": provider_market_code,
                "provider_security_identifier": provider_identifier,
                "name": clean_value(row.get("GPJC")),
                "weight_pct": weight,
                "change_from_previous_disclosure_pct_points": to_number(row.get("PCTNVCHG")),
                "provider_industry_code": clean_value(row.get("INDEXCODE")),
                "provider_industry_name": clean_value(row.get("INDEXNAME")),
            }
        )
    holdings.sort(key=lambda item: item["weight_pct"], reverse=True)
    holdings = holdings[:holdings_limit]

    raw_industries = (results.get("industry_distribution") or {}).get("Datas") or []
    industries = [
        {
            "industry_name": clean_value(row.get("HYMC")),
            "weight_pct": to_number(row.get("ZJZBL")),
            "reported_market_value_10k_cny": to_number(row.get("SZ")),
        }
        for row in raw_industries
        if clean_value(row.get("HYMC"))
        and str(row.get("HYMC")) != "合计"
        and to_number(row.get("ZJZBL")) is not None
        and to_number(row.get("ZJZBL")) > 0
    ]
    industries.sort(key=lambda item: item["weight_pct"], reverse=True)

    raw_allocation = (results.get("asset_allocation") or {}).get("Datas") or []
    allocation_row = raw_allocation[0] if raw_allocation else {}
    asset_allocation = {
        "stock_pct": to_number(allocation_row.get("GP")),
        "bond_pct": to_number(allocation_row.get("ZQ")),
        "cash_pct": to_number(allocation_row.get("HB")),
        "other_pct": to_number(allocation_row.get("QT")),
        "fund_pct": to_number(allocation_row.get("JJ")),
        "total_assets_100m_cny": to_number(allocation_row.get("JZC")),
    }
    underlying_fund_code = (
        clean_value(raw_holdings_container.get("ETFCODE"))
        if isinstance(raw_holdings_container, dict)
        else None
    )
    underlying_fund_name = (
        clean_value(raw_holdings_container.get("ETFSHORTNAME"))
        if isinstance(raw_holdings_container, dict)
        else None
    )
    if (
        not underlying_fund_code
        or not SYMBOL_PATTERN.fullmatch(str(underlying_fund_code))
        or str(underlying_fund_code) == fund_code
    ):
        underlying_fund_code = None
        underlying_fund_name = None
    underlying_fund_weight_pct = (
        asset_allocation.get("fund_pct") if underlying_fund_code else None
    )
    look_through_holdings: list[dict[str, Any]] = []
    look_through_source_errors: list[dict[str, Any]] = []
    look_through_status = "not_applicable"
    if underlying_fund_code:
        look_through_status = "depth_limit_reached"
        if look_through_depth > 0:
            try:
                underlying_payload = get_cached_fund_exposure_data(
                    str(underlying_fund_code), holdings_limit, "raw", 0
                )
                for item in underlying_payload.get("top_holdings") or []:
                    child_weight = to_number(item.get("weight_pct"))
                    if child_weight is None or underlying_fund_weight_pct is None:
                        continue
                    look_through_holdings.append(
                        {
                            **item,
                            "weight_pct": round(
                                underlying_fund_weight_pct * child_weight / 100, 8
                            ),
                            "underlying_fund_holding_weight_pct": child_weight,
                            "underlying_fund_code": str(underlying_fund_code),
                        }
                    )
                look_through_source_errors = normalize_source_errors(
                    underlying_payload.get("source_errors")
                )
                look_through_status = (
                    "available" if look_through_holdings else "partial_data"
                )
            except HTTPException as exc:
                look_through_status = "unavailable"
                look_through_source_errors.append(
                    {
                        "source": f"underlying_fund:{underlying_fund_code}",
                        "error_type": "upstream_failure",
                        "message": str(exc.detail),
                    }
                )
    allocation_sum = sum(
        value or 0
        for key, value in asset_allocation.items()
        if key.endswith("_pct")
    )
    holdings_date = fund_component_date(
        results.get("holdings") or {}, raw_holdings, "FSRQ"
    )
    industry_date = fund_component_date(
        results.get("industry_distribution") or {}, raw_industries, "FSRQ"
    )
    allocation_date = fund_component_date(
        results.get("asset_allocation") or {}, raw_allocation, "FSRQ"
    )
    disclosure_dates = sorted(
        {date for date in (holdings_date, industry_date, allocation_date) if date}
    )
    missing_fields = []
    if not holdings:
        missing_fields.append("top_holdings")
    if not industries:
        missing_fields.append("industry_distribution")
    if not any(value is not None for key, value in asset_allocation.items() if key.endswith("_pct")):
        missing_fields.append("asset_allocation")
    if underlying_fund_code and look_through_depth > 0 and not look_through_holdings:
        missing_fields.append("look_through_holdings")
    detailed_holdings = holdings
    if detail_level == "summary":
        detailed_holdings = [
            {
                key: item.get(key)
                for key in (
                    "identifier",
                    "symbol",
                    "name",
                    "weight_pct",
                    "provider_industry_name",
                )
            }
            for item in holdings
        ]
    nested_errors = [
        error
        for payload in results.values()
        if isinstance(payload, dict)
        for error in normalize_source_errors(payload.get("source_errors"))
    ]
    all_source_errors = [*source_errors, *nested_errors, *look_through_source_errors]
    return {
        "fund_code": fund_code,
        "fund_name": clean_value(basic.get("SHORTNAME")) if isinstance(basic, dict) else None,
        "fund_company": clean_value(basic.get("JJGS")) if isinstance(basic, dict) else None,
        "established_date": clean_value(basic.get("ESTABDATE")) if isinstance(basic, dict) else None,
        "latest_nav": to_number(basic.get("DWJZ")) if isinstance(basic, dict) else None,
        "nav_date": clean_value(basic.get("FSRQ")) if isinstance(basic, dict) else None,
        "disclosure_dates": disclosure_dates,
        "holdings_disclosure_date": holdings_date,
        "industry_disclosure_date": industry_date,
        "asset_allocation_date": allocation_date,
        "asset_allocation": asset_allocation,
        "asset_allocation_reported_sum_pct": round(allocation_sum, 4),
        "top_holdings": detailed_holdings,
        "top_holdings_count": len(holdings),
        "top_holdings_reported_weight_pct": round(
            sum(item["weight_pct"] for item in holdings), 4
        ),
        "holdings_scope": "latest_top_holdings_disclosed_by_public_fund_api_not_a_complete_position_book",
        "underlying_fund_code": underlying_fund_code,
        "underlying_fund_name": underlying_fund_name,
        "underlying_fund_weight_pct": underlying_fund_weight_pct,
        "look_through_holdings": (
            look_through_holdings
            if detail_level == "raw"
            else [
                {
                    key: item.get(key)
                    for key in (
                        "identifier",
                        "symbol",
                        "name",
                        "weight_pct",
                        "underlying_fund_holding_weight_pct",
                        "underlying_fund_code",
                        "provider_industry_name",
                    )
                }
                for item in look_through_holdings
            ]
        ),
        "look_through_holdings_reported_weight_pct": round(
            sum(item["weight_pct"] for item in look_through_holdings), 8
        ),
        "look_through_status": look_through_status,
        "look_through_scope": "one_explicit_provider_link_to_the_underlying_fund_then_its_latest_public_top_holdings_only",
        "industry_distribution": industries,
        "component_status": component_status,
        "source": ["eastmoney_public_fund_api"],
        "source_errors": all_source_errors,
        "missing_fields": missing_fields,
        "detail_level": detail_level,
        "data_status": (
            "full_data"
            if not all_source_errors and not missing_fields
            else "partial_data"
        ),
        "queried_at": now_iso(),
        "note": "Latest disclosed fund facts only. For an explicitly linked feeder fund, look_through_holdings makes one bounded hop into the underlying fund and scales only its public top holdings; undisclosed positions are not inferred. Weights are not recommendations.",
    }


def get_cached_fund_exposure_data(
    fund_code: str,
    holdings_limit: int,
    detail_level: str,
    look_through_depth: int = 1,
) -> dict[str, Any]:
    normalized_code = normalize_fund_code(fund_code)
    return get_cached_component_with_stale(
        cache_key(
            "fund_exposure_internal",
            {
                "fund_code": normalized_code,
                "holdings_limit": holdings_limit,
                "detail_level": detail_level,
                "look_through_depth": look_through_depth,
            },
        ),
        1800,
        86400,
        lambda: get_fund_exposure_data(
            normalized_code, holdings_limit, detail_level, look_through_depth
        ),
    )


def get_fast_portfolio_security_reference(symbol: str) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    recent_reference = get_cached_tool_snapshot(
        cache_key("security_reference_internal", {"symbol": symbol}), 604800
    )
    if recent_reference is not None:
        data, cache = recent_reference
        data["reference_cache_age_seconds"] = cache["cache_age_seconds"]
        return data

    reference_future = SECURITY_REFERENCE_EXECUTOR.submit(
        get_resilient_security_reference_data, symbol
    )

    def load_quote_identity() -> dict[str, Any]:
        quote, source, source_errors = get_fastest_public_quote(symbol)
        return {
            "symbol": symbol,
            "name": clean_value(quote.get("name")),
            "industry": None,
            "source": source,
            "source_errors": source_errors,
            "identity_scope": "fast_public_quote_name_without_industry_reference",
        }

    try:
        identity = get_cached_component_with_stale(
            cache_key("portfolio_security_identity", {"symbol": symbol}),
            30,
            300,
            load_quote_identity,
        )
    except HTTPException:
        return reference_future.result()
    if reference_future.done():
        try:
            return reference_future.result()
        except HTTPException:
            return identity
    reference_future.add_done_callback(consume_background_future)
    return identity


def normalized_portfolio_positions(
    positions: list[dict[str, Any]], normalize_weights: bool
) -> tuple[list[dict[str, Any]], float]:
    if not positions:
        raise HTTPException(status_code=400, detail="positions must contain at least one position.")
    if len(positions) > 20:
        raise HTTPException(status_code=400, detail="At most 20 portfolio positions are supported.")
    combined: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in positions:
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="Each position must be an object.")
        identifier = str(raw.get("identifier") or raw.get("symbol") or raw.get("fund_code") or "").strip()
        if not SYMBOL_PATTERN.fullmatch(identifier):
            raise HTTPException(status_code=400, detail="Each position identifier must be a six-digit code.")
        weight = to_number(raw.get("weight_pct"))
        if weight is None or weight <= 0:
            raise HTTPException(status_code=400, detail=f"weight_pct must be positive for {identifier}.")
        asset_type = str(raw.get("asset_type") or "auto").strip().lower()
        if asset_type not in {"auto", "fund", "stock"}:
            raise HTTPException(status_code=400, detail="asset_type must be auto, fund, or stock.")
        if asset_type == "auto":
            security = security_metadata(identifier)
            asset_type = "fund" if security["security_type"] in {"etf", "lof"} else "stock"
        key = (identifier, asset_type)
        combined.setdefault(
            key,
            {"identifier": identifier, "asset_type": asset_type, "input_weight_pct": 0.0},
        )
        combined[key]["input_weight_pct"] += weight
    normalized = list(combined.values())
    total = sum(item["input_weight_pct"] for item in normalized)
    if not normalize_weights and total > 100.0001:
        raise HTTPException(status_code=400, detail="Total weight_pct must not exceed 100 when normalize_weights is false.")
    factor = 100 / total if normalize_weights else 1
    for item in normalized:
        item["portfolio_weight_pct"] = round(item["input_weight_pct"] * factor, 8)
    return normalized, total


def get_portfolio_exposure_data(
    positions: list[dict[str, Any]],
    normalize_weights: bool,
    holdings_limit: int,
    detail_level: str,
) -> dict[str, Any]:
    normalized, input_total = normalized_portfolio_positions(positions, normalize_weights)
    fund_positions = [item for item in normalized if item["asset_type"] == "fund"]
    stock_positions = [item for item in normalized if item["asset_type"] == "stock"]
    if len(fund_positions) > 10:
        raise HTTPException(status_code=400, detail="At most 10 fund positions can be looked through per request.")
    loaders = {
        **{
            f"fund:{item['identifier']}": lambda item=item: get_cached_fund_exposure_data(
                item["identifier"], holdings_limit, "raw"
            )
            for item in fund_positions
        },
        **{
            f"stock:{item['identifier']}": lambda item=item: get_fast_portfolio_security_reference(
                item["identifier"]
            )
            for item in stock_positions
        },
    }
    results, component_status, source_errors = (
        collect_components(loaders, 8, COMPOSITE_TOOL_EXECUTOR)
        if loaders
        else ({}, {}, [])
    )

    underlying: dict[str, dict[str, Any]] = {}
    known_underlying_industry_exposure: dict[str, float] = {}
    fund_reported_industry_exposure: dict[str, float] = {}
    allocation = {"stock_pct": 0.0, "bond_pct": 0.0, "cash_pct": 0.0, "other_pct": 0.0, "fund_pct": 0.0}
    unresolved_weight = 0.0
    fund_disclosure_dates: dict[str, str | None] = {}
    position_results = []
    child_source_errors: list[dict[str, Any]] = []
    child_missing_fields: list[str] = []
    partial_child_components: list[str] = []

    def add_underlying(
        identifier: str,
        symbol: str | None,
        name: Any,
        contribution: float,
        source_identifier: str,
        through_type: str,
    ) -> None:
        entry = underlying.setdefault(
            identifier,
            {
                "identifier": identifier,
                "symbol": symbol,
                "name": clean_value(name),
                "exposure_pct": 0.0,
                "sources": [],
            },
        )
        if not entry.get("name") and clean_value(name):
            entry["name"] = clean_value(name)
        entry["exposure_pct"] += contribution
        entry["sources"].append(
            {
                "position_identifier": source_identifier,
                "through_type": through_type,
                "contribution_pct": round(contribution, 8),
            }
        )

    for position in normalized:
        identifier = position["identifier"]
        weight = position["portfolio_weight_pct"]
        component_key = f"{position['asset_type']}:{identifier}"
        payload = results.get(component_key)
        if position["asset_type"] == "stock":
            allocation["stock_pct"] += weight
            add_underlying(
                identifier,
                identifier,
                (payload or {}).get("name"),
                weight,
                identifier,
                "direct_stock",
            )
            industry = clean_value((payload or {}).get("industry"))
            if not industry:
                child_missing_fields.append(f"{component_key}:industry")
            for error in normalize_source_errors((payload or {}).get("source_errors")):
                child_source_errors.append(
                    {
                        **error,
                        "source": f"{component_key}/{error.get('source') or 'public_quote_source'}",
                    }
                )
            if industry:
                known_underlying_industry_exposure[str(industry)] = (
                    known_underlying_industry_exposure.get(str(industry), 0.0) + weight
                )
            position_results.append(
                {**position, "status": "available" if payload else "partial_data", "name": clean_value((payload or {}).get("name")), "industry": industry}
            )
            continue

        fund_disclosure_dates[identifier] = (payload or {}).get("holdings_disclosure_date")
        if not payload:
            unresolved_weight += weight
            position_results.append({**position, "status": "unavailable"})
            continue
        child_status = str(payload.get("data_status") or "")
        component_status.setdefault(component_key, {})["child_data_status"] = child_status or None
        if child_status and child_status != "full_data":
            partial_child_components.append(component_key)
        for error in normalize_source_errors(payload.get("source_errors")):
            child_source_errors.append(
                {
                    **error,
                    "source": f"{component_key}/{error.get('source') or 'public_fund_source'}",
                }
            )
        child_missing_fields.extend(
            f"{component_key}:{field}" for field in payload.get("missing_fields") or []
        )
        fund_allocation = payload.get("asset_allocation") or {}
        known_allocation = 0.0
        for key in allocation:
            fund_value = to_number(fund_allocation.get(key))
            if fund_value is None:
                continue
            contribution = weight * fund_value / 100
            allocation[key] += contribution
            known_allocation += contribution
        unresolved_weight += max(0.0, weight - known_allocation)
        holding_contribution = 0.0
        for holding in payload.get("top_holdings") or []:
            holding_weight = to_number(holding.get("weight_pct"))
            holding_identifier = clean_value(
                holding.get("identifier")
                or holding.get("provider_security_identifier")
                or holding.get("symbol")
            )
            holding_symbol = clean_value(holding.get("symbol"))
            if holding_identifier is None or holding_weight is None:
                continue
            contribution = weight * holding_weight / 100
            holding_contribution += contribution
            add_underlying(
                str(holding_identifier),
                str(holding_symbol) if holding_symbol else None,
                holding.get("name"),
                contribution,
                identifier,
                "fund_top_holding",
            )
            holding_industry = clean_value(holding.get("provider_industry_name"))
            if holding_industry:
                known_underlying_industry_exposure[str(holding_industry)] = (
                    known_underlying_industry_exposure.get(str(holding_industry), 0.0)
                    + contribution
                )
        look_through_contribution = 0.0
        for holding in payload.get("look_through_holdings") or []:
            effective_weight = to_number(holding.get("weight_pct"))
            holding_identifier = clean_value(
                holding.get("identifier")
                or holding.get("provider_security_identifier")
                or holding.get("symbol")
            )
            holding_symbol = clean_value(holding.get("symbol"))
            if holding_identifier is None or effective_weight is None:
                continue
            contribution = weight * effective_weight / 100
            look_through_contribution += contribution
            add_underlying(
                str(holding_identifier),
                str(holding_symbol) if holding_symbol else None,
                holding.get("name"),
                contribution,
                identifier,
                "fund_underlying_fund_top_holding",
            )
            holding_industry = clean_value(holding.get("provider_industry_name"))
            if holding_industry:
                known_underlying_industry_exposure[str(holding_industry)] = (
                    known_underlying_industry_exposure.get(str(holding_industry), 0.0)
                    + contribution
                )
        for industry in payload.get("industry_distribution") or []:
            industry_weight = to_number(industry.get("weight_pct"))
            industry_name = clean_value(industry.get("industry_name"))
            if industry_name and industry_weight is not None:
                fund_reported_industry_exposure[str(industry_name)] = (
                    fund_reported_industry_exposure.get(str(industry_name), 0.0)
                    + weight * industry_weight / 100
                )
        position_results.append(
            {
                **position,
                "status": payload.get("data_status"),
                "name": payload.get("fund_name"),
                "holdings_disclosure_date": payload.get("holdings_disclosure_date"),
                "top_holdings_covered_portfolio_pct": round(holding_contribution, 8),
                "underlying_fund_code": payload.get("underlying_fund_code"),
                "look_through_status": payload.get("look_through_status"),
                "look_through_covered_portfolio_pct": round(
                    look_through_contribution, 8
                ),
            }
        )

    underlying_items = []
    for item in underlying.values():
        item["exposure_pct"] = round(item["exposure_pct"], 8)
        item["source_position_count"] = len(
            {source["position_identifier"] for source in item["sources"]}
        )
        underlying_items.append(item)
    underlying_items.sort(key=lambda item: item["exposure_pct"], reverse=True)
    overlaps = [item for item in underlying_items if item["source_position_count"] >= 2]
    industries = [
        {"industry_name": name, "exposure_pct": round(value, 8)}
        for name, value in sorted(
            known_underlying_industry_exposure.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    reported_fund_industries = [
        {"industry_name": name, "exposure_pct": round(value, 8)}
        for name, value in sorted(
            fund_reported_industry_exposure.items(),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    allocation = {key: round(value, 8) for key, value in allocation.items()}
    disclosure_values = sorted(date for date in fund_disclosure_dates.values() if date)
    conflicts = []
    if len(set(disclosure_values)) > 1:
        conflicts.append(
            {
                "type": "fund_disclosure_date_difference",
                "dates_by_fund": fund_disclosure_dates,
                "note": "Fund look-through weights come from different public disclosure dates.",
            }
        )
    missing_fields = [
        key
        for key, status in component_status.items()
        if status.get("status") == "unavailable"
    ] + child_missing_fields
    all_source_errors = [*source_errors, *child_source_errors]
    top_exposures = underlying_items if detail_level == "raw" else underlying_items[:10]
    top_values = [item["exposure_pct"] for item in underlying_items]
    represented_weight = sum(item["portfolio_weight_pct"] for item in normalized)
    disclosed_underlying_weight = sum(top_values)
    sources = sorted(
        {
            source
            for payload in results.values()
            if isinstance(payload, dict)
            for source in normalize_sources(payload.get("source"))
        }
    )
    return {
        "input_position_count": len(positions),
        "combined_position_count": len(normalized),
        "input_weight_total_pct": round(input_total, 8),
        "weights_normalized": normalize_weights,
        "positions": position_results,
        "asset_allocation": allocation,
        "unresolved_asset_allocation_weight_pct": round(unresolved_weight, 8),
        "represented_position_weight_pct": round(represented_weight, 8),
        "unallocated_input_weight_pct": round(max(0.0, 100 - represented_weight), 8),
        "disclosed_underlying_covered_weight_pct": round(disclosed_underlying_weight, 8),
        "not_looked_through_position_weight_pct": round(
            max(0.0, represented_weight - disclosed_underlying_weight), 8
        ),
        "industry_exposure": industries,
        "industry_exposure_scope": "direct stocks plus provider industry labels on disclosed fund top holdings; provider levels can differ and undisclosed holdings are excluded",
        "fund_reported_industry_exposure": reported_fund_industries,
        "fund_reported_industry_exposure_scope": "portfolio-weighted broad industry allocations reported by each fund; direct stocks are excluded because their classification level is not comparable",
        "underlying_exposure": top_exposures,
        "underlying_exposure_scope": "direct stocks, each fund's public top holdings, and one explicitly linked underlying fund's public top holdings; undisclosed holdings are not inferred",
        "overlapping_underlyings": overlaps if detail_level == "raw" else overlaps[:10],
        "concentration": {
            "top_1_disclosed_underlying_pct": round(sum(top_values[:1]), 8),
            "top_5_disclosed_underlyings_pct": round(sum(top_values[:5]), 8),
            "top_10_disclosed_underlyings_pct": round(sum(top_values[:10]), 8),
            "scope": "known direct stocks and disclosed top holdings, including one explicit underlying-fund hop",
        },
        "fund_disclosure_dates": fund_disclosure_dates,
        "component_status": component_status,
        "partial_child_components": sorted(set(partial_child_components)),
        "source": sources,
        "source_errors": all_source_errors,
        "missing_fields": missing_fields,
        "conflicts": conflicts,
        "detail_level": detail_level,
        "data_status": (
            "full_data"
            if not all_source_errors
            and not missing_fields
            and not conflicts
            and not partial_child_components
            and unresolved_weight <= 0.0001
            else "partial_data"
        ),
        "queried_at": now_iso(),
        "note": "Mechanical look-through aggregation only. Fund-of-funds allocation is reported as fund_pct and is not recursively expanded. The tool does not access a brokerage account, infer undisclosed holdings, score diversification, or make a trading recommendation.",
    }


def get_fastest_index_component() -> dict[str, Any]:
    source_getters = (
        ("eastmoney", get_eastmoney_indices),
        ("tencent", get_tencent_indices),
        ("sina", get_sina_indices),
    )
    executor = ThreadPoolExecutor(max_workers=len(source_getters))
    futures = {executor.submit(getter): source for source, getter in source_getters}
    pending = set(futures)
    successes: list[tuple[str, list[dict[str, Any]]]] = []
    errors: list[str] = []
    deadline = perf_counter() + 8.5
    try:
        while pending:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                break
            for future in completed:
                source = futures[future]
                try:
                    rows = future.result()
                    if not rows:
                        raise HTTPException(
                            status_code=502,
                            detail=f"{source} returned no major-index rows.",
                        )
                    successes.append((source, rows))
                except HTTPException as exc:
                    errors.append(f"{source}: {exc.detail}")
                except Exception as exc:  # pragma: no cover - defensive source boundary
                    errors.append(f"{source}: {exc}")

            complete_success = next(
                (
                    item
                    for preferred_source in ("eastmoney", "tencent")
                    for item in successes
                    if item[0] == preferred_source
                    and PRIMARY_INDEX_SYMBOLS
                    <= {str(row.get("symbol")) for row in item[1]}
                    and len(item[1]) >= 9
                ),
                None,
            )
            if complete_success:
                return {
                    "indices": complete_success[1],
                    "source": complete_success[0],
                    "source_errors": errors,
                }
            rich_source_pending = any(
                futures[future] in {"eastmoney", "tencent"} for future in pending
            )
            if successes and not rich_source_pending:
                break

        for future in pending:
            future.cancel()
            errors.append(
                f"{futures[future]}: index request exceeded the 8.5 second budget"
            )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    if successes:
        source, rows = min(
            successes,
            key=lambda item: {"eastmoney": 0, "tencent": 1, "sina": 2}[item[0]],
        )
        return {"indices": rows, "source": source, "source_errors": errors}
    raise HTTPException(status_code=502, detail="; ".join(errors))


def get_overview_board_component(limit: int) -> dict[str, Any]:
    errors = []
    for source, getter in (
        ("eastmoney_industry", get_eastmoney_industry_boards),
        ("efinance_calculated", get_calculated_industry_boards),
    ):
        try:
            boards = getter(limit)
            if not boards:
                raise HTTPException(
                    status_code=502,
                    detail=f"{source} returned no industry-board rows.",
                )
            return {"boards": boards, "source": source, "source_errors": errors}
        except (HTTPException, OSError, ValueError, TypeError) as exc:
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            errors.append(f"{source}: {detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def get_eastmoney_market_aggregate() -> dict[str, Any]:
    query = urlencode(
        {
            "fltt": 2,
            "invt": 2,
            "fields": "f6,f12,f13,f14,f104,f105,f106,f124",
            "secids": "1.000002,0.399107,0.899050",
        }
    )
    hosts = (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    )
    executor = ThreadPoolExecutor(max_workers=len(hosts))
    futures = {
        executor.submit(
            read_public_json,
            f"https://{host}/api/qt/ulist.np/get?{query}",
            "https://quote.eastmoney.com/",
            5,
            1,
        ): host
        for host in hosts
    }
    pending = set(futures)
    payload: dict[str, Any] | None = None
    errors: list[str] = []
    deadline = perf_counter() + 6.5
    try:
        while pending and payload is None:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                break
            completed, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )
            if not completed:
                break
            for future in completed:
                host = futures[future]
                try:
                    candidate = future.result()
                    if not (((candidate.get("data") or {}).get("diff")) or []):
                        raise HTTPException(
                            status_code=502,
                            detail="aggregate response contained no rows",
                        )
                    payload = candidate
                    break
                except HTTPException as exc:
                    errors.append(f"{host}: {exc.detail}")
                except Exception as exc:  # pragma: no cover - defensive source boundary
                    errors.append(f"{host}: {exc}")
        for future in pending:
            future.cancel()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    if payload is None:
        raise HTTPException(
            status_code=502,
            detail="Fast market aggregate unavailable within 6.5 seconds: " + "; ".join(errors),
        )
    rows = ((payload.get("data") or {}).get("diff")) or []
    exchange_by_symbol = {"000002": "SSE", "399107": "SZSE", "899050": "BSE"}
    parsed: dict[str, dict[str, Any]] = {}
    for row in rows:
        exchange = exchange_by_symbol.get(str(row.get("f12") or ""))
        rise_count = _to_int(row.get("f104"))
        fall_count = _to_int(row.get("f105"))
        flat_count = _to_int(row.get("f106"))
        turnover = to_number(row.get("f6"))
        if not exchange or None in (rise_count, fall_count, flat_count, turnover):
            continue
        parsed[exchange] = {
            "rise_count": rise_count,
            "fall_count": fall_count,
            "flat_count": flat_count,
            "turnover": turnover,
            "market_time": market_time_from_source_update(
                format_unix_market_time(row.get("f124"))
            ),
        }
    if set(parsed) != {"SSE", "SZSE", "BSE"}:
        raise HTTPException(
            status_code=502,
            detail="Eastmoney market aggregate did not include SSE, SZSE, and BSE rows.",
        )

    unavailable_detail_fields = (
        "rise_over_3_count",
        "rise_over_5_count",
        "rise_over_7_count",
        "fall_over_3_count",
        "fall_over_5_count",
        "fall_over_7_count",
        "limit_up_count",
        "limit_down_count",
        "open_board_count",
        "consecutive_limit_up_count",
        "st_limit_up_count",
        "st_limit_down_count",
    )

    def aggregate_counts(exchange: str | None = None) -> dict[str, Any]:
        selected = [parsed[exchange]] if exchange else list(parsed.values())
        result = empty_breadth_counts()
        for key in ("rise_count", "fall_count", "flat_count"):
            result[key] = sum(item[key] for item in selected)
        result["stock_count"] = sum(
            item["rise_count"] + item["fall_count"] + item["flat_count"]
            for item in selected
        )
        for key in unavailable_detail_fields:
            result[key] = None
        return result

    market_times = [item["market_time"] for item in parsed.values() if item["market_time"]]
    market_time = max(market_times) if market_times else None
    by_exchange_turnover = {
        exchange: parsed[exchange]["turnover"] for exchange in ("SSE", "SZSE", "BSE")
    }
    return {
        "breadth": {
            "scope": "Ordinary A-share exchange aggregates for SSE, SZSE, and BSE; detailed price-band and limit statistics require the slower security-level fallback.",
            "all_market": aggregate_counts(),
            "by_exchange": {
                exchange: aggregate_counts(exchange) for exchange in ("SSE", "SZSE", "BSE")
            },
            "consecutive_limit_up_status": "unavailable_without_a_historical_limit-up_pool",
        },
        "turnover": {
            "current": sum(by_exchange_turnover.values()),
            "unit": "CNY",
            "previous_trade_day_same_time": None,
            "change": None,
            "change_pct": None,
            "comparison_status": "unavailable_without_a_reliable_prior-day_market-wide_intraday_series",
            "estimated_full_day": None,
            "estimated_full_day_status": "pending_market_time_adjustment",
            "by_exchange": by_exchange_turnover,
            "top_turnover_securities": [],
            "scope": "Ordinary A-share exchange aggregates; security-level turnover ranking is unavailable on this fast path.",
        },
        "market_time": market_time,
        "row_count": aggregate_counts()["stock_count"],
        "coverage_status": "complete_exchange_aggregate",
        "source": "eastmoney_a_share_exchange_aggregate",
        "source_errors": [],
    }


def get_overview_breadth_component() -> dict[str, Any]:
    try:
        return get_eastmoney_market_aggregate()
    except (HTTPException, OSError, ValueError, TypeError) as exc:
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        raise HTTPException(
            status_code=502,
            detail=f"eastmoney_a_share_exchange_aggregate: {detail}",
        ) from exc


def get_market_overview_data(limit: int) -> dict[str, Any]:
    started_at = perf_counter()
    response_budget_seconds = 9.0
    component_specs = {
        "indices": {
            "key": cache_key("overview_component_indices", {}),
            "ttl": 30,
            "max_stale_age": 60,
            "loader": get_fastest_index_component,
        },
        "industry_boards": {
            "key": cache_key("overview_component_boards", {"limit": limit}),
            "ttl": 30,
            "max_stale_age": 300,
            "loader": lambda: get_overview_board_component(limit),
        },
        "market_breadth": {
            "key": cache_key("overview_component_breadth", {}),
            "ttl": 30,
            "max_stale_age": 180,
            "loader": get_overview_breadth_component,
        },
        "limit_activity": {
            "key": cache_key("overview_component_limit_activity", {}),
            "ttl": 30,
            "max_stale_age": 3600,
            "loader": lambda: get_limit_activity_data(10),
        },
    }

    executor = ThreadPoolExecutor(max_workers=len(component_specs))
    futures = {
        executor.submit(
            get_cached_tool_data,
            spec["key"],
            spec["ttl"],
            spec["loader"],
        ): name
        for name, spec in component_specs.items()
    }
    done, pending = wait(futures, timeout=response_budget_seconds)
    component_results: dict[str, dict[str, Any]] = {}
    component_status: dict[str, dict[str, Any]] = {}
    source_errors: list[str] = []

    for future in done:
        name = futures[future]
        try:
            data, cache = future.result()
            component_results[name] = data
            component_status[name] = {
                "status": "fresh_cache" if cache["cache_hit"] else "live",
                "cache_age_seconds": cache["cache_age_seconds"],
            }
        except HTTPException as exc:
            source_errors.append(f"{name}: {exc.detail}")
        except Exception as exc:  # pragma: no cover - defensive component boundary
            source_errors.append(f"{name}: {exc}")

    missing_names = {futures[future] for future in pending} | (
        set(component_specs) - set(component_results) - {futures[future] for future in pending}
    )
    for name in missing_names:
        spec = component_specs[name]
        cached = get_cached_tool_snapshot(spec["key"], spec["max_stale_age"])
        if cached:
            data, cache = cached
            component_results[name] = data
            component_status[name] = {
                "status": "stale_cache",
                "cache_age_seconds": cache["cache_age_seconds"],
            }
            source_errors.append(
                f"{name}: live component unavailable within {response_budget_seconds:g} seconds; using recent cache"
            )
        else:
            component_status[name] = {
                "status": "unavailable_within_response_budget",
                "cache_age_seconds": None,
            }
            source_errors.append(
                f"{name}: unavailable within the {response_budget_seconds:g} second response budget"
            )
    executor.shutdown(wait=False, cancel_futures=False)

    index_component = component_results.get("indices")
    if not index_component:
        raise HTTPException(status_code=502, detail="; ".join(source_errors))

    indices = index_component["indices"]
    index_source = index_component["source"]
    board_component = component_results.get("industry_boards") or {}
    boards = board_component.get("boards") or []
    board_source = board_component.get("source", "unavailable")
    breadth_component = component_results.get("market_breadth") or {}
    breadth = breadth_component.get("breadth")
    turnover = breadth_component.get("turnover")
    limit_component = component_results.get("limit_activity") or {}
    limit_activity_stats = limit_component.get("statistics")
    if breadth and limit_activity_stats:
        all_counts = breadth.get("all_market") or {}
        for key in (
            "limit_up_count",
            "limit_down_count",
            "open_board_count",
            "consecutive_limit_up_count",
            "st_limit_up_count",
            "st_limit_down_count",
        ):
            all_counts[key] = limit_activity_stats.get(key)
        for exchange, exchange_counts in (limit_component.get("by_exchange") or {}).items():
            breadth_exchange = (breadth.get("by_exchange") or {}).get(exchange)
            if breadth_exchange:
                breadth_exchange.update(exchange_counts)
        breadth["consecutive_limit_up_status"] = "available_from_public_limit_up_pool"
    market_time = latest_market_time(indices) or breadth_component.get("market_time")
    if turnover and market_time:
        elapsed = trading_minutes_elapsed(market_time)
        current_turnover = turnover.get("current")
        if current_turnover is not None and elapsed:
            turnover["estimated_full_day"] = (
                round(current_turnover / elapsed * 240, 2)
                if 0 < elapsed < 240
                else current_turnover
            )
            turnover["estimated_full_day_status"] = (
                "mechanical_elapsed-time_extrapolation"
                if elapsed < 240
                else "completed_trading_day"
            )

    source_errors.extend(index_component.get("source_errors", []))
    source_errors.extend(board_component.get("source_errors", []))
    source_errors.extend(breadth_component.get("source_errors", []))
    source_errors.extend(limit_component.get("source_errors", []))
    sources = [f"indices:{index_source}"]
    if boards:
        sources.append(f"industry_boards:{board_source}")
    if breadth:
        sources.append(f"market_breadth:{breadth_component.get('source', 'unavailable')}")
    if limit_activity_stats:
        sources.append("limit_activity:eastmoney_public_pools")

    primary_indices = [
        index for index in indices if index.get("symbol") in PRIMARY_INDEX_SYMBOLS
    ]
    if not primary_indices:
        primary_indices = indices[:3]
    style_indices = [index for index in indices if index not in primary_indices]
    returned_index_symbols = {str(index.get("symbol")) for index in indices}
    missing_style_index_symbols = sorted(
        (OVERVIEW_INDEX_SYMBOLS - PRIMARY_INDEX_SYMBOLS) - returned_index_symbols
    )
    style_index_catalog = [
        {
            "symbol": symbol,
            "identifier": f"index:{symbol}",
            "eastmoney_secid": INDEX_SECID_BY_SYMBOL.get(symbol),
            **INDEX_IDENTITY[symbol],
            "available_in_current_snapshot": symbol in returned_index_symbols,
        }
        for symbol in sorted(OVERVIEW_INDEX_SYMBOLS - PRIMARY_INDEX_SYMBOLS)
    ]
    all_market_breadth = breadth.get("all_market") if breadth else None
    breadth_detail_fields = (
        "rise_over_3_count",
        "rise_over_5_count",
        "rise_over_7_count",
        "fall_over_3_count",
        "fall_over_5_count",
        "fall_over_7_count",
        "limit_up_count",
        "limit_down_count",
        "open_board_count",
    )
    unavailable_breadth_detail_fields = (
        [
            key
            for key in breadth_detail_fields
            if not all_market_breadth or all_market_breadth.get(key) is None
        ]
    )
    limit_stats = limit_activity_stats or (
        {
            key: all_market_breadth.get(key)
            for key in (
                "limit_up_count",
                "limit_down_count",
                "open_board_count",
                "consecutive_limit_up_count",
                "st_limit_up_count",
                "st_limit_down_count",
            )
        }
        if all_market_breadth
        else None
    )
    market_activity_facts = None
    if limit_activity_stats:
        rise_count = (all_market_breadth or {}).get("rise_count")
        fall_count = (all_market_breadth or {}).get("fall_count")
        market_activity_facts = {
            **limit_activity_stats,
            "rise_count": rise_count,
            "fall_count": fall_count,
            "rise_to_fall_ratio": round(rise_count / fall_count, 4)
            if rise_count is not None and fall_count
            else None,
            "limit_up_to_limit_down_ratio": round(
                limit_activity_stats["limit_up_count"]
                / limit_activity_stats["limit_down_count"],
                4,
            )
            if limit_activity_stats.get("limit_down_count")
            else None,
            "scope": "Mechanical market activity facts; no bullish, bearish, hot, cold, or trading judgement is assigned.",
        }

    return {
        "market_status": market_status_at(),
        "trade_date": market_time.split("T", 1)[0] if market_time else None,
        "market_time": market_time,
        "indices": primary_indices,
        "style_indices": style_indices,
        "style_index_catalog": style_index_catalog,
        "style_indices_status": (
            "full_data"
            if not missing_style_index_symbols
            else "partial_data"
            if style_indices
            else "unavailable"
        ),
        "missing_style_index_symbols": missing_style_index_symbols,
        "index_source": index_source,
        "industry_boards": boards,
        "industry_board_source": board_source,
        "industry_board_errors": board_component.get("source_errors", []) if not boards else [],
        "market_breadth": breadth,
        "market_breadth_source": breadth_component.get("source", "unavailable"),
        "market_breadth_coverage_status": breadth_component.get("coverage_status"),
        "market_breadth_row_count": breadth_component.get("row_count"),
        "market_breadth_detail_status": (
            "full_data" if not unavailable_breadth_detail_fields else "aggregate_only"
        ),
        "unavailable_breadth_detail_fields": unavailable_breadth_detail_fields,
        "turnover": turnover,
        "limit_stats": limit_stats,
        "limit_stats_status": (
            limit_component.get("data_status", "available")
            if limit_activity_stats
            else "unavailable"
        ),
        "market_activity_facts": market_activity_facts,
        "component_status": component_status,
        "response_budget_ms": int(response_budget_seconds * 1000),
        "source": sources,
        "source_errors": source_errors,
        "is_stale": is_market_time_stale(market_time),
        "queried_at": now_iso(),
        "latency_ms": int((perf_counter() - started_at) * 1000),
        "data_status": (
            "full_data"
            if breadth
            and turnover
            and boards
            and limit_activity_stats
            and str(breadth_component.get("coverage_status") or "").startswith("complete")
            else "partial_data"
        ),
        "note": "Facts and mechanical calculations only; slow components use recent successful cache or return as unavailable within a nine-second budget. Limit activity comes from separate public pools. Prior-day same-minute turnover remains unavailable intraday because no reliable historical market-wide minute series was found.",
    }


def normalize_snapshot_as_of(as_of: str | None) -> str | None:
    if not as_of or str(as_of).strip().lower() == "now":
        return None
    try:
        value = str(as_of).strip().replace("Z", "+00:00")
        requested = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="as_of must be an ISO date-time such as 2026-07-17T10:30:00+08:00.",
        ) from exc
    if requested.tzinfo is None:
        requested = requested.replace(tzinfo=MARKET_TIMEZONE)
    requested = requested.astimezone(MARKET_TIMEZONE)
    now = datetime.now(MARKET_TIMEZONE)
    if requested > now + timedelta(minutes=1):
        raise HTTPException(status_code=400, detail="as_of cannot be in the future.")
    if requested < now - timedelta(minutes=5):
        raise HTTPException(
            status_code=400,
            detail=(
                "Exact historical as-of reconstruction is unavailable from the current public real-time sources. "
                "Omit as_of to capture a new synchronized snapshot now."
            ),
        )
    return requested.isoformat()


def compact_market_overview_for_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "market_status",
            "trade_date",
            "market_time",
            "indices",
            "style_indices",
            "style_index_catalog",
            "industry_boards",
            "market_breadth",
            "turnover",
            "limit_stats",
            "market_activity_facts",
            "component_status",
            "data_status",
        )
    }


def snapshot_time_entry(component: str, value: Any) -> dict[str, Any] | None:
    parsed = parse_market_datetime(value)
    if parsed is None:
        return None
    return {"component": component, "market_time": parsed.isoformat(), "parsed": parsed}


def get_market_snapshot_data(
    symbol: str | None,
    peer_symbols: list[str] | None,
    as_of: str | None,
    sector_limit: int,
    detail_level: str,
) -> dict[str, Any]:
    started_at = perf_counter()
    started_iso = now_iso()
    requested_as_of = normalize_snapshot_as_of(as_of)
    target_symbol = normalize_symbol(symbol) if symbol else None
    requested_identifiers: list[str] = []
    for identifier in [target_symbol, *(peer_symbols or [])]:
        if identifier is None:
            continue
        normalized_identifier = str(identifier).strip()
        batch_security_metadata(normalized_identifier)
        if normalized_identifier not in requested_identifiers:
            requested_identifiers.append(normalized_identifier)
    if len(requested_identifiers) > 10:
        raise HTTPException(
            status_code=400, detail="A synchronized snapshot supports at most 10 target and peer identifiers."
        )

    loaders: dict[str, Any] = {
        "market_overview": lambda: get_cached_component_with_stale(
            cache_key("snapshot_market_overview", {"sector_limit": sector_limit}),
            5,
            120,
            lambda: get_market_overview_data(sector_limit),
        ),
    }
    if requested_identifiers:
        loaders["batch_quotes"] = lambda: get_cached_component_with_stale(
            cache_key(
                "snapshot_batch_quotes",
                {"identifiers": requested_identifiers},
            ),
            2,
            15,
            lambda: get_batch_quote_data(requested_identifiers),
        )
    if target_symbol:
        loaders["target_quote"] = lambda: get_cached_component_with_stale(
            cache_key("snapshot_target_quote", {"symbol": target_symbol}),
            2,
            15,
            lambda: get_quote_data(target_symbol),
        )
    results, component_status, source_errors = collect_components(loaders, 12)
    if not results:
        raise HTTPException(status_code=502, detail="All synchronized snapshot components failed.")

    overview = results.get("market_overview") or {}
    batch = results.get("batch_quotes") or {}
    target_payload = results.get("target_quote") or {}
    target_quote = target_payload.get("quote") or None
    batch_results = batch.get("results") or []
    batch_target = next(
        (
            item
            for item in batch_results
            if target_symbol and str(item.get("symbol")) == target_symbol
        ),
        None,
    )

    time_entries = [
        entry
        for entry in (
            snapshot_time_entry("market_overview", overview.get("market_time")),
            snapshot_time_entry("batch_quotes", batch.get("market_time")),
            snapshot_time_entry(
                "target_quote",
                (target_quote or {}).get("quote_time")
                or (target_quote or {}).get("market_time"),
            ),
        )
        if entry is not None
    ]
    sorted_times = sorted(time_entries, key=lambda item: item["parsed"])
    earliest_time = sorted_times[0]["market_time"] if sorted_times else None
    latest_time = sorted_times[-1]["market_time"] if sorted_times else None
    time_difference_seconds = (
        round((sorted_times[-1]["parsed"] - sorted_times[0]["parsed"]).total_seconds(), 3)
        if len(sorted_times) >= 2
        else 0.0 if sorted_times else None
    )

    direct_price = to_number((target_quote or {}).get("price"))
    batch_price = to_number((batch_target or {}).get("price"))
    source_difference_pct = (
        round(abs(direct_price - batch_price) / batch_price * 100, 6)
        if direct_price is not None and batch_price not in (None, 0)
        else None
    )
    conflicts: list[dict[str, Any]] = []
    if time_difference_seconds is not None and time_difference_seconds > 60:
        conflicts.append(
            {
                "type": "market_time_difference",
                "difference_seconds": time_difference_seconds,
                "threshold_seconds": 60,
                "components": [
                    {"component": item["component"], "market_time": item["market_time"]}
                    for item in sorted_times
                ],
            }
        )
    if source_difference_pct is not None and source_difference_pct > 0.05:
        conflicts.append(
            {
                "type": "target_price_difference",
                "difference_pct": source_difference_pct,
                "threshold_pct": 0.05,
                "direct_price": direct_price,
                "batch_price": batch_price,
            }
        )

    missing_fields = []
    if target_symbol and target_quote is None:
        missing_fields.append("target_quote")
    missing_peer_identifiers = [
        identifier
        for identifier in requested_identifiers
        if not any(
            identifier in {str(item.get("identifier")), str(item.get("symbol"))}
            for item in batch_results
        )
    ]
    if missing_peer_identifiers:
        missing_fields.append("batch_quotes_for:" + ",".join(missing_peer_identifiers))
    if not overview:
        missing_fields.append("market_overview")

    sources = sorted(
        {
            source
            for payload in results.values()
            if isinstance(payload, dict)
            for source in normalize_sources(payload.get("source"))
        }
    )
    recommended_source = normalize_sources(target_payload.get("source")) or normalize_sources(
        batch.get("source")
    )
    component_source_errors = [
        error
        for payload in results.values()
        if isinstance(payload, dict)
        for error in normalize_source_errors(payload.get("source_errors"))
    ]
    all_source_errors = [*source_errors, *component_source_errors]
    source_updated_at = (target_quote or {}).get("source_updated_at") or latest_time
    completed_at = now_iso()
    return {
        "snapshot_id": f"market-snapshot-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
        "requested_as_of": requested_as_of,
        "as_of_status": "captured_now_from_current_or_latest_public_snapshots",
        "snapshot_started_at": started_iso,
        "snapshot_completed_at": completed_at,
        "snapshot_span_ms": int((perf_counter() - started_at) * 1000),
        "symbol": target_symbol,
        "requested_identifiers": requested_identifiers,
        "market_time": latest_time,
        "market_time_range": {"earliest": earliest_time, "latest": latest_time},
        "source_time_difference_seconds": time_difference_seconds,
        "source_updated_at": source_updated_at,
        "source_difference_pct": source_difference_pct,
        "recommended_source": recommended_source,
        "recommended_source_reason": (
            "The dedicated target quote is preferred for the target security; the batch snapshot keeps peers on one public request."
            if target_quote
            else "The batch snapshot is the available synchronized security source."
        ),
        "target_quote": target_quote,
        "peer_quotes": [
            item for item in batch_results if str(item.get("symbol")) != target_symbol
        ],
        "market_overview": (
            overview if detail_level == "raw" else compact_market_overview_for_snapshot(overview)
        ),
        "component_status": component_status,
        "source": sources,
        "source_errors": all_source_errors,
        "conflicts": conflicts,
        "missing_fields": missing_fields,
        "detail_level": detail_level,
        "data_status": (
            "full_data"
            if not all_source_errors and not missing_fields and not conflicts
            else "partial_data"
        ),
        "queried_at": completed_at,
        "note": "One bounded capture with explicit component times and source differences. It does not reconstruct an arbitrary historical snapshot or produce an investment judgement.",
    }


def get_market_data_health_data() -> dict[str, Any]:
    with SOURCE_HEALTH_LOCK:
        observed = deepcopy(SOURCE_HEALTH)
    with PREFERRED_ROUTE_HEALTH_LOCK:
        preferred_routes = deepcopy(PREFERRED_ROUTE_HEALTH)

    sources = []
    for source in ("eastmoney", "tencent", "sina"):
        state = observed.get(source)
        if state is None:
            sources.append(
                {
                    "source": source,
                    "status": "unknown_not_yet_observed",
                    "attempt_count": 0,
                    "success_rate": None,
                    "average_latency_ms": None,
                    "last_success_at": None,
                    "last_error": None,
                    "consecutive_failures": 0,
                    "adaptive_fast_fallback": False,
                }
            )
            continue
        attempts = state["attempt_count"]
        success_rate = state["success_count"] / attempts if attempts else None
        status = "healthy" if state["success_count"] >= state["failure_count"] else "degraded"
        sources.append(
            {
                "source": source,
                "status": status,
                "attempt_count": attempts,
                "success_rate": round(success_rate, 3) if success_rate is not None else None,
                "average_latency_ms": state["average_latency_ms"],
                "last_success_at": state["last_success_at"],
                "last_error": state["last_error"],
                "consecutive_failures": state.get("consecutive_failures", 0),
                "adaptive_fast_fallback": source_is_temporarily_degraded(source),
            }
        )

    degraded = any(item["status"] == "degraded" for item in sources)
    with TOOL_CACHE_LOCK:
        cache_entries = len(TOOL_CACHE)
    return {
        "sources": sources,
        "quote_route": {
            "status": "configured",
            "providers": ["eastmoney", "tencent", "sina"],
            "strategy": "parallel_fastest_success_with_6_second_total_budget",
        },
        "intraday_route": {
            "status": "configured",
            "providers": ["eastmoney", "tencent"],
            "strategy": "parallel_fastest_success_with_9_second_total_budget_and_session_filter",
        },
        "kline_route": {
            "status": "configured",
            "providers": ["eastmoney", "tencent"],
            "strategy": "preferred_source_with_adaptive_fast_fallback_and_shared_source_payload_cache",
            "eastmoney_circuit": {
                **preferred_routes.get(
                    "kline:eastmoney",
                    {"attempt_count": 0, "consecutive_failures": 0},
                ),
                "adaptive_fast_fallback": preferred_route_is_temporarily_degraded(
                    "kline:eastmoney"
                ),
            },
        },
        "market_overview_route": {
            "status": "configured",
            "breadth_providers": [
                "eastmoney_push2",
                "eastmoney_push2delay",
                "eastmoney_82_push2",
            ],
            "strategy": "parallel_fastest_exchange_aggregate_v3_production_latency_budget",
        },
        "etf_route": {
            "status": "configured",
            "providers": ["eastmoney", "tencent", "sina"],
            "note": "ETF and LOF code prefixes are classified before public-source routing.",
        },
        "cache": {
            "entry_count": cache_entries,
            "max_entries": TOOL_CACHE_MAX_ENTRIES,
            "policy": "bounded_short_TTL_success_only_with_singleflight",
        },
        "routing_revision": ROUTING_REVISION,
        "degraded_mode": degraded,
        "note": "Observed request health only; this endpoint does not fabricate a live probe or investment conclusion.",
        "source": ["in_process_observability"],
    }


def mcp_error(
    symbol: str | None, exc: HTTPException, started_at: float | None = None
) -> dict[str, Any]:
    tool_queried_at = now_iso()
    message = str(exc.detail)
    result: dict[str, Any] = {
        "ok": False,
        "market_status": market_status_at(),
        "trade_date": None,
        "effective_market_time": None,
        "market_time": None,
        "source_fetch_time": None,
        "tool_queried_at": tool_queried_at,
        "queried_at": tool_queried_at,
        "source": [],
        "source_errors": [
            {
                "source": "public_market_source",
                "error_type": classify_error_type(message, exc.status_code),
                "message": message,
            }
        ],
        "is_stale": False,
        "stale_reason": None,
        "staleness_basis": "unavailable",
        "data_age_seconds": None,
        "latency_ms": int((perf_counter() - started_at) * 1000) if started_at else 0,
        "cache_hit": False,
        "cache_created_at": None,
        "cache_age_seconds": None,
        "snapshot_id": f"snapshot-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
        "source_updated_at": None,
        "missing_fields": [],
        "conflicts": [],
        "data_status": "unavailable",
        "detail_level": "summary",
        "error_type": classify_error_type(message, exc.status_code),
        "error": message,
        "data": {},
    }
    if symbol:
        result["symbol"] = symbol
    return result


def run_cached_tool(
    tool_name: str,
    parameters: dict[str, Any],
    ttl_seconds: int,
    loader: Any,
    symbol: str | None = None,
    max_stale_age_seconds: int | None = None,
) -> dict[str, Any]:
    started_at = perf_counter()
    key = cache_key(tool_name, parameters)
    try:
        data, cache = get_cached_tool_data(
            key,
            ttl_seconds,
            loader,
        )
    except HTTPException as exc:
        stale_snapshot = (
            get_cached_tool_snapshot(key, max_stale_age_seconds)
            if max_stale_age_seconds is not None
            else None
        )
        if stale_snapshot is not None:
            data, cache = stale_snapshot
            data["served_from_stale_cache"] = True
            data["live_refresh_error"] = str(exc.detail)
            data.setdefault("source_errors", []).append(
                f"live_refresh: {exc.detail}; using a recent successful cache entry"
            )
            result = standardize_tool_success(data, started_at, cache)
            result["is_stale"] = True
            result["stale_reason"] = "live_sources_failed_using_recent_cache"
            result["staleness_basis"] = "recent_success_cache_after_live_source_failure"
            result["data"]["staleness_basis"] = result["staleness_basis"]
            return result
        return mcp_error(symbol, exc, started_at)
    return standardize_tool_success(data, started_at, cache)


@mcp.tool(
    name="search_a_share",
    title="Search A-share stocks and listed funds",
    description="Search A-share stocks, ETFs, and LOFs by code or name before querying a quote.",
    annotations=READ_ONLY_TOOL,
)
def search_a_share(keyword: str, limit: int = 5) -> dict[str, Any]:
    keyword = keyword.strip()
    if not keyword:
        return mcp_error(None, HTTPException(status_code=400, detail="keyword is required."))
    normalized_limit = max(1, min(limit, 5))
    return run_cached_tool(
        "search_a_share",
        {"keyword": keyword, "limit": normalized_limit},
        60,
        lambda: search_stock_data(keyword=keyword, limit=normalized_limit),
    )


@mcp.tool(
    name="get_a_share_quote",
    title="Get a stock or listed-fund quote",
    description="Get the latest available price, daily change, trading range, volume, turnover, and available market values for one A-share stock, ETF, or LOF code. Monetary fields carry explicit CNY units.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_quote(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))

    def quote_response() -> dict[str, Any]:
        payload = get_quote_data(symbol=symbol)
        quote = payload["quote"]
        return {
            **{field: clean_value(quote.get(field)) for field in QUOTE_RESPONSE_FIELDS},
            "security_type": quote.get("security_type"),
            "exchange": quote.get("exchange"),
            "source": payload["source"],
            "source_errors": payload.get("source_errors", []),
            "trade_date": quote.get("trade_date"),
            "quote_time": quote.get("quote_time"),
            "source_updated_at": quote.get("source_updated_at"),
            "note": payload["note"],
        }

    return run_cached_tool(
        "get_a_share_quote", {"symbol": symbol}, 2, quote_response, symbol,
        max_stale_age_seconds=15,
    )


@mcp.tool(
    name="get_a_share_batch_quotes",
    title="Get A-share and ETF batch quotes",
    description=(
        "Get up to 20 A-share, ETF, LOF, or explicit index:code quotes from one public batch snapshot. "
        "Invalid or missing items are reported without discarding successful items."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_batch_quotes(symbols: list[str]) -> dict[str, Any]:
    return run_cached_tool(
        "get_a_share_batch_quotes", {"symbols": symbols}, 2,
        lambda: get_batch_quote_data(symbols),
        max_stale_age_seconds=15,
    )


@mcp.tool(
    name="get_a_share_kline",
    title="Get stock or listed-fund price history",
    description=(
        "Get up to 500 A-share stock, ETF, or LOF bars for a date range and adjustment mode, with backward pagination. "
        "Supports 1/5/15/30/60-minute, daily, weekly, and monthly periods. Minute fallback history is capped by the public source and explicitly marked partial when the requested range is longer."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_kline(
    symbol: str,
    period: str = "daily",
    limit: int = 30,
    start_date: str | None = None,
    end_date: str | None = None,
    adjust: str = "forward",
    page_token: str | None = None,
    detail_level: str = "summary",
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 500))
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            symbol,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )

    def kline_response() -> dict[str, Any]:
        payload = get_kline_data(
            symbol=symbol,
            period=period,
            limit=normalized_limit,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            page_token=page_token,
        )
        response_fields = (
            KLINE_RAW_RESPONSE_FIELDS if detail_level == "raw" else KLINE_RESPONSE_FIELDS
        )
        return {
            "symbol": payload["symbol"],
            "security_type": payload.get("security_type"),
            "exchange": payload.get("exchange"),
            "period": payload["period"],
            "adjustment": payload.get("adjustment"),
            "adjustment_source_parameter": payload.get(
                "adjustment_source_parameter"
            ),
            "requested_start_date": payload.get("requested_start_date"),
            "requested_end_date": payload.get("requested_end_date"),
            "available_start": payload.get("available_start"),
            "available_end": payload.get("available_end"),
            "coverage_status": payload.get("coverage_status"),
            "has_more": payload.get("has_more", False),
            "next_page_token": payload.get("next_page_token"),
            "detail_level": detail_level,
            "missing_fields": payload.get("missing_fields", []),
            "conflicts": [],
            "count": payload["count"],
            "items": [
                {field: clean_value(item.get(field)) for field in response_fields}
                for item in payload["items"]
            ],
            "source": payload["source"],
            "source_errors": payload.get("source_errors", []),
            "latest_trade_date": payload["latest_trade_date"],
            "note": payload["note"],
        }

    ttl_seconds = 300 if period in {"daily", "weekly", "monthly"} else 15
    max_stale_age = 86400 if period in {"daily", "weekly", "monthly"} else 120
    return run_cached_tool(
        "get_a_share_kline",
        {
            "symbol": symbol,
            "period": period,
            "limit": normalized_limit,
            "start_date": start_date,
            "end_date": end_date,
            "adjust": adjust,
            "page_token": page_token,
            "detail_level": detail_level,
        },
        ttl_seconds,
        kline_response,
        symbol,
        max_stale_age_seconds=max_stale_age,
    )


@mcp.tool(
    name="get_a_share_intraday",
    title="Get A-share intraday prices",
    description=(
        "Get up to 240 one-minute intraday records for one A-share stock, ETF, or LOF, "
        "with explicitly defined mechanical intraday indicators."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_intraday(symbol: str, limit: int = 240) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 240))
    return run_cached_tool(
        "get_a_share_intraday",
        {"symbol": symbol, "limit": normalized_limit},
        15,
        lambda: get_intraday_data(symbol, normalized_limit),
        symbol,
        max_stale_age_seconds=120,
    )


@mcp.tool(
    name="get_a_share_auction",
    title="Get A-share opening-auction facts",
    description=(
        "Get publicly verifiable opening-auction facts for one A-share stock, ETF, or LOF. "
        "Fields not provided by public sources are returned as unavailable."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_auction(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    return run_cached_tool(
        "get_a_share_auction", {"symbol": symbol}, 3,
        lambda: get_auction_data(symbol), symbol,
    )


@mcp.tool(
    name="filter_a_share_securities",
    title="Filter A-share securities by explicit conditions",
    description=(
        "Mechanically filter ordinary A-share stocks with caller-supplied price-change, turnover, turnover-rate, "
        "VWAP, and market-cap conditions. No scores or recommendations are applied."
    ),
    annotations=READ_ONLY_TOOL,
)
def filter_a_share_securities(
    security_type: str = "stock",
    exclude_st: bool = True,
    change_pct_min: float | None = None,
    change_pct_max: float | None = None,
    turnover_min: float | None = None,
    turnover_rate_min: float | None = None,
    above_average_price: bool | None = None,
    market_cap_max: float | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    normalized_limit = max(1, min(limit, 200))
    parameters = {
        "security_type": security_type,
        "exclude_st": exclude_st,
        "change_pct_min": change_pct_min,
        "change_pct_max": change_pct_max,
        "turnover_min": turnover_min,
        "turnover_rate_min": turnover_rate_min,
        "above_average_price": above_average_price,
        "market_cap_max": market_cap_max,
        "limit": normalized_limit,
    }
    return run_cached_tool(
        "filter_a_share_securities", parameters, 3,
        lambda: filter_a_share_securities_data(**parameters),
    )


@mcp.tool(
    name="screen_a_share_research_candidates",
    title="Screen auditable A-share research candidates",
    description=(
        "Build an auditable ordinary-A-share research candidate funnel with explicit liquidity, market-breadth, "
        "and 20/60/120/250-session history gates. Preselection uses turnover rather than top daily gain. "
        "Returns no_candidate=true instead of forcing a ticker when evidence or gates fail. Results are research "
        "priorities only, never buy/sell recommendations."
    ),
    annotations=READ_ONLY_TOOL,
)
def screen_a_share_research_candidates(
    change_pct_min: float = 0.5,
    change_pct_max: float = 6.0,
    turnover_min: float = 500_000_000,
    turnover_rate_min: float = 2.0,
    market_cap_max: float = 100_000_000_000,
    candidate_limit: int = 5,
    history_pool_limit: int = 8,
    minimum_market_rise_to_fall_ratio: float = 0.5,
    required_positive_history_windows: list[int] | None = None,
    minimum_history_return_pct: float = 0.0,
    detail_level: str = "summary",
) -> dict[str, Any]:
    if change_pct_min > change_pct_max:
        return mcp_error(
            None,
            HTTPException(
                status_code=400,
                detail="change_pct_min cannot be greater than change_pct_max.",
            ),
        )
    if any(
        value < 0
        for value in (
            turnover_min,
            turnover_rate_min,
            market_cap_max,
            minimum_market_rise_to_fall_ratio,
        )
    ):
        return mcp_error(
            None,
            HTTPException(
                status_code=400,
                detail="Turnover, market-cap, turnover-rate, and market-breadth thresholds must be non-negative.",
            ),
        )
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )
    windows = required_positive_history_windows or [20, 60]
    if not windows or any(window not in HISTORICAL_CONTEXT_WINDOWS for window in windows):
        return mcp_error(
            None,
            HTTPException(
                status_code=400,
                detail="required_positive_history_windows must contain only 20, 60, 120, or 250.",
            ),
        )
    parameters = {
        "change_pct_min": change_pct_min,
        "change_pct_max": change_pct_max,
        "turnover_min": turnover_min,
        "turnover_rate_min": turnover_rate_min,
        "market_cap_max": market_cap_max,
        "candidate_limit": max(1, min(candidate_limit, 10)),
        "history_pool_limit": max(1, min(history_pool_limit, 8)),
        "minimum_market_rise_to_fall_ratio": minimum_market_rise_to_fall_ratio,
        "required_positive_history_windows": sorted(set(windows)),
        "minimum_history_return_pct": minimum_history_return_pct,
        "detail_level": detail_level,
    }
    return run_cached_tool(
        "screen_a_share_research_candidates",
        parameters,
        30,
        lambda: screen_a_share_research_candidates_data(**parameters),
        max_stale_age_seconds=300,
    )


@mcp.tool(
    name="get_a_share_fund_flow",
    title="Get A-share fund flow",
    description="Get up to 10 recent daily public fund-flow estimates for one A-share stock.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_fund_flow(symbol: str, limit: int = 5) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 10))
    return run_cached_tool(
        "get_a_share_fund_flow", {"symbol": symbol, "limit": normalized_limit}, 30,
        lambda: get_fund_flow_data(symbol, normalized_limit), symbol,
        max_stale_age_seconds=3600,
    )


@mcp.tool(
    name="get_a_share_financials",
    title="Get A-share financial metrics",
    description="Get up to four recent public financial reports with revenue, profit, EPS, ROE, margins, and debt ratio.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_financials(symbol: str, limit: int = 4) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 4))
    return run_cached_tool(
        "get_a_share_financials", {"symbol": symbol, "limit": normalized_limit}, 21600,
        lambda: get_financial_data(symbol, normalized_limit), symbol,
    )


@mcp.tool(
    name="get_ipo_subscription_status",
    title="Get public IPO subscription and listing status",
    description=(
        "Look up an exact IPO stock code or company name, or list the recent and upcoming public IPO calendar. "
        "Returns the subscription code, issue price, subscription/allocation/payment/listing dates, issuer limit, "
        "board permission, same-market value or BSE cash-subscription rules, pending fields, and official rule links. "
        "It cannot inspect an account, confirm personal eligibility, application history, or winning allocation."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_ipo_subscription_status(
    symbol_or_name: str | None = None,
    days_ahead: int = 30,
    days_back: int = 7,
    limit: int = 20,
    detail_level: str = "summary",
) -> dict[str, Any]:
    try:
        query = normalize_ipo_query(symbol_or_name)
    except HTTPException as exc:
        return mcp_error(None, exc)
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )
    normalized_ahead = max(0, min(days_ahead, 90))
    normalized_back = max(0, min(days_back, 90))
    normalized_limit = max(1, min(limit, 50))
    parameters = {
        "symbol_or_name": query,
        "days_ahead": normalized_ahead,
        "days_back": normalized_back,
        "limit": normalized_limit,
        "detail_level": detail_level,
    }
    return run_cached_tool(
        "get_ipo_subscription_status",
        parameters,
        300,
        lambda: get_ipo_subscription_status_data(**parameters),
        max_stale_age_seconds=86400,
    )


@mcp.tool(
    name="get_fund_exposure",
    title="Get public fund look-through exposure",
    description=(
        "Get the latest publicly disclosed top holdings, provider industry distribution, stock/bond/cash/other "
        "allocation, disclosure dates, and coverage limits for one six-digit ETF, LOF, or public fund code. "
        "When the provider explicitly identifies a feeder fund's underlying fund, make one bounded look-through "
        "hop and scale that underlying fund's public top holdings. Holdings can lag and may not cover the "
        "complete portfolio; missing data is not inferred."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_fund_exposure(
    fund_code: str,
    holdings_limit: int = 10,
    detail_level: str = "summary",
) -> dict[str, Any]:
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            fund_code,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )
    normalized_limit = max(1, min(holdings_limit, 10))
    return run_cached_tool(
        "get_fund_exposure",
        {
            "fund_code": fund_code,
            "holdings_limit": normalized_limit,
            "detail_level": detail_level,
        },
        1800,
        lambda: get_cached_fund_exposure_data(
            fund_code, normalized_limit, detail_level
        ),
        fund_code,
        max_stale_age_seconds=86400,
    )


@mcp.tool(
    name="get_portfolio_exposure",
    title="Aggregate caller-supplied portfolio exposure",
    description=(
        "Mechanically aggregate up to 20 caller-supplied positions into asset, industry, disclosed underlying, "
        "overlap, and concentration facts. Each position needs identifier and weight_pct; use asset_type=fund for "
        "off-exchange public funds, while listed ETFs/LOFs are detected automatically. The tool does not access an "
        "account, infer undisclosed holdings, score diversification, or recommend trades."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_portfolio_exposure(
    positions: list[dict[str, Any]],
    normalize_weights: bool = True,
    holdings_limit: int = 10,
    detail_level: str = "summary",
) -> dict[str, Any]:
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )
    normalized_limit = max(1, min(holdings_limit, 10))
    parameters = {
        "positions": positions,
        "normalize_weights": normalize_weights,
        "holdings_limit": normalized_limit,
        "detail_level": detail_level,
    }
    return run_cached_tool(
        "get_portfolio_exposure",
        parameters,
        1800,
        lambda: get_portfolio_exposure_data(**parameters),
        max_stale_age_seconds=86400,
    )


@mcp.tool(
    name="get_a_share_news",
    title="Get relevant multi-source A-share news",
    description=(
        "Search recent public company news by stock code and verified company name across multiple news indexes. "
        "Returns mechanically filtered, deduplicated evidence with publisher, publication time, relevance reasons, "
        "source tier, link type, and source failures. Use this before broad web search for current company events. "
        "Set include_industry_context only when broader industry context is requested. It does not judge sentiment, "
        "importance, truth, or trading impact."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_news(
    symbol: str,
    limit: int = 5,
    days: int = 30,
    include_industry_context: bool = False,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 10))
    normalized_days = max(1, min(days, 90))
    return run_cached_tool(
        "get_a_share_news",
        {
            "symbol": symbol,
            "limit": normalized_limit,
            "days": normalized_days,
            "include_industry_context": include_industry_context,
        },
        300,
        lambda: get_news_data(
            symbol, normalized_limit, normalized_days, include_industry_context
        ),
        symbol,
        max_stale_age_seconds=3600,
    )


@mcp.tool(
    name="get_a_share_announcements",
    title="Get official A-share company announcements",
    description=(
        "Get recent official company announcements from the Shanghai or Shenzhen Stock Exchange, "
        "with mechanical event tags and original PDF links."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_announcements(
    symbol: str,
    days: int = 30,
    limit: int = 10,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="symbol is required."),
        )
    normalized_days = max(1, min(days, 365))
    normalized_limit = max(1, min(limit, 25))
    return run_cached_tool(
        "get_a_share_announcements",
        {"symbol": symbol, "days": normalized_days, "limit": normalized_limit},
        300,
        lambda: get_announcement_data(symbol, normalized_days, normalized_limit),
        symbol,
    )


@mcp.tool(
    name="get_a_share_event_timeline",
    title="Build an A-share event timeline",
    description=(
        "Mechanically cluster recent official announcements and attributed media reports for one company, preserve "
        "the distinction between disclosure/publication time and real-world event time, and attach 1/3/5-session "
        "forward-adjusted price feedback where enough trading sessions exist. No sentiment or impact judgement is made."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_event_timeline(
    symbol: str,
    days: int = 60,
    limit: int = 10,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_days = max(1, min(days, 90))
    normalized_limit = max(1, min(limit, 20))
    return run_cached_tool(
        "get_a_share_event_timeline",
        {"symbol": symbol, "days": normalized_days, "limit": normalized_limit},
        300,
        lambda: get_event_timeline_data(symbol, normalized_days, normalized_limit),
        symbol,
        max_stale_age_seconds=3600,
    )


@mcp.tool(
    name="get_a_share_historical_context",
    title="Get mechanical A-share historical context",
    description=(
        "Get forward-adjusted 20, 60, 120, and 250-session returns, volatility, drawdown, "
        "range, turnover, volume, turnover-rate, and amplitude context without scores or recommendations."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_historical_context(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="symbol is required."),
        )
    return run_cached_tool(
        "get_a_share_historical_context",
        {"symbol": symbol},
        300,
        lambda: get_cached_historical_context_data(symbol),
        symbol,
    )


@mcp.tool(
    name="get_a_share_security_status",
    title="Get A-share adjustment and security-status facts",
    description=(
        "Get price-history adjustment, listing, ST-name, standard price-limit reference, current quote observation, "
        "and recent official corporate-action announcement facts without inferring a trading decision."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_security_status(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="symbol is required."),
        )
    return run_cached_tool(
        "get_a_share_security_status",
        {"symbol": symbol},
        60,
        lambda: get_security_status_data(symbol),
        symbol,
    )


@mcp.tool(
    name="get_a_share_decision_context",
    title="Get a multi-source market evidence packet",
    description=(
        "Concurrently gather quote, compact intraday structure, historical context, security status, relative strength, "
        "official announcements, financials, and market overview. Returns evidence and missing-data reasons only; "
        "when a component misses the response budget it also returns exact recommended follow-up tool calls. "
        "It does not produce a recommendation, score, trade, or automated financial decision."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_decision_context(
    symbol: str,
    benchmark_symbol: str | None = None,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="symbol is required."),
        )
    parameters = {
        "symbol": symbol,
        "benchmark_symbol": benchmark_symbol,
    }
    return run_cached_tool(
        "get_a_share_decision_context",
        parameters,
        5,
        lambda: get_decision_context_data(symbol, benchmark_symbol),
        symbol,
    )


@mcp.tool(
    name="get_a_share_relative_strength",
    title="Compare A-share relative strength",
    description=(
        "Compare one A-share or exchange-listed fund with an index benchmark and optional peers "
        "using one public batch snapshot."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_relative_strength(
    symbol: str,
    benchmark_symbol: str | None = None,
    peer_symbols: list[str] | None = None,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="symbol is required."),
        )
    parameters = {
        "symbol": symbol,
        "benchmark_symbol": benchmark_symbol,
        "peer_symbols": peer_symbols or [],
    }
    return run_cached_tool(
        "get_a_share_relative_strength",
        parameters,
        2,
        lambda: get_relative_strength_data(
            symbol,
            benchmark_symbol,
            peer_symbols,
        ),
        symbol,
    )


@mcp.tool(
    name="scan_a_share_intraday_anomalies",
    title="Scan mechanical A-share snapshot anomalies",
    description=(
        "Scan up to 20 A-share, ETF, LOF, or explicit index identifiers for caller-controlled "
        "daily move, volume ratio, turnover, opening-gap, day-range, and benchmark-relative conditions."
    ),
    annotations=READ_ONLY_TOOL,
)
def scan_a_share_intraday_anomalies(
    symbols: list[str],
    benchmark_symbol: str | None = None,
    change_pct_min: float = 3.0,
    volume_ratio_min: float = 2.0,
    turnover_rate_min: float = 5.0,
    gap_pct_min: float = 2.0,
    near_extreme_pct: float = 0.3,
    relative_strength_min: float = 2.0,
    include_untriggered: bool = False,
) -> dict[str, Any]:
    parameters = {
        "symbols": symbols,
        "benchmark_symbol": benchmark_symbol,
        "change_pct_min": change_pct_min,
        "volume_ratio_min": volume_ratio_min,
        "turnover_rate_min": turnover_rate_min,
        "gap_pct_min": gap_pct_min,
        "near_extreme_pct": near_extreme_pct,
        "relative_strength_min": relative_strength_min,
        "include_untriggered": include_untriggered,
    }
    return run_cached_tool(
        "scan_a_share_intraday_anomalies",
        parameters,
        2,
        lambda: scan_intraday_anomalies_data(**parameters),
    )


@mcp.tool(
    name="get_a_share_sector_rankings",
    title="Get A-share sector rankings",
    description=(
        "Get a mechanically sorted public industry or concept-board snapshot. "
        "Industry requests can select one disclosed level to avoid mixed-level rankings."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_sector_rankings(
    sector_type: str = "industry",
    level: str = "2",
    sort_by: str = "change_pct",
    limit: int = 20,
) -> dict[str, Any]:
    normalized_limit = max(1, min(limit, 50))
    return run_cached_tool(
        "get_a_share_sector_rankings",
        {
            "sector_type": sector_type,
            "level": level,
            "sort_by": sort_by,
            "limit": normalized_limit,
        },
        10,
        lambda: get_sector_rankings_data(
            sector_type, level, sort_by, normalized_limit,
        ),
        max_stale_age_seconds=300,
    )


@mcp.tool(
    name="get_a_share_sector_rotation",
    title="Get multi-session A-share sector rotation facts",
    description=(
        "Compare public industry or concept boards across caller-selected 1-20 session lookbacks, including returns "
        "relative to CSI 300, current turnover and breadth, five-session persistence, and 20-session closing-high facts. "
        "Unavailable history and leader continuity remain explicit; no main-line or trading label is assigned."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_sector_rotation(
    sector_type: str = "industry",
    level: str = "2",
    lookbacks: list[int] | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    normalized_limit = max(1, min(limit, 20))
    normalized_lookbacks = lookbacks or [1, 3, 5, 10, 20]
    parameters = {
        "sector_type": sector_type,
        "level": level,
        "lookbacks": normalized_lookbacks,
        "limit": normalized_limit,
    }
    return run_cached_tool(
        "get_a_share_sector_rotation",
        parameters,
        300,
        lambda: get_sector_rotation_data(**parameters),
        max_stale_age_seconds=3600,
    )


@mcp.tool(
    name="get_overnight_risk_packet",
    title="Get an overnight cross-market observation packet",
    description=(
        "Return latest free public observations for Shanghai, LME and COMEX copper, USD/CNY, Nasdaq futures and "
        "index, FTSE China A50 futures, and Hang Seng futures with venue timestamps. Missing dollar-index or US "
        "Treasury-yield routes remain explicit. This tool reports facts only and does not predict the next A-share session."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_overnight_risk_packet(detail_level: str = "summary") -> dict[str, Any]:
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            None,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
        )
    return run_cached_tool(
        "get_overnight_risk_packet",
        {"detail_level": detail_level},
        15,
        lambda: get_overnight_risk_packet_data(detail_level),
        max_stale_age_seconds=1800,
    )


@mcp.tool(
    name="get_a_share_limit_activity",
    title="Get A-share limit activity facts",
    description=(
        "Get public limit-up, limit-down, open-board, seal-rate, consecutive-board, board-height, "
        "exchange breakdown, and selected security details for the latest available trading day. "
        "Returns mechanical facts only and does not label sentiment or infer trading impact."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_limit_activity(limit: int = 20) -> dict[str, Any]:
    normalized_limit = max(1, min(limit, 50))
    return run_cached_tool(
        "get_a_share_limit_activity",
        {"limit": normalized_limit},
        30,
        lambda: get_limit_activity_data(normalized_limit),
        max_stale_age_seconds=3600,
    )


@mcp.tool(
    name="get_a_share_market_snapshot",
    title="Capture a synchronized A-share market snapshot",
    description=(
        "Capture market overview, an optional target security, and up to nine peers in one bounded request. "
        "Returns one snapshot_id, component timestamps, maximum source-time difference, price conflicts, data age, "
        "recommended source, missing fields, and source errors. Arbitrary historical as-of reconstruction is rejected."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_market_snapshot(
    symbol: str | None = None,
    peer_symbols: list[str] | None = None,
    as_of: str | None = None,
    sector_limit: int = 5,
    detail_level: str = "summary",
) -> dict[str, Any]:
    started_at = perf_counter()
    if detail_level not in {"summary", "raw"}:
        return mcp_error(
            symbol,
            HTTPException(status_code=400, detail="detail_level must be summary or raw."),
            started_at,
        )
    normalized_symbol = symbol.strip() if symbol else None
    normalized_sector_limit = max(1, min(sector_limit, 10))
    return run_cached_tool(
        "get_a_share_market_snapshot",
        {
            "symbol": normalized_symbol,
            "peer_symbols": peer_symbols or [],
            "as_of": as_of,
            "sector_limit": normalized_sector_limit,
            "detail_level": detail_level,
        },
        2,
        lambda: get_market_snapshot_data(
            normalized_symbol,
            peer_symbols,
            as_of,
            normalized_sector_limit,
            detail_level,
        ),
        normalized_symbol,
        max_stale_age_seconds=15,
    )


@mcp.tool(
    name="get_a_share_market_overview",
    title="Get A-share market overview",
    description=(
        "Get major and style index quotes, all-market A-share breadth, turnover, limit statistics, "
        "and leading industry-board performance."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_a_share_market_overview(limit: int = 10) -> dict[str, Any]:
    normalized_limit = max(1, min(limit, 20))
    return run_cached_tool(
        "get_a_share_market_overview", {"limit": normalized_limit}, 5,
        lambda: get_market_overview_data(normalized_limit),
        max_stale_age_seconds=120,
    )


@mcp.tool(
    name="get_market_data_health",
    title="Get market data route health",
    description=(
        "Get observed availability, latency, recent failure rate, cache state, and degraded-mode status "
        "for the public quote, intraday, and ETF data routes. This is operational status, not investment advice."
    ),
    annotations=READ_ONLY_TOOL,
)
def get_market_data_health() -> dict[str, Any]:
    return run_cached_tool(
        "get_market_data_health", {}, 1, get_market_data_health_data,
    )


# Mount last so the health endpoint keeps its direct HTTP path.
app.mount("/", mcp_http_app)
