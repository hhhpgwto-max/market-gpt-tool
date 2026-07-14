import os
import json
import re
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from math import ceil
from threading import Lock
from time import perf_counter
from typing import Any
from urllib.parse import urlencode
from urllib.error import URLError
from urllib.request import Request, urlopen

import efinance as ef
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations


APP_NAME = os.getenv("MARKET_TOOL_NAME", "market-gpt-tool")

MCP_INSTRUCTIONS = (
    "Use these read-only tools for current A-share stock and exchange-traded fund market data, intraday prices, news, "
    "official announcements, fund flows, financial metrics, batch quotes, auction facts, market overview, mechanical "
    "anomaly scans, relative strength, sector rankings, and operational data-route health. "
    "Use search_a_share when the stock code is unclear. "
    "Use get_a_share_quote for the latest quote and get_a_share_kline for recent price history. "
    "All data is informational only and is not investment advice."
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
    version="0.7.0",
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
SOURCE_HEALTH: dict[str, dict[str, Any]] = {}
SOURCE_HEALTH_LOCK = Lock()


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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def cache_key(tool_name: str, parameters: Any) -> str:
    return f"{tool_name}:{json.dumps(parameters, ensure_ascii=False, sort_keys=True, default=str)}"


def get_cached_tool_data(
    key: str, ttl_seconds: int, loader: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
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

    data = loader()
    created_at = datetime.now(timezone.utc)
    with TOOL_CACHE_LOCK:
        TOOL_CACHE[key] = {"created_at": created_at, "data": deepcopy(data)}
    return (
        data,
        {
            "cache_hit": False,
            "cache_created_at": created_at.isoformat(),
            "cache_age_seconds": 0.0,
        },
    )


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
        else:
            state["failure_count"] += 1
            state["last_error_at"] = now
            state["last_error"] = error


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


def standardize_tool_success(
    data: dict[str, Any], started_at: float, cache: dict[str, Any] | None = None
) -> dict[str, Any]:
    queried_at = now_iso()
    content = deepcopy(data)
    content["queried_at"] = queried_at
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
    elif trade_date and market_status == "open" and trade_date < datetime.now(MARKET_TIMEZONE).date().isoformat():
        is_stale = True
        stale_reason = "previous_trade_day_data_during_open_market"

    cache = cache or {
        "cache_hit": False,
        "cache_created_at": queried_at,
        "cache_age_seconds": 0.0,
    }
    result = {"ok": True, **content}
    result.update(
        {
            "market_status": market_status,
            "trade_date": trade_date,
            "market_time": market_time,
            "queried_at": queried_at,
            "source": normalize_sources(content.get("source")),
            "source_errors": normalize_source_errors(content.get("source_errors")),
            "is_stale": is_stale,
            "stale_reason": stale_reason,
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
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_dict(row: pd.Series, columns: dict[str, str]) -> dict[str, Any]:
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

    if volume is not None and source in {"efinance", "eastmoney", "tencent"}:
        volume *= 100
    if turnover is not None and source == "tencent":
        turnover *= 10_000

    quote["volume"] = volume
    quote["volume_unit"] = "share"
    quote["turnover"] = turnover
    quote["turnover_unit"] = "CNY"
    return quote


def get_all_realtime_quotes() -> pd.DataFrame:
    try:
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
    errors = []
    for host in (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    ):
        try:
            payload = read_public_json(
                f"https://{host}/api/qt/ulist.np/get?{query}",
                "https://quote.eastmoney.com/",
                3,
                1,
            )
            rows = ((payload.get("data") or {}).get("diff")) or []
            if rows:
                return rows, host
            errors.append(f"{host}: no quote rows")
        except HTTPException as exc:
            errors.append(f"{host}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


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
        "circulating_market_value": to_number(row.get("f21")),
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


def get_kline_data(symbol: str, period: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    klt = KLINE_PERIODS.get(period)
    if klt is None:
        raise HTTPException(status_code=400, detail=f"Unsupported period: {period}")

    payload = get_fallback_kline(symbol, period, klt, limit)
    security = security_metadata(symbol)
    payload.update(
        {
            "security_type": security["security_type"],
            "exchange": security["exchange"],
        }
    )
    return payload


def get_eastmoney_kline(symbol: str, period: str, klt: int, limit: int) -> dict[str, Any]:
    query = urlencode(
        {
            "secid": eastmoney_secid(symbol),
            "klt": klt,
            "fqt": 1,
            "lmt": limit,
            "end": "20500101",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        }
    )
    payload = read_public_json(
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{query}",
        "https://quote.eastmoney.com/",
    )
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        raise HTTPException(status_code=404, detail=f"Kline data not found from Eastmoney: {symbol}")

    items = []
    for kline in klines[-limit:]:
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

    return {
        "symbol": symbol,
        "period": period,
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "latest_trade_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_tencent_kline(symbol: str, period: str, limit: int) -> dict[str, Any]:
    tencent_periods = {
        "daily": "day",
        "weekly": "week",
        "monthly": "month",
    }
    tencent_period = tencent_periods.get(period)
    if tencent_period is None:
        raise HTTPException(status_code=502, detail=f"Tencent Kline fallback is unavailable for {period}.")

    query = urlencode(
        {
            "param": f"{market_symbol(symbol)},{tencent_period},,,{limit},qfq",
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
    for row in rows[-limit:]:
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

    return {
        "symbol": symbol,
        "period": period,
        "count": len(items),
        "items": items,
        "source": "tencent",
        "latest_trade_date": items[-1]["date"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_fallback_kline(symbol: str, period: str, klt: int, limit: int) -> dict[str, Any]:
    errors = []
    for source, getter in (
        ("eastmoney", lambda: get_eastmoney_kline(symbol, period, klt, limit)),
        ("tencent", lambda: get_tencent_kline(symbol, period, limit)),
    ):
        try:
            payload = getter()
            payload["source_errors"] = errors
            return payload
        except HTTPException as exc:
            errors.append(f"{source}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


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

    return {
        "symbol": symbol,
        "name": clean_value(data.get("name")),
        "previous_close": clean_value(data.get("preClose")),
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
    return {
        "symbol": symbol,
        "name": clean_value(quote[1]) if len(quote) > 1 else None,
        "previous_close": to_number(quote[4]) if len(quote) > 4 else None,
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


def intraday_mechanical_indicators(items: list[dict[str, Any]]) -> dict[str, Any]:
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
    day_high = max(value for value in highs if value is not None)
    day_low = min(value for value in lows if value is not None)
    opening_price = to_number(items[0].get("open")) or to_number(items[0].get("price"))
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
        "return_from_open_pct": percentage_change(current, opening_price),
        "turnover_last_5_reported_minutes": recent_turnover,
        "turnover_previous_5_reported_minutes": prior_turnover if len(items) >= 10 else None,
        "turnover_speed_5m_vs_previous_5m_pct": (
            percentage_change(recent_turnover, prior_turnover) if len(items) >= 10 else None
        ),
        "at_intraday_high": current >= max(valid_prices),
        "at_intraday_low": current <= min(valid_prices),
        "definitions": {
            "returns": "Current price compared with the price N reported trading minutes earlier.",
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
        payload["mechanical_indicators"] = intraday_mechanical_indicators(payload["items"])
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
    payload = read_sina_object(
        "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
        f"MoneyFlow.ssi_ssfx_flzjtj?daima={code}",
        "https://finance.sina.com.cn/",
    )
    quote = get_sina_quote(symbol)
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
    errors = []
    all_not_found = True
    for source, getter in (
        ("eastmoney", get_eastmoney_fund_flow),
        ("sina", get_sina_fund_flow),
    ):
        try:
            payload = getter(symbol, limit)
            payload["source_errors"] = errors
            return payload
        except HTTPException as exc:
            errors.append(f"{source}: {exc.detail}")
            all_not_found = all_not_found and exc.status_code == 404
    status_code = 404 if all_not_found else 502
    raise HTTPException(status_code=status_code, detail="; ".join(errors))


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
    return re.sub(r"<[^>]+>", "", str(value)).replace("\r", " ").replace("\n", " ").strip()


def get_news_data(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    security = security_metadata(symbol)
    callback = "marketNewsCallback"
    parameters = {
        "uid": "",
        "keyword": symbol,
        "type": ["cmsArticleWebOld"],
        "client": "web",
        "clientType": "web",
        "clientVersion": "curr",
        "param": {
            "cmsArticleWebOld": {
                "searchScope": "default",
                "sort": "default",
                "pageIndex": 1,
                "pageSize": limit,
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
        f"https://so.eastmoney.com/news/s?keyword={symbol}",
    )
    articles = (payload.get("result") or {}).get("cmsArticleWebOld") or []
    items = [
        {
            "published_at": clean_value(article.get("date")),
            "title": strip_html(article.get("title")),
            "summary": strip_html(article.get("content")),
            "source": clean_value(article.get("mediaName")),
            "url": clean_value(article.get("url")),
        }
        for article in articles[:limit]
    ]
    return {
        "symbol": symbol,
        "security_type": security["security_type"],
        "exchange": security["exchange"],
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "queried_at": now_iso(),
        "note": "News search may include broader articles that mention the stock code.",
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
        {
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
        }
        for row in index_rows
    ]


def get_tencent_indices() -> list[dict[str, Any]]:
    try:
        text = read_market_text(
            "https://qt.gtimg.cn/q=sh000001,sz399001,sz399006",
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
            {
                "symbol": clean_value(values[2]),
                "name": clean_value(values[1]),
                "price": to_number(values[3]),
                "change_pct": to_number(values[32]),
                "change": to_number(values[31]),
                "open": to_number(values[5]),
                "high": None,
                "low": None,
                "previous_close": to_number(values[4]),
                "source_updated_at": format_market_time(values[30]),
                "market_time": market_time_from_source_update(
                    format_market_time(values[30])
                ),
            }
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
            {
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
            }
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
    for host in (
        "push2.eastmoney.com",
        "push2delay.eastmoney.com",
        "82.push2.eastmoney.com",
    ):
        try:
            payload = read_public_json(
                f"https://{host}/api/qt/clist/get?{query}",
                "https://quote.eastmoney.com/",
                3,
                1,
            )
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


def latest_market_time(indices: list[dict[str, Any]]) -> str | None:
    timestamps = [
        timestamp
        for index in indices
        if (timestamp := index.get("market_time") or index.get("source_updated_at"))
    ]
    return max(timestamps) if timestamps else None


def is_market_time_stale(market_time: str | None) -> bool:
    now = datetime.now(MARKET_TIMEZONE)
    if market_status_at(now) != "open":
        return False
    source_time = parse_market_datetime(market_time)
    if source_time is None:
        return True
    if source_time.date() != now.date():
        return True
    return now - source_time > timedelta(minutes=10)


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

    boards = get_eastmoney_sector_boards(normalized_type)
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
        "source_errors": [],
        "queried_at": now_iso(),
        "note": "Mechanical ranking of public sector quotes only; no theme, trading, or investment judgement is generated.",
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

            eastmoney_success = next(
                (item for item in successes if item[0] == "eastmoney"),
                None,
            )
            if eastmoney_success:
                return {
                    "indices": eastmoney_success[1],
                    "source": eastmoney_success[0],
                    "source_errors": errors,
                }
            eastmoney_pending = any(
                futures[future] == "eastmoney" for future in pending
            )
            if successes and not eastmoney_pending:
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
    sources = [f"indices:{index_source}"]
    if boards:
        sources.append(f"industry_boards:{board_source}")
    if breadth:
        sources.append(f"market_breadth:{breadth_component.get('source', 'unavailable')}")

    primary_indices = [
        index for index in indices if index.get("symbol") in PRIMARY_INDEX_SYMBOLS
    ]
    if not primary_indices:
        primary_indices = indices[:3]
    style_indices = [index for index in indices if index not in primary_indices]
    all_market_breadth = breadth.get("all_market") if breadth else None
    limit_stats = (
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

    return {
        "market_status": market_status_at(),
        "trade_date": market_time.split("T", 1)[0] if market_time else None,
        "market_time": market_time,
        "indices": primary_indices,
        "style_indices": style_indices,
        "index_source": index_source,
        "industry_boards": boards,
        "industry_board_source": board_source,
        "industry_board_errors": board_component.get("source_errors", []) if not boards else [],
        "market_breadth": breadth,
        "market_breadth_source": breadth_component.get("source", "unavailable"),
        "market_breadth_coverage_status": breadth_component.get("coverage_status"),
        "market_breadth_row_count": breadth_component.get("row_count"),
        "turnover": turnover,
        "limit_stats": limit_stats,
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
            and str(breadth_component.get("coverage_status") or "").startswith("complete")
            else "partial_data"
        ),
        "note": "Facts and mechanical calculations only; slow components use recent successful cache or return as unavailable within a six-second budget.",
    }


def get_market_data_health_data() -> dict[str, Any]:
    with SOURCE_HEALTH_LOCK:
        observed = deepcopy(SOURCE_HEALTH)

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
        "cache": {"entry_count": cache_entries, "policy": "short_TTL_success_only"},
        "degraded_mode": degraded,
        "note": "Observed request health only; this endpoint does not fabricate a live probe or investment conclusion.",
        "source": ["in_process_observability"],
    }


def mcp_error(
    symbol: str | None, exc: HTTPException, started_at: float | None = None
) -> dict[str, Any]:
    queried_at = now_iso()
    message = str(exc.detail)
    result: dict[str, Any] = {
        "ok": False,
        "market_status": market_status_at(),
        "trade_date": None,
        "market_time": None,
        "queried_at": queried_at,
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
        "data_age_seconds": None,
        "latency_ms": int((perf_counter() - started_at) * 1000) if started_at else 0,
        "cache_hit": False,
        "cache_created_at": None,
        "cache_age_seconds": None,
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
) -> dict[str, Any]:
    started_at = perf_counter()
    try:
        data, cache = get_cached_tool_data(
            cache_key(tool_name, parameters),
            ttl_seconds,
            loader,
        )
    except HTTPException as exc:
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
    description="Get the latest available price, daily change, trading range, volume, and turnover for one A-share stock, ETF, or LOF code.",
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
        "get_a_share_quote", {"symbol": symbol}, 2, quote_response, symbol
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
    )


@mcp.tool(
    name="get_a_share_kline",
    title="Get stock or listed-fund price history",
    description="Get up to 30 recent A-share stock, ETF, or LOF price records for a daily, weekly, monthly, or intraday period.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_kline(
    symbol: str,
    period: str = "daily",
    limit: int = 30,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 30))

    def kline_response() -> dict[str, Any]:
        payload = get_kline_data(symbol=symbol, period=period, limit=normalized_limit)
        return {
            "symbol": payload["symbol"],
            "security_type": payload.get("security_type"),
            "exchange": payload.get("exchange"),
            "period": payload["period"],
            "count": payload["count"],
            "items": [
                {field: clean_value(item.get(field)) for field in KLINE_RESPONSE_FIELDS}
                for item in payload["items"]
            ],
            "source": payload["source"],
            "source_errors": payload.get("source_errors", []),
            "latest_trade_date": payload["latest_trade_date"],
            "note": payload["note"],
        }

    ttl_seconds = 300 if period in {"daily", "weekly", "monthly"} else 15
    return run_cached_tool(
        "get_a_share_kline",
        {"symbol": symbol, "period": period, "limit": normalized_limit},
        ttl_seconds,
        kline_response,
        symbol,
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
    name="get_a_share_news",
    title="Get A-share news",
    description="Get up to 10 recent public news articles that mention one A-share stock code.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_news(symbol: str, limit: int = 5) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return mcp_error(None, HTTPException(status_code=400, detail="symbol is required."))
    normalized_limit = max(1, min(limit, 10))
    return run_cached_tool(
        "get_a_share_news", {"symbol": symbol, "limit": normalized_limit}, 120,
        lambda: get_news_data(symbol, normalized_limit), symbol,
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
