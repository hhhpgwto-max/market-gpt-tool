import os
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
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
    "Use these read-only tools for current A-share market data, intraday prices, news, "
    "fund flows, financial metrics, and market overview. "
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
    version="0.3.0",
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

INDEX_SECIDS = "1.000001,0.399001,0.399006"
MARKET_TIMEZONE = timezone(timedelta(hours=8))

SYMBOL_PATTERN = re.compile(r"^\d{6}$")


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not SYMBOL_PATTERN.fullmatch(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must be a six-digit A-share stock code.",
        )
    return normalized


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    market = "1" if symbol.startswith(("6", "9")) else "0"
    return f"{market}.{symbol}"


def market_symbol(symbol: str) -> str:
    prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
    return f"{prefix}{symbol}"


def to_number(value: Any) -> float | None:
    if value in (None, "-", ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_market_text(url: str, referer: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
        },
    )
    # Callers construct URLs from a fixed quote-provider host list.
    with urlopen(request, timeout=15) as response:  # nosec B310
        return response.read().decode("gbk", errors="replace")


def read_public_json(url: str, referer: str) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Accept": "application/json, text/plain, */*",
        },
    )
    errors = []
    for _ in range(2):
        try:
            with urlopen(request, timeout=15) as response:  # nosec B310
                return json.loads(response.read().decode("utf-8"))
        except (OSError, URLError, json.JSONDecodeError) as exc:
            errors.append(str(exc))
    raise HTTPException(status_code=502, detail=f"Failed to fetch public market data: {'; '.join(errors)}")


def read_public_jsonp(url: str, referer: str) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": referer,
            "Accept": "*/*",
        },
    )
    try:
        with urlopen(request, timeout=15) as response:  # nosec B310
            text = response.read().decode("utf-8")
        start = text.find("(")
        end = text.rfind(")")
        if start < 0 or end <= start:
            raise ValueError("Unexpected JSONP response.")
        return json.loads(text[start + 1 : end])
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch public market news: {exc}") from exc


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
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://quote.eastmoney.com/",
        },
    )

    try:
        # This Request always targets Eastmoney's fixed API host.
        with urlopen(request, timeout=15) as response:  # nosec B310
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Eastmoney quote: {exc}") from exc

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

    market_names = {"sh": "沪A", "sz": "深A", "bj": "北交所"}
    results = []
    for candidate in text.split('"', 2)[1].split("^"):
        parts = candidate.split("~")
        if len(parts) < 5 or parts[4] != "GP-A" or not SYMBOL_PATTERN.fullmatch(parts[1]):
            continue
        try:
            name = json.loads(f'"{parts[2]}"')
        except json.JSONDecodeError:
            name = parts[2]
        results.append(
            {
                "symbol": parts[1],
                "name": name,
                "market": market_names.get(parts[0], parts[0]),
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

    market_names = {"sh": "沪A", "sz": "深A", "bj": "北交所"}
    results = []
    for candidate in text.split('"', 2)[1].split(";"):
        parts = candidate.split(",")
        if len(parts) < 4 or not SYMBOL_PATTERN.fullmatch(parts[2]):
            continue
        market_prefix = parts[3][:2].lower()
        if market_prefix not in market_names:
            continue
        results.append(
            {
                "symbol": parts[2],
                "name": parts[0],
                "market": market_names[market_prefix],
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

    try:
        data = get_all_realtime_quotes()
        if "股票代码" not in data.columns:
            raise HTTPException(status_code=502, detail="Unexpected market data format.")

        rows = data[data["股票代码"].astype(str) == symbol]
        if rows.empty:
            raise HTTPException(status_code=404, detail=f"Stock not found: {symbol}")

        row = rows.iloc[0]
        quote = row_to_dict(row, QUOTE_COLUMNS)
        quote["source_updated_at"] = next(
            (
                format_market_time(row.get(column))
                for column in ("更新时间", "行情时间", "时间")
                if column in row.index and format_market_time(row.get(column))
            ),
            None,
        )
        if not quote["source_updated_at"]:
            raise HTTPException(
                status_code=502,
                detail="Realtime quote source did not provide an update timestamp.",
            )
        source = "efinance"
    except HTTPException:
        quote, source = get_fallback_quote(symbol)

    quote = normalize_quote_units(quote, source)
    quote.update(derive_quote_timestamps(quote.get("source_updated_at")))

    return {
        "quote": quote,
        "source": source,
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_kline_data(symbol: str, period: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    klt = KLINE_PERIODS.get(period)
    if klt is None:
        raise HTTPException(status_code=400, detail=f"Unsupported period: {period}")

    return get_fallback_kline(symbol, period, klt, limit)


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
            return getter()
        except HTTPException as exc:
            errors.append(f"{source}: {exc.detail}")
    raise HTTPException(status_code=502, detail="; ".join(errors))


def get_intraday_data(symbol: str, limit: int) -> dict[str, Any]:
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

    items = []
    for trend in trends[-limit:]:
        values = trend.split(",")
        if len(values) < 8:
            continue
        items.append(
            {
                "time": values[0],
                "open": to_number(values[1]),
                "price": to_number(values[2]),
                "high": to_number(values[3]),
                "low": to_number(values[4]),
                "volume": to_number(values[5]),
                "turnover": to_number(values[6]),
                "average_price": to_number(values[7]),
            }
        )

    return {
        "symbol": symbol,
        "name": clean_value(data.get("name")),
        "previous_close": clean_value(data.get("preClose")),
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "latest_market_time": items[-1]["time"],
        "queried_at": now_iso(),
        "note": "For information only. Not investment advice.",
    }


def get_fund_flow_data(symbol: str, limit: int) -> dict[str, Any]:
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


def get_financial_data(symbol: str, limit: int) -> dict[str, Any]:
    symbol = normalize_symbol(symbol)
    secucode = f"{symbol}.{'SH' if symbol.startswith(('6', '9')) else 'SZ'}"
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
        "count": len(items),
        "items": items,
        "source": "eastmoney",
        "queried_at": now_iso(),
        "note": "News search may include broader articles that mention the stock code.",
    }


def get_market_overview_data(limit: int) -> dict[str, Any]:
    index_query = urlencode(
        {
            "fltt": 2,
            "invt": 2,
            "fields": "f12,f14,f2,f3,f4,f17,f18",
            "secids": INDEX_SECIDS,
        }
    )
    index_payload = read_public_json(
        f"https://push2.eastmoney.com/api/qt/ulist.np/get?{index_query}",
        "https://quote.eastmoney.com/",
    )
    index_rows = ((index_payload.get("data") or {}).get("diff")) or []
    indices = [
        {
            "symbol": clean_value(row.get("f12")),
            "name": clean_value(row.get("f14")),
            "price": clean_value(row.get("f2")),
            "change_pct": clean_value(row.get("f3")),
            "change": clean_value(row.get("f4")),
            "open": clean_value(row.get("f17")),
            "previous_close": clean_value(row.get("f18")),
        }
        for row in index_rows
    ]

    boards: list[dict[str, Any]] = []
    board_source = "unavailable"
    try:
        data = get_all_realtime_quotes()
        industry_column = next(
            (column for column in ("所属行业", "所处行业", "行业") if column in data.columns),
            None,
        )
        if industry_column and "涨跌幅" in data.columns:
            grouped: dict[str, list[float]] = {}
            for _, row in data.iterrows():
                industry = clean_value(row.get(industry_column))
                change_pct = to_number(row.get("涨跌幅"))
                if industry and change_pct is not None:
                    grouped.setdefault(str(industry), []).append(change_pct)
            boards = [
                {
                    "name": name,
                    "average_change_pct": round(sum(changes) / len(changes), 2),
                    "stock_count": len(changes),
                }
                for name, changes in grouped.items()
            ]
            boards.sort(key=lambda item: item["average_change_pct"], reverse=True)
            boards = boards[:limit]
            board_source = "efinance_calculated"
    except HTTPException:
        pass

    return {
        "indices": indices,
        "industry_boards": boards,
        "industry_board_source": board_source,
        "queried_at": now_iso(),
        "note": "Industry-board changes are calculated from available constituent quotes when present.",
    }


def mcp_error(symbol: str | None, exc: HTTPException) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "error": str(exc.detail),
        "time": now_iso(),
    }
    if symbol:
        result["symbol"] = symbol
    return result


@mcp.tool(
    name="search_a_share",
    title="Search A-share stocks",
    description="Search A-share stocks by a stock code or company name before querying a quote.",
    annotations=READ_ONLY_TOOL,
)
def search_a_share(keyword: str, limit: int = 5) -> dict[str, Any]:
    keyword = keyword.strip()
    if not keyword:
        return {
            "ok": False,
            "error": "keyword is required.",
            "time": now_iso(),
        }

    try:
        payload = search_stock_data(
            keyword=keyword,
            limit=max(1, min(limit, 5)),
        )
    except HTTPException:
        return {
            "ok": False,
            "error": "Stock search is temporarily unavailable. Try a six-digit stock code.",
            "queried_at": now_iso(),
        }

    return {
        "ok": True,
        "keyword": payload["keyword"],
        "count": payload["count"],
        "results": payload["results"],
        "source": payload["source"],
        "queried_at": payload["queried_at"],
    }


@mcp.tool(
    name="get_a_share_quote",
    title="Get an A-share quote",
    description="Get the latest available price, daily change, trading range, volume, and turnover for one A-share stock code.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_quote(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {
            "ok": False,
            "error": "symbol is required.",
            "time": now_iso(),
        }

    try:
        payload = get_quote_data(symbol=symbol)
    except HTTPException as exc:
        return mcp_error(symbol, exc)

    quote = payload["quote"]
    return {
        "ok": True,
        **{field: clean_value(quote.get(field)) for field in QUOTE_RESPONSE_FIELDS},
        "source": payload["source"],
        "trade_date": quote.get("trade_date"),
        "quote_time": quote.get("quote_time"),
        "source_updated_at": quote.get("source_updated_at"),
        "queried_at": payload["queried_at"],
        "note": payload["note"],
    }


@mcp.tool(
    name="get_a_share_kline",
    title="Get A-share price history",
    description="Get up to 30 recent A-share price records for a daily, weekly, monthly, or intraday period.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_kline(
    symbol: str,
    period: str = "daily",
    limit: int = 30,
) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {
            "ok": False,
            "error": "symbol is required.",
            "time": now_iso(),
        }

    try:
        payload = get_kline_data(
            symbol=symbol,
            period=period,
            limit=max(1, min(limit, 30)),
        )
    except HTTPException as exc:
        return mcp_error(symbol, exc)

    return {
        "ok": True,
        "symbol": payload["symbol"],
        "period": payload["period"],
        "count": payload["count"],
        "items": [
            {field: clean_value(item.get(field)) for field in KLINE_RESPONSE_FIELDS}
            for item in payload["items"]
        ],
        "source": payload["source"],
        "latest_trade_date": payload["latest_trade_date"],
        "queried_at": payload["queried_at"],
        "note": payload["note"],
    }


@mcp.tool(
    name="get_a_share_intraday",
    title="Get A-share intraday prices",
    description="Get up to 240 one-minute intraday records for one A-share stock.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_intraday(symbol: str, limit: int = 240) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {"ok": False, "error": "symbol is required.", "time": now_iso()}

    try:
        return {"ok": True, **get_intraday_data(symbol, max(1, min(limit, 240)))}
    except HTTPException as exc:
        return mcp_error(symbol, exc)


@mcp.tool(
    name="get_a_share_fund_flow",
    title="Get A-share fund flow",
    description="Get up to 10 recent daily public fund-flow estimates for one A-share stock.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_fund_flow(symbol: str, limit: int = 5) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {"ok": False, "error": "symbol is required.", "time": now_iso()}

    try:
        return {"ok": True, **get_fund_flow_data(symbol, max(1, min(limit, 10)))}
    except HTTPException as exc:
        return mcp_error(symbol, exc)


@mcp.tool(
    name="get_a_share_financials",
    title="Get A-share financial metrics",
    description="Get up to four recent public financial reports with revenue, profit, EPS, ROE, margins, and debt ratio.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_financials(symbol: str, limit: int = 4) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {"ok": False, "error": "symbol is required.", "time": now_iso()}

    try:
        return {"ok": True, **get_financial_data(symbol, max(1, min(limit, 4)))}
    except HTTPException as exc:
        return mcp_error(symbol, exc)


@mcp.tool(
    name="get_a_share_news",
    title="Get A-share news",
    description="Get up to 10 recent public news articles that mention one A-share stock code.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_news(symbol: str, limit: int = 5) -> dict[str, Any]:
    symbol = symbol.strip()
    if not symbol:
        return {"ok": False, "error": "symbol is required.", "time": now_iso()}

    try:
        return {"ok": True, **get_news_data(symbol, max(1, min(limit, 10)))}
    except HTTPException as exc:
        return mcp_error(symbol, exc)


@mcp.tool(
    name="get_a_share_market_overview",
    title="Get A-share market overview",
    description="Get major A-share index quotes and, when available, leading industry-board performance.",
    annotations=READ_ONLY_TOOL,
)
def get_a_share_market_overview(limit: int = 5) -> dict[str, Any]:
    try:
        return {"ok": True, **get_market_overview_data(max(1, min(limit, 10)))}
    except HTTPException as exc:
        return mcp_error(None, exc)


# Mount last so the health endpoint keeps its direct HTTP path.
app.mount("/", mcp_http_app)
