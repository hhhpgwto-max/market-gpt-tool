import os
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import efinance as ef
import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations


APP_NAME = os.getenv("MARKET_TOOL_NAME", "market-gpt-tool")
API_TOKEN = os.getenv("MARKET_TOOL_TOKEN", "").strip()

MCP_INSTRUCTIONS = (
    "Use these read-only tools for current A-share market data. "
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
)
mcp_http_app = mcp.streamable_http_app()


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(
    title="Market GPT Tool",
    version="0.2.0",
    description="A small market data API for a ChatGPT MCP app and Custom GPT Action.",
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
    "turnover",
)

KLINE_RESPONSE_FIELDS = (
    "date",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "turnover",
    "change_pct",
)

SYMBOL_PATTERN = re.compile(r"^\d{6}$")


def require_token(x_api_key: str | None) -> None:
    if API_TOKEN and x_api_key != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing x-api-key.")


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


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_dict(row: pd.Series, columns: dict[str, str]) -> dict[str, Any]:
    return {
        output_key: clean_value(row.get(input_key))
        for input_key, output_key in columns.items()
        if input_key in row.index
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


def get_tencent_quote(symbol: str) -> dict[str, Any]:
    url = f"http://qt.gtimg.cn/q={market_symbol(symbol)}"
    try:
        text = read_market_text(url, "https://stockapp.finance.qq.com/")
    except OSError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Tencent quote: {exc}") from exc

    if '"' not in text:
        raise HTTPException(status_code=502, detail="Unexpected Tencent quote format.")
    parts = text.split('"', 2)[1].split("~")
    if len(parts) < 46 or not parts[1]:
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


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "name": APP_NAME,
        "time": now_iso(),
    }


@app.get("/search")
def search_stock(
    keyword: str = Query(..., description="Stock code or Chinese stock name keyword."),
    limit: int = Query(10, ge=1, le=20),
    x_api_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_token(x_api_key)
    keyword = keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword is required.")

    data = get_all_realtime_quotes()
    code_col = "股票代码"
    name_col = "股票名称"
    if code_col not in data.columns or name_col not in data.columns:
        raise HTTPException(status_code=502, detail="Unexpected market data format.")

    mask = (
        data[code_col].astype(str).str.contains(keyword, case=False, na=False)
        | data[name_col].astype(str).str.contains(keyword, case=False, na=False)
    )
    rows = data[mask].head(limit)

    results = [
        {
            "symbol": str(row.get(code_col)),
            "name": clean_value(row.get(name_col)),
            "price": clean_value(row.get("最新价")),
            "change_pct": clean_value(row.get("涨跌幅")),
        }
        for _, row in rows.iterrows()
    ]

    return {
        "keyword": keyword,
        "count": len(results),
        "results": results,
        "source": "efinance",
        "time": now_iso(),
    }


@app.get("/quote")
def get_quote(
    symbol: str = Query(..., description="A-share stock code, such as 600519 or 000001."),
    x_api_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_token(x_api_key)
    symbol = normalize_symbol(symbol)

    try:
        data = get_all_realtime_quotes()
        if "股票代码" not in data.columns:
            raise HTTPException(status_code=502, detail="Unexpected market data format.")

        rows = data[data["股票代码"].astype(str) == symbol]
        if rows.empty:
            raise HTTPException(status_code=404, detail=f"Stock not found: {symbol}")

        quote = row_to_dict(rows.iloc[0], QUOTE_COLUMNS)
        source = "efinance"
    except HTTPException:
        quote, source = get_fallback_quote(symbol)

    return {
        "quote": quote,
        "source": source,
        "time": now_iso(),
        "note": "For information only. Not investment advice.",
    }


@app.get("/kline")
def get_kline(
    symbol: str = Query(..., description="A-share stock code, such as 600519 or 000001."),
    period: str = Query("daily", description="daily, weekly, monthly, 1m, 5m, 15m, 30m, 60m."),
    limit: int = Query(120, ge=1, le=500),
    x_api_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_token(x_api_key)
    symbol = normalize_symbol(symbol)
    klt = KLINE_PERIODS.get(period)
    if klt is None:
        raise HTTPException(status_code=400, detail=f"Unsupported period: {period}")

    try:
        data = ef.stock.get_quote_history(stock_codes=symbol, klt=klt, fqt=1)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch kline data: {exc}") from exc

    if data is None or data.empty:
        raise HTTPException(status_code=404, detail=f"Kline data not found: {symbol}")

    items = [row_to_dict(row, KLINE_COLUMNS) for _, row in data.tail(limit).iterrows()]
    return {
        "symbol": symbol,
        "period": period,
        "count": len(items),
        "items": items,
        "source": "efinance",
        "time": now_iso(),
        "note": "For information only. Not investment advice.",
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
        payload = search_stock(
            keyword=keyword,
            limit=max(1, min(limit, 5)),
            x_api_key=API_TOKEN or None,
        )
    except HTTPException as exc:
        return mcp_error(None, exc)

    return {
        "ok": True,
        "keyword": payload["keyword"],
        "count": payload["count"],
        "results": payload["results"],
        "source": payload["source"],
        "time": payload["time"],
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
        payload = get_quote(symbol=symbol, x_api_key=API_TOKEN or None)
    except HTTPException as exc:
        return mcp_error(symbol, exc)

    quote = payload["quote"]
    return {
        "ok": True,
        **{field: clean_value(quote.get(field)) for field in QUOTE_RESPONSE_FIELDS},
        "source": payload["source"],
        "time": payload["time"],
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
        payload = get_kline(
            symbol=symbol,
            period=period,
            limit=max(1, min(limit, 30)),
            x_api_key=API_TOKEN or None,
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
        "time": payload["time"],
        "note": payload["note"],
    }


# Mount last so the legacy HTTP endpoints keep their existing paths.
app.mount("/", mcp_http_app)
