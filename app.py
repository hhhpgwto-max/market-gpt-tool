import os
from datetime import datetime, timezone
from typing import Any

import efinance as ef
import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


APP_NAME = os.getenv("MARKET_TOOL_NAME", "market-gpt-tool")
API_TOKEN = os.getenv("MARKET_TOOL_TOKEN", "").strip()

app = FastAPI(
    title="Market GPT Tool",
    version="0.1.0",
    description="A small market data API for a Custom GPT Action.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
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


def require_token(x_api_key: str | None) -> None:
    if API_TOKEN and x_api_key != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing x-api-key.")


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
    symbol = symbol.strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required.")

    data = get_all_realtime_quotes()
    if "股票代码" not in data.columns:
        raise HTTPException(status_code=502, detail="Unexpected market data format.")

    rows = data[data["股票代码"].astype(str) == symbol]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"Stock not found: {symbol}")

    quote = row_to_dict(rows.iloc[0], QUOTE_COLUMNS)
    return {
        "quote": quote,
        "source": "efinance",
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
    symbol = symbol.strip()
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
