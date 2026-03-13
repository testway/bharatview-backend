"""
BharatView — Dhan API Proxy Backend
Fetches live option chain, quotes, OI, Greeks from Dhan
and serves them to the BharatView dashboard with CORS enabled.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from datetime import datetime, timedelta
from typing import Optional
import asyncio

app = FastAPI(title="BharatView Proxy", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
DHAN_BASE = "https://api.dhan.co"

HEADERS = {
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SECURITY_IDS = {
    "NIFTY": {"id": "13", "exch": "IDX_I"},
    "BANKNIFTY": {"id": "25", "exch": "IDX_I"},
    "FINNIFTY": {"id": "27", "exch": "IDX_I"},
    "MIDCPNIFTY": {"id": "442", "exch": "IDX_I"},
}

FO_STOCKS = {
    "RELIANCE": "2885",
    "TCS": "11536",
    "HDFCBANK": "1333",
    "INFY": "1594",
    "ICICIBANK": "4963",
    "HINDUNILVR": "1394",
    "BHARTIARTL": "10604",
    "KOTAKBANK": "1922",
    "WIPRO": "3787",
    "HCLTECH": "7229",
    "SUNPHARMA": "3351",
    "AXISBANK": "5900",
    "BAJFINANCE": "317",
    "MARUTI": "10999",
    "TATAMOTORS": "3456",
    "TATASTEEL": "3499",
    "JSWSTEEL": "11723",
    "SBIN": "3045",
    "NTPC": "11630",
    "ONGC": "2475",
}

_cache = {}

def cache_get(key):
    entry = _cache.get(key)
    if entry and (datetime.now() - entry["ts"]).seconds < 30:
        return entry["data"]
    return None

def cache_set(key, data):
    _cache[key] = {"ts": datetime.now(), "data": data}


@app.get("/")
async def root():
    return {"message": "BharatView API running"}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time": datetime.now().isoformat(),
        "dhan_configured": bool(DHAN_ACCESS_TOKEN),
    }


@app.get("/api/indices")
async def get_indices():

    cached = cache_get("indices")
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=10) as client:

        securities = []

        for meta in SECURITY_IDS.values():
            securities.append(
                {
                    "securityId": meta["id"],
                    "exchangeSegment": meta["exch"],
                }
            )

        securities.append(
            {
                "securityId": "234235",
                "exchangeSegment": "IDX_I",
            }
        )

        payload = {"securities": securities}

        resp = await client.post(
            f"{DHAN_BASE}/v2/marketfeed/ltp",
            headers=HEADERS,
            json=payload,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    raw = resp.json().get("data", {})

    names = list(SECURITY_IDS.keys()) + ["VIX"]
    ids = [meta["id"] for meta in SECURITY_IDS.values()] + ["234235"]

    result = {}

    for i, name in enumerate(names):

        sid = ids[i]
        quote = raw.get(sid, {})

        ltp = quote.get("ltp", 0)
        close = quote.get("close", ltp)

        chg = round((ltp - close) / close * 100, 2) if close else 0

        result[name] = {
            "ltp": round(ltp, 2),
            "chg": chg,
            "close": round(close, 2),
        }

    cache_set("indices", result)

    return result


@app.get("/api/option-chain")
async def get_option_chain(
    symbol: str = Query("NIFTY"),
    expiry: Optional[str] = Query(None),
):

    if not expiry:
        today = datetime.today()
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7

        expiry = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    sec = SECURITY_IDS.get(symbol.upper())

    if not sec:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    payload = {
        "UnderlyingScrip": int(sec["id"]),
        "UnderlyingSeg": sec["exch"],
        "Expiry": expiry,
    }

    async with httpx.AsyncClient(timeout=15) as client:

        resp = await client.post(
            f"{DHAN_BASE}/v2/optionchain",
            headers=HEADERS,
            json=payload,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    raw = resp.json()

    return raw


@app.get("/api/fo-stocks")
async def get_fo_stocks():

    security_list = [
        {"securityId": sid, "exchangeSegment": "NSE_EQ"}
        for sid in FO_STOCKS.values()
    ]

    async with httpx.AsyncClient(timeout=15) as client:

        resp = await client.post(
            f"{DHAN_BASE}/v2/marketfeed/ltp",
            headers=HEADERS,
            json={"securities": security_list},
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()


@app.get("/api/expiries")
async def get_expiries(symbol: str = Query("NIFTY")):

    sec = SECURITY_IDS.get(symbol.upper())

    if not sec:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    async with httpx.AsyncClient(timeout=10) as client:

        resp = await client.get(
            f"{DHAN_BASE}/v2/optionchain/expirylist",
            headers=HEADERS,
            params={
                "UnderlyingScrip": sec["id"],
                "UnderlyingSeg": sec["exch"],
            },
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()
