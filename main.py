"""
BharatView — Dhan API Proxy Backend
Fetches live option chain, quotes, OI, Greeks from Dhan
and serves them to the BharatView dashboard with CORS enabled.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import os
from datetime import datetime, timedelta
from typing import Optional
import asyncio

app = FastAPI(title="BharatView Proxy", version="1.0.0")

# ── CORS: allow the dashboard to call this from any origin ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Dhan credentials — set these as environment variables in Railway ──
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
DHAN_BASE = "https://api.dhan.co"

HEADERS = {
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ── Dhan security IDs for indices ──
SECURITY_IDS = {
    "NIFTY":      {"id": "13", "exch": "IDX_I"},
    "BANKNIFTY":  {"id": "25",  "exch": "IDX_I"},
    "FINNIFTY":   {"id": "27",  "exch": "IDX_I"},
    "MIDCPNIFTY": {"id": "442", "exch": "IDX_I"},
}

# ── Nifty 50 F&O stocks with Dhan security IDs ──
FO_STOCKS = {
    "RELIANCE":   "2885", "TCS":        "11536", "HDFCBANK":   "1333",
    "INFY":       "1594", "ICICIBANK":  "4963",  "HINDUNILVR": "1394",
    "BHARTIARTL": "10604","KOTAKBANK":  "1922",  "WIPRO":      "3787",
    "HCLTECH":    "7229", "SUNPHARMA":  "3351",  "AXISBANK":   "5900",
    "BAJFINANCE": "317",  "MARUTI":     "10999", "TATAMOTORS": "3456",
    "TATASTEEL":  "3499", "JSWSTEEL":   "11723", "SBIN":       "3045",
    "NTPC":       "11630","ONGC":       "2475",  "LT":         "11483",
    "ADANIENT":   "25",   "TITAN":      "3506",  "TECHM":      "13538",
    "INDUSINDBK": "5258", "DRREDDY":    "881",   "CIPLA":      "694",
    "M&M":        "2031", "BPCL":       "526",   "BEL":        "383",
}

# ── Simple in-memory cache (30s TTL) ──
_cache: dict = {}

def cache_get(key: str):
    entry = _cache.get(key)
    if entry and (datetime.now() - entry["ts"]).seconds < 30:
        return entry["data"]
    return None

def cache_set(key: str, data):
    _cache[key] = {"ts": datetime.now(), "data": data}


# ════════════════════════════════════════════
# ENDPOINTS
# ════════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat(), "dhan_configured": bool(DHAN_ACCESS_TOKEN)}


@app.get("/api/indices")
async def get_indices():
    """Live quotes for NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, VIX"""
    cached = cache_get("indices")
    if cached:
        return cached

    async with httpx.AsyncClient(timeout=10) as client:
        tasks = []
        for name, meta in SECURITY_IDS.items():
            payload = {
                "securityId": meta["id"],
                "exchangeSegment": meta["exch"],
            }
            tasks.append(client.post(f"{DHAN_BASE}/v2/marketfeed/ltp", headers=HEADERS, json=payload))

        # Also fetch VIX
        vix_payload = {"securityId": "234235", "exchangeSegment": "IDX_I"}
        tasks.append(client.post(f"{DHAN_BASE}/v2/marketfeed/ltp", headers=HEADERS, json=vix_payload))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    result = {}
    names = list(SECURITY_IDS.keys()) + ["VIX"]
    for i, resp in enumerate(responses):
        name = names[i]
        if isinstance(resp, Exception):
            result[name] = {"error": str(resp)}
            continue
        try:
            data = resp.json()
            ltp = data.get("data", {}).get("ltp", 0)
            close = data.get("data", {}).get("close", ltp)
            chg = round((ltp - close) / close * 100, 2) if close else 0
            result[name] = {"ltp": round(ltp, 2), "chg": chg, "close": round(close, 2)}
        except Exception as e:
            result[name] = {"error": str(e)}

    cache_set("indices", result)
    return result


@app.get("/api/option-chain")
async def get_option_chain(
    symbol: str = Query("NIFTY", description="NIFTY or BANKNIFTY"),
    expiry: Optional[str] = Query(None, description="Expiry date YYYY-MM-DD")
):
    """Full option chain with OI, IV, Greeks for a given symbol + expiry"""
    cache_key = f"oc_{symbol}_{expiry}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # Get next Thursday expiry if not provided
    if not expiry:
        today = datetime.today()
        days_ahead = (3 - today.weekday()) % 7  # Thursday = 3
        if days_ahead == 0:
            days_ahead = 7
        expiry = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    sec = SECURITY_IDS.get(symbol.upper())
    if not sec:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {symbol}")

    payload = {
        "UnderlyingScrip": int(sec["id"]),
        "UnderlyingSeg": sec["exch"],
        "Expiry": expiry,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"{DHAN_BASE}/v2/optionchain", headers=HEADERS, json=payload)

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    raw = resp.json()
    chain_data = raw.get("data", [])

    # Build structured option chain
    strikes = {}
    total_ce_oi = 0
    total_pe_oi = 0

    for row in chain_data:
        strike = row.get("strikePrice", 0)
        opt_type = row.get("optionType", "")
        entry = {
            "ltp":        round(row.get("lastTradedPrice", 0), 2),
            "oi":         row.get("openInterest", 0),
            "oi_chg":     row.get("changeInOpenInterest", 0),
            "volume":     row.get("volumeTradedToday", 0),
            "iv":         round(row.get("impliedVolatility", 0), 2),
            "delta":      round(row.get("delta", 0), 4),
            "theta":      round(row.get("theta", 0), 4),
            "gamma":      round(row.get("gamma", 0), 6),
            "vega":       round(row.get("vega", 0), 4),
            "bid":        round(row.get("bidPrice", 0), 2),
            "ask":        round(row.get("askPrice", 0), 2),
            "chg_pct":    round(row.get("priceChange", 0), 2),
        }
        if strike not in strikes:
            strikes[strike] = {}
        strikes[strike][opt_type] = entry

        if opt_type == "CE":
            total_ce_oi += entry["oi"]
        elif opt_type == "PE":
            total_pe_oi += entry["oi"]

    pcr = round(total_pe_oi / total_ce_oi, 3) if total_ce_oi else 0
    result = {
        "symbol": symbol,
        "expiry": expiry,
        "spot": raw.get("last", 0),
        "pcr": pcr,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "strikes": dict(sorted(strikes.items())),
        "fetched_at": datetime.now().isoformat(),
    }

    cache_set(cache_key, result)
    return result


@app.get("/api/fo-stocks")
async def get_fo_stocks():
    """Live quotes + OI summary for top F&O stocks"""
    cached = cache_get("fo_stocks")
    if cached:
        return cached

    security_list = [
        {"securityId": sid, "exchangeSegment": "NSE_EQ"}
        for sid in list(FO_STOCKS.values())[:20]
    ]

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{DHAN_BASE}/v2/marketfeed/ltp",
            headers=HEADERS,
            json={"securities": security_list}
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    raw = resp.json().get("data", {})
    result = {}
    stock_names = list(FO_STOCKS.keys())[:20]
    stock_ids = list(FO_STOCKS.values())[:20]

    for i, name in enumerate(stock_names):
        sid = stock_ids[i]
        quote = raw.get(sid, {})
        ltp = quote.get("ltp", 0)
        close = quote.get("close", ltp)
        chg = round((ltp - close) / close * 100, 2) if close else 0
        result[name] = {
            "ltp":   round(ltp, 2),
            "chg":   chg,
            "high":  round(quote.get("high", 0), 2),
            "low":   round(quote.get("low", 0), 2),
            "vol":   quote.get("volume", 0),
        }

    cache_set("fo_stocks", result)
    return result


@app.get("/api/expiries")
async def get_expiries(symbol: str = Query("NIFTY")):
    """Get available expiry dates for a symbol"""
    sec = SECURITY_IDS.get(symbol.upper())
    if not sec:
        raise HTTPException(status_code=400, detail=f"Unknown symbol: {symbol}")

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{DHAN_BASE}/v2/optionchain/expirylist",
            headers=HEADERS,
            params={"UnderlyingScrip": sec["id"], "UnderlyingSeg": sec["exch"]}
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    dates = resp.json().get("data", [])
    return {"symbol": symbol, "expiries": dates}


@app.get("/api/pcr-dashboard")
async def get_pcr_dashboard():
    """PCR for all major indices — calls option chain in parallel"""
    cached = cache_get("pcr_dashboard")
    if cached:
        return cached

    symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
    results = {}

    async with httpx.AsyncClient(timeout=15) as client:
        for sym in symbols:
            try:
                today = datetime.today()
                days_ahead = (3 - today.weekday()) % 7
                if days_ahead == 0:
                    days_ahead = 7
                expiry = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
                sec = SECURITY_IDS[sym]
                payload = {
                    "UnderlyingScrip": int(sec["id"]),
                    "UnderlyingSeg": sec["exch"],
                    "Expiry": expiry,
                }
                resp = await client.post(f"{DHAN_BASE}/v2/optionchain", headers=HEADERS, json=payload)
                data = resp.json().get("data", [])
                ce_oi = sum(r.get("openInterest", 0) for r in data if r.get("optionType") == "CE")
                pe_oi = sum(r.get("openInterest", 0) for r in data if r.get("optionType") == "PE")
                results[sym] = {
                    "pcr": round(pe_oi / ce_oi, 3) if ce_oi else 0,
                    "ce_oi": ce_oi,
                    "pe_oi": pe_oi,
                    "expiry": expiry,
                }
            except Exception as e:
                results[sym] = {"error": str(e)}

    cache_set("pcr_dashboard", results)
    return results
