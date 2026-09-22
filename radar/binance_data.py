"""
binance_data.py — async Binance USDT-M Futures public data client.

Runs on YOUR machine (Binance public market data, no API key needed).
Pulls: perpetual universe + 24h volume, multi-timeframe klines (with taker-buy
volume for CVD), open-interest history, and funding.

Kline row layout (Binance):
  [0]openTime [1]open [2]high [3]low [4]close [5]volume [6]closeTime
  [7]quoteVol [8]trades [9]takerBuyBase [10]takerBuyQuote [11]ignore
"""
from __future__ import annotations
import asyncio
import aiohttp

FAPI = "https://fapi.binance.com"


async def _get_json(session, url, params=None, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 429 or r.status == 418:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                r.raise_for_status()
                return await r.json()
        except Exception:
            if attempt == retries - 1:
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def fetch_universe(session, cfg):
    """Tradeable USDT-M perpetuals with their 24h quote volume, sorted desc."""
    info = await _get_json(session, f"{FAPI}/fapi/v1/exchangeInfo")
    tickers = await _get_json(session, f"{FAPI}/fapi/v1/ticker/24hr")
    if not info or not tickers:
        return []
    perp = set()
    for s in info.get("symbols", []):
        if (s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"):
            perp.add(s["symbol"])
    excl = set(cfg.get("exclude_symbols", []))
    out = []
    for t in tickers:
        sym = t.get("symbol")
        if sym in perp and sym not in excl:
            try:
                out.append({"symbol": sym,
                            "quote_vol_24h": float(t.get("quoteVolume", 0.0)),
                            "price": float(t.get("lastPrice", 0.0))})
            except (TypeError, ValueError):
                continue
    out = [o for o in out if o["quote_vol_24h"] >= cfg.get("min_quote_vol_24h", 0)]
    out.sort(key=lambda x: x["quote_vol_24h"], reverse=True)
    top_n = cfg.get("universe_top_n")
    return out[:top_n] if top_n else out


def _parse_klines(rows):
    if not rows:
        return None
    f = {"time": [], "open": [], "high": [], "low": [], "close": [],
         "volume": [], "taker_buy": []}
    for r in rows:
        f["time"].append(int(r[0]))
        f["open"].append(float(r[1])); f["high"].append(float(r[2]))
        f["low"].append(float(r[3])); f["close"].append(float(r[4]))
        f["volume"].append(float(r[5])); f["taker_buy"].append(float(r[9]))
    return f


async def fetch_klines(session, symbol, interval, limit):
    rows = await _get_json(session, f"{FAPI}/fapi/v1/klines",
                           {"symbol": symbol, "interval": interval, "limit": limit})
    return _parse_klines(rows)


async def fetch_open_interest(session, symbol, period="5m", limit=48):
    rows = await _get_json(session, f"{FAPI}/futures/data/openInterestHist",
                           {"symbol": symbol, "period": period, "limit": limit})
    if not rows:
        return {"time": [], "oi": []}
    return {"time": [int(x["timestamp"]) for x in rows],
            "oi": [float(x["sumOpenInterest"]) for x in rows]}


async def fetch_funding(session, symbol):
    d = await _get_json(session, f"{FAPI}/fapi/v1/premiumIndex", {"symbol": symbol})
    try:
        return float(d.get("lastFundingRate", 0.0)) if d else 0.0
    except (TypeError, ValueError):
        return 0.0


async def fetch_symbol_bundle(session, meta, cfg, sem):
    """Fetch everything the detector needs for one symbol."""
    sym = meta["symbol"]
    tf_limits = cfg.get("tf_limits", {"1m": 120, "5m": 288, "15m": 200, "1h": 168})
    async with sem:
        frames = {}
        for tf, lim in tf_limits.items():
            fr = await fetch_klines(session, sym, tf, lim)
            if fr:
                frames[tf] = fr
        oi = await fetch_open_interest(session, sym,
                                       period=cfg.get("oi_period", "5m"),
                                       limit=cfg.get("oi_limit", 48))
        funding = await fetch_funding(session, sym)
    return frames, oi, funding, meta
