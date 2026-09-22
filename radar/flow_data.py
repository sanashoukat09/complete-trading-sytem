"""
flow_data.py — EXECUTED-ONLY order-flow data (nothing spoofable).

Sources (Binance USDT-M futures, public):
  - aggTrades         → true CVD / delta, footprint (volume-at-price), whale prints, aggression
  - global/top L-S    → crowd vs smart-money positioning (accounts & positions)
  - taker ratio       → aggressive buy vs sell volume

Aggressor rule for aggTrades: field 'm' = isBuyerMaker.
  m == True  → the buyer was the maker, so the AGGRESSOR is the SELLER  → delta -= q
  m == False → the AGGRESSOR is the BUYER                                → delta += q

The compute_* functions are pure (numpy/stdlib) and unit-testable offline.
"""
from __future__ import annotations
import numpy as np
import aiohttp
import asyncio

from . import indicators as ind

FAPI = "https://fapi.binance.com"


# ─────────────────────────────────────────────────────────────────────────────
# PURE COMPUTE (offline-testable)
# ─────────────────────────────────────────────────────────────────────────────
def analyze_aggtrades(trades, whale_usd=50000.0, bins=24):
    """
    trades: list of dicts with p (price), q (qty), m (isBuyerMaker), T (ms).
    Returns a compact, forensic summary of true executed flow.
    """
    if not trades:
        return {"n": 0}
    p = np.array([float(t["p"]) for t in trades], float)
    q = np.array([float(t["q"]) for t in trades], float)
    m = np.array([bool(t["m"]) for t in trades])
    T = np.array([int(t.get("T", 0)) for t in trades], float)

    aggressor = np.where(m, -1.0, 1.0)          # +1 buy-aggressor, -1 sell-aggressor
    delta = aggressor * q
    notional = p * q

    buy_vol = float(q[~m].sum())
    sell_vol = float(q[m].sum())
    tot = buy_vol + sell_vol
    cvd = float(delta.sum())
    cvd_norm = cvd / tot if tot > 0 else 0.0

    # footprint: volume-at-price with buy/sell split
    lo, hi = float(p.min()), float(p.max())
    footprint = []
    poc_price = float(p[np.argmax(q)]) if len(q) else lo
    if hi > lo:
        edges = np.linspace(lo, hi, bins + 1)
        idx = np.clip(((p - lo) / (hi - lo) * bins).astype(int), 0, bins - 1)
        buyb = np.zeros(bins); selb = np.zeros(bins)
        for i, bi in enumerate(idx):
            if m[i]:
                selb[bi] += q[i]
            else:
                buyb[bi] += q[i]
        centers = (edges[:-1] + edges[1:]) / 2
        footprint = [{"price": round(float(centers[i]), 10),
                      "buy": round(float(buyb[i]), 4), "sell": round(float(selb[i]), 4)}
                     for i in range(bins) if (buyb[i] + selb[i]) > 0]
        poc_price = round(float(centers[int(np.argmax(buyb + selb))]), 10)

    # TRUE tick value area: volume-at-price of the actual executed trades (no spreading)
    va_poc, va_vah, va_val = ind.value_area_from_trades(p, q, bins=60, va_pct=0.70)

    # whales: prints above a USD notional
    wmask = notional >= whale_usd
    whale_delta = float(delta[wmask].sum())
    whale_notional = float(notional[wmask].sum())
    biggest = float(notional.max()) if len(notional) else 0.0

    span_min = (float(T.max()) - float(T.min())) / 60000.0 if len(T) and T.max() > 0 else None

    return {
        "n": int(len(trades)),
        "span_min": round(span_min, 1) if span_min is not None else None,
        "buy_vol": round(buy_vol, 4), "sell_vol": round(sell_vol, 4),
        "cvd": round(cvd, 4), "cvd_norm": round(cvd_norm, 4),
        "delta_buy_frac": round(buy_vol / tot, 4) if tot > 0 else 0.5,
        "whale_count": int(wmask.sum()),
        "whale_delta": round(whale_delta, 4),
        "whale_notional": round(whale_notional, 2),
        "biggest_notional": round(biggest, 2),
        "poc_price": poc_price,
        "va": {"poc": round(float(va_poc), 10), "vah": round(float(va_vah), 10),
               "val": round(float(va_val), 10)},
        "footprint": footprint,
    }


def ls_trend(series):
    """Latest value + slope sign for a long/short ratio series (list of floats)."""
    s = np.array([x for x in series if x is not None], float)
    if len(s) == 0:
        return None, 0.0
    if len(s) < 3:
        return float(s[-1]), 0.0
    x = np.arange(len(s)) - (len(s) - 1) / 2
    slope = float(np.sum(x * (s - s.mean())) / max(np.sum(x * x), 1e-9))
    return float(s[-1]), round(slope, 5)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC FETCHERS (run on your machine)
# ─────────────────────────────────────────────────────────────────────────────
async def _get(session, url, params=None, retries=2):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status in (429, 418):
                    await asyncio.sleep(1.5 * (attempt + 1)); continue
                r.raise_for_status()
                return await r.json()
        except Exception:
            if attempt == retries:
                return None
            await asyncio.sleep(0.4 * (attempt + 1))
    return None


async def fetch_aggtrades(session, symbol, limit=1000, start_ms=None):
    params = {"symbol": symbol, "limit": min(limit, 1000)}
    if start_ms:
        params["startTime"] = int(start_ms)
    rows = await _get(session, f"{FAPI}/fapi/v1/aggTrades", params)
    if not rows:
        return []
    # aggTrade fields: a,p,q,f,l,T,m
    return [{"p": r["p"], "q": r["q"], "m": r["m"], "T": r.get("T", 0)} for r in rows]


async def fetch_positioning(session, symbol, period="5m", limit=12):
    """Long/short account & position ratios + taker ratio. All executed/derived, not book."""
    out = {}
    async def one(path, key):
        rows = await _get(session, f"{FAPI}/futures/data/{path}",
                          {"symbol": symbol, "period": period, "limit": limit})
        if not rows:
            return
        try:
            if path == "takerlongshortRatio":
                vals = [float(x["buySellRatio"]) for x in rows]
            else:
                vals = [float(x["longShortRatio"]) for x in rows]
            last, slope = ls_trend(vals)
            out[key] = {"last": round(last, 4) if last is not None else None, "slope": slope}
        except (KeyError, TypeError, ValueError):
            return
    await asyncio.gather(
        one("globalLongShortAccountRatio", "global_account"),
        one("topLongShortAccountRatio", "top_account"),
        one("topLongShortPositionRatio", "top_position"),
        one("takerlongshortRatio", "taker"),
    )
    return out


async def fetch_flow_bundle(session, symbol, cfg):
    """Everything executed-only for one symbol, for a pre-release or forward snapshot."""
    aggs = await fetch_aggtrades(session, symbol, limit=cfg.get("aggtrades_limit", 1000)) \
        if cfg.get("capture_flow", True) else []
    flow = analyze_aggtrades(aggs, whale_usd=cfg.get("whale_usd", 50000.0)) if aggs else {"n": 0}
    pos = await fetch_positioning(session, symbol) if cfg.get("capture_ls", True) else {}
    return {"flow": flow, "positioning": pos}
