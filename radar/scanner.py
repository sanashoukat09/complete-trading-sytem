"""
scanner.py — orchestrates a scan: universe → per-symbol data → detector verdict
             → writes JSON the dashboard reads.

Outputs (into cfg['data_dir']):
  watchlist.json   ranked summary of quality-filtered coils + firing coins
  meta.json        scan timestamp, universe size, state counts
  <SYMBOL>.json    full detail (candles, CVD, OI, levels) for charting
"""
from __future__ import annotations
import asyncio
import json
import os
import time

import aiohttp
import numpy as np

from . import binance_data as bd
from . import indicators as ind
from .detector import analyze_symbol, DEFAULTS
from .coin_quality import annotate_verdict, apply_btc_regime

WATCH_STATES = ("FIRING", "COILED_LOADED")


def _trim_frame_for_chart(f, keep):
    """Return a chart-ready frame (last `keep` bars) with a cumulative CVD series."""
    n = len(f["close"])
    s = max(0, n - keep)
    tb = np.asarray(f["taker_buy"][s:], float)
    vol = np.asarray(f["volume"][s:], float)
    _, cvd = ind.cvd_from_klines(tb, vol)
    return {
        "time": [int(t // 1000) for t in f["time"][s:]],
        "open": [round(x, 10) for x in f["open"][s:]],
        "high": [round(x, 10) for x in f["high"][s:]],
        "low": [round(x, 10) for x in f["low"][s:]],
        "close": [round(x, 10) for x in f["close"][s:]],
        "volume": [round(x, 4) for x in vol.tolist()],
        "cvd": [round(x, 4) for x in cvd.tolist()],
    }


def _summary(v):
    return {
        "symbol": v["symbol"], "state": v["state"], "readiness": v["readiness"],
        "compression_score": v["compression_score"], "fuel_score": v["fuel_score"],
        "bias": v["bias"], "bias_confidence": v["bias_confidence"],
        "firing": v["firing"], "fire_direction": v.get("fire_direction", ""),
        "fire_reason": v.get("fire_reason", ""),
        "fire_vol_spike": v.get("fire_vol_spike"),
        "price": v["price"], "quote_vol_24h": v.get("quote_vol_24h"),
        "bbwp": v["coil"].get("bbwp"), "squeeze_run": v["coil"].get("squeeze_run"),
        "duration_min": v["coil"].get("duration_min"),
        "oi_chg_pct": v["oi"].get("chg_pct"), "funding": v.get("funding"),
        "cvd_slope": v.get("cvd_slope"),
        "levels": v["levels"],
        # quality fields
        "quality_score": v.get("quality_score", 0.0),
        "quality_tier": v.get("quality_tier", "C"),
        "quality_detail": v.get("quality_detail"),
        "btc_regime": v.get("btc_regime"),
    }


def _rank_key(s):
    # FIRING first (by quality desc), then COILED_LOADED by readiness desc
    is_firing = 0 if s["state"] == "FIRING" else 1
    return (is_firing, -(s.get("quality_score") or 0), -(s["readiness"] or 0))


async def scan_once(session, cfg, log=print):
    t0 = time.time()
    universe = await bd.fetch_universe(session, cfg)
    if not universe:
        log("[scan] universe fetch failed (network/geo?). Nothing to do.")
        return {"ok": False}
    log(f"[scan] universe: {len(universe)} perps (top by 24h vol)")

    sem = asyncio.Semaphore(cfg.get("max_concurrency", 8))

    async def one(meta):
        try:
            frames, oi, funding, meta = await bd.fetch_symbol_bundle(session, meta, cfg, sem)
            if cfg["primary_tf"] not in frames:
                return None
            v = analyze_symbol(frames, oi, funding, meta, cfg)
            return v, frames, oi
        except Exception:
            return None

    results = await asyncio.gather(*[one(m) for m in universe])
    results = [r for r in results if r]

    data_dir = cfg.get("data_dir", "data")
    os.makedirs(data_dir, exist_ok=True)

    verdicts = [r[0] for r in results]
    counts = {}
    quality_min = cfg.get("quality_min", 50.0)

    # ── find BTC verdict for regime context ──────────────────────────────────
    btc_verdict = next((v for v in verdicts if v.get("symbol") in ("BTCUSDT", "BTCFDUSD")), None)
    btc_regime = _derive_btc_regime(btc_verdict)

    # ── score every coin ─────────────────────────────────────────────────────
    for v in verdicts:
        counts[v["state"]] = counts.get(v["state"], 0) + 1
        annotate_verdict(v, cfg)
        apply_btc_regime(v, btc_regime, cfg)

    # watchlist = quality-filtered FIRING + COILED_LOADED, ranked
    watch = [_summary(v) for v in verdicts
             if v["state"] in WATCH_STATES and v.get("quality_score", 0) >= quality_min]
    watch.sort(key=_rank_key)

    quality_firing = [s for s in watch if s["state"] == "FIRING"]

    # write detail JSON for watchlist symbols (for chart on click)
    detail_written = 0
    detail_syms = {s["symbol"] for s in watch}
    for v, frames, oi in results:
        if v["symbol"] not in detail_syms:
            continue
        detail = {
            "verdict": v,
            "generated_at": int(time.time()),
            "frames": {tf: _trim_frame_for_chart(frames[tf], 200 if tf != "1m" else 120)
                       for tf in frames},
            "oi": {"time": [int(t // 1000) for t in oi.get("time", [])], "oi": oi.get("oi", [])},
        }
        with open(os.path.join(data_dir, f"{v['symbol']}.json"), "w") as fh:
            json.dump(detail, fh)
        detail_written += 1

    meta_out = {
        "generated_at": int(time.time()),
        "generated_at_str": time.strftime("%Y-%m-%d %H:%M:%S"),
        "universe_size": len(universe),
        "scanned": len(verdicts),
        "state_counts": counts,
        "watchlist_size": len(watch),
        "quality_firing_count": len(quality_firing),
        "quality_min": quality_min,
        "btc_regime": btc_regime,
        "scan_secs": round(time.time() - t0, 1),
        "config_summary": {k: cfg.get(k) for k in
                           ("primary_tf", "comp_strong", "fuel_min", "ready_min",
                            "min_quote_vol_24h", "quality_min")},
    }
    with open(os.path.join(data_dir, "watchlist.json"), "w") as fh:
        json.dump({"meta": meta_out, "watchlist": watch}, fh, indent=2)
    with open(os.path.join(data_dir, "meta.json"), "w") as fh:
        json.dump(meta_out, fh, indent=2)

    log(f"[scan] done in {meta_out['scan_secs']}s | states={counts} | "
        f"quality_watchlist={len(watch)} "
        f"(FIRING={len(quality_firing)}, LOADED={len(watch)-len(quality_firing)}) | "
        f"btc_regime={btc_regime} | detail={detail_written}")
    if watch:
        log("      TOP: " + ", ".join(
            f"{s['symbol']}({s['state'][:4]} "
            f"q{s['quality_score']:.0f}{s['quality_tier']} "
            f"r{s['readiness']:.0f})"
            for s in watch[:8]))

    return {"ok": True, "meta": meta_out}


def _derive_btc_regime(btc_v):
    """Map BTC's detector state → a regime string the quality scorer uses."""
    if not btc_v:
        return "UNKNOWN"
    state = btc_v.get("state", "")
    firing = btc_v.get("firing", False)
    fire_dir = btc_v.get("fire_direction", "")
    if firing or state == "FIRING":
        return f"TRENDING_{fire_dir}" if fire_dir else "TRENDING"
    if state == "EXPANDED":
        # check if it expanded recently (post-break)
        return "EXPANDING"
    if state in ("COILED_LOADED", "FORMING"):
        return "RANGING"
    return "NEUTRAL"


async def run_loop(cfg, once=False, log=print):
    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await scan_once(session, cfg, log=log)
                except Exception as e:
                    log(f"[scan] error: {e}")
                if once:
                    break
                await asyncio.sleep(cfg.get("scan_interval_sec", 90))
    except asyncio.CancelledError:
        pass


def load_config(path="config.json"):
    cfg = dict(DEFAULTS)
    if os.path.exists(path):
        with open(path) as fh:
            user = json.load(fh)
        cfg.update({k: v for k, v in user.items() if not k.startswith("_")})
    return cfg
