"""
detector.py — the scoring brain (pure, network-free, unit-testable).

Given multi-timeframe candles + open interest + funding for ONE symbol, produce a
verdict that separates the three questions your old code blurred:

  1. COMPRESSION (depth, not change)  -> how tightly is it coiled, across timeframes?
  2. FUEL / ENERGY                     -> is there stored pressure, or is it just DEAD?
  3. RELEASE                           -> is it leaving the balance area right now?

Plus a dominant-side read (accumulation vs distribution) so you get a verdict, not a trade.

State machine:
  DEAD           compressed but no participation / no fuel  -> ignore
  FORMING        contracting, not deep enough yet
  COILED_LOADED  deep compression + real fuel               -> WATCHLIST
  FIRING         leaving the balance area now (imbalance)   -> ALERT
  EXPANDED       already moved / no longer compressed       -> drop
"""
from __future__ import annotations
import numpy as np
from . import indicators as ind


DEFAULTS = {
    "primary_tf": "15m",
    "confluence_tfs": ["5m", "15m", "1h"],
    "tf_weights": {"5m": 0.25, "15m": 0.50, "1h": 0.25},

    "bb_n": 20, "bb_k": 2.0, "kc_mult": 1.5,
    "bbwp_lookback": 120,
    "coil_bars": 20,              # bars that define the "current balance area"
    "atr_n": 14,

    # compression thresholds  (CALIBRATE on real data — see METHODOLOGY.md §Calibration)
    "comp_strong": 62.0,          # >= this = strongly compressed
    "comp_forming": 45.0,
    "squeeze_target_bars": 6,     # a squeeze this long = fully coiled

    # fuel thresholds
    "fuel_min": 45.0,             # below this = not loaded enough for watchlist
    "fuel_dead": 25.0,            # below this = DEAD
    "min_quote_vol_24h": 30_000_000.0,   # liquidity/participation floor (USDT)

    # readiness (combine compression & fuel; BOTH must be high)
    "ready_min": 55.0,

    # release / firing
    "release_atr_mult": 1.1,      # expansion bar range vs coil ATR
    "release_vol_mult": 1.8,      # break-bar volume vs coil baseline
    "release_close_beyond_atr": 0.15,  # close must be this many ATR beyond VA edge

    # --- tradeable-move floor (added 2026-09-11, data-driven) --------------------
    # DATA FINDING: among fired releases, compression/readiness/fuel/bbwp do NOT
    # predict move size (AUC ~0.50, corr ~0.00). The ONLY pre-fire feature that
    # predicts whether a big, cost-clearing move follows is the value-area WIDTH %
    # (its volatility). So instead of tightening compression (which only cuts trade
    # count with no quality gain), we require a minimum VA width to fire.
    #   floor 0.0 = OFF (original behaviour).  1.0% keeps ~84% of fires and lifts the
    #   >=2%-move rate 67%->75%; 1.5% keeps ~67% and lifts it to ~82%.
    "min_va_width_pct": 0.0,

    # --- fire only on a REAL coil break (added 2026-09-12) -----------------------
    # A fire should mean "a coin that WAS compressed just left its balance area",
    # not "any coin printed a big bar". Require compression >= this at fire, else the
    # break is demoted (not a coil release). Default = comp_forming (a real coil).
    # Set 0.0 to disable. NOTE (data): this enforces strategy-fit (coil-only
    # breakouts); it does NOT increase move size — blocked non-coil breaks moved just
    # as much on average. It removes ~15-18% of fires (off-strategy, non-coil noise).
    "fire_min_compression": 45.0,
    # How to time the break: False (default) fires on the still-forming bar in real time
    # so a fast sweep/reclaim is caught (best for mean-reversion). True waits for the bar
    # to close first — fewer, later, cleaner signals, but can miss a poke that reclaims
    # within the bar. Either way the break is judged by the bar's high/low extreme.
    "fire_on_closed_bar": False,

    # Build the value area / balance from the 1-minute profile (precise) over the same
    # span as the primary coil window. False = use the primary timeframe (15m) profile.
    "va_use_1m": True,
}


def _frame_arrays(f):
    return (np.asarray(f["high"], float), np.asarray(f["low"], float),
            np.asarray(f["close"], float), np.asarray(f["volume"], float),
            np.asarray(f.get("taker_buy", np.zeros(len(f["close"]))), float))


# ─────────────────────────────────────────────────────────────────────────────
# Per-frame compression depth (0-100)
# ─────────────────────────────────────────────────────────────────────────────
def frame_compression(f, cfg):
    high, low, close, vol, _ = _frame_arrays(f)
    if len(close) < cfg["bb_n"] + 5:
        return 0.0, {}

    # 1. BBWP — how tight are the Bollinger bands vs their own history (0=tightest)
    bw = ind.bbwp(close, cfg["bb_n"], cfg["bbwp_lookback"], cfg["bb_k"])
    depth_bbwp = ind.clamp(100.0 - bw)                       # low bbwp -> high depth

    # 2. TTM squeeze on + how long it's been on
    sqz = ind.ttm_squeeze(high, low, close, cfg["bb_n"], cfg["bb_k"], cfg["kc_mult"])
    run = ind.consecutive_true(sqz)
    squeeze_now = bool(sqz[-1]) if len(sqz) else False
    depth_sqz = ind.clamp((run / cfg["squeeze_target_bars"]) * 100.0) if squeeze_now else ind.clamp(run * 8.0)

    # 3. ATR percentile (low = quiet)
    a = ind.atr(high, low, close, cfg["atr_n"])
    atr_pct = ind.percentile_rank(a, cfg["bbwp_lookback"])
    depth_atr = ind.clamp(100.0 - atr_pct)

    # 4. Range percentile (low = tight)
    rng_pct = ind.range_pct_percentile(high, low, close, cfg["coil_bars"], cfg["bbwp_lookback"])
    depth_rng = ind.clamp(100.0 - rng_pct)

    # 5. Path efficiency (low = choppy/coiling)
    eff = ind.path_efficiency(close, cfg["coil_bars"])
    depth_eff = ind.clamp((0.60 - eff) / 0.55 * 100.0)

    # 6. VCP-style contraction (last bar among narrowest + consecutive narrowing)
    nr_frac, nr_run = ind.nr_contraction(high, low, lookback=cfg["coil_bars"])
    depth_nr = ind.clamp(nr_frac * 70.0 + min(nr_run, 5) * 6.0)

    # BBWP (percentile depth) is the most robust, research-backed measure, so it
    # carries the most weight; the TTM-squeeze boolean is a confirming bonus, not the anchor.
    depth = (0.40 * depth_bbwp + 0.14 * depth_sqz + 0.18 * depth_atr +
             0.16 * depth_rng + 0.06 * depth_eff + 0.06 * depth_nr)
    return ind.clamp(depth), {
        "bbwp": round(bw, 1), "squeeze_on": squeeze_now, "squeeze_run": run,
        "atr_pct": round(atr_pct, 1), "range_pct": round(rng_pct, 1),
        "path_eff": round(eff, 3), "nr_run": nr_run,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Fuel / energy (0-100) — dead vs loaded
# ─────────────────────────────────────────────────────────────────────────────
def compute_fuel(frames, oi_series, funding, meta, cfg, poc, vah, val):
    f = frames[cfg["primary_tf"]]
    high, low, close, vol, tb = _frame_arrays(f)
    cw = cfg["coil_bars"]
    if len(close) < cw + 2:
        return 0.0, {}, 0.0  # fuel, sub, cvd_slope

    hi_w, lo_w, cl_w, vol_w, tb_w = high[-cw:], low[-cw:], close[-cw:], vol[-cw:], tb[-cw:]
    a = ind.atr(high, low, close, cfg["atr_n"])
    atr_now = a[-1] if len(a) else (close[-1] * 0.005)
    price = close[-1]

    # --- CVD divergence inside the coil: price ~flat but flow trending = absorption ---
    delta_w, cvd_w = ind.cvd_from_klines(tb_w, vol_w)
    cvd_slope = ind.linreg_slope_norm(cvd_w)                 # sign = who is aggressing
    price_disp_atr = abs(cl_w[-1] - cl_w[0]) / max(atr_now, 1e-12)
    # strong flow with little price movement => stored energy
    absorption = abs(cvd_slope) * max(0.0, 1.0 - price_disp_atr / 3.0)
    fuel_cvd = ind.clamp(absorption * 45.0)                  # tuned so ~2 std slope flat-price ~ 90

    # --- OI build during compression: rising OI + flat price = leverage stacking ---
    fuel_oi = 30.0; oi_slope = 0.0; oi_chg_pct = 0.0
    if oi_series and len(oi_series.get("oi", [])) >= 5:
        oi = np.asarray(oi_series["oi"], float)
        oi_slope = ind.linreg_slope_norm(oi[-min(len(oi), 2 * cw):])
        first = oi[max(0, len(oi) - cw)]
        oi_chg_pct = (oi[-1] - first) / first * 100.0 if first > 0 else 0.0
        if oi_slope > 0:
            fuel_oi = ind.clamp(45.0 + oi_slope * 30.0)      # rising OI = fuel
        else:
            fuel_oi = ind.clamp(35.0 + oi_slope * 15.0)      # falling OI = de-energizing

    # --- Funding: crowded positioning adds squeeze fuel (mild) ---
    fund_abs = abs(funding or 0.0)
    fuel_funding = ind.clamp((fund_abs / 0.0005) * 60.0)     # 0.05%/8h ~ elevated

    # --- Edge absorption: repeated tests of an edge that hold, opposing delta ---
    # Use a value area computed on THIS (primary) frame's own coil window, independent
    # of the display/firing VA (which may now be the 1m profile). This keeps the fuel
    # score — and therefore the DEAD classification — consistent regardless of which
    # timeframe the balance levels are drawn from.
    pocF, vahF, valF = ind.value_area(hi_w, lo_w, cl_w, vol_w, bins=40, va_pct=0.70)
    edge_tests = 0; edge_hold_delta = 0.0
    if vahF > valF > 0:
        for i in range(len(cl_w)):
            touched_hi = hi_w[i] >= vahF - 0.05 * atr_now
            touched_lo = lo_w[i] <= valF + 0.05 * atr_now
            closed_in = valF <= cl_w[i] <= vahF
            if (touched_hi or touched_lo) and closed_in:
                edge_tests += 1
                edge_hold_delta += delta_w[i]
    fuel_edge = ind.clamp(min(edge_tests, 6) * 14.0)

    # --- Liveness: volume drying but ALIVE (not flatlined-dead) ---
    base_vol = np.mean(vol[-3 * cw:-cw]) if len(vol) >= 3 * cw else np.mean(vol[:-1] or [1.0])
    recent_vol = np.mean(vol_w)
    dry_ratio = recent_vol / max(base_vol, 1e-9)             # <1 = drying (coil), ~0 = dead
    live_bars = float(np.sum(vol_w > 0)) / max(len(vol_w), 1)
    # reward moderate drying (0.4–0.9) with life; punish flatline (<0.15) or no drying (>1.3)
    if recent_vol <= 0 or live_bars < 0.5:
        fuel_live = 0.0
    elif dry_ratio < 0.15:
        fuel_live = 15.0
    elif dry_ratio <= 0.9:
        fuel_live = 80.0
    elif dry_ratio <= 1.3:
        fuel_live = 55.0
    else:
        fuel_live = 35.0

    fuel = (0.34 * fuel_cvd + 0.24 * fuel_oi + 0.10 * fuel_funding +
            0.16 * fuel_edge + 0.16 * fuel_live)

    # participation / liquidity floor: below it, it's a dead/untrustworthy coin
    qv = meta.get("quote_vol_24h", 0.0)
    dead_liquidity = qv < cfg["min_quote_vol_24h"]
    if dead_liquidity or live_bars < 0.5:
        fuel = min(fuel, 15.0)

    sub = {
        "fuel_cvd": round(fuel_cvd, 1), "fuel_oi": round(fuel_oi, 1),
        "fuel_funding": round(fuel_funding, 1), "fuel_edge": round(fuel_edge, 1),
        "fuel_live": round(fuel_live, 1),
        "cvd_slope": round(cvd_slope, 2), "oi_slope": round(oi_slope, 2),
        "oi_chg_pct": round(oi_chg_pct, 2), "dry_ratio": round(float(dry_ratio), 2),
        "edge_tests": edge_tests, "funding": funding,
        "dead_liquidity": bool(dead_liquidity),
    }
    return ind.clamp(fuel), sub, cvd_slope


# ─────────────────────────────────────────────────────────────────────────────
# Dominant side (accumulation vs distribution)
# ─────────────────────────────────────────────────────────────────────────────
def compute_bias(cvd_slope, oi_sub, price, poc, vah, val):
    votes = 0.0; weight = 0.0
    # 1. CVD slope inside the coil (main)
    votes += np.sign(cvd_slope) * min(abs(cvd_slope), 2.0) / 2.0 * 3.0; weight += 3.0
    # 2. OI + direction combo: rising OI reinforces the CVD direction
    if oi_sub.get("oi_slope", 0) > 0.2:
        votes += np.sign(cvd_slope) * 1.5; weight += 1.5
    # 3. Position within value area: near VAL = accumulation lean, near VAH = distribution lean
    if vah > val > 0:
        pos = (price - val) / (vah - val)
        if pos < 0.4:
            votes += 1.0; weight += 1.0
        elif pos > 0.6:
            votes -= 1.0; weight += 1.0
    score = votes / max(weight, 1e-9)   # -1..+1
    conf = ind.clamp(abs(score) * 100.0)
    if score > 0.15:
        return "ACCUMULATION → long-lean", conf
    if score < -0.15:
        return "DISTRIBUTION → short-lean", conf
    return "UNCLEAR", conf


# ─────────────────────────────────────────────────────────────────────────────
# Release / firing (leaving the balance area)
# ─────────────────────────────────────────────────────────────────────────────
def detect_release(frames, cfg, vah, val, cvd_slope_coil):
    """Fire when price LEAVES the balance area with a real push.

    A break = the value-area edge EXCEEDED by the bar's HIGH/LOW (a wick/poke counts),
    not merely a close beyond it. This is deliberate: a fast sweep that pierces the edge
    and reclaims within the same minute (a spring / upthrust — the start of a
    mean-reversion trade) still fires, instead of being lost while waiting for a close.
    A real push = volume spike + an expansion-range bar. Direction is the user's manual
    call, so flow is reported but NOT required (a reclaim can flip flow, which would
    otherwise block the exact MR sweep we want). The reason tags 'sweep+reclaim' (poked
    out then closed back inside = MR candidate) vs 'close beyond' (holding = continuation
    candidate).
    """
    for tf in ("1m", "5m"):
        f = frames.get(tf)
        if not f or len(f["close"]) < cfg["atr_n"] + 3:
            continue
        high, low, close, vol, tb = _frame_arrays(f)
        n = len(close)
        # Default: judge the still-forming (current) bar so a poke fires in real time.
        # fire_on_closed_bar=True instead waits for the last bar to close (fewer, later
        # signals). Either way the break is judged by the bar EXTREME, so a reclaim
        # inside the bar is never lost.
        use_closed = cfg.get("fire_on_closed_bar", False) and n >= cfg["atr_n"] + 4
        j = (n - 2) if use_closed else (n - 1)
        a = ind.atr(high[:j + 1], low[:j + 1], close[:j + 1], cfg["atr_n"])
        atr_now = a[-1] if len(a) else close[j] * 0.005
        bar_high = float(high[j]); bar_low = float(low[j]); bar_close = float(close[j])
        last_range = bar_high - bar_low
        lo_b = max(0, j - 30)
        base_vol = np.mean(vol[lo_b:j]) if (j - lo_b) >= 2 else np.mean(vol[:max(j, 1)] or [1.0])
        vol_spike = vol[j] / max(base_vol, 1e-9)
        d0 = max(0, j - 2)
        delta_last, _ = ind.cvd_from_klines(tb[d0:j + 1], vol[d0:j + 1])
        flow_dir = float(np.sign(np.sum(delta_last)))

        beyond = cfg["release_close_beyond_atr"] * atr_now
        exc_up = bar_high - (vah + beyond)      # >0 => high pierced above VAH
        exc_dn = (val - beyond) - bar_low       # >0 => low pierced below VAL
        big_bar = last_range >= cfg["release_atr_mult"] * atr_now
        vol_ok = vol_spike >= cfg["release_vol_mult"]

        if big_bar and vol_ok and (exc_up > 0 or exc_dn > 0):
            fl = "+" if flow_dir > 0 else ("-" if flow_dir < 0 else "0")
            if exc_up >= max(exc_dn, 0.0):
                tag = "sweep+reclaim" if bar_close <= vah else "close beyond"
                return True, "UP", f"{tf}: high>VAH ({tag}) +{vol_spike:.1f}x vol flow{fl}", vol_spike
            tag = "sweep+reclaim" if bar_close >= val else "close beyond"
            return True, "DOWN", f"{tf}: low<VAL ({tag}) +{vol_spike:.1f}x vol flow{fl}", vol_spike
    return False, "", "", 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Compression onset (for the "not limited to 300s" duration + dashboard marker)
# ─────────────────────────────────────────────────────────────────────────────
def compression_onset_index(f, cfg):
    """First bar of the current squeeze run, else first bar where BBWP dropped and stayed low."""
    high, low, close, _, _ = _frame_arrays(f)
    if len(close) < cfg["bb_n"] + 5:
        return len(close) - 1
    sqz = ind.ttm_squeeze(high, low, close, cfg["bb_n"], cfg["bb_k"], cfg["kc_mult"])
    run = ind.consecutive_true(sqz)
    if run > 0:
        return len(close) - run
    # fallback: walk back while band width stays in the lower 30th percentile
    _, _, _, width = ind.bollinger(close, cfg["bb_n"], cfg["bb_k"])
    valid = width[~np.isnan(width)]
    if len(valid) < 10:
        return len(close) - 1
    # how far back has band width stayed in the lower half of its own range (a coil)
    thresh = np.percentile(valid[-cfg["bbwp_lookback"]:], 50)
    idx = len(close) - 1
    # tolerate the current bar poking slightly above the median before giving up
    misses = 0
    while idx > 0 and not np.isnan(width[idx]):
        if width[idx] <= thresh:
            misses = 0
        else:
            misses += 1
            if misses > 2:
                break
        idx -= 1
    return idx


# ─────────────────────────────────────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────────────────────────────────────
def analyze_symbol(frames, oi_series, funding, meta, cfg=None):
    cfg = {**DEFAULTS, **(cfg or {})}
    primary = cfg["primary_tf"]
    if primary not in frames or len(frames[primary]["close"]) < cfg["bb_n"] + 5:
        return {"symbol": meta.get("symbol"), "state": "INSUFFICIENT_DATA"}

    # --- compression per frame + weighted multi-TF blend ---
    per_frame = {}
    for tf in cfg["confluence_tfs"]:
        if tf in frames and len(frames[tf]["close"]) >= cfg["bb_n"] + 5:
            d, sub = frame_compression(frames[tf], cfg)
            per_frame[tf] = {"depth": round(d, 1), **sub}
    if primary not in per_frame:
        return {"symbol": meta.get("symbol"), "state": "INSUFFICIENT_DATA"}

    wsum = sum(cfg["tf_weights"].get(tf, 0) for tf in per_frame)
    comp = sum(per_frame[tf]["depth"] * cfg["tf_weights"].get(tf, 0) for tf in per_frame) / max(wsum, 1e-9)
    # confluence bonus: coil visible on multiple timeframes = the big-move setup
    strong_frames = sum(1 for tf in per_frame if per_frame[tf]["depth"] >= 60)
    if strong_frames >= 3:
        comp = min(100.0, comp * 1.12)
    elif strong_frames >= 2:
        comp = min(100.0, comp * 1.06)
    compression_score = ind.clamp(comp)

    # --- value area (BALANCE) ------------------------------------------------
    # Computed from the 1-MINUTE volume profile for precision, over the SAME time
    # span as the primary coil window (so the width stays comparable to the
    # validated floor). Falls back to the primary timeframe if 1m is unavailable
    # or too short. `source` is recorded so the dashboard/recorder can show it.
    hp = frames[primary]
    price = float(hp["close"][-1])
    bal_bars = max(cfg["coil_bars"] * 2, 40)               # e.g. 40 primary bars
    _tf_min = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60}.get(primary, 15)
    bal_minutes = bal_bars * _tf_min                       # same span, in minutes
    f1 = frames.get("1m")
    if cfg.get("va_use_1m", True) and f1 and len(f1.get("close", [])) >= 60:
        w1 = min(len(f1["close"]), bal_minutes)            # 1 bar == 1 minute
        poc, vah, val = ind.value_area_hires(f1["high"], f1["low"], f1["close"], f1["volume"],
                                             bins=80, va_pct=0.70, window=w1)
        va_source = "1m_profile"
    else:
        poc, vah, val = ind.value_area(hp["high"], hp["low"], hp["close"], hp["volume"],
                                       bins=60, va_pct=0.70, window=bal_bars)
        va_source = f"{primary}_profile"

    # --- fuel ---
    fuel_score, fuel_sub, cvd_slope = compute_fuel(frames, oi_series, funding, meta, cfg, poc, vah, val)

    # --- bias ---
    bias, bias_conf = compute_bias(cvd_slope, fuel_sub, price, poc, vah, val)

    # --- release ---
    firing, fire_dir, fire_reason, fire_vol_spike = detect_release(frames, cfg, vah, val, cvd_slope)

    # --- tradeable-move floor: a release only counts if the coin's value-area width
    #     (its volatility) is large enough to yield a cost-clearing move. This is the
    #     one pre-fire filter the data supports; compression strength does not. ---
    va_width_pct = ((vah - val) / price * 100.0) if (price > 0 and vah > val) else None
    min_vw = cfg.get("min_va_width_pct", 0.0) or 0.0
    if firing and va_width_pct is not None and va_width_pct < min_vw:
        firing, fire_dir, fire_reason = False, "", f"below min_va_width ({va_width_pct:.2f}%<{min_vw}%)"

    # --- coil gate: a fire must be a COMPRESSED coil leaving its balance area, not
    #     just any coin printing a big bar. Require compression >= fire_min_compression
    #     at fire; otherwise there was no real balance/coil and the break is demoted.
    #     Enforces strategy-fit ("a real balance existed and broke"); per the data it
    #     does NOT change move size — it removes off-strategy non-coil breaks. ---
    fire_min_comp = cfg.get("fire_min_compression", 0.0) or 0.0
    if firing and compression_score < fire_min_comp:
        firing, fire_dir, fire_reason = False, "", \
            f"break but no coil (comp {compression_score:.0f}<{fire_min_comp:.0f})"

    # --- readiness: BOTH compression and fuel must be high (geometric mean) ---
    readiness = float(np.sqrt(max(compression_score, 0) * max(fuel_score, 0)))

    # --- onset / duration (measured from real contraction, not a fixed timer) ---
    onset_idx = compression_onset_index(hp, cfg)
    times = hp.get("time", [])
    onset_time = times[onset_idx] if times and 0 <= onset_idx < len(times) else None
    now_time = times[-1] if times else None
    dur_min = None
    if onset_time and now_time:
        dur_min = round((now_time - onset_time) / 60000.0, 1)  # times in ms

    # --- state machine ---
    if fuel_sub.get("dead_liquidity") or fuel_score < cfg["fuel_dead"]:
        state = "DEAD"
    elif firing:
        state = "FIRING"
    elif compression_score >= cfg["comp_strong"] and fuel_score >= cfg["fuel_min"] and readiness >= cfg["ready_min"]:
        state = "COILED_LOADED"
    elif compression_score >= cfg["comp_forming"]:
        state = "FORMING"
    elif per_frame[primary].get("bbwp", 100) > 75:
        state = "EXPANDED"
    else:
        state = "NEUTRAL"

    return {
        "symbol": meta.get("symbol"),
        "price": price,
        "quote_vol_24h": meta.get("quote_vol_24h"),
        "primary_tf": primary,
        "state": state,
        "readiness": round(readiness, 1),
        "compression_score": round(compression_score, 1),
        "fuel_score": round(fuel_score, 1),
        "bias": bias,
        "bias_confidence": round(bias_conf, 1),
        "firing": firing, "fire_direction": fire_dir, "fire_reason": fire_reason,
        "fire_vol_spike": round(fire_vol_spike, 2) if firing else None,
        "levels": {"poc": poc, "vah": vah, "val": val, "source": va_source,
                   "width_pct": va_width_pct},
        "coil": {"onset_time": onset_time, "onset_index": onset_idx,
                 "duration_min": dur_min,
                 "squeeze_run": per_frame[primary].get("squeeze_run", 0),
                 "bbwp": per_frame[primary].get("bbwp")},
        "oi": {"slope": fuel_sub.get("oi_slope"), "chg_pct": fuel_sub.get("oi_chg_pct")},
        "funding": funding,
        "cvd_slope": round(cvd_slope, 2),
        "per_frame": per_frame,
        "fuel_sub": fuel_sub,
    }
