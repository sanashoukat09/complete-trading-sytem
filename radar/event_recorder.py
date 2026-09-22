"""
event_recorder.py — forensic capture of every compression RELEASE.

For each coil that fires/leaves its balance area, this records a complete, durable
research record built ONLY from executed/validated data:

  PRE-RELEASE  : the full coil context the instant it fired
                 (compression, fuel, bias, value area + WIDTH%, coil age, bbwp, OI,
                  funding, candle-CVD, TRUE tick CVD/delta, footprint POC, whale prints,
                  long/short & top-trader positioning, liquidations just before)
  RELEASE      : direction, how far beyond the value area, break volume, OI move,
                 liquidations fueling the break
  POST-RELEASE : the actual forward outcome, tracked to 15/30/60/120/240 min —
                 MFE% / MAE% (how big the move really was), close% per horizon,
                 forward flow (CVD, OI build vs flush, liquidation cascade, L/S shift),
                 and a label: CONTINUATION / MEAN_REVERSION / CHOP.

Records append to  <data>/research/events.jsonl  (one line per finalized event) plus a
full  <data>/research/events/<id>.json.  Mine them later with analyze_events.py.

The math helpers are pure and unit-testable; EventRecorder.update() does the fetching.
"""
from __future__ import annotations
import json
import os
import time

from . import binance_data as bd
from . import flow_data as fd


# ─────────────────────────────────────────────────────────────────────────────
# PURE HELPERS (offline-testable)
# ─────────────────────────────────────────────────────────────────────────────
def _sign(direction):
    return 1.0 if str(direction).upper() == "UP" else -1.0


def mfe_mae(klines, release_ms, release_px, direction, horizons_min, now_ms):
    """Forward excursions in PERCENT from the release, using 1m bars since release.
    Favorable = in the release direction; adverse = against it (both reported positive)."""
    s = _sign(direction)
    bars = [(int(t), float(h), float(l), float(c))
            for t, h, l, c in zip(klines["time"], klines["high"], klines["low"], klines["close"])
            if int(t) >= release_ms - 60_000]
    if not bars or release_px <= 0:
        return {"mfe_pct": 0.0, "mae_pct": 0.0, "last_pct": 0.0, "horizons": {}, "returned_into": None}

    def fav(px):  return s * (px - release_px) / release_px * 100.0
    mfe = mae = 0.0
    for t, h, l, c in bars:
        # favorable extreme = the more-favorable of the bar's high/low; adverse = the less
        best = max(fav(h), fav(l)); worst = min(fav(h), fav(l))
        mfe = max(mfe, best)
        mae = max(mae, -worst)
    last_close = bars[-1][3]
    last_pct = fav(last_close)

    horizons = {}
    for H in horizons_min:
        cutoff = release_ms + H * 60_000
        upto = [b for b in bars if b[0] <= cutoff]
        if not upto or now_ms < cutoff - 60_000:
            continue                                    # horizon not reached yet
        fav_h = max((max(fav(b[1]), fav(b[2])) for b in upto), default=0.0)
        adv_h = max((-min(fav(b[1]), fav(b[2])) for b in upto), default=0.0)
        horizons[str(H)] = {"close_pct": round(fav(upto[-1][3]), 3),
                            "mfe_pct": round(max(fav_h, 0.0), 3),
                            "mae_pct": round(max(adv_h, 0.0), 3)}
    return {"mfe_pct": round(max(mfe, 0.0), 3), "mae_pct": round(max(mae, 0.0), 3),
            "last_pct": round(last_pct, 3), "horizons": horizons}


def classify(fav, adv, last_pct, va_width_pct, min_move_pct=0.4):
    """Label the outcome for edge-mining. Raw numbers are always kept too."""
    scale = max(va_width_pct, min_move_pct)
    if last_pct >= scale and fav >= adv:
        return "CONTINUATION"
    if last_pct <= 0 and adv >= scale:
        return "MEAN_REVERSION"
    if last_pct <= -0.5 * scale:
        return "MEAN_REVERSION"
    if fav >= scale and adv < scale and last_pct > 0:
        return "CONTINUATION"
    return "CHOP"


def build_pre_snapshot(v, flow_bundle, liq_pre):
    """Assemble the pre-release feature record from a verdict + executed-flow bundle."""
    lv = v.get("levels", {}) or {}
    vah, val, poc = lv.get("vah"), lv.get("val"), lv.get("poc")
    va_width_pct = ((vah - val) / poc * 100.0) if (vah and val and poc) else None
    flow = (flow_bundle or {}).get("flow", {}) or {}
    pos = (flow_bundle or {}).get("positioning", {}) or {}
    # TRUE tick value area from the executed aggTrades captured this instant (no spreading)
    vt = flow.get("va") or {}
    vt_h, vt_l, vt_p = vt.get("vah"), vt.get("val"), vt.get("poc")
    vt_width = ((vt_h - vt_l) / vt_p * 100.0) if (vt_h and vt_l and vt_p) else None
    va_true = ({"vah": vt_h, "val": vt_l, "poc": vt_p,
                "width_pct": round(vt_width, 3) if vt_width is not None else None,
                "n_trades": flow.get("n"), "span_min": flow.get("span_min"),
                "source": "aggtrades_true"} if (vt_h and vt_l and vt_h > vt_l) else None)
    return {
        "compression_score": v.get("compression_score"),
        "fuel_score": v.get("fuel_score"),
        "readiness": v.get("readiness"),
        "bias": v.get("bias"), "bias_confidence": v.get("bias_confidence"),
        "coil": v.get("coil", {}),
        "va": {"vah": vah, "val": val, "poc": poc,
               "width_pct": round(va_width_pct, 3) if va_width_pct is not None else None,
               "source": lv.get("source")},
        "va_true": va_true,
        "oi_over_coil_pct": (v.get("oi", {}) or {}).get("chg_pct"),
        "funding": v.get("funding"),
        "cvd_slope_candle": v.get("cvd_slope"),
        "true_flow": {k: flow.get(k) for k in
                      ("n", "span_min", "cvd_norm", "delta_buy_frac", "whale_count",
                       "whale_delta", "whale_notional", "biggest_notional", "poc_price")},
        "footprint": flow.get("footprint", []),
        "positioning": pos,
        "liq_pre": liq_pre,
        "per_frame": v.get("per_frame", {}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# RECORDER
# ─────────────────────────────────────────────────────────────────────────────
class EventRecorder:
    def __init__(self, cfg, liq=None, log=print):
        self.cfg = cfg
        self.liq = liq
        self.log = log
        self.horizons = cfg.get("research", {}).get("horizons_min", [15, 30, 60, 120, 240])
        self.max_h = max(self.horizons)
        self.cooldown_ms = cfg.get("research", {}).get("event_cooldown_min", 30) * 60_000
        self.track_max = cfg.get("research", {}).get("track_max", 40)
        self.data_dir = cfg["data_dir"]
        self.research_dir = os.path.join(self.data_dir, "research")
        self.events_dir = os.path.join(self.research_dir, "events")
        os.makedirs(self.events_dir, exist_ok=True)
        self.events_jsonl = os.path.join(self.research_dir, "events.jsonl")
        self.open = {}                       # event_id -> event dict
        self.last_event_ts = {}              # symbol -> ms of last opened event
        self._load_open()

    # ---- persistence ----
    def _open_path(self):
        return os.path.join(self.research_dir, "open_events.json")

    def _load_open(self):
        try:
            with open(self._open_path()) as fh:
                self.open = {e["id"]: e for e in json.load(fh)}
            self.log(f"[recorder] resumed {len(self.open)} open events")
        except Exception:
            self.open = {}

    def _save_open(self):
        try:
            with open(self._open_path(), "w") as fh:
                json.dump(list(self.open.values()), fh)
        except Exception as e:
            self.log(f"[recorder] save_open error: {e}")

    def _finalize(self, ev):
        # prefer the TRUE tick value-area width as the move yardstick when we have it
        va_true = ev["pre"].get("va_true") or {}
        vw = (va_true.get("width_pct") or ev["pre"]["va"]["width_pct"] or 0.4)
        fwd = ev.get("forward", {})
        ev["label"] = classify(fwd.get("mfe_pct", 0), fwd.get("mae_pct", 0),
                               fwd.get("last_pct", 0), vw,
                               self.cfg.get("research", {}).get("min_move_pct", 0.4))
        ev["finalized_at"] = int(time.time() * 1000)
        ev["status"] = "FINAL"
        # append compact row
        row = {
            "id": ev["id"], "symbol": ev["symbol"], "release_time": ev["release_time"],
            "release_time_str": ev["release_time_str"], "direction": ev["direction"],
            "label": ev["label"],
            "mfe_pct": fwd.get("mfe_pct"), "mae_pct": fwd.get("mae_pct"),
            "last_pct": fwd.get("last_pct"),
            "va_width_pct": ev["pre"]["va"]["width_pct"],
            "va_true_width_pct": (ev["pre"].get("va_true") or {}).get("width_pct"),
            "va_source": (ev["pre"]["va"] or {}).get("source"),
            "compression": ev["pre"]["compression_score"], "fuel": ev["pre"]["fuel_score"],
            "readiness": ev["pre"]["readiness"], "bias": ev["pre"]["bias"],
            "coil_age_min": (ev["pre"]["coil"] or {}).get("duration_min"),
            "bbwp": (ev["pre"]["coil"] or {}).get("bbwp"),
            "oi_over_coil_pct": ev["pre"]["oi_over_coil_pct"],
            "true_cvd_norm": (ev["pre"]["true_flow"] or {}).get("cvd_norm"),
            "whale_delta": (ev["pre"]["true_flow"] or {}).get("whale_delta"),
            "global_ls": (ev["pre"]["positioning"] or {}).get("global_account", {}).get("last"),
            "top_pos_ls": (ev["pre"]["positioning"] or {}).get("top_position", {}).get("last"),
            "liq_pre_notional": (ev["pre"]["liq_pre"] or {}).get("notional"),
            "liq_pre_count": (ev["pre"]["liq_pre"] or {}).get("count"),
            "liq_pre_long": (ev["pre"]["liq_pre"] or {}).get("long_liq"),
            "liq_pre_short": (ev["pre"]["liq_pre"] or {}).get("short_liq"),
            "liq_pre_side": (ev["pre"]["liq_pre"] or {}).get("net_side"),
            "release_break_atr": ev["release"].get("beyond_atr"),
            "release_vol_spike": ev["release"].get("vol_spike"),
            "fwd_oi_chg_pct": fwd.get("oi_chg_pct"),
            "fwd_liq_notional": fwd.get("liq_notional"),
            "fwd_liq_side": fwd.get("liq_side"),
            "fwd_liq_count": fwd.get("liq_count"),
            "fwd_liq_long": fwd.get("liq_long_liq"),
            "fwd_liq_short": fwd.get("liq_short_liq"),
        }
        try:
            with open(self.events_jsonl, "a") as fh:
                fh.write(json.dumps(row) + "\n")
            with open(os.path.join(self.events_dir, ev["id"] + ".json"), "w") as fh:
                json.dump(ev, fh)
        except Exception as e:
            self.log(f"[recorder] finalize write error: {e}")
        self.log(f"[recorder] FINAL {ev['symbol']} {ev['direction']} -> {ev['label']} "
                 f"(MFE {fwd.get('mfe_pct')}% / MAE {fwd.get('mae_pct')}% / close {fwd.get('last_pct')}%)")

    # ---- main tick ----
    async def update(self, session, verdicts_by_symbol, now_ms=None):
        now_ms = now_ms or int(time.time() * 1000)
        if not self.cfg.get("research", {}).get("enabled", True):
            return

        # 1) detect new release events
        for sym, v in verdicts_by_symbol.items():
            if len(self.open) >= self.track_max:
                break
            is_release = (v.get("state") == "FIRING") or bool(v.get("firing"))
            if not is_release:
                continue
            if any(e["symbol"] == sym for e in self.open.values()):
                continue
            if now_ms - self.last_event_ts.get(sym, 0) < self.cooldown_ms:
                continue
            await self._open_event(session, sym, v, now_ms)

        # 2) forward-track open events
        for ev in list(self.open.values()):
            try:
                await self._forward(session, ev, now_ms)
            except Exception as e:
                self.log(f"[recorder] forward {ev['symbol']} error: {e}")
            if now_ms - ev["release_time"] >= self.max_h * 60_000:
                self._finalize(ev)
                self.open.pop(ev["id"], None)
        self._save_open()

    async def _open_event(self, session, sym, v, now_ms):
        lv = v.get("levels", {}) or {}
        direction = v.get("fire_direction") or ("UP" if (v.get("cvd_slope", 0) >= 0) else "DOWN")
        release_px = float(v.get("price") or 0) or (lv.get("poc") or 0)
        if release_px <= 0:
            return
        # executed-flow bundle at the release instant
        flow_bundle = await fd.fetch_flow_bundle(session, sym, self.cfg.get("research", {}))
        liq_pre = self.liq.summarize(sym, now_ms - 15 * 60_000, now_ms) if self.liq else \
            {"count": 0, "notional": 0.0, "net_side": "NONE"}
        # release bar context from detector verdict or fallback to klines
        rel_meta = v.get("release_meta") or {}
        beyond_atr = rel_meta.get("beyond_atr")
        vol_spike = rel_meta.get("vol_spike")
        if beyond_atr is None or vol_spike is None:
            k1 = await bd.fetch_klines(session, sym, "1m", 60)
            if k1 and len(k1["close"]) > 20:
                import numpy as np
                from . import indicators as ind
                a = ind.atr(k1["high"], k1["low"], k1["close"], 14)
                atr_now = float(a[-1]) if len(a) else release_px * 0.005
                edge = lv.get("vah") if direction == "UP" else lv.get("val")
                if edge and beyond_atr is None:
                    beyond_atr = round(abs(release_px - edge) / max(atr_now, 1e-9), 2)
                base = float(np.mean(k1["volume"][-30:-1])) if len(k1["volume"]) > 31 else 1.0
                if vol_spike is None:
                    vol_spike = round(float(k1["volume"][-2] if len(k1["volume"]) > 1 else k1["volume"][-1]) / max(base, 1e-9), 2)
        oi_series = await bd.fetch_open_interest(session, sym, self.cfg.get("oi_period", "5m"), 48)
        oi_release = oi_series["oi"][-1] if oi_series.get("oi") else None

        eid = f"{sym}_{int(now_ms/1000)}"
        ev = {
            "id": eid, "symbol": sym, "status": "OPEN",
            "release_time": now_ms,
            "release_time_str": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
            "release_price": release_px, "direction": direction,
            "pre": build_pre_snapshot(v, flow_bundle, liq_pre),
            "release": {"beyond_atr": beyond_atr, "vol_spike": vol_spike,
                        "fire_reason": v.get("fire_reason", ""), "oi_release": oi_release},
            "forward": {"mfe_pct": 0.0, "mae_pct": 0.0, "last_pct": 0.0, "horizons": {},
                        "flow_by_horizon": {}, "oi_chg_pct": None,
                        "liq_notional": 0.0, "liq_side": "NONE"},
            "_oi_release": oi_release,
            "_horizons_done": [],
        }
        self.open[eid] = ev
        self.last_event_ts[sym] = now_ms
        self.log(f"[recorder] OPEN  {sym} {direction} @ {release_px} "
                 f"(comp {ev['pre']['compression_score']} fuel {ev['pre']['fuel_score']} "
                 f"bias {ev['pre']['bias']}) - tracking {self.max_h}m")

    async def _forward(self, session, ev, now_ms):
        sym = ev["symbol"]
        k1 = await bd.fetch_klines(session, sym, "1m", 500)
        if k1:
            mm = mfe_mae(k1, ev["release_time"], ev["release_price"], ev["direction"],
                        self.horizons, now_ms)
            ev["forward"]["mfe_pct"] = max(ev["forward"]["mfe_pct"], mm["mfe_pct"])
            ev["forward"]["mae_pct"] = max(ev["forward"]["mae_pct"], mm["mae_pct"])
            ev["forward"]["last_pct"] = mm["last_pct"]
            ev["forward"]["horizons"].update(mm["horizons"])
        # liquidations since release
        if self.liq:
            ls = self.liq.summarize(sym, ev["release_time"], now_ms)
            ev["forward"]["liq_notional"] = ls["notional"]
            ev["forward"]["liq_side"] = ls["net_side"]
            ev["forward"]["liq_count"] = ls["count"]
            ev["forward"]["liq_long_liq"] = ls["long_liq"]
            ev["forward"]["liq_short_liq"] = ls["short_liq"]
        # OI change since release
        oi_series = await bd.fetch_open_interest(session, sym, self.cfg.get("oi_period", "5m"), 6)
        if oi_series.get("oi") and ev.get("_oi_release"):
            oi_now = oi_series["oi"][-1]
            ev["forward"]["oi_chg_pct"] = round((oi_now - ev["_oi_release"]) / ev["_oi_release"] * 100.0, 3)
        # capture a forward FLOW snapshot the first time each horizon is crossed
        for H in self.horizons:
            if H in ev["_horizons_done"]:
                continue
            if now_ms - ev["release_time"] >= H * 60_000:
                fb = await fd.fetch_flow_bundle(session, sym, self.cfg.get("research", {}))
                ls_h = self.liq.summarize(sym, ev["release_time"], ev["release_time"] + H * 60_000) if self.liq else {}
                ev["forward"]["flow_by_horizon"][str(H)] = {
                    "cvd_norm": (fb.get("flow", {}) or {}).get("cvd_norm"),
                    "whale_delta": (fb.get("flow", {}) or {}).get("whale_delta"),
                    "global_ls": (fb.get("positioning", {}) or {}).get("global_account", {}).get("last"),
                    "top_pos_ls": (fb.get("positioning", {}) or {}).get("top_position", {}).get("last"),
                    "oi_chg_pct": ev["forward"]["oi_chg_pct"],
                    "liq_notional": ls_h.get("notional", 0.0),
                    "liq_count": ls_h.get("count", 0),
                    "liq_long_liq": ls_h.get("long_liq", 0.0),
                    "liq_short_liq": ls_h.get("short_liq", 0.0),
                    "liq_side": ls_h.get("net_side", "NONE"),
                }
                ev["_horizons_done"].append(H)
