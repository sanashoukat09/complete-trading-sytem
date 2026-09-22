"""
confirmation_tracker.py — fire on CONFIRMATION, not on the raw break.

Replaces "price broke + volume -> fire" with a stateful, event-driven layer that
watches each break as it develops (bar level, across the 90s scan loop) and only
surfaces a trade when mean-reversion OR continuation is CONFIRMED, with a real
target (next volume node / value) and a stop, requiring >= min_rr reward:risk and
enough move still left to capture. Observe-mode: it surfaces trades on the
dashboard; it never places orders.

Grounded in the order-flow / auction concepts in your own material
(Order Flow Decoded: zones, defense vs surrender, absorption, cumulative-delta
divergence, "take profit into the next heavy volume node") + Dalton acceptance vs
failed auction + Wyckoff spring / up-thrust. It reads BAR-LEVEL flow (1m delta,
value area), deliberately NOT single ticks — micro ticks are noise and a bad basis
for a decision, exactly as you said.

Decision per break (evaluated every scan on the 1m frame since the break):
  CONTINUATION  (surrender / acceptance): price accepts & extends beyond the broken
      value edge AND aggressive delta stays with the break AND no absorption stalls
      it. Enter with the break; target = a measured move to the next node; stop =
      back inside the broken edge.
  MEAN_REVERSION (defense / failed auction): price pokes beyond the edge then
      RECLAIMS back inside value AND shows defense — absorption at the extreme,
      cumulative-delta divergence, or a delta flip against the break. Enter the fade;
      target = POC (heaviest node); stop = beyond the swept extreme.
  Only surfaced if reward:risk >= min_rr and the target is not already reached
  (so you never chase a move that is mostly gone). Times out otherwise.
"""
from __future__ import annotations
import json, os, time
import numpy as np


def _arr(f, k):
    return np.asarray(f.get(k, []), float)

def _cvd(taker_buy, volume):
    tb = np.asarray(taker_buy, float); v = np.asarray(volume, float)
    delta = 2.0 * tb - v            # taker buy vol - taker sell vol
    return delta, np.cumsum(delta)


class ConfirmationTracker:
    DEF = dict(enabled=True, min_rr=1.0, max_watch_min=60, cooldown_min=30,
               accept_bars=2, atr_stop_buf=0.25, keep_recent=40,
               min_remaining_atr=0.8)

    def __init__(self, cfg, log=print):
        c = {**self.DEF, **(cfg.get("confirmation", {}) or {})}
        self.cfg = c
        self.log = log
        self.pending = {}        # symbol -> setup state dict
        self.active = []         # confirmed, still-open trades (rolling)
        self.recent = []         # recently closed/expired (rolling, for dashboard history)
        self.last_arm_ts = {}

    # ---------------------------------------------------------------- arming
    def _raw_break(self, v):
        return bool(v.get("firing")) or v.get("state") == "FIRING"

    def _arm(self, sym, v, frames, now_ms):
        lv = v.get("levels", {}) or {}
        poc, vah, val = lv.get("poc"), lv.get("vah"), lv.get("val")
        f1 = frames.get("1m") or frames.get("5m")
        if not f1 or None in (poc, vah, val) or not (vah > val):
            return
        high, low, close = _arr(f1, "high"), _arr(f1, "low"), _arr(f1, "close")
        if len(close) < 20:
            return
        tr = np.maximum(high[1:] - low[1:],
                        np.maximum(abs(high[1:] - close[:-1]), abs(low[1:] - close[:-1])))
        atr = float(np.mean(tr[-14:])) if len(tr) >= 14 else float(np.mean(tr)) if len(tr) else 0.0
        self.pending[sym] = {
            "symbol": sym, "break_dir": v.get("fire_direction") or "UP",
            "t0": now_ms, "release_price": float(v.get("price") or poc),
            "poc": float(poc), "vah": float(vah), "val": float(val),
            "atr": atr or float(v.get("price") or poc) * 0.004,
            "reason": v.get("fire_reason", ""),
        }
        self.last_arm_ts[sym] = now_ms

    # ------------------------------------------------------------- evaluate
    def _evaluate(self, st, frames, now_ms):
        """Return a confirmed-trade dict, 'DROP', or None (keep watching)."""
        f1 = frames.get("1m") or frames.get("5m")
        if not f1:
            return None
        t = _arr(f1, "time");
        if len(t) < 3:
            return None
        # bars since the break
        mask = t >= (st["t0"] - 60_000)
        o, h, l, c = _arr(f1, "open")[mask], _arr(f1, "high")[mask], _arr(f1, "low")[mask], _arr(f1, "close")[mask]
        vol = _arr(f1, "volume")[mask]; tb = _arr(f1, "taker_buy")[mask]
        n = len(c)
        if n < st["cfg_accept"] + 1:
            return None
        up = st["break_dir"] == "UP"
        vah, val, poc, atr = st["vah"], st["val"], st["poc"], max(st["atr"], 1e-9)
        edge = vah if up else val
        delta, cvd = _cvd(tb, vol)
        price = float(c[-1])
        buf = st["cfg_atr_stop_buf"] * atr

        # running extreme beyond the break
        ext = float(h.max()) if up else float(l.min())
        min_rr = st["cfg_min_rr"]; min_rem = st["cfg_min_remaining_atr"] * atr

        # ---------- MEAN REVERSION (defense / failed auction) ----------
        # reclaim back inside value after poking beyond the edge
        poked = (h.max() > vah) if up else (l.min() < val)
        reclaimed = (val <= price <= vah)
        # defense signals (bar level, not ticks):
        #   absorption: a bar made a new extreme on strong volume but closed rejecting it
        vbar = vol / (np.mean(vol) + 1e-9)
        rej = ((h - np.maximum(o, c)) if up else (np.minimum(o, c) - l)) / (h - l + 1e-9)
        absorption = bool(np.any((rej[-3:] > 0.55) & (vbar[-3:] > 1.3)))
        #   cumulative-delta divergence: price makes a FRESH extreme in the recent half of
        #   the post-break window while CVD FAILS to make a matching extreme. (The old test
        #   `cvd[-1] > cvd.min()*0.6` was ~always True and did not measure divergence at all.)
        _half = max(2, n // 2)
        if up:
            price_hh = h[_half:].max() > h[:_half].max()        # higher high, recent half
            cvd_no_hh = cvd[_half:].max() <= cvd[:_half].max()  # CVD did NOT confirm it
            cvd_div = bool(price_hh and cvd_no_hh)
        else:
            price_ll = l[_half:].min() < l[:_half].min()        # lower low, recent half
            cvd_no_ll = cvd[_half:].min() >= cvd[:_half].min()  # CVD did NOT confirm it
            cvd_div = bool(price_ll and cvd_no_ll)
        #   delta flip against the break on the latest bar
        flip = (delta[-1] < 0) if up else (delta[-1] > 0)
        if poked and reclaimed and (absorption or cvd_div or flip):
            entry = price
            target = poc                                   # heaviest node = first target
            stop = (ext + buf) if up else (ext - buf)      # beyond the swept extreme
            risk = abs(entry - stop); reward = abs(target - entry)
            if risk > 0 and reward >= min_rr * risk and reward >= min_rem:
                return self._mk(st, "MEAN_REVERSION", ("SHORT" if up else "LONG"),
                                entry, stop, target, risk, reward, now_ms,
                                "reclaim + " + ("absorption" if absorption else "CVD divergence" if cvd_div else "delta flip"))

        # ---------- CONTINUATION (surrender / acceptance) ----------
        ab = st["cfg_accept"]
        accepted = all((c[-i] > edge) for i in range(1, ab + 1)) if up else all((c[-i] < edge) for i in range(1, ab + 1))
        progressing = (c[-1] >= h[-2]) if up else (c[-1] <= l[-2])
        flow_with = (cvd[-1] > 0 and delta[-3:].sum() > 0) if up else (cvd[-1] < 0 and delta[-3:].sum() < 0)
        not_absorbed = not absorption
        if accepted and progressing and flow_with and not_absorbed:
            entry = price
            # target = measured move into the next node (project the value width beyond the edge)
            width = max(vah - val, 1.2 * atr)
            target = entry + width if up else entry - width
            stop = (edge - buf) if up else (edge + buf)    # back inside the broken edge
            risk = abs(entry - stop); reward = abs(target - entry)
            if risk > 0 and reward >= min_rr * risk and reward >= min_rem:
                return self._mk(st, "CONTINUATION", ("LONG" if up else "SHORT"),
                                entry, stop, target, risk, reward, now_ms, "acceptance + delta with break")

        # ---------- timeout ----------
        if now_ms - st["t0"] >= st["cfg_max_watch"] * 60_000:
            return "DROP"
        return None

    def _mk(self, st, setup, side, entry, stop, target, risk, reward, now_ms, why):
        rr = round(reward / risk, 2) if risk else 0
        return {
            "symbol": st["symbol"], "setup": setup, "side": side,
            "break_dir": st["break_dir"],
            "entry": round(entry, 8), "stop": round(stop, 8), "target": round(target, 8),
            "risk_pct": round(risk / entry * 100, 2), "reward_pct": round(reward / entry * 100, 2),
            "rr": rr, "confirmed_at": now_ms,
            "confirmed_at_str": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now_ms / 1000)),
            "wait_min": round((now_ms - st["t0"]) / 60000, 1),
            "why": why, "status": "CONFIRMED",
        }

    # ---------------------------------------------------------------- tick
    def update(self, results, now_ms=None):
        if not self.cfg.get("enabled", True):
            return
        now_ms = now_ms or int(time.time() * 1000)
        frames_by = {}; verd_by = {}
        for item in results:
            try:
                v, frames = item[0], item[1]
            except Exception:
                continue
            s = v.get("symbol")
            if s:
                frames_by[s] = frames; verd_by[s] = v
        # 1) arm new raw breaks (not already pending, not already an active trade, cooldown ok)
        active_syms = {a["symbol"] for a in self.active}
        for s, v in verd_by.items():
            if not self._raw_break(v):
                continue
            if s in self.pending or s in active_syms:
                continue
            if now_ms - self.last_arm_ts.get(s, 0) < self.cfg["cooldown_min"] * 60_000:
                continue
            self._arm(s, v, frames_by.get(s, {}), now_ms)
        # inject per-setup cfg so _evaluate is self-contained
        for st in self.pending.values():
            st.setdefault("cfg_min_rr", self.cfg["min_rr"])
            st.setdefault("cfg_max_watch", self.cfg["max_watch_min"])
            st.setdefault("cfg_accept", self.cfg["accept_bars"])
            st.setdefault("cfg_atr_stop_buf", self.cfg["atr_stop_buf"])
            st.setdefault("cfg_min_remaining_atr", self.cfg["min_remaining_atr"])
        # 2) evaluate pending
        for s, st in list(self.pending.items()):
            frames = frames_by.get(s)
            if not frames:
                # symbol left the scanned set; keep until timeout
                if now_ms - st["t0"] >= st["cfg_max_watch"] * 60_000:
                    self.pending.pop(s, None)
                continue
            try:
                d = self._evaluate(st, frames, now_ms)
            except Exception as e:
                self.log(f"[confirm] {s} eval error: {e}"); d = None
            if d == "DROP":
                self.pending.pop(s, None)
            elif isinstance(d, dict):
                self.active.append(d)
                self.pending.pop(s, None)
                self.log(f"[confirm] ✅ {s} {d['setup']} {d['side']} entry {d['entry']} "
                         f"stop {d['stop']} tp {d['target']} RR {d['rr']} ({d['why']})")
        # 3) manage active trades: mark hit stop/target using latest price (observe only)
        for a in list(self.active):
            frames = frames_by.get(a["symbol"])
            if not frames:
                continue
            f1 = frames.get("1m") or frames.get("5m")
            if not f1:
                continue
            px = float(_arr(f1, "close")[-1])
            long = a["side"] == "LONG"
            hit = None
            if long:
                if px <= a["stop"]: hit = "STOP"
                elif px >= a["target"]: hit = "TARGET"
            else:
                if px >= a["stop"]: hit = "STOP"
                elif px <= a["target"]: hit = "TARGET"
            if hit:
                a["status"] = hit
                a["closed_at_str"] = time.strftime("%Y-%m-%d %H:%M:%S")
                self.recent.insert(0, a); self.active.remove(a)
        self.recent = self.recent[: self.cfg["keep_recent"]]

    def write(self, data_dir):
        out = {"generated_at": int(time.time()), "generated_at_str": time.strftime("%Y-%m-%d %H:%M:%S"),
               "watching": [{"symbol": s, "break_dir": st["break_dir"],
                             "wait_min": round((time.time()*1000 - st["t0"])/60000, 1)}
                            for s, st in self.pending.items()],
               "active": self.active, "recent": self.recent}
        try:
            with open(os.path.join(data_dir, "confirmed_trades.json"), "w") as fh:
                json.dump(out, fh, indent=2)
        except Exception as e:
            self.log(f"[confirm] write error: {e}")
