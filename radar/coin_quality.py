"""
coin_quality.py — score each coin 0-100 for "setup cleanliness".

Research basis (Dalton AMT, Wyckoff VSA, Minervini VCP, microstructure):

  [40pts] Liquidity tier      — 24h vol: institutional floor ensures levels HOLD
  [20pts] Impulse strength    — fire-bar volume spike vs baseline (real conviction)
  [20pts] Expected move       — value-area WIDTH %: the ONLY pre-fire feature that
                                predicts move size in our own data (AUC 0.74).
                                Compression tightness does NOT (AUC ~0.50) — so it is
                                deliberately NOT scored, to avoid re-encoding a
                                disproven belief.
  [10pts] Funding health      — crowded positioning → HVNs get swept, not defended
  [10pts] OI structure        — rising OI during coil = real stacking vs de-energized

BTC context:
  Purely informational (shown in dashboard header / coin detail).
  NEVER penalizes altcoins — ensures no altcoin breakout is missed when BTC is sideways/choppy.

Tier assignment:
  S  ≥ 80 — BTC/ETH-class institutional
  A  ≥ 65 — high-quality altcoin
  B  ≥ quality_min — tradeable, meets threshold
  C  < quality_min — below threshold, filtered off watchlist / no alert
"""
from __future__ import annotations
import math

# ─── Volume tier thresholds (USDT 24h) ───────────────────────────────────────
# Dalton / institutional microstructure: ≥$100M means real institutional OI;
# HVNs at these levels are built by size that actually defends them.
_TIER1_VOL = 100_000_000   # BTC, ETH, SOL, BNB … institutional grade
_TIER2_VOL =  30_000_000   # tradeable altcoins; below this = min_quote_vol_24h gate




def score_coin(verdict: dict, cfg: dict) -> tuple[float, dict]:
    """
    Score a coin 0–100.  Returns (total_score, detail_dict).
    detail_dict has per-component scores so the dashboard can show breakdown.
    """
    # ── 1. Liquidity tier [0-40 pts] ─────────────────────────────────────────
    # Research: instruments >$100M 24h vol have deep enough books that large
    # orders defend HVNs; below $30M the book is so thin a single whale can
    # pierce any level.
    qv = float(verdict.get("quote_vol_24h") or 0.0)
    if qv >= _TIER1_VOL:
        liq = 40.0
    elif qv >= _TIER2_VOL:
        liq = 20.0 + (qv - _TIER2_VOL) / (_TIER1_VOL - _TIER2_VOL) * 20.0
    else:
        liq = max(0.0, qv / _TIER2_VOL * 20.0)

    # ── 2. Impulse / fire-bar strength [0-20 pts] ────────────────────────────
    # Research (Wyckoff VSA, Minervini): a real institutional breakout needs
    # ≥2× baseline volume on the break bar. 1.1× = retail dribble = unreliable.
    # We extract the spike from fire_vol_spike (added by detector) or parse it
    # from fire_reason as a fallback.
    spike = verdict.get("fire_vol_spike")
    if spike is None:
        # fallback: try parsing "5m: close>VAH +2.3x vol, flow+"
        reason = verdict.get("fire_reason", "")
        try:
            idx = reason.index("x vol")
            num_str = reason[:idx].rsplit("+", 1)[-1].strip()
            spike = float(num_str)
        except Exception:
            spike = 0.0
    spike = float(spike or 0.0)
    # Full 20pts at ≥3× vol; 0pts at ≤1×; linear between 1–3×
    if spike >= 3.0:
        impulse = 20.0
    elif spike >= 1.0:
        impulse = (spike - 1.0) / 2.0 * 20.0
    else:
        impulse = 0.0

    # If coin is not currently FIRING (just coiled), award partial impulse based
    # on CVD absorption (the "stored" impulse potential)
    if verdict.get("state") != "FIRING":
        fuel_sub = verdict.get("fuel_sub") or {}
        fuel_cvd = float(fuel_sub.get("fuel_cvd") or 0.0)
        impulse = max(impulse, max(0.0, (fuel_cvd - 20.0) / 80.0 * 16.0))

    # ── 3. Expected move: value-area width % [0-20 pts] ──────────────────────
    # DATA-BACKED (events.jsonl, 970 events): value-area WIDTH % is the only
    # pre-fire feature that predicts move size (AUC 0.74). Compression tightness
    # does NOT (corr ~0.00, AUC ~0.50), so it is deliberately NOT scored here —
    # scoring it would just re-encode a disproven belief. A wider balance leaves
    # more room for a cost-clearing move once price releases.
    lv = verdict.get("levels") or {}
    vah, val = lv.get("vah"), lv.get("val")
    ref_px = float(verdict.get("price") or lv.get("poc") or 0.0)
    va_w = ((float(vah) - float(val)) / ref_px * 100.0) \
        if (vah and val and ref_px and float(vah) > float(val)) else 0.0
    # 0 pts at <=0.5% width, full 20 at >=3.0% width (linear) — mirrors the firing floor
    struct = max(0.0, min(20.0, (va_w - 0.5) / 2.5 * 20.0))

    # ── 4. Funding health [0-10 pts] ─────────────────────────────────────────
    # Dalton "failed auction" / microstructure: extreme funding = one-sided
    # crowd positioning. HVNs in these conditions become stop-hunt zones, NOT
    # defended areas.  Neutral funding = genuine two-sided interest → levels hold.
    funding = float(verdict.get("funding") or 0.0)
    fab = abs(funding)
    if fab <= 0.0001:
        fund = 10.0
    elif fab <= 0.0003:
        fund = 10.0 - (fab - 0.0001) / 0.0002 * 5.0
    elif fab <= 0.0005:
        fund = 5.0 - (fab - 0.0003) / 0.0002 * 4.0
    else:
        fund = max(0.0, 1.0 - (fab - 0.0005) / 0.001)

    # ── 5. OI structure [0-10 pts] ───────────────────────────────────────────
    # Rising OI during compression = real position stacking (institutions
    # accumulating/distributing into the balance area → loaded for a big move).
    # Falling OI = de-energizing = move less likely to sustain.
    fuel_sub = verdict.get("fuel_sub") or {}
    oi_slope = float(fuel_sub.get("oi_slope") or 0.0)
    oi_chg = float(fuel_sub.get("oi_chg_pct") or 0.0)
    if oi_slope > 0.5 and oi_chg > 2.0:
        oi_pts = 10.0
    elif oi_slope > 0:
        oi_pts = 5.0 + min(5.0, oi_slope * 5.0)
    elif oi_slope > -0.3:
        oi_pts = 3.0
    else:
        oi_pts = 0.0

    total = liq + impulse + struct + fund + oi_pts
    total = round(min(100.0, max(0.0, total)), 1)

    detail = {
        "liq": round(liq, 1),
        "impulse": round(impulse, 1),
        "struct": round(struct, 1),
        "fund": round(fund, 1),
        "oi": round(oi_pts, 1),
        "fire_vol_spike": round(spike, 2),
    }
    return total, detail


def annotate_verdict(verdict: dict, cfg: dict) -> dict:
    """Add quality_score, quality_tier, quality_detail to verdict in-place."""
    total, detail = score_coin(verdict, cfg)
    verdict["quality_score"] = total
    verdict["quality_detail"] = detail
    _set_tier(verdict, cfg)
    return verdict


def apply_btc_regime(verdict: dict, btc_regime: str, cfg: dict | None = None) -> dict:
    """
    Attach BTC regime context purely as informational metadata.
    Never alters quality_score or tier, ensuring altcoins moving independently
    (e.g., when BTC is sideways or ranging) are NEVER penalized or missed.
    """
    verdict["btc_regime"] = btc_regime
    return verdict


def _set_tier(verdict: dict, cfg: dict) -> None:
    qs = verdict.get("quality_score", 0.0)
    qmin = cfg.get("quality_min", 50.0)
    if qs >= 80:
        verdict["quality_tier"] = "S"
    elif qs >= 65:
        verdict["quality_tier"] = "A"
    elif qs >= qmin:
        verdict["quality_tier"] = "B"
    else:
        verdict["quality_tier"] = "C"
