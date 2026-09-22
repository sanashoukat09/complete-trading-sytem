"""
indicators.py — pure, network-free indicator math for the Compression Radar.

Everything here works on plain numpy arrays / python lists so it can be unit-tested
without any live data. No Binance, no I/O.

Core ideas this module supports (see METHODOLOGY.md):
  - Compression DEPTH (not just change): Bollinger Band Width Percentile (BBWP),
    ATR percentile, TTM squeeze (Bollinger inside Keltner), range percentile.
  - Balance area: volume-profile Value Area (POC / VAH / VAL).
  - Energy / fuel: CVD from kline taker-buy volume, slopes, absorption.
"""
from __future__ import annotations
import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Basic moving stats
# ─────────────────────────────────────────────────────────────────────────────
def ema(x, n):
    x = np.asarray(x, dtype=float)
    if len(x) == 0:
        return x
    alpha = 2.0 / (n + 1.0)
    out = np.empty_like(x)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = alpha * x[i] + (1 - alpha) * out[i - 1]
    return out


def sma(x, n):
    x = np.asarray(x, dtype=float)
    if len(x) < 1:
        return x
    c = np.cumsum(np.insert(x, 0, 0.0))
    out = (c[n:] - c[:-n]) / n
    pad = np.full(n - 1, out[0] if len(out) else np.nan)
    return np.concatenate([pad, out])


def rolling_std(x, n):
    x = np.asarray(x, dtype=float)
    out = np.full(len(x), np.nan)
    for i in range(len(x)):
        lo = max(0, i - n + 1)
        w = x[lo:i + 1]
        if len(w) >= 2:
            out[i] = np.std(w, ddof=0)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# True Range / ATR (Wilder)
# ─────────────────────────────────────────────────────────────────────────────
def true_range(high, low, close):
    high = np.asarray(high, float); low = np.asarray(low, float); close = np.asarray(close, float)
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return tr


def atr(high, low, close, n=14):
    tr = true_range(high, low, close)
    if len(tr) == 0:
        return tr
    out = np.empty_like(tr)
    out[0] = tr[0]
    a = 1.0 / n
    for i in range(1, len(tr)):
        out[i] = a * tr[i] + (1 - a) * out[i - 1]
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Bollinger / Keltner / TTM squeeze
# ─────────────────────────────────────────────────────────────────────────────
def bollinger(close, n=20, k=2.0):
    close = np.asarray(close, float)
    mid = sma(close, n)
    sd = rolling_std(close, n)
    upper = mid + k * sd
    lower = mid - k * sd
    width_pct = np.where(mid > 0, (upper - lower) / mid, np.nan)  # band width as % of price
    return mid, upper, lower, width_pct


def keltner(high, low, close, n=20, mult=1.5):
    close = np.asarray(close, float)
    mid = ema(close, n)
    a = atr(high, low, close, n)
    return mid, mid + mult * a, mid - mult * a


def ttm_squeeze(high, low, close, n=20, bb_k=2.0, kc_mult=1.5):
    """Return bool array: True where Bollinger Bands are INSIDE Keltner Channels
    (classic 'squeeze is on' = volatility compressed)."""
    _, bb_u, bb_l, _ = bollinger(close, n, bb_k)
    _, kc_u, kc_l = keltner(high, low, close, n, kc_mult)
    on = (bb_u < kc_u) & (bb_l > kc_l)
    return np.nan_to_num(on, nan=0).astype(bool)


def consecutive_true(mask):
    """How many True values at the tail of a boolean array (current run length)."""
    mask = np.asarray(mask, bool)
    c = 0
    for v in mask[::-1]:
        if v:
            c += 1
        else:
            break
    return c


def percentile_rank(series, lookback):
    """Percentile (0..100) of the LAST value vs the trailing `lookback` window.
    Low percentile of band width => extreme compression."""
    s = np.asarray(series, float)
    s = s[~np.isnan(s)]
    if len(s) < 5:
        return 50.0
    w = s[-lookback:] if len(s) > lookback else s
    last = w[-1]
    return float((np.sum(w <= last) / len(w)) * 100.0)


def bbwp(close, n=20, lookback=126, k=2.0):
    """Bollinger Band Width Percentile: rank current BB width against its own
    trailing history. 0 = tightest in `lookback` bars (deep squeeze)."""
    _, _, _, width = bollinger(close, n, k)
    return percentile_rank(width, lookback)


# ─────────────────────────────────────────────────────────────────────────────
# Range / path
# ─────────────────────────────────────────────────────────────────────────────
def range_pct(high, low, close, window):
    high = np.asarray(high, float); low = np.asarray(low, float); close = np.asarray(close, float)
    if len(close) == 0:
        return 0.0
    w = min(window, len(close))
    hi = np.max(high[-w:]); lo = np.min(low[-w:]); ref = close[-1]
    return float((hi - lo) / ref) if ref > 0 else 0.0


def range_pct_percentile(high, low, close, window, lookback):
    """Percentile of the current `window`-bar range vs history of that same measure."""
    high = np.asarray(high, float); low = np.asarray(low, float); close = np.asarray(close, float)
    n = len(close)
    if n < window + 5:
        return 50.0
    vals = []
    start = max(window, n - lookback)
    for i in range(start, n):
        hi = np.max(high[i - window + 1:i + 1]); lo = np.min(low[i - window + 1:i + 1]); ref = close[i]
        if ref > 0:
            vals.append((hi - lo) / ref)
    if len(vals) < 5:
        return 50.0
    vals = np.array(vals)
    return float((np.sum(vals <= vals[-1]) / len(vals)) * 100.0)


def path_efficiency(close, window):
    """Net displacement / total path length over last `window` bars. Low = choppy/coiling."""
    close = np.asarray(close, float)
    w = close[-window:] if len(close) > window else close
    if len(w) < 3:
        return 1.0
    total = np.sum(np.abs(np.diff(w)))
    net = abs(w[-1] - w[0])
    return float(net / total) if total > 1e-12 else 1.0


def nr_contraction(high, low, lookback=10):
    """Volatility-contraction-pattern proxy: is the most recent bar range among the
    narrowest of the last `lookback`? Returns fraction of bars the last one is <= (0..1),
    plus count of consecutive narrowing ranges at the tail."""
    high = np.asarray(high, float); low = np.asarray(low, float)
    rng = high - low
    if len(rng) < 3:
        return 0.0, 0
    w = rng[-lookback:]
    frac = float(np.sum(w >= w[-1]) / len(w))  # higher => last bar is among the narrowest
    # consecutive narrowing
    c = 0
    for i in range(len(rng) - 1, 0, -1):
        if rng[i] <= rng[i - 1]:
            c += 1
        else:
            break
    return frac, c


# ─────────────────────────────────────────────────────────────────────────────
# Value area (volume profile) — REAL profile engine
#
# Three builders share one engine (_value_area_from_profile):
#   value_area()             candle bars, volume spread by TRUE price-overlap (fallback)
#   value_area_hires()       1-minute bars — a real intraday volume profile (live default)
#   value_area_from_trades() actual executed trades (aggTrades) — tick-true (firing events)
# ─────────────────────────────────────────────────────────────────────────────
def _value_area_from_profile(prof, centers, va_pct=0.70):
    """Given a volume-at-price histogram, return (poc, vah, val): the POC and the
    narrowest band around it that holds `va_pct` of total volume."""
    prof = np.asarray(prof, float)
    n = len(prof)
    if n == 0:
        return 0.0, 0.0, 0.0
    total = float(prof.sum())
    poc_idx = int(np.argmax(prof))
    poc = float(centers[poc_idx])
    if total <= 0:
        return poc, float(centers[-1]), float(centers[0])
    target = total * va_pct
    lo_i = hi_i = poc_idx
    covered = float(prof[poc_idx])
    while covered < target and (lo_i > 0 or hi_i < n - 1):
        left = prof[lo_i - 1] if lo_i > 0 else -1.0
        right = prof[hi_i + 1] if hi_i < n - 1 else -1.0
        if right >= left:
            hi_i += 1; covered += float(prof[hi_i])
        else:
            lo_i -= 1; covered += float(prof[lo_i])
    return poc, float(centers[hi_i]), float(centers[lo_i])


def _add_bar_overlap(prof, lo, hi, bins, bar_low, bar_high, v):
    """Spread one bar's volume `v` across profile bins in PROPORTION to how much of the
    bar's [bar_low, bar_high] range overlaps each bin — the exact 'uniform across the
    bar's own range' distribution. Unlike integer bucketing it never dumps a whole bar
    into one bin nor mis-weights the edge bins."""
    if v <= 0:
        return
    span = hi - lo
    if span <= 0:
        return
    if bar_high <= bar_low:                       # zero-range bar: all at its price
        idx = int((bar_low - lo) / span * bins)
        prof[min(bins - 1, max(0, idx))] += v
        return
    f_lo = (bar_low - lo) / span * bins           # fractional bin coordinates
    f_hi = (bar_high - lo) / span * bins
    width = f_hi - f_lo
    if width <= 0:
        prof[min(bins - 1, max(0, int(f_lo)))] += v
        return
    i_lo = max(0, int(np.floor(f_lo)))
    i_hi = min(bins - 1, int(np.ceil(f_hi)) - 1)
    for bi in range(i_lo, i_hi + 1):
        overlap = min(f_hi, bi + 1.0) - max(f_lo, float(bi))
        if overlap > 0:
            prof[bi] += v * overlap / width


def _profile_from_bars(high, low, volume, bins, lo, hi):
    prof = np.zeros(bins)
    for h, l, v in zip(high, low, volume):
        _add_bar_overlap(prof, lo, hi, bins, float(l), float(h), max(float(v), 0.0))
    return prof


def value_area(high, low, close, volume, bins=60, va_pct=0.70, window=None):
    """Volume-profile value area from candle bars (fallback path). Volume is spread
    across each bar's high-low by true price-overlap. Returns (poc, vah, val)."""
    high = np.asarray(high, float); low = np.asarray(low, float)
    close = np.asarray(close, float); volume = np.asarray(volume, float)
    n = len(close)
    if n == 0:
        return 0.0, 0.0, 0.0
    if window:
        s = max(0, n - window)
        high, low, close, volume = high[s:], low[s:], close[s:], volume[s:]
    lo = float(np.min(low)); hi = float(np.max(high))
    if hi <= lo:
        return float(close[-1]), hi, lo
    edges = np.linspace(lo, hi, bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2.0
    prof = _profile_from_bars(high, low, volume, bins, lo, hi)
    return _value_area_from_profile(prof, centers, va_pct)


def value_area_hires(high, low, close, volume, bins=80, va_pct=0.70, window=None):
    """A REAL intraday volume profile from fine (1-minute) bars. Because 1m bars are
    ~15x narrower than 15m bars, spreading their volume introduces ~15x less error —
    this is a genuine volume profile, not an approximation. Same engine/return as
    value_area()."""
    return value_area(high, low, close, volume, bins=bins, va_pct=va_pct, window=window)


def value_area_from_trades(prices, qtys, bins=80, va_pct=0.70):
    """TICK-TRUE value area from actual executed trades (aggTrades): each trade's size
    is placed at its exact traded price — a real volume-at-price histogram, no spreading.
    Returns (poc, vah, val)."""
    p = np.asarray(prices, float); q = np.asarray(qtys, float)
    ok = np.isfinite(p) & np.isfinite(q) & (q > 0)
    p = p[ok]; q = q[ok]
    if len(p) == 0:
        return 0.0, 0.0, 0.0
    lo = float(p.min()); hi = float(p.max())
    if hi <= lo:
        return float(p[0]), hi, lo
    edges = np.linspace(lo, hi, bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2.0
    idx = np.clip(((p - lo) / (hi - lo) * bins).astype(int), 0, bins - 1)
    prof = np.zeros(bins)
    np.add.at(prof, idx, q)
    return _value_area_from_profile(prof, centers, va_pct)


# ─────────────────────────────────────────────────────────────────────────────
# CVD / order-flow proxies from klines
# ─────────────────────────────────────────────────────────────────────────────
def cvd_from_klines(taker_buy_base, volume):
    """Per-candle delta and cumulative CVD.
    delta = taker_buy - taker_sell = taker_buy - (volume - taker_buy) = 2*taker_buy - volume.
    """
    tb = np.asarray(taker_buy_base, float)
    vol = np.asarray(volume, float)
    delta = 2.0 * tb - vol
    cvd = np.cumsum(delta)
    return delta, cvd


def linreg_slope_norm(y):
    """Slope of a least-squares line through y, normalized by the std of y.
    Sign = direction, magnitude ~ how consistently it trends. Robust to scale."""
    y = np.asarray(y, float)
    y = y[~np.isnan(y)]
    n = len(y)
    if n < 3:
        return 0.0
    x = np.arange(n)
    x = (x - x.mean())
    denom = np.sum(x * x)
    if denom <= 0:
        return 0.0
    slope = np.sum(x * (y - y.mean())) / denom
    sd = np.std(y)
    if sd < 1e-12:
        return 0.0
    # normalize: slope over the window expressed in std units
    return float(slope * n / sd)


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))
