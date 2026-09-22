"""
liq_collector.py — background liquidation collector (executed, un-spoofable).

Subscribes to Binance USDT-M all-market liquidation stream (!forceOrder@arr) over
WebSocket and maintains a durable, rolling per-symbol buffer of forced closes.
Persists to <data>/research/liquidations.json across restarts so that event_recorder
and dashboard always have access to pre-release and forward liquidation context.

forceOrder side semantics:
  S == "SELL"  → LONG positions being force-closed  (forced selling)
  S == "BUY"   → SHORT positions being force-closed  (forced buying)

Runs as an asyncio background task. If the socket reconnects, it re-establishes
with backoff and retains loaded buffer without losing data.
"""
from __future__ import annotations
import asyncio
import json
import os
import time
from collections import defaultdict, deque

import aiohttp

# Active Binance Futures Market WebSocket endpoint
PRIMARY_WS_URL = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
FALLBACK_WS_URL = "wss://fstream.binance.com/market/stream?streams=!forceOrder@arr"


class LiquidationCollector:
    def __init__(self, retain_min=360, per_symbol_max=800, data_dir=None, log=print):
        self.buf = defaultdict(lambda: deque(maxlen=per_symbol_max))
        self.per_symbol_max = per_symbol_max
        self.retain_ms = retain_min * 60_000
        self.connected = False
        self.total = 0
        self._stop = False
        self.log = log
        self.data_dir = data_dir
        self.storage_path = os.path.join(data_dir, "research", "liquidations.json") if data_dir else None
        self._last_save_ts = 0
        self._unflushed = 0
        self._load_from_disk()

    def _load_from_disk(self):
        """Loads cached liquidations from disk on startup to preserve continuity."""
        if not self.storage_path or not os.path.exists(self.storage_path):
            return
        try:
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - self.retain_ms
            loaded_count = 0
            with open(self.storage_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    for sym, items in data.items():
                        if isinstance(items, list):
                            for x in items:
                                if isinstance(x, dict) and x.get("t", 0) >= cutoff:
                                    self.buf[sym].append(x)
                                    loaded_count += 1
            self.total += loaded_count
            if loaded_count > 0:
                self.log(f"[liq] restored {loaded_count} liquidations across {len(self.buf)} symbols from disk")
        except Exception as e:
            self.log(f"[liq] note loading liquidations cache: {e}")

    def save_to_disk(self):
        """Persists rolling liquidation buffer to disk (atomic write)."""
        if not self.storage_path:
            return
        try:
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - self.retain_ms
            dump_data = {}
            for sym, d in self.buf.items():
                valid = [x for x in d if x.get("t", 0) >= cutoff]
                if valid:
                    dump_data[sym] = valid
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
            temp_path = self.storage_path + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as fh:
                json.dump(dump_data, fh)
            os.replace(temp_path, self.storage_path)
            self._last_save_ts = now_ms
            self._unflushed = 0
        except Exception as e:
            self.log(f"[liq] error saving liquidations: {e}")

    def _add(self, o):
        try:
            sym = o["s"]
            side = o["S"]
            qty = float(o.get("q", 0))
            price = float(o.get("ap", o.get("p", 0)))
            t = int(o.get("T", time.time() * 1000))
        except (KeyError, TypeError, ValueError):
            return
        notional = round(qty * price, 2)
        self.buf[sym].append({
            "t": t, "side": side, "qty": qty, "price": price, "notional": notional
        })
        self.total += 1
        self._unflushed += 1

        # Periodically persist every 15 items or 10 seconds
        now = time.time() * 1000
        if self._unflushed >= 15 or (now - self._last_save_ts > 10_000 and self._unflushed > 0):
            self.save_to_disk()

    def summarize(self, symbol, start_ms, end_ms=None):
        """Aggregate liquidations for `symbol` in [start_ms, end_ms]."""
        d = self.buf.get(symbol)
        if not d:
            return {"count": 0, "notional": 0.0, "long_liq": 0.0, "short_liq": 0.0, "net_side": "NONE"}
        end_ms = end_ms or int(time.time() * 1000)
        long_liq = short_liq = 0.0
        count = 0
        for x in d:
            if start_ms <= x["t"] <= end_ms:
                count += 1
                if x["side"] == "SELL":
                    long_liq += x["notional"]     # longs getting liquidated
                else:
                    short_liq += x["notional"]    # shorts getting liquidated
        notional = long_liq + short_liq
        net = "NONE"
        if notional > 0:
            net = "LONGS_LIQUIDATED" if long_liq >= short_liq else "SHORTS_LIQUIDATED"
        return {
            "count": count,
            "notional": round(notional, 2),
            "long_liq": round(long_liq, 2),
            "short_liq": round(short_liq, 2),
            "net_side": net,
        }

    def recent_list(self, symbol, start_ms):
        d = self.buf.get(symbol)
        if not d:
            return []
        return [x for x in d if x["t"] >= start_ms]

    async def run(self):
        """Background task: keep the websocket alive, reconnecting with backoff."""
        urls = [PRIMARY_WS_URL, FALLBACK_WS_URL]
        url_idx = 0
        backoff = 1.0

        while not self._stop:
            url = urls[url_idx % len(urls)]
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(url, heartbeat=20, timeout=aiohttp.ClientTimeout(total=30)) as ws:
                        self.connected = True
                        backoff = 1.0
                        self.log(f"[liq] connected to Binance liquidation stream ({url})")
                        async for msg in ws:
                            if self._stop:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = msg.json()
                                    # Support both direct stream and combined stream structures
                                    inner = data.get("data", data) if isinstance(data, dict) else None
                                    o = inner.get("o") if isinstance(inner, dict) else None
                                    if o:
                                        self._add(o)
                                except Exception:
                                    continue
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                self.log(f"[liq] stream reconnecting ({e})")
                url_idx += 1
            self.connected = False
            if self._stop:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 20.0)

        self.save_to_disk()

    def stop(self):
        self._stop = True
        self.save_to_disk()
