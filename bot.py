#!/usr/bin/env python3
"""
ZUBBU_SCALPER_V4.4_VELOCITY_GUARD – Full bot.py

Core Logic:
-----------
- Bybit v5 public WS (orderbook.1 + publicTrade) – linear USDT perp
- Robust orderbook parser (new compact format + legacy)
- Orderflow engine:
    * imbalance (top 5 levels)
    * micro burst (last 0.35s trades)
    * accumulation burst (last 1.5s trades)
    * burst_slope (acceleration of accum burst)
    * score = |imbalance| * |accum_burst| / spread
- Velocity confirmation (enough trades in window)
- Continuation filter (short-term price direction)
- BTC volatility guard (pause entries during BTC shock)
- 1m structure filter (support/resistance zones)
- 5m trend filter (up / down / flat)
- Single open position at a time (best symbol chosen)
- FIXED TP = +0.40%
- FIXED SL = -0.30% (hard stop via watchdog)
- Detailed skip logs (rate-limited) so you can see WHY it skipped
"""

import os
import time
import json
import math
import asyncio
import threading
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
import ccxt  # sync ccxt, we wrap blocking calls with asyncio.to_thread

# ---------------- VERSION ----------------
BOT_VERSION = "ZUBBU_SCALPER_V4.4_VELOCITY_GUARD"

# ---------------- ENV / BASIC CONFIG ----------------

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("BYBIT_TESTNET", "0") in ("1", "true", "True")

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Missing BYBIT_API_KEY or BYBIT_API_SECRET env vars.")

# WebSocket symbols (Bybit format)
SYMBOLS_WS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]

# WebSocket ready flags (symbol -> bool)
ws_ready: Dict[str, bool] = {s: False for s in SYMBOLS_WS}

# ccxt unified symbols for USDT perpetual
SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

# --------------- CORRECT WEBSOCKET URL -----------------
if TESTNET:
    WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"
else:
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

# --------------- TRADING CONFIG -----------------

LEVERAGE = 3
EQUITY_USE_FRACTION = 0.95  # use up to 95% equity * leverage as position notional

# Fixed SL / TP (decimal, relative to entry)
SL_PCT = 0.0030   # 0.30% hard stop (watchdog)
TP_PCT = 0.0040   # 0.40% take profit

# --- Orderflow filters (balanced-aggressive, with accumulation) ---
SCORE_MIN        = 0.90    # impulse score threshold
IMBALANCE_THRESH = 0.06    # 6% imbalance
BURST_MICRO_MIN  = 0.030   # micro burst (0.35s) minimum
BURST_ACCUM_MIN  = 0.060   # accumulation burst minimum

# NEW: acceleration + velocity + BTC guard
BURST_SLOPE_MIN    = 0.020   # minimum burst acceleration
VEL_MIN_TRADES     = 8       # minimum trades in accumulation window
BTC_VOL_GUARD_PCT  = 0.004   # 0.4% BTC 1m move triggers guard

MAX_SPREAD       = 0.0012  # 0.12% max spread
MIN_RANGE_PCT    = 0.00020 # 0.012% minimum micro-range

# Market data timing
RECENT_TRADE_WINDOW = 0.35  # 350 ms micro burst window
ACCUM_BURST_WINDOW  = 1.5   # 1.5 second accumulation window (faster entries)
BOOK_STALE_SEC      = 6.0   # ignore orderbook older than 6 seconds

# rate-limit logs
LAST_SKIP_LOG: Dict[str, float] = {}
SKIP_LOG_COOLDOWN = 5.0  # seconds per (symbol,reason)

# Risk & heartbeat
KILL_SWITCH_DD      = 0.05   # 5% equity drawdown -> stop trading
HEARTBEAT_IDLE_SEC  = 1800   # 30 minutes idle heartbeat

MIN_QTY_MAP = {
    "BTCUSDT": 0.001,
    "ETHUSDT": 0.01,
    "BNBUSDT": 0.01,
    "SOLUSDT": 0.1,
    "DOGEUSDT": 5.0,
}

# --------------- TELEGRAM -----------------

_last_tg_ts = 0.0
TG_MIN_INTERVAL = 30.0   # at most 1 msg every 30s to avoid spam


async def send_telegram(msg: str):
    """Safe, rate-limited Telegram sender."""
    global _last_tg_ts
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    now = time.time()
    if now - _last_tg_ts < TG_MIN_INTERVAL:
        return
    _last_tg_ts = now
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3.0)
        ) as session:
            await session.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT_ID, "text": msg},
            )
    except Exception:
        # never let TG issues break the bot
        pass


# --------------- UTILS -----------------


def safe_float(x, default: Optional[float] = None) -> Optional[float]:
    """Convert to float or return default if None/invalid."""
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# --------------- EXCHANGE CLIENT (ccxt sync wrapped into async) -----------------

class ExchangeClient:
    """
    Thin async wrapper around ccxt.bybit (sync HTTP).
    All heavy calls are done in a thread (asyncio.to_thread) so WS stays smooth.
    """

    def __init__(self):
        cfg = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},  # USDT perpetual
        }
        if TESTNET:
            cfg["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }
        self.client = ccxt.bybit(cfg)

    # ---------------- LEVERAGE ----------------
    async def set_leverage(self, sym_ws: str, lev: int) -> None:
        symbol = SYMBOL_MAP[sym_ws]

        def _work():
            try:
                self.client.set_leverage(
                    lev, symbol, params={"category": "linear"}
                )
            except Exception:
                pass

        await asyncio.to_thread(_work)

    # ---------------- EQUITY (USDT + PNL) ----------------
    async def fetch_equity(self) -> float:
        def _work():
            bal = self.client.fetch_balance()
            total = safe_float(bal.get("USDT", {}).get("total"), 0.0) or 0.0
            upnl = 0.0
            try:
                positions = self.client.fetch_positions()
                for p in positions:
                    upnl += safe_float(p.get("unrealizedPnl"), 0.0) or 0.0
            except Exception:
                pass
            return total + upnl

        return await asyncio.to_thread(_work)

    # ---------------- GET POSITION SIZE (contracts) ----------------
    async def get_position_size(self, sym_ws: str) -> float:
        """Return open contracts qty for symbol (0.0 if flat)."""
        symbol = SYMBOL_MAP[sym_ws]

        def _work():
            try:
                pos = self.client.fetch_positions([symbol])
                for p in pos:
                    contracts = safe_float(p.get("contracts"), 0.0) or 0.0
                    return contracts
            except Exception:
                return 0.0
            return 0.0

        return await asyncio.to_thread(_work)

    # ---------------- MARKET CLOSE ----------------
    async def close_position_market(self, sym_ws: str):
        """Close any open position in given symbol at market."""
        symbol = SYMBOL_MAP[sym_ws]

        def _work():
            try:
                positions = self.client.fetch_positions([symbol])
            except Exception:
                return
            for p in positions:
                contracts = safe_float(p.get("contracts"), 0.0) or 0.0
                if contracts == 0:
                    continue
                side = "sell" if p.get("side") == "long" else "buy"
                params = {"category": "linear", "reduceOnly": True}
                try:
                    self.client.create_order(
                        symbol, "market", side, abs(contracts), None, params
                    )
                except Exception:
                    pass

        await asyncio.to_thread(_work)

    # ---------------- MARKET ORDER ----------------
    async def create_market_order(
        self, sym_ws: str, side: str, qty: float, reduce_only: bool = False
    ):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear"}
        if reduce_only:
            params["reduceOnly"] = True

        def _work():
            return self.client.create_order(symbol, "market", side, qty, None, params)

        return await asyncio.to_thread(_work)

    # ---------------- LIMIT ORDER ----------------
    async def create_limit_order(
        self,
        sym_ws: str,
        side: str,
        qty: float,
        price: float,
        reduce_only: bool = False,
        post_only: bool = False,
    ):
        symbol = SYMBOL_MAP[sym_ws]
        side = side.lower()
        params = {"category": "linear", "timeInForce": "GTC"}
        if reduce_only:
            params["reduceOnly"] = True
        if post_only:
            params["timeInForce"] = "PostOnly"

        def _work():
            return self.client.create_order(symbol, "limit", side, qty, price, params)

        return await asyncio.to_thread(_work)


# --------------- DATA STRUCTURES -----------------


@dataclass
class Position:
    symbol_ws: str
    side: str          # "buy" or "sell"
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    opened_ts: float


class MarketState:
    """
    Holds orderbook + trades for all symbols, updated from WS.
    """

    def __init__(self):
        self.books: Dict[str, dict] = {
            s: {"bids": {}, "asks": {}, "ts": 0.0} for s in SYMBOLS_WS
        }
        self.trades: Dict[str, deque] = {s: deque(maxlen=2000) for s in SYMBOLS_WS}
        self.last_signal_ts: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}
        # NEW: store last accumulation burst to compute acceleration (slope)
        self.last_burst: Dict[str, float] = {s: 0.0 for s in SYMBOLS_WS}

    # -------- ORDERBOOK UPDATE (Bybit V5 + legacy) --------
    def update_book(self, symbol: str, data: dict):
        """
        Correct parser for Bybit V5 "orderbook.1.<symbol>" stream.
        Supports:
          - NEW format: {"b": [...], "a": [...], "ts": ...}
          - DELTA where b/a contain qty 0 (delete) or qty>0 (update)
          - OLD format fallback: {"type": "snapshot"/"delta", "bids": [...], "asks": [...]}
        """
        book = self.books[symbol]

        # --- New V5 compact format ---
        if isinstance(data, dict) and ("b" in data or "a" in data):
            ts_raw = data.get("ts") or data.get("t") or (time.time() * 1000)
            book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

            # Snapshot: both sides present
            if "b" in data and "a" in data:
                book["bids"].clear()
                book["asks"].clear()
                for px, qty in data.get("b", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q:
                        book["bids"][p] = q
                for px, qty in data.get("a", []):
                    p = safe_float(px)
                    q = safe_float(qty)
                    if p and q:
                        book["asks"][p] = q
                return

            # Delta: only b or a
            if "b" in data:
                for px, qty in data["b"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q

            if "a" in data:
                for px, qty in data["a"]:
                    p = safe_float(px)
                    q = safe_float(qty)
                    if not p:
                        continue
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q

            return

        # --- Legacy format fallback ---
        ts_raw = data.get("ts")
        book["ts"] = safe_float(ts_raw, time.time() * 1000) / 1000.0

        typ = data.get("type")
        if typ == "snapshot":
            book["bids"].clear()
            book["asks"].clear()
            for px, qty in data.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p and q:
                    book["bids"][p] = q
            for px, qty in data.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p and q:
                    book["asks"][p] = q
            return

        # delta update for legacy
        for key in ("delete", "update", "insert"):
            part = data.get(key, {})
            for px, qty in part.get("bids", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p:
                    if q == 0:
                        book["bids"].pop(p, None)
                    else:
                        book["bids"][p] = q
            for px, qty in part.get("asks", []):
                p = safe_float(px)
                q = safe_float(qty)
                if p:
                    if q == 0:
                        book["asks"].pop(p, None)
                    else:
                        book["asks"][p] = q

    def add_trade(self, symbol: str, trade: dict):
        # trade: {price, size, side, ts}
        self.trades[symbol].append(trade)

    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None
        best_bid = max(book["bids"].keys())
        best_ask = min(book["asks"].keys())
        return best_bid, best_ask

    def compute_features(self, symbol: str) -> Optional[dict]:
        """
        Compute mid, spread, imbalance, micro burst, accumulation burst,
        burst_slope, trade_count, trade_velocity, micro-range.
        Returns None if book/trades are not usable.
        """
        book = self.books[symbol]
        if not book["bids"] or not book["asks"]:
            return None

        now = time.time()
        if now - book["ts"] > BOOK_STALE_SEC:
            return None

        # Top 5 levels imbalance
        bids_sorted = sorted(book["bids"].items(), key=lambda x: -x[0])[:5]
        asks_sorted = sorted(book["asks"].items(), key=lambda x: x[0])[:5]
        bid_vol = sum(q for _, q in bids_sorted)
        ask_vol = sum(q for _, q in asks_sorted)
        if bid_vol + ask_vol == 0:
            return None
        imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)

        best_bid = bids_sorted[0][0]
        best_ask = asks_sorted[0][0]
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None
        spread = (best_ask - best_bid) / mid

        # --- Burst engine: micro (0.35s) + accumulation (1.5s) ---
        cutoff_micro = now - RECENT_TRADE_WINDOW
        cutoff_accum = now - ACCUM_BURST_WINDOW

        micro_burst = 0.0
        accum_burst = 0.0
        recent_prices: List[float] = []
        trade_count = 0  # NEW: for velocity

        for t in reversed(self.trades[symbol]):
            ts = t["ts"]
            if ts < cutoff_accum:
                break
            side_mult = 1.0 if t["side"] == "buy" else -1.0
            sz = t["size"]

            accum_burst += side_mult * sz
            if ts >= cutoff_micro:
                micro_burst += side_mult * sz

            recent_prices.append(t["price"])
            trade_count += 1

        if not recent_prices:
            return None

        # micro-range for small volatility check (last ~ACCUM_BURST_WINDOW seconds)
        if len(recent_prices) >= 2:
            high = max(recent_prices)
            low = min(recent_prices)
            rng = (high - low) / mid if mid > 0 else 0.0
        else:
            rng = 0.0

        # NEW: burst acceleration (slope)
        prev_burst = self.last_burst.get(symbol, 0.0)
        burst_slope = accum_burst - prev_burst
        self.last_burst[symbol] = accum_burst

        # NEW: trade velocity
        window = max(ACCUM_BURST_WINDOW, 0.001)
        trade_velocity = trade_count / window

        return {
            "mid": mid,
            "spread": spread,
            "imbalance": imbalance,
            "burst": accum_burst,          # main burst used in score
            "burst_micro": micro_burst,    # confirmation burst
            "range_pct": rng,
            "burst_slope": burst_slope,    # acceleration
            "trade_count": trade_count,    # raw trades in window
            "trade_velocity": trade_velocity,  # trades/sec
        }


# =====================================================
# MOMENTUM (for logging / score filter)
# =====================================================

def compute_momentum_score(imbalance: float, burst: float, spread: float) -> float:
    """
    Simple impulse strength score.
    Higher = stronger push in one direction.
    """
    if spread <= 0:
        spread = 1e-6
    return abs(imbalance) * abs(burst) / spread


# --------------- CORE BOT LOGIC -----------------

class ScalperBot:
    def __init__(self, exchange: ExchangeClient, mkt: MarketState):
        self.exchange = exchange
        self.mkt = mkt

        self.position: Optional[Position] = None
        self.start_equity: Optional[float] = None
        self.last_trade_time: float = 0.0
        self.last_heartbeat_ts: float = 0.0

        self.price_1m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}
        self.price_5m: Dict[str, deque] = {s: deque() for s in SYMBOLS_WS}

        self.last_skip_log = LAST_SKIP_LOG
        self.last_btc_guard_ts: float = 0.0

    # -------------------------------------------------
    # helpers: structure + trend + logging
    # -------------------------------------------------
    def _update_price_buffers(self, sym: str, mid: float, now: float) -> None:
        buf1 = self.price_1m[sym]
        buf5 = self.price_5m[sym]

        buf1.append((now, mid))
        buf5.append((now, mid))

        cutoff1 = now - 60.0
        cutoff5 = now - 300.0

        while buf1 and buf1[0][0] < cutoff1:
            buf1.popleft()
        while buf5 and buf5[0][0] < cutoff5:
            buf5.popleft()

    def _get_1m_context(self, sym: str) -> Optional[dict]:
        buf = self.price_1m[sym]
        if len(buf) < 5:
            return None
        prices = [p for _, p in buf]
        high = max(prices)
        low = min(prices)
        rng = high - low
        if rng <= 0:
            return None

        last_price = prices[-1]
        pos_in_range = (last_price - low) / rng

        return {
            "high": high,
            "low": low,
            "range": rng,
            "pos": pos_in_range,
            "near_support": pos_in_range <= 0.2,
            "near_resistance": pos_in_range >= 0.8,
        }

    def _get_5m_trend(self, sym: str) -> Optional[dict]:
        buf = self.price_5m[sym]
        if len(buf) < 10:
            return None
        prices = [p for _, p in buf]
        first = prices[0]
        last = prices[-1]
        if first <= 0:
            return None
        change = (last - first) / first

        if change > 0.002:
            trend = "up"
        elif change < -0.002:
            trend = "down"
        else:
            trend = "flat"

        return {"trend": trend, "change": change}

    def _log_skip(self, sym: str, reason: str, feat: dict, extra: str = "") -> None:
        now = time.time()
        key = f"{sym}:{reason}"
        last = self.last_skip_log.get(key, 0.0)
        if now - last < SKIP_LOG_COOLDOWN:
            return

        self.last_skip_log[key] = now
        msg = (
            f"[SKIP] {sym} {reason} {extra}"
        )
        print(msg, flush=True)

    # -------------------------------------------------
    # lifecycle
    # -------------------------------------------------
    async def init_equity_and_leverage(self):
        eq = await self.exchange.fetch_equity()
        self.start_equity = eq
        print(f"[INIT] Equity: {eq:.2f} USDT — {BOT_VERSION}")
        await send_telegram(
            f"🟢 Bot started ({BOT_VERSION}). Equity: {eq:.2f} USDT. "
            f"Kill at {KILL_SWITCH_DD*100:.1f}% DD."
        )
        for s in SYMBOLS_WS:
            await self.exchange.set_leverage(s, LEVERAGE)

    async def maybe_kill_switch(self):
        if self.start_equity is None:
            return
        eq = await self.exchange.fetch_equity()
        if self.start_equity <= 0:
            return
        dd = (self.start_equity - eq) / self.start_equity
        if dd >= KILL_SWITCH_DD:
            if self.position:
                await self.exchange.close_position_market(self.position.symbol_ws)
                self.position = None
            raise SystemExit("Kill-switch triggered")

    # -------------------------------------------------
    # main decision loop
    # -------------------------------------------------
    async def eval_symbols_and_maybe_enter(self):

        if self.position is not None:
            return

        best_score = 0.0
        best_sym = None
        best_feat = None
        best_side = None
        now = time.time()

        # BTC Guard
        btc_buf = self.price_1m.get("BTCUSDT")
        if btc_buf and len(btc_buf) >= 2:
            p_first = btc_buf[0][1]
            p_last = btc_buf[-1][1]
            if p_first > 0:
                btc_change = abs(p_last - p_first) / p_first
                if btc_change > BTC_VOL_GUARD_PCT:
                    return

        for sym in SYMBOLS_WS:
            feat = self.mkt.compute_features(sym)
            if not feat:
                continue

            imb = feat["imbalance"]
            burst = feat["burst"]
            b_micro = feat["burst_micro"]
            spread = feat["spread"]
            rng = feat["range_pct"]
            mid = feat["mid"]
            burst_slope = feat.get("burst_slope", 0.0)
            trade_count = feat.get("trade_count", 0)

            self._update_price_buffers(sym, mid, now)
            ctx_1m = self._get_1m_context(sym)
            ctx_5m = self._get_5m_trend(sym)

            # Basic Filters
            if spread <= 0 or spread > MAX_SPREAD:
                continue
            if rng < MIN_RANGE_PCT:
                continue
            if abs(imb) < IMBALANCE_THRESH:
                continue
            if abs(burst) < BURST_ACCUM_MIN:
                continue
            if abs(b_micro) < BURST_MICRO_MIN:
                continue
            if trade_count < VEL_MIN_TRADES:
                continue
            if abs(burst_slope) < BURST_SLOPE_MIN:
                continue

            if imb > 0 and burst > 0 and b_micro > 0 and burst_slope > 0:
                side = "buy"
            elif imb < 0 and burst < 0 and b_micro < 0 and burst_slope < 0:
                side = "sell"
            else:
                continue

            # Continuation
            buf1 = self.price_1m[sym]
            if len(buf1) >= 3:
                p_old = buf1[-3][1]
                p_new = buf1[-1][1]
                if p_old > 0:
                    price_chg = (p_new - p_old) / p_old
                    if side == "buy" and price_chg <= 0:
                        continue
                    if side == "sell" and price_chg >= 0:
                        continue

            score = compute_momentum_score(imb, burst, spread)
            if score < SCORE_MIN:
                continue

            # 1m structure
            if ctx_1m:
                if side == "buy" and ctx_1m["near_resistance"]:
                    continue
                if side == "sell" and ctx_1m["near_support"]:
                    continue

            # 5m trend
            if ctx_5m:
                trend = ctx_5m["trend"]
                if trend == "up" and side == "sell":
                    if score < SCORE_MIN * 1.8:
                        continue
                if trend == "down" and side == "buy":
                    if score < SCORE_MIN * 1.8:
                        continue

            # ====================================================
            # TP ROOM CHECK (ADDED PATCH)
            # ====================================================
            if ctx_1m:
                if side == "buy":
                    distance_to_res = (ctx_1m["high"] - mid) / mid
                    if distance_to_res < (TP_PCT + 0.0062):
                        continue
                if side == "sell":
                    distance_to_sup = (mid - ctx_1m["low"]) / mid
                    if distance_to_sup < (TP_PCT + 0.0062):
                        continue
            # ====================================================

            if score > best_score:
                best_score = score
                best_sym = sym
                best_feat = feat
                best_side = side

        if not best_sym:
            return

        await self.open_position(best_sym, best_feat, best_side)        
    # -------------------------------------------------
    # order execution
    # -------------------------------------------------
    async def open_position(self, sym_ws: str, feat: dict, side: str):
        """
        Open a new position with FIXED TP and FIXED SL (watchdog-based SL).
        """
        try:
            equity = await self.exchange.fetch_equity()
        except Exception:
            return
        if equity <= 0:
            return

        mid = feat["mid"]

        notional = equity * EQUITY_USE_FRACTION * LEVERAGE
        if mid <= 0:
            return
        qty = notional / mid

        min_qty = MIN_QTY_MAP.get(sym_ws, 0.0)
        if qty < min_qty:
            print(f"[SKIP ENTRY] {sym_ws} qty {qty:.6f} < min {min_qty}", flush=True)
            return

        # round qty down to 3 decimals
        qty = math.floor(qty * 1000) / 1000.0
        if qty <= 0:
            return

        # ENTRY: market order
        try:
            order = await self.exchange.create_market_order(
                sym_ws, side, qty, reduce_only=False
            )
            entry_price = safe_float(
                order.get("average") or order.get("price"), mid
            ) or mid
        except Exception as e:
            print(f"[ENTRY ERROR] {sym_ws}: {e}")
            await send_telegram(f"❌ Entry failed for {sym_ws}")
            return

        # recompute TP / SL from actual entry
        if side == "buy":
            tp_price = entry_price * (1.0 + TP_PCT)
            sl_price = entry_price * (1.0 - SL_PCT)
        else:
            tp_price = entry_price * (1.0 - TP_PCT)
            sl_price = entry_price * (1.0 + SL_PCT)

        # TP as limit reduce-only maker
        opp_side = "sell" if side == "buy" else "buy"
        try:
            await self.exchange.create_limit_order(
                sym_ws,
                opp_side,
                qty,
                tp_price,
                reduce_only=True,
                post_only=True,
            )
        except Exception as e:
            print(f"[TP ERROR] {sym_ws}: {e}")
            await send_telegram(f"⚠️ TP order placement failed for {sym_ws}")

        self.position = Position(
            symbol_ws=sym_ws,
            side=side,
            qty=qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            opened_ts=time.time(),
        )
        self.last_trade_time = time.time()
        print(
            f"[ENTRY] {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL≈{sl_price:.4f}"
        )
        await send_telegram(
            f"📌 ENTRY {sym_ws} {side.upper()} qty={qty} entry={entry_price:.4f} "
            f"TP={tp_price:.4f} SL≈{sl_price:.4f}"
        )

    # -------------------------------------------------
    # risk watchdog: detect TP/SL hits + backup SL
    # -------------------------------------------------
    async def watchdog_position(self):
        """
        - Detect if exchange closed position (TP hit or SL hit)
        - If still open, apply backup SL check
        """
        if not self.position:
            return

        pos = self.position
        sym = pos.symbol_ws

        # ========== 1) CHECK IF POSITION SIZE OPEN OR CLOSED ==========
        size = await self.exchange.get_position_size(sym)

        # If exchange shows no open position -> TP or SL executed
        if size <= 0:
            # Recompute last move for reason calculation
            feat = self.mkt.compute_features(sym)
            reason = "unknown"

            if feat:
                mid = feat["mid"]
                if pos.side == "buy":
                    move = (mid - pos.entry_price) / pos.entry_price
                else:
                    move = (pos.entry_price - mid) / pos.entry_price

                # Profit or loss detection
                if move > 0:
                    reason = "TP filled (profit)"
                else:
                    reason = "SL filled (loss)"

            # -------- LOG & TELEGRAM --------
            print(
                f"[EXIT DETECTED] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"tp={pos.tp_price:.4f} sl={pos.sl_price:.4f} reason={reason}"
            )
            await send_telegram(
                f"📤 EXIT — {sym} {pos.side.upper()} entry={pos.entry_price:.4f}\n"
                f"➡️ {reason}"
            )

            # CLEAR POSITION
            self.position = None
            return

        # ========== 2) IF STILL OPEN, GET CURRENT PRICE ==========
        feat = self.mkt.compute_features(sym)
        if not feat:
            return
        mid = feat["mid"]
        if mid <= 0:
            return

        # ========== 3) BACKUP SL CHECK ==========
        hit = False

        # long backup SL
        if pos.side == "buy" and mid <= pos.sl_price:
            hit = True

        # short backup SL
        if pos.side == "sell" and mid >= pos.sl_price:
            hit = True

        if hit:
            # Force close market
            await self.exchange.close_position_market(sym)

            print(
                f"[BACKUP SL] {sym} {pos.side.upper()} entry={pos.entry_price:.4f} "
                f"SL={pos.sl_price:.4f} now={mid:.4f}"
            )
            await send_telegram(
                f"🛑 BACKUP SL — {sym} {pos.side.upper()}\n"
                f"Entry={pos.entry_price:.4f}  SL={pos.sl_price:.4f}"
            )

            self.position = None
            return

    # -------------------------------------------------
    # idle heartbeat
    # -------------------------------------------------
    async def maybe_heartbeat(self):
        """
        Idle heartbeat every HEARTBEAT_IDLE_SEC when no trades.
        """
        now = time.time()
        if now - self.last_trade_time < HEARTBEAT_IDLE_SEC:
            return
        if now - self.last_heartbeat_ts < HEARTBEAT_IDLE_SEC:
            return
        self.last_heartbeat_ts = now
        eq = await self.exchange.fetch_equity()
        print(f"[HEARTBEAT] idle, equity={eq:.2f}")
        await send_telegram(
            f"💤 Bot idle {HEARTBEAT_IDLE_SEC/60:.0f} min. Equity={eq:.2f} USDT"
        )


# --------------- WEBSOCKET LOOP -----------------


async def ws_loop(mkt: MarketState):
    """
    One WS connection, multiple topics: orderbook.1 + publicTrade for all symbols.
    Updates MarketState in real-time.
    """

    # Correct topics
    topics = []
    for s in SYMBOLS_WS:
        topics.append(f"orderbook.1.{s}")
        topics.append(f"publicTrade.{s}")

    print("⚡ WS loop started... connecting...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    WS_URL,
                    receive_timeout=40,
                    heartbeat=20
                ) as ws:

                    print("📡 Connected to WS server, subscribing...")
                    await ws.send_json({
                        "op": "subscribe",
                        "args": topics
                    })

                    # Mark symbols as connected
                    for sym in SYMBOLS_WS:
                        ws_ready[sym] = True

                    await send_telegram("📡 WS Connected: All symbols")

                    async for msg in ws:

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                            # Ignore subscription confirmation
                            if data.get("success") is True:
                                continue

                            topic = data.get("topic")
                            if not topic:
                                continue

                            # -------- ORDERBOOK ----------
                            if topic.startswith("orderbook"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue
                                # Bybit sends array sometimes
                                if isinstance(payload, list):
                                    payload = payload[0]
                                if isinstance(payload, dict):
                                    mkt.update_book(sym, payload)

                            # -------- TRADES ----------
                            elif topic.startswith("publicTrade"):
                                sym = topic.split(".")[-1]
                                payload = data.get("data")
                                if not payload:
                                    continue

                                trades = payload if isinstance(payload, list) else [payload]
                                now_ts = time.time()

                                for t in trades:
                                    price = safe_float(t.get("p") or t.get("price"))
                                    qty = safe_float(t.get("v") or t.get("q") or t.get("size"))
                                    side = (t.get("S") or t.get("side") or "Buy").lower()

                                    if price is None or qty is None:
                                        continue

                                    mkt.add_trade(sym, {
                                        "price": price,
                                        "size": qty,
                                        "side": "buy" if side == "buy" else "sell",
                                        "ts": now_ts
                                    })

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("⚠ WS ERROR — reconnecting")
                            break

        except Exception as e:
            print(f"❌ WS Loop Crashed: {e}")
            for s in SYMBOLS_WS:
                ws_ready[s] = False
            await asyncio.sleep(1)


# --------------- DEBUG CONSOLE (runs in separate thread) -----------------


def debug_console(mkt: MarketState, bot: ScalperBot):
    """
    Simple blocking console running in a daemon thread.
    You can type commands directly in tmux where bot is running.

    Commands:
      ws      -> show websocket state
      book    -> show last orderbook timestamps
      trades  -> show # of recent trades per symbol
      pos     -> show current open position
      help    -> show commands
    """
    help_text = (
        "[DEBUG] Commands:\n"
        "  ws      - show websocket ready flags\n"
        "  book    - show last orderbook timestamps\n"
        "  trades  - show count of recent trades per symbol\n"
        "  pos     - show current open position\n"
        "  help    - show this message\n"
    )
    print(help_text, flush=True)
    while True:
        try:
            cmd = input("")
        except EOFError:
            # stdin closed, just stop thread
            return
        cmd = cmd.strip().lower()
        if cmd == "ws":
            print("[DEBUG] ws_ready =", ws_ready, flush=True)
        elif cmd == "book":
            ts_map = {s: mkt.books[s]["ts"] for s in SYMBOLS_WS}
            print("[DEBUG] book ts =", ts_map, flush=True)
        elif cmd == "trades":
            counts = {s: len(mkt.trades[s]) for s in SYMBOLS_WS}
            print("[DEBUG] trades len =", counts, flush=True)
        elif cmd == "pos":
            print("[DEBUG] position =", bot.position, flush=True)
        elif cmd == "help":
            print(help_text, flush=True)
        elif cmd == "":
            continue
        else:
            print("[DEBUG] Unknown cmd. Type 'help' for list.", flush=True)


# --------------- MAIN LOOP -----------------


async def main():
    print(f"Starting {BOT_VERSION} ...")
    exchange = ExchangeClient()
    mkt = MarketState()
    bot = ScalperBot(exchange, mkt)

    # start debug console thread
    threading.Thread(
        target=debug_console, args=(mkt, bot), daemon=True
    ).start()

    await bot.init_equity_and_leverage()

    ws_task = asyncio.create_task(ws_loop(mkt))

    try:
        while True:
            await bot.maybe_kill_switch()
            await bot.eval_symbols_and_maybe_enter()
            await bot.watchdog_position()
            await asyncio.sleep(1.0)  # 1-second scan loop
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except Exception:
            pass


if __name__ == "__main__":
    try:
        import uvloop  # optional, for speed

        uvloop.install()
    except Exception:
        pass
    asyncio.run(main())
