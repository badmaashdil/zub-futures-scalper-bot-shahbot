import os
import sys
import time
import json
import logging
import threading
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional, List

import ccxt
import websocket
import requests

# ================================================================
# CONFIG
# ================================================================

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")

if not API_KEY or not API_SECRET:
    raise Exception("API keys missing. Please set BYBIT_API_KEY and BYBIT_API_SECRET.")

# True = testnet, False = live
TESTNET = False

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]

CCXT_SYMBOL_MAP = {
    "BTCUSDT": "BTC/USDT:USDT",
    "ETHUSDT": "ETH/USDT:USDT",
    "SOLUSDT": "SOL/USDT:USDT",
    "BNBUSDT": "BNB/USDT:USDT",
    "DOGEUSDT": "DOGE/USDT:USDT",
}

PUBLIC_WS_MAINNET = "wss://stream.bybit.com/v5/public/linear"
PUBLIC_WS_TESTNET = "wss://stream-testnet.bybit.com/v5/public/linear"

SCAN_INTERVAL = 5.0               # seconds between decision scans
LEVERAGE = 3
MARGIN_FRACTION = 0.95

TP_PCT_ON_POSITION = 0.01         # 1% TP
SL_PCT_ON_POSITION = 0.005        # 0.5% SL

GLOBAL_KILL_TRIGGER = 0.05        # 5% equity loss
STARTING_EQUITY: Optional[float] = None
bot_killed: bool = False
kill_alert_sent: bool = False     # only one Telegram alert for kill-switch

MAX_CONCURRENT_POSITIONS = 1

POST_ONLY_TIMEOUT = 3.0
VOL_MOVE_PCT_1S = 0.004
VOL_MOVE_PCT_3S = 0.008

IMBALANCE_LEVELS = 5
IMBALANCE_THRESHOLD = 0.05

STALE_OB_MAX_SEC = 2.0
LATENCY_MAX_SEC = 1.5
MIN_SL_DIST_PCT = 0.0005
MIN_TRADE_INTERVAL_SEC = 5.0
SPREAD_MAX_PCT = 0.001
ERROR_PAUSE_SEC = 10.0

# WebSocket safety: if no messages for this many seconds, treat as dead
WS_SILENCE_SEC = 3.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("scalper_bot")


def now_ts() -> float:
    return time.time()


# ================================================================
# TELEGRAM (NO SPAM)
# ================================================================

TELEGRAM_TOKEN = os.getenv("TG_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TG_CHAT_ID")

_last_tg_time: float = 0.0
_last_tg_text: str = ""
_last_tg_text_time: float = 0.0

TELEGRAM_MIN_INTERVAL_SEC = 10.0
TELEGRAM_DUP_SUPPRESS_SEC = 30.0
TELEGRAM_HTTP_TIMEOUT = 3.0


def tg(msg: str) -> None:
    """
    Telegram helper:
    - Skips if no token/chat.
    - Rate-limited.
    - Duplicate-suppressed.
    - Never crashes the bot.
    """
    global _last_tg_time, _last_tg_text, _last_tg_text_time

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    now = now_ts()

    if now - _last_tg_time < TELEGRAM_MIN_INTERVAL_SEC:
        return

    if msg == _last_tg_text and (now - _last_tg_text_time) < TELEGRAM_DUP_SUPPRESS_SEC:
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
        requests.post(url, data=payload, timeout=TELEGRAM_HTTP_TIMEOUT)
        _last_tg_time = now
        _last_tg_text = msg
        _last_tg_text_time = now
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


# ================================================================
# HEARTBEAT (EVERY 10 MIN)
# ================================================================

def heartbeat_loop() -> None:
    while True:
        tg("💓 Bot running (heartbeat)")
        time.sleep(600)  # 10 minutes


def start_heartbeat() -> None:
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()


# ================================================================
# POSITION STRUCT
# ================================================================

@dataclass
class Position:
    symbol: str
    side: str
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    notional: float
    ts_open: float = field(default_factory=now_ts)


# ================================================================
# PnL & POSITION SIZING
# ================================================================

def calc_pnl(side: str, entry: float, exit: float, qty: float) -> float:
    if side.lower() == "buy":
        return round((exit - entry) * qty, 4)
    else:
        return round((entry - exit) * qty, 4)


def compute_position_and_prices(
    equity_usd: float,
    entry_price: float,
    side: str,
) -> Tuple[float, float, float, float]:
    if equity_usd <= 0 or entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0

    notional = equity_usd * MARGIN_FRACTION * LEVERAGE
    qty = notional / entry_price
    qty = float(f"{qty:.6f}")

    if side == "buy":
        tp = entry_price * (1.0 + TP_PCT_ON_POSITION)
        sl = entry_price * (1.0 - SL_PCT_ON_POSITION)
    else:
        tp = entry_price * (1.0 - TP_PCT_ON_POSITION)
        sl = entry_price * (1.0 + SL_PCT_ON_POSITION)

    return qty, notional, tp, sl


# ================================================================
# EXCHANGE CLIENT
# ================================================================

class ExchangeClient:
    def __init__(self, key: str, secret: str, testnet: bool = True):
        cfg: Dict[str, Any] = {
            "apiKey": key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if testnet:
            cfg["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }
        self.client = ccxt.bybit(cfg)
        self.lock = threading.Lock()

    def _sym(self, s: str) -> str:
        return CCXT_SYMBOL_MAP[s]

    def get_balance(self) -> float:
        with self.lock:
            bal = self.client.fetch_balance()
        usdt = bal.get("USDT", {})
        return float(usdt.get("total", 0.0))

    def set_leverage(self, symbol: str, lev: int) -> None:
        try:
            with self.lock:
                self.client.set_leverage(lev, self._sym(symbol))
        except Exception as e:
            logger.warning(f"{symbol}: leverage set failed: {e}")

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            with self.lock:
                p = self.client.fetch_positions([self._sym(symbol)])
        except Exception as e:
            logger.warning(f"{symbol}: fetch_positions error: {e}")
            return None
        for x in p:
            size = float(x.get("contracts") or 0.0)
            if size != 0:
                return x
        return None

    def place_limit(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        reduce: bool = False,
        post: bool = True,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if reduce:
            params["reduce_only"] = True
        if post:
            params["timeInForce"] = "PostOnly"
        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "limit",
                side.lower(),
                qty,
                price,
                params,
            )

    def place_market(
        self,
        symbol: str,
        side: str,
        qty: float,
        reduce: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if reduce:
            params["reduce_only"] = True
        with self.lock:
            # price must be None for market orders in ccxt
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def place_stop_market(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> Dict[str, Any]:
        # Bybit stop-loss via ccxt: market with stopLossPrice param
        params: Dict[str, Any] = {
            "stopLossPrice": stop_price,
            "reduce_only": True,
        }
        with self.lock:
            return self.client.create_order(
                self._sym(symbol),
                "market",
                side.lower(),
                qty,
                None,
                params,
            )

    def order_status(self, symbol: str, oid: Optional[str]) -> Dict[str, Any]:
        if not oid:
            return {}
        try:
            with self.lock:
                o = self.client.fetch_order(oid, self._sym(symbol))
            return {
                "status": (o.get("status") or "").lower(),
                "avg_price": o.get("average") or o.get("price"),
                "amount": o.get("amount"),
            }
        except Exception as e:
            logger.warning(f"{symbol}: fetch_order error: {e}")
            return {}

    def cancel(self, symbol: str, oid: str) -> bool:
        try:
            with self.lock:
                self.client.cancel_order(oid, self._sym(symbol))
            return True
        except Exception as e:
            logger.warning(f"{symbol}: cancel_order error: {e}")
            return False

    def close_market(self, symbol: str) -> None:
        p = self.get_position(symbol)
        if not p:
            return
        size = float(p.get("contracts") or 0.0)
        if size == 0:
            return
        side = (p.get("side") or "").lower()
        close_side = "sell" if side == "long" else "buy"
        with self.lock:
            try:
                self.client.create_order(
                    self._sym(symbol),
                    "market",
                    close_side,
                    abs(size),
                    None,
                    {"reduce_only": True},
                )
                logger.warning(f"{symbol}: emergency market close size={size}")
            except Exception as e:
                logger.error(f"{symbol}: emergency close failed: {e}")


# ================================================================
# EMERGENCY CLOSE ALL
# ================================================================

def emergency_close_all(exchange: ExchangeClient, symbols: List[str]) -> None:
    for s in symbols:
        try:
            exchange.close_market(s)
        except Exception as e:
            logger.error(f"{s}: emergency close error: {e}")


# ================================================================
# SPOOF / WHALE DETECTORS
# ================================================================

class SpoofDetector:
    def __init__(self, window: float = 1.0, repeat: int = 3):
        self.window = window
        self.repeat = repeat
        self.data: Dict[Tuple[str, float], deque] = defaultdict(deque)

    def on_event(self, side: str, price: float, etype: str, ts: float) -> None:
        k = (side, float(price))
        dq = self.data[k]
        dq.append((etype, ts))
        while dq and ts - dq[0][1] > self.window:
            dq.popleft()

    def check(self, side: str, price: float) -> bool:
        dq = self.data.get((side, float(price)), deque())
        cancels = sum(1 for x, _ in dq if x == "cancel")
        return cancels >= self.repeat


class WhaleCancelDetector:
    def __init__(self, th: float, window: float = 5.0):
        self.th = th
        self.window = window
        self.data: Dict[float, deque] = defaultdict(deque)

    def on_event(self, price: float, size: float, etype: str, ts: float) -> None:
        if size < self.th:
            return
        dq = self.data[float(price)]
        dq.append((etype, ts, size))
        while dq and ts - dq[0][1] > self.window:
            dq.popleft()

    def check(self, price: float) -> bool:
        dq = self.data.get(float(price), deque())
        if not dq:
            return False
        cancels = sum(1 for t, _, _ in dq if t == "cancel")
        adds = sum(1 for t, _, _ in dq if t == "new")
        if adds + cancels == 0:
            return False
        return (cancels / (adds + cancels)) > 0.3


# ================================================================
# MICRO FILTER HELPERS
# ================================================================

def compute_imbalance(book: Dict[str, Dict[float, float]]) -> float:
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:IMBALANCE_LEVELS]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:IMBALANCE_LEVELS]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    tot = bv + av
    if tot == 0:
        return 0.0
    return (bv - av) / tot


def micro_delta(book: Dict[str, Dict[float, float]]) -> float:
    bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:2]
    asks = sorted(book["asks"].items(), key=lambda x: x[0])[:2]
    bv = sum(q for _, q in bids)
    av = sum(q for _, q in asks)
    if bv + av == 0:
        return 0.0
    return (bv - av) / (bv + av)


def micro_burst(trades: List[Dict[str, Any]], window_sec: float = 0.15) -> float:
    if not trades:
        return 0.0
    now = trades[-1]["ts"]
    b = 0.0
    for t in reversed(trades):
        if now - t["ts"] > window_sec:
            break
        if t["side"] == "buy":
            b += t["size"]
        else:
            b -= t["size"]
    return b


def liquidity_gap(book: Dict[str, Dict[float, float]], th: float = 0.15) -> bool:
    bids = sum(x for _, x in sorted(book["bids"].items(), reverse=True)[:5])
    asks = sum(x for _, x in sorted(book["asks"].items())[:5])
    if bids == 0 or asks == 0:
        return True
    return bids < th * asks or asks < th * bids


def unpredictable(prices: List[Tuple[float, float]]) -> bool:
    if len(prices) < 3:
        return False
    t0, p0 = prices[-1]
    p1s = None
    p3s = None
    for ts, p in reversed(prices):
        dt = t0 - ts
        if p1s is None and dt >= 1.0:
            p1s = p
        if p3s is None and dt >= 3.0:
            p3s = p
        if p1s and p3s:
            break
    if p1s and abs(p0 - p1s) / p1s >= VOL_MOVE_PCT_1S:
        return True
    if p3s and abs(p0 - p3s) / p3s >= VOL_MOVE_PCT_3S:
        return True
    return False


# ================================================================
# DECISION ENGINE
# ================================================================

class DecisionEngine:
    def __init__(self, exchange: ExchangeClient, symbols: List[str]):
        self.exchange = exchange
        self.symbols = symbols
        self.open_positions: Dict[str, Position] = {}
        self.spoof = SpoofDetector()
        self.whale = {s: WhaleCancelDetector(1.0) for s in symbols}
        self.pause_until = 0.0
        self.last_trade_ts = 0.0
        self.lock = threading.Lock()

    def pause(self, sec: float) -> None:
        self.pause_until = max(self.pause_until, now_ts() + sec)
        logger.warning(f"Engine paused for {sec:.1f}s")

    def is_paused(self) -> bool:
        return now_ts() < self.pause_until

    def check_kill(self, equity: float) -> bool:
        global STARTING_EQUITY, bot_killed, kill_alert_sent

        if bot_killed:
            return True

        if STARTING_EQUITY is None:
            STARTING_EQUITY = equity
            logger.info(f"Recorded STARTING_EQUITY={STARTING_EQUITY:.4f}")
            return False

        loss_pct = (STARTING_EQUITY - equity) / STARTING_EQUITY if STARTING_EQUITY > 0 else 0.0
        if loss_pct >= GLOBAL_KILL_TRIGGER:
            bot_killed = True
            if not kill_alert_sent:
                tg(f"❌ GLOBAL KILL SWITCH TRIGGERED\nEquity loss={loss_pct*100:.2f}%")
                kill_alert_sent = True
            logger.error("Global kill-switch active, closing all positions & pausing.")
            emergency_close_all(self.exchange, self.symbols)
            self.pause(99999999)
            return True

        return False

    def on_close(self, symbol: str, exit_price: float) -> None:
        pos = self.open_positions.get(symbol)
        if not pos:
            return
        del self.open_positions[symbol]

        dist_tp = abs(exit_price - pos.tp_price)
        dist_sl = abs(exit_price - pos.sl_price)
        hit = "TP" if dist_tp < dist_sl else "SL"

        pnl = calc_pnl(pos.side, pos.entry_price, exit_price, pos.qty)
        tg(f"📤 EXIT | {symbol}\nReason: {hit}\nPnL: {pnl} USDT")

    def evaluate(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        c5: List[Dict[str, Any]],
        c1: List[Dict[str, Any]],
        prices: List[Tuple[float, float]],
        last_ob_ts: float,
    ) -> None:
        with self.lock:
            self._evaluate_locked(symbol, book, trades, c5, c1, prices, last_ob_ts)

    def _evaluate_locked(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        c5: List[Dict[str, Any]],
        c1: List[Dict[str, Any]],
        prices: List[Tuple[float, float]],
        last_ob_ts: float,
    ) -> None:
        now = now_ts()
        equity = self.exchange.get_balance()

        if self.check_kill(equity):
            return

        if self.is_paused():
            return

        if self.last_trade_ts and (now - self.last_trade_ts) < MIN_TRADE_INTERVAL_SEC:
            return

        if now - last_ob_ts > STALE_OB_MAX_SEC:
            logger.warning(f"{symbol}: stale orderbook, pausing briefly.")
            self.pause(2.0)
            return

        if unpredictable(prices):
            logger.warning(f"{symbol}: unpredictable micro-move, short pause.")
            self.pause(5.0)
            return

        if len(c1) < 5 or len(trades) < 10:
            return

        last = c1[-1]
        rng = last["high"] - last["low"]
        if last["close"] == 0:
            return
        if (rng / last["close"]) < 0.002:
            return

        bids = book["bids"]
        asks = book["asks"]
        if not bids or not asks:
            return

        best_bid = max(bids)
        best_ask = min(asks)
        mid = (best_bid + best_ask) / 2.0
        spread = (best_ask - best_bid) / mid
        if spread > SPREAD_MAX_PCT:
            return

        md = micro_delta(book)
        if abs(md) < 0.05:
            return

        tb = micro_burst(trades)
        if abs(tb) < 0.5:
            return

        if liquidity_gap(book):
            return

        imb = compute_imbalance(book)
        if abs(imb) < IMBALANCE_THRESHOLD:
            return

        if self.spoof.check("bid", best_bid) or self.spoof.check("ask", best_ask):
            return
        if self.whale[symbol].check(best_bid):
            return

        direction = 1 if tb > 0 else -1
        side = "Buy" if direction == 1 else "Sell"

        qty, notional, tp, sl = compute_position_and_prices(equity, mid, side.lower())
        if qty <= 0 or notional <= 0:
            return

        sl_dist = abs(mid - sl) / mid
        if sl_dist < MIN_SL_DIST_PCT:
            return

        # ENTRY
        try:
            entry = self.exchange.place_limit(symbol, side, mid, qty, reduce=False, post=True)
            oid = entry.get("id")
            start_t = now_ts()
            filled = False
            fill_price = mid

            while now_ts() - start_t < POST_ONLY_TIMEOUT:
                st = self.exchange.order_status(symbol, oid)
                status = st.get("status", "")
                if status in ("closed", "filled"):
                    filled = True
                    fill_price = float(st.get("avg_price", mid))
                    break
                time.sleep(0.1)

            if not filled:
                if oid:
                    self.exchange.cancel(symbol, oid)
                mkt = self.exchange.place_market(symbol, side, qty, reduce=False)
                fill_price = float(mkt.get("average", mid))
        except Exception as e:
            logger.exception(f"{symbol}: entry failed: {e}")
            tg(f"❌ Entry failed {symbol}: {e}")
            self.pause(ERROR_PAUSE_SEC)
            return

        # TP & SL
        try:
            reduce_side = "Sell" if side == "Buy" else "Buy"
            self.exchange.place_limit(symbol, reduce_side, tp, qty, reduce=True, post=False)
            self.exchange.place_stop_market(symbol, reduce_side, qty, sl)
        except Exception as e:
            logger.exception(f"{symbol}: TP/SL placement failed: {e}")
            tg(f"❌ TP/SL failed {symbol}: {e}")
            self.exchange.close_market(symbol)
            self.pause(ERROR_PAUSE_SEC)
            return

        pos = Position(symbol, side, qty, fill_price, tp, sl, notional)
        self.open_positions[symbol] = pos
        self.last_trade_ts = now_ts()
        tg(f"📌 ENTRY | {symbol}\nSide: {side}\nEntry: {fill_price}\nTP: {tp}\nSL: {sl}")


# ================================================================
# MARKET WORKER (WEBSOCKET + WS SAFETY)
# ================================================================

class MarketWorker(threading.Thread):
    def __init__(self, symbol: str, engine: DecisionEngine, exchange: ExchangeClient, testnet: bool):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.engine = engine
        self.exchange = exchange
        self.testnet = testnet

        self.ws: Optional[websocket.WebSocketApp] = None
        self.running: bool = True

        self.book: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.trades: deque = deque(maxlen=500)
        self.c1: deque = deque(maxlen=500)
        self.c5: deque = deque(maxlen=500)
        self.price_samples: deque = deque(maxlen=200)
        self.last_ob_ts: float = 0.0
        self.last_eval: float = 0.0

        # WebSocket health tracking
        self.last_ws_msg_ts: float = now_ts()
        self.ws_alive: bool = True   # For transition detection (dead -> alive)

    def url(self) -> str:
        return PUBLIC_WS_TESTNET if self.testnet else PUBLIC_WS_MAINNET

    def run(self) -> None:
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.url(),
                    on_open=self.on_open,
                    on_close=self.on_close,
                    on_error=self.on_error,
                    on_message=self.on_message,
                )
                logger.info(f"{self.symbol}: connecting WebSocket...")
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                logger.exception(f"{self.symbol}: WebSocket run_forever error: {e}")
                tg(f"❌ WebSocket fatal error for {self.symbol}: {e}")
            if self.running:
                logger.info(f"{self.symbol}: reconnecting WebSocket in 3s...")
                time.sleep(3.0)

    def on_open(self, ws) -> None:
        logger.info(f"{self.symbol}: WebSocket opened")
        self.last_ws_msg_ts = now_ts()
        self.ws_alive = True
        try:
            sub = {
                "op": "subscribe",
                "args": [
                    f"orderbook.50.{self.symbol}",
                    f"publicTrade.{self.symbol}",
                ],
            }
            ws.send(json.dumps(sub))
        except Exception as e:
            logger.exception(f"{self.symbol}: error sending subscribe: {e}")

    def handle_ob(self, j: Dict[str, Any]) -> None:
    data = j.get("data", [])

    # ---- FIX: ignore empty or invalid orderbook messages ----
    if not isinstance(data, list) or len(data) == 0:
        return

    payload = data[0]
    if not isinstance(payload, dict):
        return

    typ = j.get("type", "snapshot")
    ts = now_ts()

    bids = self.book["bids"]
    asks = self.book["asks"]

    if typ == "snapshot":
        bids.clear()
        asks.clear()

        for p, s in payload.get("b", []):
            price = float(p)
            size = float(s)
            if size > 0:
                bids[price] = size

        for p, s in payload.get("a", []):
            price = float(p)
            size = float(s)
            if size > 0:
                asks[price] = size

    else:
        for p, s in payload.get("b", []):
            price = float(p)
            size = float(s)
            old = bids.get(price, 0.0)

            if size == 0.0:
                if old > 0.0:
                    self.engine.spoof.on_event("bid", price, "cancel", ts)
                    self.engine.whale[self.symbol].on_event(price, old, "cancel", ts)
                    bids.pop(price, None)
            else:
                if size > old:
                    self.engine.spoof.on_event("bid", price, "new", ts)
                    self.engine.whale[self.symbol].on_event(price, size, "new", ts)
                bids[price] = size

        for p, s in payload.get("a", []):
            price = float(p)
            size = float(s)
            old = asks.get(price, 0.0)

            if size == 0.0:
                if old > 0.0:
                    self.engine.spoof.on_event("ask", price, "cancel", ts)
                    self.engine.whale[self.symbol].on_event(price, old, "cancel", ts)
                    asks.pop(price, None)
            else:
                if size > old:
                    self.engine.spoof.on_event("ask", price, "new", ts)
                    self.engine.whale[self.symbol].on_event(price, size, "new", ts)
                asks[price] = size

    self.book["bids"] = {p: s for p, s in bids.items() if s > 0}
    self.book["asks"] = {p: s for p, s in asks.items() if s > 0}
    self.last_ob_ts = ts


    def handle_trades(self, j: Dict[str, Any]) -> None:
        data = j.get("data", [])
        for t in data:
            price = float(t.get("p") or 0.0)
            size = float(t.get("v") or 0.0)
            side = str(t.get("S") or "").lower()
            ts = float(t.get("T") or (now_ts() * 1000.0)) / 1000.0
            self.trades.append({"price": price, "size": size, "side": side, "ts": ts})
            self.price_samples.append((ts, price))
        self.update_candles()

    def update_candles(self) -> None:
        if not self.trades:
            return
        last_trade = self.trades[-1]
        price = last_trade["price"]
        ts = last_trade["ts"]
        bucket = int(ts // 60)

        if self.c1 and self.c1[-1]["bucket"] == bucket:
            c = self.c1[-1]
            c["close"] = price
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["volume"] += last_trade["size"]
        else:
            self.c1.append({
                "bucket": bucket,
                "open": price,
                "close": price,
                "high": price,
                "low": price,
                "volume": last_trade["size"],
            })

        if len(self.c1) >= 5 and (not self.c5 or len(self.c5) < len(self.c1) // 5):
            chunk = list(self.c1)[-5:]
            self.c5.append({
                "open": chunk[0]["open"],
                "high": max(c["high"] for c in chunk),
                "low": min(c["low"] for c in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(c["volume"] for c in chunk),
            })

    def maybe_eval(self) -> None:
        # 1) First, check if previously open position is now closed on exchange (normal TP/SL)
        pos = self.engine.open_positions.get(self.symbol)
        if pos:
            try:
                ex = self.exchange.get_position(self.symbol)
            except Exception:
                ex = None
            size = float(ex.get("contracts") or 0.0) if ex else 0.0
            if size == 0.0:
                bids = self.book["bids"]
                asks = self.book["asks"]
                if bids and asks:
                    exit_price = (max(bids) + min(asks)) / 2.0
                else:
                    exit_price = pos.tp_price
                self.engine.on_close(self.symbol, exit_price)

        # 2) WebSocket health rule:
        # If WS not giving messages for WS_SILENCE_SEC → treat as dead.
        silence = now_ts() - self.last_ws_msg_ts
        if silence > WS_SILENCE_SEC:
            if self.ws_alive:
                # Transition: was alive, now dead
                self.ws_alive = False
                logger.error(f"{self.symbol}: WebSocket silent for {silence:.2f}s → emergency close & pause.")
                tg(f"⚠️ WebSocket DEAD for {self.symbol} ({silence:.2f}s). Closing positions & pausing trades.")
                emergency_close_all(self.exchange, [self.symbol])
                # Hard pause engine so no trades without live WS data
                self.engine.pause(10.0)
            # Do NOT evaluate for new trades while WS dead
            return
        else:
            # If previously dead and now alive again → notify & resume
            if not self.ws_alive:
                self.ws_alive = True
                logger.info(f"{self.symbol}: WebSocket data restored, trading can resume.")
                tg(f"✅ WebSocket back for {self.symbol}. Trading resumed.")

        # 3) If orderbook or candles not ready → skip
        if len(self.trades) < 10:
            return
        if len(self.c1) < 5:
            return
        if not self.book["bids"] or not self.book["asks"]:
            return

        # 4) Normal strategy evaluation
        self.engine.evaluate(
            self.symbol,
            self.book,
            list(self.trades),
            list(self.c5),
            list(self.c1),
            list(self.price_samples),
            self.last_ob_ts,
        )


# ================================================================
# MAIN
# ================================================================

def main() -> None:
    global STARTING_EQUITY, bot_killed, kill_alert_sent

    logger.info("Starting Bybit scalper bot...")
    tg("🟢 Bot started")

    start_heartbeat()

    exchange = ExchangeClient(API_KEY, API_SECRET, testnet=TESTNET)

    # Close any leftover positions from previous run
    emergency_close_all(exchange, SYMBOLS)

    STARTING_EQUITY = exchange.get_balance()
    bot_killed = False
    kill_alert_sent = False

    logger.info(f"Kill-switch starting equity: {STARTING_EQUITY:.4f}, trigger={GLOBAL_KILL_TRIGGER*100:.1f}%")
    tg(f"📊 Starting equity: {STARTING_EQUITY:.4f} USDT. Kill at {GLOBAL_KILL_TRIGGER*100:.1f}% loss.")

    for s in SYMBOLS:
        exchange.set_leverage(s, LEVERAGE)

    engine = DecisionEngine(exchange, SYMBOLS)
    workers: List[MarketWorker] = []

    for s in SYMBOLS:
        w = MarketWorker(s, engine, exchange, TESTNET)
        w.start()
        workers.append(w)

    logger.info("Workers started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Stopping workers...")
        tg("🛑 Bot stopping (KeyboardInterrupt).")
        # Threads will naturally stop when process exits


if __name__ == "__main__":
    main()
