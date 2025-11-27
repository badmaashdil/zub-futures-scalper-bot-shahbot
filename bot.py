import time
import json
import logging
import threading
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, Optional, List

import ccxt  # pip install ccxt
import websocket  # pip install websocket-client

# ===== CONFIG =====

API_KEY = "0ZmKZEMHfNQL6oqHlM"
API_SECRET = "X5WYuy3G82MR4gFTYLyj4ojFEybNSUWNznMw"

# True = testnet, False = live
TESTNET = False   # ⚠️ set carefully

# Internal symbols (Bybit linear USDT perp) and ccxt mapping
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

SCAN_INTERVAL = 5.0           # seconds between decision evaluations
LEVERAGE = 3                  # fixed leverage
MARGIN_FRACTION = 0.95        # 95% of equity per trade (compounding)
TP_PCT_ON_POSITION = 0.01     # 1% TP
SL_PCT_ON_POSITION = 0.005    # 0.5% SL

MAX_DRAWDOWN_PCT = 0.10       # 10% equity loss -> bot stops
MAX_CONCURRENT_POSITIONS = 1  # only 1 position at a time

POST_ONLY_TIMEOUT = 3.0       # wait for maker fill before taker
VOL_MOVE_PCT_1S = 0.4 / 100.0
VOL_MOVE_PCT_3S = 0.8 / 100.0
IMBALANCE_LEVELS = 5
IMBALANCE_THRESHOLD = 0.05    # ignore tiny imbalance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ws_scalper")


def now_ts() -> float:
    return time.time()


# ===== ExchangeClient using ccxt (REST for trading, balance, positions) =====

class ExchangeClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = True):
        self.lock = threading.Lock()
        options: Dict[str, Any] = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",  # USDT futures
            },
        }
        if testnet:
            options["urls"] = {
                "api": {
                    "public": "https://api-testnet.bybit.com",
                    "private": "https://api-testnet.bybit.com",
                }
            }
        self.client = ccxt.bybit(options)

    def _ccxt_symbol(self, symbol: str) -> str:
        return CCXT_SYMBOL_MAP[symbol]

    def get_balance(self) -> float:
        with self.lock:
            bal = self.client.fetch_balance()
        usdt = bal.get("USDT") or {}
        total = usdt.get("total")
        if total is None:
            return float(bal.get("total", {}).get("USDT", 0.0))
        return float(total)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        ccxt_symbol = self._ccxt_symbol(symbol)
        with self.lock:
            try:
                self.client.set_leverage(leverage, ccxt_symbol)
            except Exception as e:
                logger.warning(f"{symbol}: set_leverage error: {e}")

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        ccxt_symbol = self._ccxt_symbol(symbol)
        with self.lock:
            try:
                positions = self.client.fetch_positions([ccxt_symbol])
            except Exception as e:
                logger.warning(f"{symbol}: fetch_positions error: {e}")
                return None
        for p in positions:
            size = float(p.get("contracts") or p.get("size") or 0.0)
            if size != 0:
                return p
        return None

    def place_limit_order(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        reduce_only: bool = False,
        post_only: bool = True,
    ) -> Dict[str, Any]:
        ccxt_symbol = self._ccxt_symbol(symbol)
        params: Dict[str, Any] = {}
        if reduce_only:
            params["reduce_only"] = True
        if post_only:
            params["timeInForce"] = "PostOnly"
        with self.lock:
            order = self.client.create_order(
                symbol=ccxt_symbol,
                type="limit",
                side=side.lower(),
                amount=qty,
                price=price,
                params=params,
            )
        return order

    def place_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        ccxt_symbol = self._ccxt_symbol(symbol)
        params: Dict[str, Any] = {}
        if reduce_only:
            params["reduce_only"] = True
        with self.lock:
            order = self.client.create_order(
                symbol=ccxt_symbol,
                type="market",
                side=side.lower(),
                amount=qty,
                params=params,
            )
        return order

    def place_stop_market_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
        reduce_only: bool = True,
    ) -> Dict[str, Any]:
        """
        Stop-market order (SL). Using stopLossPrice param that ccxt maps for Bybit.
        """
        ccxt_symbol = self._ccxt_symbol(symbol)
        params: Dict[str, Any] = {
            "stopLossPrice": stop_price,
            "reduce_only": reduce_only,
        }
        with self.lock:
            order = self.client.create_order(
                symbol=ccxt_symbol,
                type="market",
                side=side.lower(),
                amount=qty,
                params=params,
            )
        return order

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        ccxt_symbol = self._ccxt_symbol(symbol)
        with self.lock:
            try:
                self.client.cancel_order(order_id, ccxt_symbol)
                return True
            except Exception as e:
                logger.warning(f"{symbol}: cancel_order error: {e}")
                return False

    def get_order_status(self, symbol: str, order_id: Optional[str]) -> Dict[str, Any]:
        if not order_id:
            return {}
        ccxt_symbol = self._ccxt_symbol(symbol)
        with self.lock:
            try:
                o = self.client.fetch_order(order_id, ccxt_symbol)
            except Exception as e:
                logger.warning(f"{symbol}: fetch_order error: {e}")
                return {}
        status = (o.get("status") or "").lower()
        return {
            "status": status,  # 'open', 'closed', 'canceled'
            "avg_price": o.get("average") or o.get("price"),
        }

    def close_position_market(self, symbol: str) -> None:
        pos = self.get_position(symbol)
        if not pos:
            return
        ccxt_symbol = self._ccxt_symbol(symbol)
        size = float(pos.get("contracts") or pos.get("size") or 0.0)
        if size == 0:
            return
        side = (pos.get("side") or "").lower()  # 'long' or 'short'
        close_side = "sell" if side == "long" else "buy"
        params = {"reduce_only": True}
        with self.lock:
            try:
                self.client.create_order(
                    symbol=ccxt_symbol,
                    type="market",
                    side=close_side,
                    amount=abs(size),
                    params=params,
                )
            except Exception as e:
                logger.error(f"{symbol}: close_position_market error: {e}")


# ===== Detectors & filters =====

class SpoofDetector:
    def __init__(self, cancel_window: float = 1.0, repeat_count: int = 3):
        self.cancel_window = cancel_window
        self.repeat_count = repeat_count
        self.recent_orders: Dict[Tuple[str, float], deque] = defaultdict(deque)

    def on_order_event(self, side: str, price: float, event_type: str, ts: Optional[float] = None) -> None:
        ts = ts or now_ts()
        key = (side, float(price))
        dq = self.recent_orders[key]
        dq.append((event_type, ts))
        while dq and ts - dq[0][1] > self.cancel_window:
            dq.popleft()

    def is_spoof(self, side: str, price: float) -> Tuple[bool, float]:
        key = (side, float(price))
        dq = self.recent_orders.get(key, deque())
        cancels = sum(1 for ev, _ in dq if ev == "cancel")
        if cancels >= self.repeat_count:
            conf = min(1.0, cancels / float(self.repeat_count))
            return True, conf
        return False, 0.0


class WhaleCancelDetector:
    def __init__(self, size_threshold: float, window_sec: float = 5.0):
        self.size_threshold = size_threshold
        self.window = window_sec
        self.large_orders: Dict[float, deque] = defaultdict(deque)

    def on_order_event(self, price: float, size: float, event_type: str, ts: Optional[float] = None) -> None:
        ts = ts or now_ts()
        if size < self.size_threshold:
            return
        dq = self.large_orders[float(price)]
        dq.append((event_type, ts, size))
        while dq and ts - dq[0][1] > self.window:
            dq.popleft()

    def is_whale_cancelling(self, price: float) -> Tuple[bool, float]:
        dq = self.large_orders.get(float(price), deque())
        if not dq:
            return False, 0.0
        cancels = sum(1 for t, _, _ in dq if t == "cancel")
        adds = sum(1 for t, _, _ in dq if t == "new")
        total = cancels + adds
        if total == 0:
            return False, 0.0
        cancel_rate = cancels / float(total)
        if cancel_rate > 0.3 and total >= 2:
            return True, cancel_rate
        return False, cancel_rate


def compute_imbalance(book: Dict[str, Dict[float, float]], levels: int = 5) -> float:
    bids = sorted(book.get("bids", {}).items(), key=lambda x: -x[0])[:levels]
    asks = sorted(book.get("asks", {}).items(), key=lambda x: x[0])[:levels]
    bid_vol = sum(q for _, q in bids)
    ask_vol = sum(q for _, q in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / float(total)


def compute_short_term_cvd(trades: List[Dict[str, Any]]) -> float:
    cvd = 0.0
    for t in trades:
        size = float(t.get("size", 0.0))
        side = str(t.get("side", "")).lower()
        if side == "buy":
            cvd += size
        elif side == "sell":
            cvd -= size
    return cvd


def compute_delta_burst(trades: List[Dict[str, Any]], last_n: int = 30) -> float:
    burst = 0.0
    for t in trades[-last_n:]:
        size = float(t.get("size", 0.0))
        side = str(t.get("side", "")).lower()
        if side == "buy":
            burst += size
        elif side == "sell":
            burst -= size
    return burst


def compute_ema_slope_5m(candles_5m: List[Dict[str, float]]) -> float:
    if len(candles_5m) < 20:
        return 0.0
    closes = [float(c["close"]) for c in candles_5m][-20:]
    ema_fast = sum(closes[-5:]) / 5.0
    ema_slow = sum(closes) / len(closes)
    if ema_slow == 0:
        return 0.0
    return (ema_fast - ema_slow) / ema_slow


def compute_volume_range_flags(candles_1m: List[Dict[str, float]], lookback: int = 20) -> Tuple[bool, bool]:
    if len(candles_1m) < lookback + 1:
        return True, True
    recent = candles_1m[-(lookback + 1):-1]
    last = candles_1m[-1]
    avg_vol = sum(float(c.get("volume", 0.0)) for c in recent) / float(len(recent) or 1)
    last_vol = float(last.get("volume", 0.0))
    volume_ok = last_vol >= 0.5 * avg_vol if avg_vol > 0 else True
    last_high = float(last["high"])
    last_low = float(last["low"])
    last_close = float(last["close"])
    rng = last_high - last_low
    if last_close <= 0:
        range_ok = True
    else:
        range_ok = (rng / last_close) >= 0.002
    return volume_ok, range_ok


def check_unpredictable(recent_price_changes: List[Tuple[float, float]]) -> bool:
    if len(recent_price_changes) < 2:
        return False
    now_t, now_p = recent_price_changes[-1]
    p_1s = None
    p_3s = None
    for ts, p in reversed(recent_price_changes):
        dt = now_t - ts
        if p_1s is None and dt >= 1.0:
            p_1s = p
        if p_3s is None and dt >= 3.0:
            p_3s = p
        if p_1s and p_3s:
            break
    if p_1s:
        move1 = abs(now_p - p_1s) / p_1s
        if move1 >= VOL_MOVE_PCT_1S:
            return True
    if p_3s:
        move3 = abs(now_p - p_3s) / p_3s
        if move3 >= VOL_MOVE_PCT_3S:
            return True
    return False


# ===== Risk & position sizing =====

@dataclass
class Position:
    symbol: str
    side: str   # "Buy" or "Sell"
    qty: float
    entry_price: float
    tp_price: float
    sl_price: float
    notional: float
    ts_open: float = field(default_factory=now_ts)


class RiskManager:
    def __init__(self, max_drawdown_pct: float):
        self.max_drawdown_pct = max_drawdown_pct
        self.start_equity: Optional[float] = None

    def update_and_check(self, equity: float) -> bool:
        if self.start_equity is None:
            self.start_equity = equity
            logger.info(f"Starting equity recorded: {equity:.4f}")
        if equity <= self.start_equity * (1.0 - self.max_drawdown_pct):
            logger.error(
                f"Max drawdown hit! Equity={equity:.4f}, Start={self.start_equity:.4f}, "
                f"DD={self.max_drawdown_pct*100:.1f}% -> stopping bot"
            )
            return False
        return True


def compute_position_and_prices(
    equity_usd: float,
    entry_price: float,
    side: str,
) -> Tuple[float, float, float, float]:
    if equity_usd <= 0 or entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0
    position_notional = equity_usd * MARGIN_FRACTION * LEVERAGE
    qty = position_notional / entry_price
    qty = float(f"{qty:.6f}")
    tp_frac = TP_PCT_ON_POSITION
    sl_frac = SL_PCT_ON_POSITION
    if side.lower() in ("buy", "long"):
        tp_price = entry_price * (1.0 + tp_frac)
        sl_price = entry_price * (1.0 - sl_frac)
    else:
        tp_price = entry_price * (1.0 - tp_frac)
        sl_price = entry_price * (1.0 + sl_frac)
    return qty, position_notional, tp_price, sl_price


# ===== Decision Engine =====

class DecisionEngine:
    def __init__(self, exchange: ExchangeClient, symbols: List[str]):
        self.exchange = exchange
        self.symbols = symbols
        self.spoof = SpoofDetector()
        self.whale = {s: WhaleCancelDetector(size_threshold=1.0) for s in symbols}
        self.open_positions: Dict[str, Position] = {}
        self.pause_until = 0.0
        self.risk = RiskManager(MAX_DRAWDOWN_PCT)
        self.btc_blocked = False
        self.lock = threading.Lock()

    def set_pause(self, seconds: float) -> None:
        self.pause_until = now_ts() + seconds
        logger.warning(f"Bot paused for {seconds:.1f}s (until {self.pause_until:.0f})")

    def is_paused(self) -> bool:
        return now_ts() < self.pause_until

    def evaluate_symbol(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        candles_5m: List[Dict[str, float]],
        candles_1m: List[Dict[str, float]],
        recent_prices: List[Tuple[float, float]],
    ) -> None:
        with self.lock:
            self._evaluate_locked(symbol, book, trades, candles_5m, candles_1m, recent_prices)

    def _evaluate_locked(
        self,
        symbol: str,
        book: Dict[str, Dict[float, float]],
        trades: List[Dict[str, Any]],
        candles_5m: List[Dict[str, float]],
        candles_1m: List[Dict[str, float]],
        recent_prices: List[Tuple[float, float]],
    ) -> None:
        # equity & drawdown
        equity = self.exchange.get_balance()
        if not self.risk.update_and_check(equity):
            self.set_pause(3600.0)
            return

        if self.is_paused():
            return

        # unpredictable volatility
        if check_unpredictable(recent_prices):
            logger.warning(f"{symbol}: unpredictable 1s/3s move -> pause")
            self.set_pause(5.0)
            return

        # BTC master trend filter placeholder (you can later wire BTC logic)
        if symbol != "BTCUSDT" and self.btc_blocked:
            logger.info(f"{symbol}: blocked by BTC filter")
            return

        # volume & range
        volume_ok, range_ok = compute_volume_range_flags(candles_1m)
        if not volume_ok or not range_ok:
            logger.debug(f"{symbol}: volume/range filter -> skip")
            return

        # trend / RSI placeholders
        ema_slope = compute_ema_slope_5m(candles_5m)
        rsi_5m = 50.0           # later you can compute real RSI
        trend_15m = 0           # later HTF trend

        if abs(ema_slope) < 0.01:
            logger.debug(f"{symbol}: flat EMA slope {ema_slope:.4f} -> skip")
            return

        avoid_longs = rsi_5m > 80
        avoid_shorts = rsi_5m < 20

        # orderbook imbalance
        imb = compute_imbalance(book, levels=IMBALANCE_LEVELS)
        if abs(imb) < IMBALANCE_THRESHOLD:
            logger.debug(f"{symbol}: small imbalance {imb:.3f} -> skip")
            return

        bids = book.get("bids", {})
        asks = book.get("asks", {})
        if not bids or not asks:
            return
        best_bid = max(bids)
        best_ask = min(asks)

        # spoof / whale checks at top levels
        spf_bid, _ = self.spoof.is_spoof("bid", best_bid)
        if spf_bid:
            logger.info(f"{symbol}: spoof on bid {best_bid} -> skip")
            return
        wc_flag_bid, _ = self.whale[symbol].is_whale_cancelling(best_bid)
        if wc_flag_bid:
            logger.info(f"{symbol}: whale cancelling bids -> skip")
            return
        spf_ask, _ = self.spoof.is_spoof("ask", best_ask)
        if spf_ask:
            logger.info(f"{symbol}: spoof on ask {best_ask} -> skip")
            return

        # orderflow
        short_cvd = compute_short_term_cvd(trades)
        delta_burst = compute_delta_burst(trades)
        if short_cvd > 0 and delta_burst > 0:
            of_score = 1.0
        elif short_cvd < 0 and delta_burst < 0:
            of_score = -1.0
        else:
            of_score = 0.0

        # composite score
        trend_score = max(-1.0, min(1.0, ema_slope * 20.0))
        imb_score = max(-1.0, min(1.0, imb * 5.0))
        final_score = trend_score * 0.4 + imb_score * 0.3 + of_score * 0.3

        decision: Optional[str] = None
        reason = f"trend={trend_score:.2f} imb={imb_score:.2f} of={of_score:.2f}"

        if final_score >= 0.6 and not avoid_longs and trend_15m >= 0:
            decision = "LONG"
        elif final_score <= -0.6 and not avoid_shorts and trend_15m <= 0:
            decision = "SHORT"
        else:
            logger.debug(f"{symbol}: no strong signal score={final_score:.2f} ({reason})")
            return

        # global concurrency
        if len(self.open_positions) >= MAX_CONCURRENT_POSITIONS:
            logger.info(f"{symbol}: max positions open -> skip")
            return

        # position sizing
        side_for_calc = "buy" if decision == "LONG" else "sell"
        mid_price = (best_bid + best_ask) / 2.0
        qty, notional, tp_price, sl_price = compute_position_and_prices(
            equity_usd=equity,
            entry_price=mid_price,
            side=side_for_calc,
        )
        if qty <= 0 or notional <= 0:
            logger.warning(f"{symbol}: invalid qty/notional -> skip")
            return

        side = "Buy" if decision == "LONG" else "Sell"
        logger.info(
            f"{symbol}: {decision} qty={qty} entry≈{mid_price:.4f} "
            f"tp={tp_price:.4f} sl={sl_price:.4f} ({reason})"
        )

        # maker-first, then taker
        try:
            entry_order = self.exchange.place_limit_order(
                symbol=symbol,
                side=side,
                price=mid_price,
                qty=qty,
                reduce_only=False,
                post_only=True,
            )
            order_id = entry_order.get("id") or entry_order.get("order_id") if entry_order else None
            start_t = now_ts()
            filled = False
            fill_price = mid_price

            while now_ts() - start_t < POST_ONLY_TIMEOUT:
                status = self.exchange.get_order_status(symbol, order_id)
                st = status.get("status", "")
                if st in ("closed", "filled"):
                    filled = True
                    fill_price = float(status.get("avg_price", mid_price))
                    break
                time.sleep(0.1)

            if not filled:
                if order_id:
                    self.exchange.cancel_order(symbol, order_id)
                mkt = self.exchange.place_market_order(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    reduce_only=False,
                )
                fill_price = float(mkt.get("average") or mkt.get("price") or mid_price) if mkt else mid_price

        except Exception as e:
            logger.exception(f"{symbol}: entry order failed: {e}")
            return

        # TP/SL placement (automatic, secure)
        try:
            reduce_side = "Sell" if side.lower() in ("buy", "long") else "Buy"
            tp_order = self.exchange.place_limit_order(
                symbol=symbol,
                side=reduce_side,
                price=tp_price,
                qty=qty,
                reduce_only=True,
                post_only=False,
            )
            sl_order = self.exchange.place_stop_market_order(
                symbol=symbol,
                side=reduce_side,
                qty=qty,
                stop_price=sl_price,
                reduce_only=True,
            )
            if not tp_order or not sl_order:
                logger.error(f"{symbol}: TP or SL placement failed -> emergency close")
                self.exchange.place_market_order(
                    symbol=symbol,
                    side=reduce_side,
                    qty=qty,
                    reduce_only=True,
                )
                return
        except Exception as e:
            logger.exception(f"{symbol}: TP/SL placement failed: {e}")
            try:
                reduce_side = "Sell" if side.lower() in ("buy", "long") else "Buy"
                self.exchange.place_market_order(
                    symbol=symbol,
                    side=reduce_side,
                    qty=qty,
                    reduce_only=True,
                )
            except Exception as e2:
                logger.exception(f"{symbol}: emergency close failed: {e2}")
            return

        pos = Position(
            symbol=symbol,
            side=side,
            qty=qty,
            entry_price=fill_price,
            tp_price=tp_price,
            sl_price=sl_price,
            notional=notional,
        )
        self.open_positions[symbol] = pos
        logger.info(f"{symbol}: position opened {pos}")


# ===== MarketWorker with WebSocket (one per symbol) =====

class MarketWorker(threading.Thread):
    def __init__(self, symbol: str, engine: DecisionEngine, exchange: ExchangeClient, testnet: bool):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.engine = engine
        self.exchange = exchange
        self.testnet = testnet

        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = True

        self.orderbook: Dict[str, Dict[float, float]] = {"bids": {}, "asks": {}}
        self.last_orderbook_ts: float = 0.0

        self.trades: deque = deque(maxlen=500)
        self.candles_1m: deque = deque(maxlen=500)
        self.candles_5m: deque = deque(maxlen=500)
        self.price_samples: deque = deque(maxlen=200)

        self.last_eval_ts: float = 0.0

    def _ws_url(self) -> str:
        return PUBLIC_WS_TESTNET if self.testnet else PUBLIC_WS_MAINNET

    def _on_open(self, ws):
        logger.info(f"{self.symbol}: WebSocket opened")
        sub = {
            "op": "subscribe",
            "args": [
                f"orderbook.50.{self.symbol}",
                f"publicTrade.{self.symbol}",
            ],
        }
        ws.send(json.dumps(sub))

    def _on_message(self, ws, message: str):
        try:
            msg = json.loads(message)
        except Exception:
            return

        topic = msg.get("topic", "")
        if not topic:
            return

        if topic.startswith("orderbook.50."):
            self._handle_orderbook(msg)
        elif topic.startswith("publicTrade."):
            self._handle_trades(msg)

        # run decision engine every SCAN_INTERVAL
        now = now_ts()
        if now - self.last_eval_ts >= SCAN_INTERVAL:
            self.last_eval_ts = now
            self._maybe_evaluate()

    def _handle_orderbook(self, msg: Dict[str, Any]) -> None:
        data_list = msg.get("data") or []
        if not data_list:
            return
        payload = data_list[0]
        typ = msg.get("type", "snapshot")
        bids = self.orderbook["bids"]
        asks = self.orderbook["asks"]

        ts = now_ts()

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
        else:  # delta
            for p, s in payload.get("b", []):
                price = float(p)
                size = float(s)
                old_size = bids.get(price, 0.0)
                if size == 0:
                    if old_size > 0:
                        # cancellation event
                        self.engine.spoof.on_order_event("bid", price, "cancel", ts)
                        self.engine.whale[self.symbol].on_order_event(price, old_size, "cancel", ts)
                        bids.pop(price, None)
                else:
                    if size > old_size:
                        # new/add
                        self.engine.spoof.on_order_event("bid", price, "new", ts)
                        self.engine.whale[self.symbol].on_order_event(price, size, "new", ts)
                    bids[price] = size
            for p, s in payload.get("a", []):
                price = float(p)
                size = float(s)
                old_size = asks.get(price, 0.0)
                if size == 0:
                    if old_size > 0:
                        self.engine.spoof.on_order_event("ask", price, "cancel", ts)
                        self.engine.whale[self.symbol].on_order_event(price, old_size, "cancel", ts)
                        asks.pop(price, None)
                else:
                    if size > old_size:
                        self.engine.spoof.on_order_event("ask", price, "new", ts)
                        self.engine.whale[self.symbol].on_order_event(price, size, "new", ts)
                    asks[price] = size

        self.last_orderbook_ts = ts

    def _handle_trades(self, msg: Dict[str, Any]) -> None:
        data_list = msg.get("data") or []
        ts_now = now_ts()
        for t in data_list:
            price = float(t.get("p") or 0.0)
            size = float(t.get("v") or 0.0)
            side = str(t.get("S") or "").lower()  # Buy/Sell
            ts = float(t.get("T") or ts_now * 1000.0) / 1000.0
            self.trades.append({"price": price, "size": size, "side": side, "ts": ts})
            self.price_samples.append((ts, price))
        self._update_candles()

    def _update_candles(self) -> None:
        if not self.trades:
            return
        # build 1m candle from recent trades
        last_trade = self.trades[-1]
        price = float(last_trade["price"])
        ts = float(last_trade["ts"])
        minute_bucket = int(ts // 60)
        size_sum = sum(float(t["size"]) for t in list(self.trades)[-50:])  # approximate

        if self.candles_1m and int(self.candles_1m[-1].get("bucket", 0)) == minute_bucket:
            c = self.candles_1m[-1]
            c["close"] = price
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["volume"] += size_sum
        else:
            self.candles_1m.append({
                "bucket": minute_bucket,
                "open": price,
                "close": price,
                "high": price,
                "low": price,
                "volume": size_sum,
            })

        # 5m aggregation
        while len(self.candles_1m) >= 5 and (not self.candles_5m or len(self.candles_5m) < len(self.candles_1m) // 5):
            chunk = list(self.candles_1m)[-5:]
            o = chunk[0]["open"]
            h = max(c["high"] for c in chunk)
            l = min(c["low"] for c in chunk)
            cl = chunk[-1]["close"]
            vol = sum(c["volume"] for c in chunk)
            self.candles_5m.append({
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": vol,
            })

    def _maybe_evaluate(self) -> None:
        if not self.orderbook["bids"] or not self.orderbook["asks"]:
            return
        trades_list = list(self.trades)
        candles_1m = list(self.candles_1m)
        candles_5m = list(self.candles_5m)
        recent_prices = list(self.price_samples)
        if len(trades_list) < 10 or len(candles_1m) < 5 or len(candles_5m) < 1:
            return
        self.engine.evaluate_symbol(
            symbol=self.symbol,
            book=self.orderbook,
            trades=trades_list,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
            recent_prices=recent_prices,
        )

    def _on_error(self, ws, error):
        logger.error(f"{self.symbol}: WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"{self.symbol}: WebSocket closed: {close_status_code} {close_msg}")
        # loop in run() will reconnect

    def run(self) -> None:
        url = self._ws_url()
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                logger.info(f"{self.symbol}: connecting WebSocket {url}")
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                logger.exception(f"{self.symbol}: WebSocket run_forever error: {e}")
            if self.running:
                logger.info(f"{self.symbol}: reconnecting WebSocket in 3s")
                time.sleep(3.0)

    def stop(self) -> None:
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


# ===== Main =====

def main():
    logger.info("Starting multi-symbol WebSocket scalper bot...")
    exchange = ExchangeClient(API_KEY, API_SECRET, testnet=TESTNET)

    # set leverage for each symbol
    for s in SYMBOLS:
        exchange.set_leverage(s, LEVERAGE)

    engine = DecisionEngine(exchange, SYMBOLS)
    workers: List[MarketWorker] = []

    for s in SYMBOLS:
        w = MarketWorker(symbol=s, engine=engine, exchange=exchange, testnet=TESTNET)
        w.start()
        workers.append(w)

    logger.info("Workers started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Stopping workers...")
        for w in workers:
            w.stop()
        time.sleep(2.0)
        logger.info("Exited.")


if __name__ == "__main__":
    main()
