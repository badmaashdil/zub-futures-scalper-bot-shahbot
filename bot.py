#!/usr/bin/env python3
# Zub Bybit Futures Scalper Bot - MODE B (Aggressive + Safe)
# Symbols: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, DOGEUSDT

import os
import time
import json
import threading
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
import websocket

# ================= CONFIG ===================

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]

TF_5M = "5"      # scalping timeframe
TF_15M = "15"    # confirmation timeframe

LEVERAGE = 3
EQUITY_FRACTION = 0.95     # 95% of equity per trade

TP_PERCENT = 1.0           # 1% TP
SL_PERCENT = 0.5           # 0.5% SL

SCAN_INTERVAL = 20         # seconds between scans (Mode B = more trades than Mode A)

MIN_VOLUME_USDT = 800_000  # min 5m quote volume
IMB_THRESHOLD = 0.04       # min orderbook imbalance
MAX_SPREAD_PCT = 0.0003    # 0.03% max spread

RSI_5M_LONG = 52
RSI_5M_SHORT = 48
RSI_15M_LONG = 50
RSI_15M_SHORT = 50

MAX_DAILY_LOSS = 0.10      # 10% daily loss limit
MAX_OPEN_POSITIONS = 1     # only 1 open position at a time

MIN_REQUEST_INTERVAL = 0.20  # seconds between HTTP calls

WS_URL = "wss://stream.bybit.com/v5/public/linear"

# ============== GLOBAL STATE =================

load_dotenv()

API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")

client = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
    testnet=False
)

last_request_time = 0.0

orderbook_data = {s: None for s in SYMBOLS}   # symbol -> {"bids": [...], "asks":[...]}
open_positions = {}                           # symbol -> {side, size, entry}

start_equity = None
daily_loss = 0.0
last_reset_date = datetime.now(timezone.utc).date()
trading_paused = False


# ============= COMMON UTILS =========================

def rate_limited_request(func, *args, **kwargs):
    """Basic rate limiter for Bybit HTTP."""
    global last_request_time
    now = time.time()
    delta = now - last_request_time
    if delta < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - delta)
    last_request_time = time.time()
    return func(*args, **kwargs)


def utc_date():
    return datetime.now(timezone.utc).date()


def reset_daily_if_needed():
    global daily_loss, last_reset_date, trading_paused, start_equity
    today = utc_date()
    if today != last_reset_date:
        daily_loss = 0.0
        last_reset_date = today
        trading_paused = False
        start_equity = None
        print(f"[RESET] New trading day: {today}, daily loss reset.")


def get_equity_usdt():
    """Get unified account USDT equity."""
    global start_equity
    try:
        res = rate_limited_request(
            client.get_wallet_balance,
            accountType="UNIFIED"
        )
        coins = res["result"]["list"][0]["coin"]
        usdt = next((c for c in coins if c["coin"] == "USDT"), None)
        if not usdt:
            return 0.0
        eq = float(usdt["equity"])
        if start_equity is None:
            start_equity = eq
        return eq
    except Exception as e:
        print("[ERROR] equity error:", e)
        return 0.0


def update_daily_loss():
    global daily_loss, trading_paused
    if start_equity is None:
        return
    current = get_equity_usdt()
    if current <= 0:
        return
    loss = max(0.0, (start_equity - current) / start_equity)
    daily_loss = loss
    if daily_loss >= MAX_DAILY_LOSS:
        trading_paused = True
        print(f"[RISK] Daily loss {daily_loss:.2%} >= {MAX_DAILY_LOSS:.0%}. Trading PAUSED.")


def can_open_new_position():
    if trading_paused:
        print("[SKIP] Trading paused due to daily loss.")
        return False
    if len(open_positions) >= MAX_OPEN_POSITIONS:
        print("[SKIP] Max open positions reached.")
        return False
    return True


# ============ MARKET DATA HELPERS ============


def fetch_klines(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
    """Get recent kline for symbol and timeframe."""
    try:
        res = rate_limited_request(
            client.get_kline,
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit
        )
        rows = res["result"]["list"]
        rows = rows[::-1]  # newest last
        data = []
        for r in rows:
            data.append(
                {
                    "open_time": int(r[0]),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": float(r[5]),
                    "turnover": float(r[6])
                }
            )
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"[ERROR] fetch_klines {symbol} {interval}m:", e)
        return pd.DataFrame()


def calc_rsi(close_arr, period: int = 14) -> float:
    """Simple RSI calculation."""
    if len(close_arr) < period + 1:
        return 50.0
    delta = np.diff(close_arr)
    gain = np.maximum(delta, 0)
    loss = -np.minimum(delta, 0)
    gain_ema = pd.Series(gain).ewm(alpha=1/period, adjust=False).mean()
    loss_ema = pd.Series(loss).ewm(alpha=1/period, adjust=False).mean()
    rs = gain_ema / (loss_ema + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def add_indicators_5m(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMA, volume, VWAP and RSI to 5m dataframe."""
    if df.empty:
        return df

    df["ema_fast"] = df["close"].ewm(span=20).mean()
    df["ema_slow"] = df["close"].ewm(span=50).mean()
    df["quote_vol"] = df["turnover"]

    # VWAP: typical price * volume
    tp = (df["high"] + df["low"] + df["close"]) / 3
    vol = df["volume"]
    df["vwap"] = (tp * vol).cumsum() / (vol.cumsum() + 1e-9)

    closes = df["close"].values
    df["rsi"] = np.nan
    if len(closes) > 20:
        r = calc_rsi(closes, period=14)
        df.loc[df.index[-1], "rsi"] = r

    return df


def volatility_ok(df_5m: pd.DataFrame) -> bool:
    """Filter too violent or too dead moves (5m)."""
    try:
        if df_5m.empty or len(df_5m) < 3:
            return False
        c1 = df_5m["close"].iloc[-1]
        c0 = df_5m["close"].iloc[-2]
        move_pct = abs(c1 - c0) / c0

        # Mode B: a bit looser than A
        if move_pct > 0.02:   # > 2% in 5m → crazy news
            print(f"[VOL] Too violent move {move_pct:.2%}")
            return False
        if move_pct < 0.0005:  # < 0.05% → dead
            print(f"[VOL] Too dead move {move_pct:.2%}")
            return False
        return True
    except Exception as e:
        print("[ERROR] volatility check:", e)
        return False


def volume_ok(df_5m: pd.DataFrame) -> bool:
    """Check last candle quote volume."""
    try:
        if df_5m.empty:
            return False
        v = df_5m["quote_vol"].iloc[-1]
        avg = df_5m["quote_vol"].tail(30).mean()
        if v < MIN_VOLUME_USDT:
            print(f"[VOL] Last vol {v:.0f} < {MIN_VOLUME_USDT}")
            return False
        if v < avg * 0.5:
            print(f"[VOL] Last vol {v:.0f} < 50% of avg {avg:.0f}")
            return False
        return True
    except Exception as e:
        print("[ERROR] volume check:", e)
        return False


# ============ ORDERBOOK FILTERS (L2) ============

def refresh_orderbook_http(symbol: str):
    """HTTP snapshot (fallback if WS not ready)."""
    try:
        res = rate_limited_request(
            client.get_orderbook,
            category="linear",
            symbol=symbol,
            limit=50
        )
        bids = res["result"]["b"]
        asks = res["result"]["a"]
        orderbook_data[symbol] = {"bids": bids, "asks": asks}
    except Exception as e:
        print(f"[ERROR] HTTP orderbook {symbol}:", e)

def ob_ready(symbol: str) -> bool:
    ob = orderbook_data.get(symbol)
    if not ob:
        return False
    if not ob.get("bids") or not ob.get("asks"):
        return False
    return True


def orderbook_imbalance(symbol: str) -> float:
    try:
        ob = orderbook_data.get(symbol)
        if not ob:
            return 0.0
        bids = ob["bids"][:15]
        asks = ob["asks"][:15]
        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)
        total = bid_vol + ask_vol
        if total == 0:
            return 0.0
        return (bid_vol - ask_vol) / total
    except Exception as e:
        print("[ERROR] imbalance:", e)
        return 0.0


def ob_spread_ok(symbol: str) -> bool:
    try:
        ob = orderbook_data.get(symbol)
        if not ob:
            return True
        best_bid = float(ob["bids"][0][0])
        best_ask = float(ob["asks"][0][0])
        mid = (best_bid + best_ask) / 2
        spread = (best_ask - best_bid) / mid
        if spread > MAX_SPREAD_PCT:
            print(f"[SPREAD] {symbol} spread {spread:.4%} too wide")
            return False
        return True
    except Exception as e:
        print("[ERROR] spread:", e)
        return True


def detect_spoof(symbol: str):
    """Detect spoof buy/sell walls."""
    ob = orderbook_data.get(symbol)
    if not ob:
        return (False, False)
    try:
        bids = ob["bids"][:10]
        asks = ob["asks"][:10]
        if not bids or not asks:
            return (False, False)
        bid_sizes = [float(b[1]) for b in bids]
        ask_sizes = [float(a[1]) for a in asks]
        max_bid = max(bid_sizes)
        max_ask = max(ask_sizes)
        avg_bid = sum(bid_sizes) / len(bid_sizes)
        avg_ask = sum(ask_sizes) / len(ask_sizes)
        mul = 3.0
        spoof_buy = max_bid > avg_bid * mul
        spoof_sell = max_ask > avg_ask * mul
        if spoof_buy:
            print("🟩 SPOOF BUY WALL")
        if spoof_sell:
            print("🟥 SPOOF SELL WALL")
        return (spoof_buy, spoof_sell)
    except Exception as e:
        print("[ERROR] spoof:", e)
        return (False, False)


def hidden_liq(symbol: str) -> bool:
    """Simple iceberg-like detection."""
    ob = orderbook_data.get(symbol)
    if not ob:
        return False
    try:
        bids = ob["bids"]
        asks = ob["asks"]
        if len(bids) < 2 or len(asks) < 2:
            return False
        refill_buy = float(bids[0][1]) > float(bids[1][1]) * 3
        refill_sell = float(asks[0][1]) > float(asks[1][1]) * 3
        return refill_buy or refill_sell
    except Exception:
        return False


# ============ BTC TREND FILTER ============

def btc_trend_up() -> bool | None:
    """BTC 15m trend filter via EMA."""
    df = fetch_klines("BTCUSDT", TF_15M, limit=120)
    if df.empty:
        return None
    df = add_indicators_5m(df)  # reuse EMA logic
    fast = df["ema_fast"].iloc[-1]
    slow = df["ema_slow"].iloc[-1]
    return fast > slow


# ============ SIGNAL ENGINE (MODE B) ============

def decide_direction(symbol: str) -> str | None:
    """
    MODE B decision:
    - 5m EMA trend
    - 5m & 15m RSI
    - Volume filter
    - Volatility filter
    - Orderbook imbalance
    - Spread quality
    - Spoof + Hidden liquidity
    - BTC global trend guard
    Returns: "long", "short" or None
    """

    if not ob_ready(symbol):
        print(f"[OB] {symbol} WS not ready, HTTP snapshot.")
        refresh_orderbook_http(symbol)

    # 5m data
    df5 = fetch_klines(symbol, TF_5M, limit=120)
    if df5.empty:
        return None
    df5 = add_indicators_5m(df5)

    if not volatility_ok(df5):
        return None
    if not volume_ok(df5):
        return None
    if not ob_spread_ok(symbol):
        return None

    rsi5 = df5["rsi"].iloc[-1]
    ema_fast = df5["ema_fast"].iloc[-1]
    ema_slow = df5["ema_slow"].iloc[-1]
    vwap = df5["vwap"].iloc[-1]
    last_close = df5["close"].iloc[-1]

    if np.isnan(rsi5):
        return None

    trend_up = ema_fast > ema_slow
    trend_down = ema_fast < ema_slow

> Crypto:
# 15m RSI confirmation
    df15 = fetch_klines(symbol, TF_15M, limit=120)
    if df15.empty:
        return None
    closes15 = df15["close"].values
    rsi15 = calc_rsi(closes15, period=14)

    # Orderbook filters
    imb = orderbook_imbalance(symbol)
    spoof_buy, spoof_sell = detect_spoof(symbol)
    ice = hidden_liq(symbol)

    # BTC trend guard
    btc_up = btc_trend_up()

    print(
        f"{symbol} | close={last_close:.2f} rsi5={rsi5:.1f} rsi15={rsi15:.1f} "
        f"trend_up={trend_up} imb={imb:.3f} spoofB={spoof_buy} spoofS={spoof_sell} "
        f"ice={ice} btc_up={btc_up}"
    )

    direction = None

    # LONG conditions
    if (
        trend_up
        and last_close > vwap
        and rsi5 >= RSI_5M_LONG
        and rsi15 >= RSI_15M_LONG
        and imb >= IMB_THRESHOLD
        and (spoof_buy or ice)
    ):
        direction = "long"

    # SHORT conditions
    if (
        trend_down
        and last_close < vwap
        and rsi5 <= RSI_5M_SHORT
        and rsi15 <= RSI_15M_SHORT
        and imb <= -IMB_THRESHOLD
        and (spoof_sell or ice)
    ):
        # If both long & short trigger (super rare) → skip
        if direction == "long":
            direction = None
        else:
            direction = "short"

    # BTC guard – follow master trend
    if direction == "long" and btc_up is False:
        return None
    if direction == "short" and btc_up is True:
        return None

    return direction


# ============ POSITION & ORDER EXECUTION ============

def refresh_positions():
    """Refresh open positions from Bybit."""
    global open_positions
    try:
        res = rate_limited_request(
            client.get_positions,
            category="linear"
        )
        items = res["result"]["list"]
        active = {}
        for p in items:
            sz = float(p["size"])
            if sz == 0:
                continue
            sym = p["symbol"]
            side = p["side"]  # "Buy" or "Sell"
            entry = float(p["avgPrice"])
            active[sym] = {"side": side, "size": sz, "entry": entry}
        open_positions = active
    except Exception as e:
        print("[ERROR] refresh_positions:", e)


def calc_order_size(symbol: str, price: float) -> float:
    eq = get_equity_usdt()
    if eq <= 0 or price <= 0:
        return 0.0
    notional = eq * EQUITY_FRACTION * LEVERAGE
    qty = notional / price
    return round(qty, 3)


def place_bracket_order(symbol: str, direction: str, price: float):
    global open_positions

    side = "Buy" if direction == "long" else "Sell"
    qty = calc_order_size(symbol, price)
    if qty <= 0:
        print("[ORDER] size <= 0, abort.")
        return

    if direction == "long":
        tp_price = price * (1 + TP_PERCENT / 100.0)
        sl_price = price * (1 - SL_PERCENT / 100.0)
    else:
        tp_price = price * (1 - TP_PERCENT / 100.0)
        sl_price = price * (1 + SL_PERCENT / 100.0)

    print(
        f"[ORDER] {symbol} {direction.upper()} size={qty} "
        f"entry={price:.2f} TP={tp_price:.2f} SL={sl_price:.2f}"
    )

    try:
        rate_limited_request(
            client.set_leverage,
            category="linear",
            symbol=symbol,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE),
        )
    except Exception as e:
        print("[ERROR] set_leverage:", e)

    try:
        rate_limited_request(
            client.place_order,
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=str(qty),
            timeInForce="GoodTillCancel",
            reduceOnly=False,
            closeOnTrigger=False,
            takeProfit=str(tp_price),
            stopLoss=str(sl_price),
            tpTriggerBy="LastPrice",
            slTriggerBy="LastPrice",
            positionIdx=0
        )
        open_positions[symbol] = {"side": side, "size": qty, "entry": price}
    except Exception as e:
        print("[ERROR] place_order:", e)


# ============ MAIN LOOP ============

> Crypto:
def main_loop():
    print("=== ZUB BYBIT FUTURES SCALPER – MODE B (Aggressive + Safe) ===")
    print("Symbols:", SYMBOLS)
    print("Timeframe: 5m with 15m confirmation")
    print("95% equity, 3x leverage, 1% TP, 0.5% SL")
    while True:
        reset_daily_if_needed()
        update_daily_loss()
        refresh_positions()

        if trading_paused:
            time.sleep(60)
            continue

        if not can_open_new_position():
            time.sleep(10)
            continue

        for sym in SYMBOLS:
            if sym in open_positions:
                continue

            direction = decide_direction(sym)
            if direction is None:
                continue

            df5 = fetch_klines(sym, TF_5M, limit=2)
            if df5.empty:
                continue
            entry_price = df5["close"].iloc[-1]

            place_bracket_order(sym, direction, entry_price)
            time.sleep(2)   # tiny delay after entry

        time.sleep(SCAN_INTERVAL)


# ============ WEBSOCKET HANDLER ============

def ws_on_message(ws, message):
    try:
        data = json.loads(message)
        topic = data.get("topic", "")
        if not topic.startswith("orderbook.50."):
            return
        symbol = topic.split(".")[-1]
        ob_list = data.get("data", [])
        if not ob_list:
            return
        ob = ob_list[0]
        bids = ob.get("b", [])
        asks = ob.get("a", [])
        orderbook_data[symbol] = {"bids": bids, "asks": asks}
    except Exception as e:
        print("[WS ERROR] on_message:", e)


def ws_on_open(ws):
    try:
        args = [f"orderbook.50.{s}" for s in SYMBOLS]
        sub = {"op": "subscribe", "args": args}
        ws.send(json.dumps(sub))
        print("[WS] Subscribed to:", args)
    except Exception as e:
        print("[WS ERROR] on_open:", e)


def ws_on_error(ws, error):
    print("[WS ERROR]", error)


def ws_on_close(ws, status, msg):
    print("[WS] Closed:", status, msg)


def ws_runner():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=ws_on_open,
                on_message=ws_on_message,
                on_error=ws_on_error,
                on_close=ws_on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("[WS] Runner exception:", e)
        print("[WS] Reconnecting in 3s...")
        time.sleep(3)


# ============ ENTRYPOINT ============

if name == "main":
    if not API_KEY or not API_SECRET:
        print("Missing BYBIT_API_KEY or BYBIT_API_SECRET in environment.")
        raise SystemExit

    t = threading.Thread(target=ws_runner, daemon=True)
    t.start()

    time.sleep(3)  # small warm-up for WS

    try:
        main_loop()
    except KeyboardInterrupt:
        print("Bot stopped by user.")
