# ============================================================
# GOD-JAISRIRAM BGBI FAST REACTION V1.0
# Binance (primary) + Coinbase WebSocket (failover)
# ============================================================
# FIXES vs previous V7.0:
#   [1] REMOVED blocking Coinbase REST depth poller (was locking GIL)
#   [2] Coinbase now uses WebSocket ticker only (non-blocking)
#   [3] Binance everything preserved exactly as V6.0
#   [4] V6.0 endpoints all intact for EA compatibility
#   [5] Non-blocking failover logic preserved
# ============================================================

import websocket
import json
import csv
import time
import threading
import math
import os
import sys
import subprocess

import urllib.request
import urllib.error

from pathlib import Path
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ============================================================
# CONFIG
# ============================================================

SYMBOL = os.getenv("BGBI_SYMBOL", "BTCUSDT").upper()
SYMBOL_LOWER = SYMBOL.lower()
_DEFAULT_COINBASE = {
    "BTCUSDT": "BTC-USD",
    "SOLUSDT": "SOL-USD",
    "XRPUSDT": "XRP-USD",
    "ETHUSDT": "ETH-USD",
}
COINBASE_PRODUCT = os.getenv("BGBI_COINBASE_PRODUCT", _DEFAULT_COINBASE.get(SYMBOL, "")).strip()
# BNBUSDT is intentionally Binance-primary: Coinbase USD failover is unavailable
# in this bridge configuration. Do not crash its worker for an empty product.
COINBASE_ENABLED = bool(COINBASE_PRODUCT)

HOST = "127.0.0.1"
PORT = int(os.getenv("BGBI_PORT", "8787"))
FUTURES_ENABLED = os.getenv("BGBI_FUTURES_ENABLED", "1").strip().lower() not in ("0", "false", "no")

# --- Binance Spot streams
BASE_WS = "wss://stream.binance.com:9443/ws"
TRADE_WS = f"{BASE_WS}/{SYMBOL_LOWER}@aggTrade"
BOOK_WS  = f"{BASE_WS}/{SYMBOL_LOWER}@bookTicker"
DEPTH_WS = f"{BASE_WS}/{SYMBOL_LOWER}@depth5@100ms"

# --- Binance Futures streams
FUT_WS = "wss://fstream.binance.com/ws"
LIQ_WS = f"{FUT_WS}/!forceOrder@arr"

# --- Coinbase WebSocket (failover only)
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
COINBASE_SUB = json.dumps({
    "type": "subscribe",
    "product_ids": [COINBASE_PRODUCT],
    "channels": ["matches", "ticker"]
})

# --- REST URLs (Binance only)
KLINES_URL   = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval=1m&limit=500"
DEPTH_REST   = f"https://api.binance.com/api/v3/depth?symbol={SYMBOL}&limit=10"
FUNDING_URL  = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={SYMBOL}"
OI_URL       = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={SYMBOL}"
LS_URL       = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={SYMBOL}&period=5m&limit=1"

CSV_FILE = Path(__file__).resolve().parent / f"binance_flow_{SYMBOL.lower()}.csv"
FAST_CSV_FILE = Path(__file__).resolve().parent / f"bgbi_fast_reaction_signals_{SYMBOL.lower()}.csv"

HEALTH_MAX_AGE_MS = 5000
FAILOVER_THRESHOLD_MS = 5000
RECOVERY_THRESHOLD_MS = 1500
RECOVERY_HOLD_MS = 3000
DISPLAY_INTERVAL = 1.0

MAX_TRADES  = 30000
MAX_PRICES  = 15000
FLOW_HISTORY_MAX = 300
LIQ_HISTORY_MAX  = 5000

M1_HISTORY_MAX = 500
VP_LOOKBACK_BARS = 200
VP_BINS = 30
VP_VALUE_AREA_PCT = 70.0

# VPIN config
VPIN_SIGMA_WINDOW = 100
VPIN_WINDOW = 50
VPIN_HISTORY_MAX = 300
VPIN_QUIET_MAX = 40
VPIN_NORMAL_MAX = 80
VPIN_ELEVATED_MAX = 95

# Naked POC
POC_SNAPSHOT_INTERVAL_MS = 15 * 60 * 1000
POC_SNAPSHOT_MAX = 30
POC_VISITED_ATR_MULT = 0.5

# VWAP-Z
VWAP_Z_WINDOW = 60
VWAP_EXH_VOL_LEN = 20
VWAP_EXH_THRESHOLD = 0.5

LIQ_WINDOW_MS = 60 * 1000

# Poll intervals
VP_POLL_SEC       = 5
WALLS_POLL_SEC    = 10
FUNDING_POLL_SEC  = 60
OI_POLL_SEC       = 30
LS_POLL_SEC       = 30

# Flow throttle (slower aggregate/context lane)
FLOW_UPDATE_THROTTLE_MS = 200

# BGBI FAST REACTION: event-driven, observation-only signal detection.
FAST_REACTION_ENABLED = True
FAST_TAPE_KEEP_MS = 6000
FAST_RANGE_LOOKBACK_MS = 2000
FAST_SHORT_WINDOW_MS = 250
FAST_MEDIUM_WINDOW_MS = 500
FAST_LONG_WINDOW_MS = 1000
FAST_MIN_TRADE_EVENTS_500MS = 4
FAST_MIN_PRESSURE_250 = 0.58
FAST_MIN_PRESSURE_1000 = 0.54
FAST_MIN_MOVE_BPS = 0.60
FAST_MIN_MOVE_ATR = 0.025
FAST_BREAK_BUFFER_SPREAD_MULT = 1.50
FAST_BREAK_BUFFER_ATR = 0.010
FAST_MIN_SCORE = 74.0
FAST_STRONG_SCORE = 86.0
FAST_STRONG_MOVE_ATR = 0.080
FAST_COUNTER_M15_SCORE = 86.0
FAST_SIGNAL_TTL_MS = 3000
FAST_SIGNAL_COOLDOWN_MS = 2500
FAST_SAME_SIDE_REARM_PULLBACK_ATR = 0.20
FAST_MAX_TRADE_AGE_MS = 1000
FAST_MAX_BOOK_AGE_MS = 1000

# CVD velocity
CVD_VELOCITY_WINDOW = 20
CVD_VELOCITY_MAX = 120


# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.RLock()
active_feed = "BINANCE"

state_binance = {
    "price": 0.0, "best_bid": 0.0, "best_ask": 0.0, "mid_price": 0.0,
    "spread": 0.0, "spread_bps": 0.0, "cvd": 0.0, "cvd_velocity": 0.0,
    "cvd_acceleration": 0.0, "book_imbalance": 0.0, "depth_imbalance": 0.0,
    "bid5": 0.0, "ask5": 0.0, "bid5_notional": 0.0, "ask5_notional": 0.0,
    "ofi": 0.0, "ofi_prev": 0.0, "ofi_ema": 0.0,
    "ofi_velocity": 0.0, "ofi_acceleration": 0.0, "microprice": 0.0,
    "ofi_update_ms": 0, "liquidity_add": 0.0, "liquidity_remove": 0.0, "absorption_score": 0.0,
    "flow_score": 0.0,
    "buy_score": 50.0, "sell_score": 50.0, "action": "NO_DATA",
    "velocity": 0.0, "acceleration": 0.0, "trade_rate": 0.0, "buy_rate": 0.0,
    "sell_rate": 0.0, "buy_quote_rate": 0.0, "sell_quote_rate": 0.0,
    "flow_persistence": 0.0, "flow_persistence_count": 0,
    "last_trade_receive_ms": 0, "last_book_receive_ms": 0, "last_depth_receive_ms": 0,
    "last_trade_event_ms": 0, "last_book_event_ms": 0, "last_depth_event_ms": 0,
    "trade_events": 0, "book_events": 0, "depth_events": 0,
    "connected": False, "trade_connected": False, "book_connected": False, "depth_connected": False,
    "trade_reconnects": 0, "book_reconnects": 0, "depth_reconnects": 0,
    "last_error": "",
    "depth_levels": {"bids": [], "asks": []},
    "timestamp_ms": 0, "server_time_ms": 0,
    "vp_valid": False, "vp_poc": 0.0, "vp_vah": 0.0, "vp_val": 0.0,
    "vp_width_pct": 0.0, "vp_min_p": 0.0, "vp_max_p": 0.0, "vp_update_ms": 0,
    "vroc": 0.0, "range_exp": 0.0, "atr_m1": 0.0, "atr_update_ms": 0, "vroc_sustained": False,
    "vwap": 0.0, "vwap_slope": 0.0, "vwap_update_ms": 0,
    "rolling_vwap": 0.0, "vwap_stdev": 0.0, "vwap_z": 0.0, "vwap_exhausted": False,
    "vpin": 0.0, "vpin_pct": 0.0, "vpin_regime": "Quiet", "vpin_pressure": 50.0,
    "oi_regime": "NEUTRAL", "naked_above": 0.0, "naked_below": 0.0, "naked_count": 0,
    "funding": 0.0, "oi": 0.0, "oi_delta_10s": 0.0, "oi_delta_30s": 0.0, "oi_delta_1m": 0.0,
    "long_short_ratio": 0.0, "liquidation_buy": 0.0, "liquidation_sell": 0.0,
    "liquidation_imbalance": 0.0, "liquidation_events": 0,
    "bid_wall_price": 0.0, "bid_wall_size": 0.0, "ask_wall_price": 0.0, "ask_wall_size": 0.0,
    "walls_update_ms": 0,
    "delta_1": 0.0, "delta_3": 0.0, "delta_5": 0.0,
    "delta_15": 0.0, "delta_30": 0.0, "delta_60": 0.0,
    "buy_quote_1": 0.0, "sell_quote_1": 0.0,
    "buy_quote_3": 0.0, "sell_quote_3": 0.0,
    "buy_quote_5": 0.0, "sell_quote_5": 0.0,
    "buy_quote_15": 0.0, "sell_quote_15": 0.0,
    "buy_quote_30": 0.0, "sell_quote_30": 0.0,
    "buy_quote_60": 0.0, "sell_quote_60": 0.0,
    "trades_1": 0, "trades_3": 0, "trades_5": 0,
    "trades_15": 0, "trades_30": 0, "trades_60": 0,
    "buy_trades_1": 0, "sell_trades_1": 0,
    "price_return_1": 0.0, "price_return_5": 0.0, "price_return_15": 0.0,
    "long_account": 0.0, "short_account": 0.0,
    "funding_update_ms": 0, "oi_update_ms": 0, "ls_update_ms": 0,
    "liq_update_ms": 0, "vwap_z_update_ms": 0,
    "last_liq_receive_ms": 0, "last_poc_snapshot_ms": 0,
    "poc_snapshots": [], "vpin_history": [], "vroc_history": [],
}

state_coinbase = dict(state_binance)
state_coinbase["last_error"] = "Coinbase standby"

trade_history_binance = deque(maxlen=MAX_TRADES)
price_history_binance = deque(maxlen=MAX_PRICES)
trade_history_coinbase = deque(maxlen=MAX_TRADES)
price_history_coinbase = deque(maxlen=MAX_PRICES)

trade_history = trade_history_binance
price_history = price_history_binance
state = state_binance

m1_bars = deque(maxlen=M1_HISTORY_MAX)
current_bar = None

flow_history  = deque(maxlen=FLOW_HISTORY_MAX)
liq_history   = deque(maxlen=LIQ_HISTORY_MAX)
oi_history    = deque(maxlen=240)
cvd_history   = deque(maxlen=CVD_VELOCITY_MAX)

_last_flow_ms = 0
_last_cvd_vel_ms = 0
_prev_cvd_for_vel = 0.0
_prev_book = {"bid":0.0,"bid_q":0.0,"ask":0.0,"ask_q":0.0,"ts":0}
_prev_ofi = 0.0
_prev_ofi_ts = 0

# Event-time tape is deliberately separate from the 200ms derived-flow lane.
fast_trade_tape = deque(maxlen=20000)
fast_price_samples = deque(maxlen=1200)  # one price sample per ~10ms bucket
# Monotonic deques keep the rolling breakout range O(1) per trade event.
fast_high_deque = deque()  # newest at right; prices descend left-to-right
fast_low_deque = deque()   # newest at right; prices ascend left-to-right
FAST_WINDOWS_MS = (250, 500, 1000)
fast_window_tapes = {w: deque() for w in FAST_WINDOWS_MS}
fast_window_totals = {w: {"buy": 0.0, "sell": 0.0, "count": 0} for w in FAST_WINDOWS_MS}
fast_context = {"m1_dir": 0, "m5_dir": 0, "m15_dir": 0, "updated_ms": 0, "closed_m1_ts": 0, "m1_bars_available": 0, "m5_bars_available": 0, "m15_bars_available": 0, "m15_context_ready": False}
_fast_last_context_minute = -1
_fast_last_signal_ms = 0
_fast_last_signal_dir = 0
_fast_last_signal_price = 0.0
fast_state = {
    "ok": False, "status": "WARMING", "action": "NEUTRAL", "last_signal": "NONE",
    "last_signal_ms": 0, "last_signal_snapshot": {}, "event_ms": 0, "source": "NONE", "price": 0.0, "spread": 0.0,
    "score": 0.0, "pressure_250ms": 0.0, "pressure_1000ms": 0.0,
    "return_250ms_bps": 0.0, "return_500ms_bps": 0.0, "return_1000ms_bps": 0.0,
    "ticks_250ms": 0, "ticks_500ms": 0, "ticks_1000ms": 0,
    "ofi_ema": 0.0, "atr_m1": 0.0, "impulse_atr": 0.0, "breakout": "NONE",
    "m1_dir": 0, "m5_dir": 0, "m15_dir": 0, "reason": "waiting for fresh trades and book",
    "m1_bars_available": 0, "m5_bars_available": 0, "m15_bars_available": 0, "m15_context_ready": False,
    "trade_age_ms": 999999, "book_age_ms": 999999, "processing_us": 0,
}


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)

def safe_float(value, default=0.0):
    try:
        x = float(value)
        if math.isfinite(x): return x
    except Exception:
        pass
    return default

def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

def clamp(value, low, high):
    return max(low, min(high, value))

def imbalance(buy, sell):
    total = buy + sell
    if total <= 0: return 0.0
    return clamp((buy - sell) / total, -1.0, 1.0)

def percent_return(old_price, new_price):
    if old_price <= 0 or new_price <= 0: return 0.0
    return ((new_price - old_price) / old_price) * 100.0

def http_get_json(url, timeout=3.0):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BTCBot/7.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


# ============================================================
# FAILOVER LOGIC (non-blocking)
# ============================================================

_binance_good_since_ms = 0

def _feed_age_ms(s):
    now = now_ms()
    trade_age = now - s["last_trade_receive_ms"] if s["last_trade_receive_ms"] > 0 else 999999
    book_age = now - s["last_book_receive_ms"] if s["last_book_receive_ms"] > 0 else 999999
    return max(trade_age, book_age)

def get_master_state():
    global active_feed, state, trade_history, price_history, _binance_good_since_ms

    with lock:
        now = now_ms()
        b_age = _feed_age_ms(state_binance)
        c_age = _feed_age_ms(state_coinbase)

        if b_age > FAILOVER_THRESHOLD_MS and c_age < FAILOVER_THRESHOLD_MS:
            if active_feed != "COINBASE":
                print(f"[FAILOVER] Binance stale ({b_age}ms) -> Coinbase")
                active_feed = "COINBASE"
                state = state_coinbase
                trade_history = trade_history_coinbase
                price_history = price_history_coinbase
            _binance_good_since_ms = 0

        elif active_feed == "COINBASE" and b_age < RECOVERY_THRESHOLD_MS:
            if _binance_good_since_ms == 0:
                _binance_good_since_ms = now
            elif now - _binance_good_since_ms >= RECOVERY_HOLD_MS:
                print(f"[RECOVERY] Binance stable ({b_age}ms for {now-_binance_good_since_ms}ms) -> Binance")
                active_feed = "BINANCE"
                state = state_binance
                trade_history = trade_history_binance
                price_history = price_history_binance
                _binance_good_since_ms = 0
        else:
            _binance_good_since_ms = 0

        return state


# ============================================================
# TRADE ADDER
# ============================================================

def add_trade(event_ts, receive_ts, price, quote, buy_aggressor, exchange):
    global current_bar

    tgt_state = state_binance if exchange == "BINANCE" else state_coinbase
    tgt_trades = trade_history_binance if exchange == "BINANCE" else trade_history_coinbase
    tgt_prices = price_history_binance if exchange == "BINANCE" else price_history_coinbase

    with lock:
        if buy_aggressor:
            tgt_state["cvd"] += quote
        else:
            tgt_state["cvd"] -= quote

        tgt_trades.append({
            "event_ts": event_ts, "ts": receive_ts,
            "price": price, "quote": quote,
            "buy": quote if buy_aggressor else 0.0,
            "sell": 0.0 if buy_aggressor else quote,
            "buy_trade": 1 if buy_aggressor else 0,
            "sell_trade": 0 if buy_aggressor else 1,
        })
        tgt_prices.append({"ts": receive_ts, "price": price})

        # M1 bar from Binance only (Coinbase is fallback and short-lived)
        if exchange == "BINANCE":
            minute_ms = (receive_ts // 60000) * 60000
            if current_bar is None:
                current_bar = {"ts": minute_ms, "o": price, "h": price, "l": price, "c": price, "v": quote}
            elif minute_ms != current_bar["ts"]:
                m1_bars.append(current_bar)
                current_bar = {"ts": minute_ms, "o": price, "h": price, "l": price, "c": price, "v": quote}
            else:
                current_bar["h"] = max(current_bar["h"], price)
                current_bar["l"] = min(current_bar["l"], price)
                current_bar["c"] = price
                current_bar["v"] += quote


# ============================================================
# WINDOW HELPERS
# ============================================================

def get_window(seconds):
    cutoff = now_ms() - seconds * 1000
    buy = sell = 0.0
    bt = st = 0
    count = 0
    with lock:
        for item in reversed(trade_history):
            if item["ts"] < cutoff: break
            buy += item["buy"]; sell += item["sell"]
            bt += item["buy_trade"]; st += item["sell_trade"]
            count += 1
    return buy, sell, count, bt, st

def get_price_return(seconds):
    cutoff = now_ms() - seconds * 1000
    with lock:
        cur = state["price"]
        if cur <= 0: return 0.0
        old = 0.0
        for item in reversed(price_history):
            if item["ts"] <= cutoff:
                old = item["price"]; break
        if old <= 0: return 0.0
        return percent_return(old, cur)

def calc_trade_rate():
    cutoff = now_ms() - 1000
    total = buys = sells = 0
    bq = sq = 0.0
    with lock:
        for item in reversed(trade_history):
            if item["ts"] < cutoff: break
            total += 1
            buys += item["buy_trade"]; sells += item["sell_trade"]
            bq += item["buy"]; sq += item["sell"]
    return total, buys, sells, bq, sq


# ============================================================
# CVD VELOCITY
# ============================================================

def compute_cvd_velocity():
    global _last_cvd_vel_ms, _prev_cvd_for_vel
    now = now_ms()
    with lock:
        cur_cvd = state["cvd"]

    if _last_cvd_vel_ms == 0:
        _last_cvd_vel_ms = now
        _prev_cvd_for_vel = cur_cvd
        return

    dt = (now - _last_cvd_vel_ms) / 1000.0
    if dt < 1.0: return

    vel = (cur_cvd - _prev_cvd_for_vel) / dt
    _prev_cvd_for_vel = cur_cvd
    _last_cvd_vel_ms = now

    with lock:
        state["cvd_velocity"] = vel
        cvd_history.append({"ts": now, "cvd": cur_cvd, "vel": vel})
        if len(cvd_history) >= 3:
            recent = list(cvd_history)[-3:]
            prev_vel = recent[-2]["vel"] if len(recent) >= 2 else vel
            state["cvd_acceleration"] = (vel - prev_vel) / dt


# ============================================================
# FLOW UPDATE
# ============================================================

def update_flow():
    global _last_flow_ms
    now = now_ms()
    if now - _last_flow_ms < FLOW_UPDATE_THROTTLE_MS: return
    _last_flow_ms = now

    windows = {s: get_window(s) for s in [1, 3, 5, 15, 30, 60]}

    with lock:
        for s, vals in windows.items():
            buy, sell, count, bt, st = vals
            state[f"buy_quote_{s}"] = buy
            state[f"sell_quote_{s}"] = sell
            state[f"delta_{s}"] = buy - sell
            state[f"trades_{s}"] = count

        total, buys, sells, bq, sq = calc_trade_rate()
        state["trade_rate"] = float(total)
        state["buy_rate"] = float(buys)
        state["sell_rate"] = float(sells)
        state["buy_trades_1"] = buys
        state["sell_trades_1"] = sells
        state["buy_quote_rate"] = bq
        state["sell_quote_rate"] = sq

        f1  = imbalance(windows[1][0],  windows[1][1])
        f3  = imbalance(windows[3][0],  windows[3][1])
        f5  = imbalance(windows[5][0],  windows[5][1])
        f15 = imbalance(windows[15][0], windows[15][1])
        trade_flow = f1*0.40 + f3*0.30 + f5*0.20 + f15*0.10

        r1 = get_price_return(1)
        r5 = get_price_return(5)
        r15 = get_price_return(15)
        state["price_return_1"] = r1
        state["price_return_5"] = r5
        state["price_return_15"] = r15

        prev_vel = state["velocity"]
        state["velocity"] = r5
        state["acceleration"] = r5 - prev_vel

        book_flow = state["book_imbalance"]
        depth_flow = state["depth_imbalance"]
        price_comp = clamp(r5 / 0.10, -1.0, 1.0)

        final_flow = (trade_flow*0.60 + book_flow*0.15 + depth_flow*0.15 + price_comp*0.10)
        final_flow = clamp(final_flow, -1.0, 1.0)
        score = final_flow * 100.0

        state["flow_score"] = score
        state["buy_score"] = clamp(50.0 + score*0.50, 0.0, 100.0)
        state["sell_score"] = clamp(50.0 - score*0.50, 0.0, 100.0)

        if abs(score) < 12: action = "NEUTRAL"
        elif score >= 12: action = "BUY_FLOW"
        else: action = "SELL_FLOW"
        state["action"] = action

        flow_history.append(score)
        if len(flow_history) >= 2:
            prev = flow_history[-2]
            same_dir = (score > 0 and prev > 0) or (score < 0 and prev < 0)
            if same_dir: state["flow_persistence_count"] += 1
            else: state["flow_persistence_count"] = 1
        else:
            state["flow_persistence_count"] = 1

        pc = min(state["flow_persistence_count"], 20)
        state["flow_persistence"] = clamp(pc / 10.0, 0.0, 1.0)


# ============================================================
# M1 SEED
# ============================================================

def seed_m1_bars():
    data = http_get_json(KLINES_URL, timeout=5.0)
    if not data or not isinstance(data, list): return
    with lock:
        m1_bars.clear()
        for k in data:
            try:
                ts = int(k[0]); o = float(k[1]); h = float(k[2]); l = float(k[3]); c = float(k[4]); v = float(k[5])
                m1_bars.append({"ts": ts, "o": o, "h": h, "l": l, "c": c, "v": c * v})
            except Exception:
                continue
    print(f"[SEED] loaded {len(m1_bars)} M1 bars")


# ============================================================
# FEATURE CALCULATIONS
# ============================================================

def build_volume_profile():
    with lock:
        bars = list(m1_bars)
        if current_bar is not None: bars.append(current_bar)
        if len(bars) < 20: state["vp_valid"] = False; return
        lookback = bars[-VP_LOOKBACK_BARS:] if len(bars) > VP_LOOKBACK_BARS else bars
        bins = VP_BINS
        high = max(b["h"] for b in lookback)
        low = min(b["l"] for b in lookback)
        if high <= low: state["vp_valid"] = False; return
        bin_size = (high - low) / bins
        bin_vol = [0.0] * bins
        for b in lookback:
            br = b["h"] - b["l"]
            if br <= 0 or b["v"] <= 0: continue
            for i in range(bins):
                bl = low + i * bin_size
                bh = bl + bin_size
                ovl = min(b["h"], bh) - max(b["l"], bl)
                if ovl > 0: bin_vol[i] += b["v"] * (ovl / br)
        total = sum(bin_vol)
        if total <= 0: state["vp_valid"] = False; return
        poc_idx = max(range(bins), key=lambda i: bin_vol[i])
        poc = low + (poc_idx + 0.5) * bin_size
        target = total * VP_VALUE_AREA_PCT / 100.0
        left = right = poc_idx
        cum = bin_vol[poc_idx]
        while cum < target and (left > 0 or right < bins - 1):
            lv = bin_vol[left-1] if left > 0 else -1
            rv = bin_vol[right+1] if right < bins - 1 else -1
            if rv >= lv and right < bins - 1: right += 1; cum += bin_vol[right]
            elif left > 0: left -= 1; cum += bin_vol[left]
            else: break
        val = low + left * bin_size
        vah = low + (right + 1) * bin_size
        width = ((vah - val) / poc * 100.0) if poc > 0 else 100.0
        state["vp_poc"] = poc
        state["vp_vah"] = vah
        state["vp_val"] = val
        state["vp_width_pct"] = width
        state["vp_min_p"] = low
        state["vp_max_p"] = high
        state["vp_valid"] = True
        state["vp_update_ms"] = now_ms()

def compute_vroc_and_range():
    with lock:
        bars = list(m1_bars)
        if current_bar is not None: bars.append(current_bar)
        if len(bars) < 22:
            state["vroc"] = 0.0; state["range_exp"] = 0.0; state["atr_m1"] = 0.0; state["atr_update_ms"] = now_ms()
            return
        ref = bars[-(22):-2]
        avg_v = sum(b["v"] for b in ref) / max(1, len(ref))
        cur_v = bars[-1]["v"] + bars[-2]["v"]
        vroc = (cur_v / (avg_v * 2.0)) if avg_v > 0 else 0.0
        recent = bars[-15:]
        atr_sum = sum(b["h"] - b["l"] for b in recent)
        atr = atr_sum / len(recent) if recent else 0.0
        last_range = bars[-1]["h"] - bars[-1]["l"]
        range_exp = (last_range / atr) if atr > 0 else 0.0
        hist = state.get("vroc_history", [])
        hist.append(vroc)
        if len(hist) > 5: hist = hist[-5:]
        state["vroc_history"] = hist
        state["vroc_sustained"] = (sum(1 for v in hist if v >= 3.0) >= 3)
        state["vroc"] = vroc
        state["range_exp"] = range_exp
        state["atr_m1"] = atr
        state["atr_update_ms"] = now_ms()

def compute_session_vwap():
    with lock:
        bars = list(m1_bars)
        if current_bar is not None: bars.append(current_bar)
        if len(bars) < 15: return
        now_sec = int(time.time())
        day_start_ms = ((now_sec // 86400) * 86400) * 1000
        session_bars = [b for b in bars if b["ts"] >= day_start_ms]
        if len(session_bars) < 5: return
        def vwap_of(sample):
            pv = 0.0; vv = 0.0
            for b in sample:
                typ = (b["h"] + b["l"] + b["c"]) / 3.0
                vv += b["v"]; pv += typ * b["v"]
            return (pv / vv) if vv > 0 else 0.0
        vwap_now = vwap_of(session_bars)
        older = session_bars[:-10] if len(session_bars) > 10 else []
        vwap_old = vwap_of(older) if older else vwap_now
        slope = (vwap_now - vwap_old) / 10.0 if vwap_now > 0 else 0.0
        state["vwap"] = vwap_now
        state["vwap_slope"] = slope
        state["vwap_update_ms"] = now_ms()

def _normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def compute_vwap_z():
    with lock:
        bars = list(m1_bars)
        if current_bar is not None: bars.append(current_bar)
        if len(bars) < VWAP_Z_WINDOW + VWAP_EXH_VOL_LEN: return
        window = bars[-VWAP_Z_WINDOW:]
        pv = 0.0; vol_sum = 0.0
        for b in window:
            typ = (b["h"] + b["l"] + b["c"]) / 3.0
            pv += typ * b["v"]; vol_sum += b["v"]
        if vol_sum <= 0: return
        vwap = pv / vol_sum
        var_sum = 0.0
        for b in window:
            typ = (b["h"] + b["l"] + b["c"]) / 3.0
            diff = typ - vwap
            var_sum += (diff * diff) * b["v"]
        var = var_sum / vol_sum
        sd = math.sqrt(var) if var > 0 else 0.0
        price = state["mid_price"] if state["mid_price"] > 0 else state["price"]
        if price <= 0: return
        z = (price - vwap) / sd if sd > 0 else 0.0
        recent = bars[-VWAP_EXH_VOL_LEN:]
        avg_v = sum(b["v"] for b in recent) / len(recent) if recent else 0.0
        cur_v = bars[-1]["v"]
        exh = (avg_v > 0 and cur_v < avg_v * VWAP_EXH_THRESHOLD)
        state["rolling_vwap"] = vwap
        state["vwap_stdev"] = sd
        state["vwap_z"] = z
        state["vwap_exhausted"] = exh
        state["vwap_z_update_ms"] = now_ms()

def compute_vpin():
    with lock:
        bars = list(m1_bars)
        if current_bar is not None: bars.append(current_bar)
        need = VPIN_SIGMA_WINDOW + VPIN_WINDOW + 5
        if len(bars) < need: return
        recent = bars[-need:]
        closes = [b["c"] for b in recent]
        volumes = [b["v"] for b in recent]
        dP = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        if len(dP) < VPIN_SIGMA_WINDOW: return
        sig_slice = dP[-VPIN_SIGMA_WINDOW:]
        mean_s = sum(sig_slice) / len(sig_slice)
        var_s = sum((x - mean_s) ** 2 for x in sig_slice) / len(sig_slice)
        sigma = math.sqrt(var_s)
        if sigma <= 0: return
        vpin_start = len(closes) - VPIN_WINDOW
        if vpin_start < 1: return
        sum_abs = 0.0; sum_imb = 0.0; sum_vol = 0.0
        for i in range(vpin_start, len(closes)):
            if i < 1: continue
            z = (closes[i] - closes[i-1]) / sigma
            buy_frac = _normal_cdf(z)
            imb = volumes[i] * (2.0 * buy_frac - 1.0)
            sum_abs += abs(imb)
            sum_imb += imb
            sum_vol += volumes[i]
        if sum_vol <= 0: return
        vpin = sum_abs / sum_vol
        netflow = sum_imb / sum_vol
        pressure = 50.0 * (1.0 + netflow)
        state["vpin"] = vpin
        state["vpin_pressure"] = pressure
        hist = state.get("vpin_history", [])
        hist.append(vpin)
        if len(hist) > VPIN_HISTORY_MAX: hist = hist[-VPIN_HISTORY_MAX:]
        state["vpin_history"] = hist
        if len(hist) >= 50:
            sorted_h = sorted(hist)
            rank = sum(1 for x in sorted_h if x <= vpin)
            pct = 100.0 * rank / len(sorted_h)
            state["vpin_pct"] = pct
            if pct >= VPIN_ELEVATED_MAX and vpin >= 0.75: state["vpin_regime"] = "Burst"
            elif pct >= VPIN_NORMAL_MAX and vpin >= 0.60: state["vpin_regime"] = "Elevated"
            elif pct >= VPIN_QUIET_MAX: state["vpin_regime"] = "Normal"
            else: state["vpin_regime"] = "Quiet"

def compute_oi_regime():
    with lock:
        oi_chg = state.get("oi_delta_1m", 0.0)
        price_chg = state.get("price_return_5", 0.0)
        if oi_chg > 0.001 and price_chg > 0.02: state["oi_regime"] = "NEW_LONGS"
        elif oi_chg < -0.001 and price_chg > 0.02: state["oi_regime"] = "SHORT_COVERING"
        elif oi_chg > 0.001 and price_chg < -0.02: state["oi_regime"] = "NEW_SHORTS"
        elif oi_chg < -0.001 and price_chg < -0.02: state["oi_regime"] = "LONG_LIQUIDATION"
        else: state["oi_regime"] = "NEUTRAL"

def compute_naked_pocs():
    with lock:
        now = now_ms()
        if now - state.get("last_poc_snapshot_ms", 0) >= POC_SNAPSHOT_INTERVAL_MS:
            if state["vp_valid"] and state["vp_poc"] > 0:
                snap = {"price": state["vp_poc"], "ts": now, "naked": True}
                snaps = state.get("poc_snapshots", [])
                snaps.append(snap)
                if len(snaps) > POC_SNAPSHOT_MAX: snaps = snaps[-POC_SNAPSHOT_MAX:]
                state["poc_snapshots"] = snaps
                state["last_poc_snapshot_ms"] = now
        price = state["price"]
        atr = state.get("atr_m1", 0.0)
        if price <= 0 or atr <= 0: return
        snaps = state.get("poc_snapshots", [])
        for snap in snaps:
            if not snap["naked"]: continue
            if now - snap["ts"] < 60000: continue
            if abs(price - snap["price"]) >= atr * POC_VISITED_ATR_MULT:
                snap["naked"] = False
        near_above = 0.0; near_below = 0.0; cnt = 0
        for snap in snaps:
            if not snap["naked"]: continue
            cnt += 1
            p = snap["price"]
            if p > price:
                if near_above == 0.0 or p < near_above: near_above = p
            elif p < price:
                if near_below == 0.0 or p > near_below: near_below = p
        state["naked_above"] = near_above
        state["naked_below"] = near_below
        state["naked_count"] = cnt

def compute_liquidation_totals():
    now = now_ms()
    cutoff = now - LIQ_WINDOW_MS
    bt = 0.0; st = 0.0; ev = 0
    with lock:
        for item in reversed(liq_history):
            if item["ts"] < cutoff: break
            if item["side"] == "BUY": bt += item["quote"]
            else: st += item["quote"]
            ev += 1
        state["liquidation_buy"] = bt
        state["liquidation_sell"] = st
        state["liquidation_imbalance"] = imbalance(bt, st)
        state["liquidation_events"] = ev
        state["liq_update_ms"] = now


# ============================================================
# REST POLLERS (Binance only, non-blocking)
# ============================================================

def poll_walls():
    while True:
        try:
            data = http_get_json(DEPTH_REST, timeout=2.0)
            if data and "bids" in data and "asks" in data:
                bids = data["bids"][:50]
                asks = data["asks"][:50]
                bb = max(bids, key=lambda x: float(x[0]) * float(x[1]), default=None)
                ba = max(asks, key=lambda x: float(x[0]) * float(x[1]), default=None)
                with lock:
                    if bb:
                        state_binance["bid_wall_price"] = float(bb[0])
                        state_binance["bid_wall_size"]  = float(bb[1])
                    if ba:
                        state_binance["ask_wall_price"] = float(ba[0])
                        state_binance["ask_wall_size"]  = float(ba[1])
                    state_binance["walls_update_ms"] = now_ms()
        except Exception:
            pass
        time.sleep(WALLS_POLL_SEC)

def poll_funding():
    while True:
        try:
            data = http_get_json(FUNDING_URL, timeout=3.0)
            if data and "lastFundingRate" in data:
                with lock:
                    state_binance["funding"] = float(data["lastFundingRate"])
                    state_binance["funding_update_ms"] = now_ms()
        except Exception:
            pass
        time.sleep(FUNDING_POLL_SEC)

def _rolling_oi_delta(now_value, seconds):
    cutoff = now_value - int(seconds * 1000)
    base = None
    for ts, value in reversed(oi_history):
        if ts <= cutoff:
            base = value
            break
    if base is None and oi_history:
        base = oi_history[0][1]
    current = oi_history[-1][1] if oi_history else 0.0
    if base and base > 0.0:
        return (current - base) / base
    return 0.0

def poll_oi():
    while True:
        try:
            data = http_get_json(OI_URL, timeout=3.0)
            if data and "openInterest" in data:
                oi = float(data["openInterest"])
                now_value = now_ms()
                with lock:
                    state_binance["oi"] = oi
                    oi_history.append((now_value, oi))
                    state_binance["oi_delta_10s"] = _rolling_oi_delta(now_value, 10)
                    state_binance["oi_delta_30s"] = _rolling_oi_delta(now_value, 30)
                    state_binance["oi_delta_1m"] = _rolling_oi_delta(now_value, 60)
                    state_binance["oi_update_ms"] = now_value
        except Exception:
            pass
        time.sleep(OI_POLL_SEC)

def poll_ls():
    while True:
        try:
            data = http_get_json(LS_URL, timeout=3.0)
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                with lock:
                    state_binance["long_short_ratio"] = safe_float(item.get("longShortRatio", 0.0))
                    state_binance["long_account"] = safe_float(item.get("longAccount", 0.0))
                    state_binance["short_account"] = safe_float(item.get("shortAccount", 0.0))
                    state_binance["ls_update_ms"] = now_ms()
        except Exception:
            pass
        time.sleep(LS_POLL_SEC)


# ============================================================
# BGBI FAST REACTION — event-driven observation layer
# ============================================================

def _fast_ema(values, period):
    if not values:
        return 0.0
    alpha = 2.0 / (float(period) + 1.0)
    result = float(values[0])
    for value in values[1:]:
        result = alpha * float(value) + (1.0 - alpha) * result
    return result


def _fast_dir_from_closes(closes):
    if len(closes) < 15:
        return 0
    fast = _fast_ema(closes[-80:], 5)
    slow = _fast_ema(closes[-100:], 13)
    if fast > slow:
        return 1
    if fast < slow:
        return -1
    return 0


def _fast_aggregate_closed_bars(bars, minutes, closed_before_ms):
    span = minutes * 60 * 1000
    groups = {}
    for bar in bars:
        ts = int(bar.get("ts", 0))
        bucket = (ts // span) * span
        # Only use fully closed higher-timeframe bars; never infer direction from a future/incomplete bar.
        if bucket + span > closed_before_ms:
            continue
        row = groups.get(bucket)
        if row is None:
            groups[bucket] = {"ts": bucket, "o": bar["o"], "h": bar["h"], "l": bar["l"], "c": bar["c"], "n": 1}
        else:
            row["h"] = max(row["h"], bar["h"])
            row["l"] = min(row["l"], bar["l"])
            row["c"] = bar["c"]
            row["n"] += 1
    required = minutes
    return [groups[k] for k in sorted(groups) if groups[k]["n"] == required]


def _fast_refresh_context_locked(now):
    global _fast_last_context_minute
    cur = current_bar
    if not cur:
        return
    minute_ts = int(cur.get("ts", 0))
    if minute_ts == _fast_last_context_minute and fast_context["updated_ms"] > 0:
        return
    _fast_last_context_minute = minute_ts
    bars = list(m1_bars)
    m1_closes = [float(b["c"]) for b in bars if float(b.get("c", 0)) > 0]
    m5_close_before = (minute_ts // (5 * 60 * 1000)) * (5 * 60 * 1000)
    m15_close_before = (minute_ts // (15 * 60 * 1000)) * (15 * 60 * 1000)
    m5_bars = _fast_aggregate_closed_bars(bars, 5, m5_close_before)
    m15_bars = _fast_aggregate_closed_bars(bars, 15, m15_close_before)
    fast_context["m1_bars_available"] = len(m1_closes)
    fast_context["m5_bars_available"] = len(m5_bars)
    fast_context["m15_bars_available"] = len(m15_bars)
    fast_context["m15_context_ready"] = len(m15_bars) >= 15
    fast_context["m1_dir"] = _fast_dir_from_closes(m1_closes)
    fast_context["m5_dir"] = _fast_dir_from_closes([float(b["c"]) for b in m5_bars])
    fast_context["m15_dir"] = _fast_dir_from_closes([float(b["c"]) for b in m15_bars])
    fast_context["updated_ms"] = now
    fast_context["closed_m1_ts"] = minute_ts - 60_000


def _fast_price_before_locked(cutoff_ms):
    # Samples are bucketed at ~10ms. Return the last observed price no later than cutoff.
    for item in reversed(fast_price_samples):
        if item["ts"] <= cutoff_ms:
            return float(item["price"])
    return 0.0


def _fast_prune_windows_locked(now):
    for window_ms in FAST_WINDOWS_MS:
        q = fast_window_tapes[window_ms]
        totals = fast_window_totals[window_ms]
        cutoff = now - window_ms
        while q and q[0]["ts"] < cutoff:
            old = q.popleft()
            if old["buy"]:
                totals["buy"] -= old["quote"]
            else:
                totals["sell"] -= old["quote"]
            totals["count"] -= 1
        # Guard against tiny floating-point residue after many subtract/add cycles.
        if totals["buy"] < 1e-8: totals["buy"] = 0.0
        if totals["sell"] < 1e-8: totals["sell"] = 0.0
        if totals["count"] < 0: totals["count"] = 0


def _fast_add_trade_to_windows_locked(item):
    now = int(item["ts"])
    _fast_prune_windows_locked(now)
    for window_ms in FAST_WINDOWS_MS:
        q = fast_window_tapes[window_ms]
        totals = fast_window_totals[window_ms]
        q.append(item)
        if item["buy"]: totals["buy"] += item["quote"]
        else: totals["sell"] += item["quote"]
        totals["count"] += 1


def _fast_window_locked(now, window_ms):
    _fast_prune_windows_locked(now)
    totals = fast_window_totals.get(window_ms, {"buy": 0.0, "sell": 0.0, "count": 0})
    buy = float(totals["buy"]); sell = float(totals["sell"]); count = int(totals["count"])
    total = buy + sell
    return (buy / total if total > 0 else 0.0,
            sell / total if total > 0 else 0.0, count, buy, sell)


def _fast_append_log(row):
    try:
        fields = [
            "utc", "event_ms", "source", "signal", "score", "price", "spread",
            "return_250ms_bps", "return_500ms_bps", "return_1000ms_bps",
            "pressure_250ms", "pressure_1000ms", "ticks_250ms", "ticks_500ms", "ticks_1000ms",
            "ofi_ema", "atr_m1", "impulse_atr", "breakout", "m1_dir", "m5_dir", "m15_dir",
            "m1_bars_available", "m5_bars_available", "m15_bars_available", "m15_context_ready",
            "trade_age_ms", "book_age_ms", "processing_us", "reason"
        ]
        output_path = FAST_CSV_FILE
        # Never append rows with a changed schema beneath an older CSV header.
        if output_path.exists() and output_path.stat().st_size > 0:
            with output_path.open("r", newline="", encoding="utf-8-sig") as existing:
                old_header = next(csv.reader(existing), [])
            if old_header != fields:
                output_path = output_path.with_name(f"{output_path.stem}_v2{output_path.suffix}")
                suffix = 2
                while output_path.exists() and output_path.stat().st_size > 0:
                    with output_path.open("r", newline="", encoding="utf-8-sig") as existing:
                        candidate_header = next(csv.reader(existing), [])
                    if candidate_header == fields:
                        break
                    suffix += 1
                    output_path = FAST_CSV_FILE.with_name(f"{FAST_CSV_FILE.stem}_v{suffix}{FAST_CSV_FILE.suffix}")
        need_header = (not output_path.exists()) or output_path.stat().st_size == 0
        with output_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if need_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception as exc:
        print(f"[FAST-CSV] write error: {exc}")


def _fast_evaluate_locked(now, price, spread, source, prior_high, prior_low, started_ns):
    global _fast_last_signal_ms, _fast_last_signal_dir, _fast_last_signal_price
    if price <= 0:
        return None
    while fast_trade_tape and fast_trade_tape[0]["ts"] < now - FAST_TAPE_KEEP_MS:
        fast_trade_tape.popleft()
    while fast_price_samples and fast_price_samples[0]["ts"] < now - FAST_TAPE_KEEP_MS:
        fast_price_samples.popleft()
    _fast_prune_windows_locked(now)

    trade_age = now - int(state_binance.get("last_trade_receive_ms", 0)) if state_binance.get("last_trade_receive_ms", 0) else 999999
    book_age = now - int(state_binance.get("last_book_receive_ms", 0)) if state_binance.get("last_book_receive_ms", 0) else 999999
    atr_age = now - int(state_binance.get("atr_update_ms", 0)) if state_binance.get("atr_update_ms", 0) else 999999
    stored_atr = float(state_binance.get("atr_m1", 0.0))
    atr = max(stored_atr, price * 0.00005) if atr_age <= 15000 else price * 0.0005
    # Bridge ATR/profile calculations are a slower context lane; stale ATR is never reused.
    if trade_age > FAST_MAX_TRADE_AGE_MS or book_age > FAST_MAX_BOOK_AGE_MS:
        fast_state.update({"ok": False, "status": "STALE_FEED", "action": "NEUTRAL", "event_ms": now,
                           "source": source, "price": price, "spread": spread, "reason": "trade/book feed stale",
                           "trade_age_ms": trade_age, "book_age_ms": book_age, "m1_dir": fast_context["m1_dir"],
                           "m5_dir": fast_context["m5_dir"], "m15_dir": fast_context["m15_dir"]})
        return None

    old250 = _fast_price_before_locked(now - FAST_SHORT_WINDOW_MS)
    old500 = _fast_price_before_locked(now - FAST_MEDIUM_WINDOW_MS)
    old1000 = _fast_price_before_locked(now - FAST_LONG_WINDOW_MS)
    ret250 = ((price - old250) / old250 * 10000.0) if old250 > 0 else 0.0
    ret500 = ((price - old500) / old500 * 10000.0) if old500 > 0 else 0.0
    ret1000 = ((price - old1000) / old1000 * 10000.0) if old1000 > 0 else 0.0
    b250, s250, n250, _, _ = _fast_window_locked(now, FAST_SHORT_WINDOW_MS)
    b1s, s1s, n1s, _, _ = _fast_window_locked(now, FAST_LONG_WINDOW_MS)
    b500, s500, n500, _, _ = _fast_window_locked(now, FAST_MEDIUM_WINDOW_MS)
    ofi = float(state_binance.get("ofi_ema", 0.0))
    move250 = abs(price * ret250 / 10000.0)
    move500 = abs(price * ret500 / 10000.0)
    move1000 = abs(price * ret1000 / 10000.0)
    min_move500 = max(atr * FAST_MIN_MOVE_ATR, price * FAST_MIN_MOVE_BPS / 10000.0)
    min_move1000 = max(atr * FAST_MIN_MOVE_ATR * 1.4, price * FAST_MIN_MOVE_BPS * 1.25 / 10000.0)
    impulse_atr = max(move250, move500, move1000) / max(atr, 1e-9)
    buffer = max(spread * FAST_BREAK_BUFFER_SPREAD_MULT, atr * FAST_BREAK_BUFFER_ATR, price * 0.00001)
    break_up = prior_high > 0 and price >= prior_high + buffer
    break_down = prior_low > 0 and price <= prior_low - buffer
    breakout = "BUY" if break_up else ("SELL" if break_down else "NONE")
    direction = 1 if break_up else (-1 if break_down else 0)
    p250 = b250 if direction > 0 else (s250 if direction < 0 else max(b250, s250))
    p1s = b1s if direction > 0 else (s1s if direction < 0 else max(b1s, s1s))
    side_ofi = direction * ofi if direction else 0.0

    score = 0.0
    if direction:
        score += 25.0
        score += 20.0 * min(max(max(move250 / max(min_move500 * 0.75, 1e-9), move500 / max(min_move500, 1e-9), move1000 / max(min_move1000, 1e-9)), 0.0), 1.0)
        score += 20.0 * max(0.0, min(1.0, (p250 - 0.50) / 0.30))
        score += 10.0 * max(0.0, min(1.0, (p1s - 0.50) / 0.25))
        score += 10.0 * max(0.0, min(1.0, side_ofi / 0.35))
        score += 10.0 * min(1.0, n500 / 8.0)
        for key, bonus in (("m1_dir", 7.0), ("m5_dir", 10.0), ("m15_dir", 4.0)):
            ctx = int(fast_context.get(key, 0))
            score += bonus if ctx == direction else (-8.0 if ctx == -direction else 0.0)
    score = round(max(0.0, min(100.0, score)), 2)

    reason = "monitoring"
    action = "NEUTRAL"
    strong_move = impulse_atr >= FAST_STRONG_MOVE_ATR
    impulse_ok = (move250 >= min_move500 * 0.75) or (move500 >= min_move500) or (move1000 >= min_move1000)
    flow_ok = direction != 0 and p250 >= FAST_MIN_PRESSURE_250 and p1s >= FAST_MIN_PRESSURE_1000
    activity_ok = n500 >= FAST_MIN_TRADE_EVENTS_500MS
    context_ok = True
    if direction:
        m1 = int(fast_context.get("m1_dir", 0)); m5 = int(fast_context.get("m5_dir", 0)); m15 = int(fast_context.get("m15_dir", 0))
        if m1 == -direction and not strong_move and score < FAST_STRONG_SCORE:
            context_ok = False
            reason = "M1 context opposed and no strong impulse"
        elif m5 == -direction and not (strong_move and score >= FAST_STRONG_SCORE and m1 == direction):
            context_ok = False
            reason = "M5 thesis opposed"
        elif m15 == -direction and not (score >= FAST_COUNTER_M15_SCORE and m1 == direction and m5 == direction):
            context_ok = False
            reason = "counter-M15 burst not sufficiently proven"

    candidate = bool(direction and impulse_ok and flow_ok and activity_ok and context_ok and score >= FAST_MIN_SCORE)
    row = None
    processing_us = int(max(0, (time.perf_counter_ns() - started_ns) // 1000))
    fast_state.update({
        "ok": True, "status": "SIGNAL" if candidate else "MONITORING", "action": "NEUTRAL",
        "event_ms": now, "source": source, "price": price, "spread": spread, "score": score,
        "pressure_250ms": round(p250, 4), "pressure_1000ms": round(p1s, 4),
        "return_250ms_bps": round(ret250, 4), "return_500ms_bps": round(ret500, 4), "return_1000ms_bps": round(ret1000, 4),
        "ticks_250ms": n250, "ticks_500ms": n500, "ticks_1000ms": n1s,
        "ofi_ema": round(ofi, 4), "atr_m1": round(atr, 6), "impulse_atr": round(impulse_atr, 4),
        "breakout": breakout, "m1_dir": int(fast_context.get("m1_dir", 0)),
        "m5_dir": int(fast_context.get("m5_dir", 0)), "m15_dir": int(fast_context.get("m15_dir", 0)),
        "m1_bars_available": int(fast_context.get("m1_bars_available", 0)),
        "m5_bars_available": int(fast_context.get("m5_bars_available", 0)),
        "m15_bars_available": int(fast_context.get("m15_bars_available", 0)),
        "m15_context_ready": bool(fast_context.get("m15_context_ready", False)),
        "trade_age_ms": trade_age, "book_age_ms": book_age, "processing_us": processing_us,
        "reason": reason if not candidate else "breakout + impulse + flow + MTF context"
    })

    if candidate:
        # Suppress duplicate side signals until there is a genuine pullback or a side change.
        can_emit = (_fast_last_signal_ms == 0 or direction != _fast_last_signal_dir)
        if not can_emit and atr > 0:
            pullback = (_fast_last_signal_dir == 1 and price <= _fast_last_signal_price - atr * FAST_SAME_SIDE_REARM_PULLBACK_ATR) or \
                       (_fast_last_signal_dir == -1 and price >= _fast_last_signal_price + atr * FAST_SAME_SIDE_REARM_PULLBACK_ATR)
            can_emit = pullback and (now - _fast_last_signal_ms >= FAST_SIGNAL_COOLDOWN_MS)
        if can_emit:
            signal = "BUY_CANDIDATE" if direction > 0 else "SELL_CANDIDATE"
            _fast_last_signal_ms = now
            _fast_last_signal_dir = direction
            _fast_last_signal_price = price
            fast_state["last_signal"] = signal
            fast_state["last_signal_ms"] = now
            fast_state["action"] = signal
            fast_state["status"] = "SIGNAL"
            # Freeze the exact candidate metadata. /fast monitoring fields continue
            # changing on every market event, so consumers must not use them as
            # substitutes for the signal that was emitted at this timestamp.
            fast_state["last_signal_snapshot"] = {
                "last_signal": signal, "last_signal_ms": now, "event_ms": now,
                "source": source, "price": price, "spread": spread, "score": score,
                "pressure_250ms": round(p250, 4), "pressure_1000ms": round(p1s, 4),
                "return_250ms_bps": round(ret250, 4), "return_500ms_bps": round(ret500, 4),
                "return_1000ms_bps": round(ret1000, 4), "ticks_250ms": n250,
                "ticks_500ms": n500, "ticks_1000ms": n1s, "ofi_ema": round(ofi, 4),
                "atr_m1": round(atr, 6), "impulse_atr": round(impulse_atr, 4),
                "breakout": breakout, "m1_dir": fast_state["m1_dir"],
                "m5_dir": fast_state["m5_dir"], "m15_dir": fast_state["m15_dir"],
                "m1_bars_available": fast_state.get("m1_bars_available", 0),
                "m5_bars_available": fast_state.get("m5_bars_available", 0),
                "m15_bars_available": fast_state.get("m15_bars_available", 0),
                "m15_context_ready": fast_state.get("m15_context_ready", False),
                "trade_age_ms": trade_age, "book_age_ms": book_age,
                "processing_us": processing_us, "reason": fast_state["reason"],
            }
            row = {
                "utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now / 1000.0)) + f".{now % 1000:03d}Z",
                "event_ms": now, "source": source, "signal": signal, "score": score, "price": price, "spread": spread,
                "return_250ms_bps": round(ret250, 4), "return_500ms_bps": round(ret500, 4), "return_1000ms_bps": round(ret1000, 4),
                "pressure_250ms": round(p250, 4), "pressure_1000ms": round(p1s, 4),
                "ticks_250ms": n250, "ticks_500ms": n500, "ticks_1000ms": n1s,
                "ofi_ema": round(ofi, 4), "atr_m1": round(atr, 6), "impulse_atr": round(impulse_atr, 4),
                "breakout": breakout, "m1_dir": fast_state["m1_dir"], "m5_dir": fast_state["m5_dir"], "m15_dir": fast_state["m15_dir"],
                "m1_bars_available": fast_state.get("m1_bars_available", 0),
                "m5_bars_available": fast_state.get("m5_bars_available", 0),
                "m15_bars_available": fast_state.get("m15_bars_available", 0),
                "m15_context_ready": fast_state.get("m15_context_ready", False),
                "trade_age_ms": trade_age, "book_age_ms": book_age, "processing_us": processing_us,
                "reason": fast_state["reason"]
            }
            print(f"[BGBI-FAST] {signal} score={score:.1f} price={price:.2f} pressure250={p250:.2f} impulseATR={impulse_atr:.3f} M1/M5/M15={fast_state['m1_dir']}/{fast_state['m5_dir']}/{fast_state['m15_dir']} process={processing_us}us")
        else:
            fast_state["action"] = "NEUTRAL"
            fast_state["status"] = "DUPLICATE_SUPPRESSED"
            fast_state["reason"] = "same-side event suppressed; waiting for pullback/re-arm"
    return row


def _fast_on_trade(price, quote, buy_aggressor, recv, event_time):
    if not FAST_REACTION_ENABLED:
        return
    started_ns = time.perf_counter_ns()
    row = None
    with lock:
        _fast_refresh_context_locked(recv)
        while fast_trade_tape and fast_trade_tape[0]["ts"] < recv - FAST_TAPE_KEEP_MS:
            fast_trade_tape.popleft()
        while fast_high_deque and fast_high_deque[0]["ts"] < recv - FAST_RANGE_LOOKBACK_MS:
            fast_high_deque.popleft()
        while fast_low_deque and fast_low_deque[0]["ts"] < recv - FAST_RANGE_LOOKBACK_MS:
            fast_low_deque.popleft()
        # Read the previous range BEFORE inserting this event, so the current trade
        # cannot manufacture its own breakout level.
        prior_high = float(fast_high_deque[0]["price"]) if fast_high_deque else 0.0
        prior_low = float(fast_low_deque[0]["price"]) if fast_low_deque else 0.0
        while fast_high_deque and fast_high_deque[-1]["price"] <= price:
            fast_high_deque.pop()
        fast_high_deque.append({"ts": recv, "price": price})
        while fast_low_deque and fast_low_deque[-1]["price"] >= price:
            fast_low_deque.pop()
        fast_low_deque.append({"ts": recv, "price": price})
        item = {"ts": recv, "event_ms": event_time, "price": price, "quote": quote, "buy": bool(buy_aggressor)}
        fast_trade_tape.append(item)
        bucket = recv // 10
        if fast_price_samples and fast_price_samples[-1].get("bucket") == bucket:
            fast_price_samples[-1]["ts"] = recv
            fast_price_samples[-1]["price"] = price
        else:
            fast_price_samples.append({"ts": recv, "bucket": bucket, "price": price})
        _fast_add_trade_to_windows_locked(item)
        spread = max(0.0, float(state_binance.get("spread", 0.0)))
        row = _fast_evaluate_locked(recv, price, spread, "BINANCE_TRADE", prior_high, prior_low, started_ns)
    if row:
        _fast_append_log(row)


def _fast_on_book(mid, recv, spread):
    if not FAST_REACTION_ENABLED:
        return
    started_ns = time.perf_counter_ns()
    row = None
    with lock:
        _fast_refresh_context_locked(recv)
        while fast_trade_tape and fast_trade_tape[0]["ts"] < recv - FAST_TAPE_KEEP_MS:
            fast_trade_tape.popleft()
        while fast_high_deque and fast_high_deque[0]["ts"] < recv - FAST_RANGE_LOOKBACK_MS:
            fast_high_deque.popleft()
        while fast_low_deque and fast_low_deque[0]["ts"] < recv - FAST_RANGE_LOOKBACK_MS:
            fast_low_deque.popleft()
        prior_high = float(fast_high_deque[0]["price"]) if fast_high_deque else 0.0
        prior_low = float(fast_low_deque[0]["price"]) if fast_low_deque else 0.0
        _fast_prune_windows_locked(recv)
        row = _fast_evaluate_locked(recv, mid, spread, "BINANCE_BOOK", prior_high, prior_low, started_ns)
    if row:
        _fast_append_log(row)


def fast_snapshot():
    with lock:
        out = dict(fast_state)
    # Return a detached copy so callers cannot mutate bridge-owned signal metadata.
    if isinstance(out.get("last_signal_snapshot"), dict):
        out["last_signal_snapshot"] = dict(out["last_signal_snapshot"])
    now = now_ms()
    last_ms = int(out.get("last_signal_ms", 0))
    if not last_ms or now - last_ms > FAST_SIGNAL_TTL_MS:
        out["active_action"] = "NEUTRAL"
    else:
        out["active_action"] = out.get("last_signal", "NEUTRAL")
    out["age_ms"] = max(0, now - int(out.get("event_ms", 0))) if out.get("event_ms", 0) else 999999
    out["signal_age_ms"] = max(0, now - last_ms) if last_ms else 999999
    out["enabled"] = FAST_REACTION_ENABLED
    out["mode"] = "OBSERVE_ONLY_NO_ORDERS"
    return out


# ============================================================
# BINANCE WEBSOCKET HANDLERS
# ============================================================

def process_trade_binance(data):
    try:
        price = safe_float(data.get("p"))
        qty = safe_float(data.get("q"))
        if price <= 0 or qty <= 0: return
        quote = price * qty
        event_time = safe_int(data.get("E", now_ms()))
        recv = now_ms()
        buy_aggr = not bool(data.get("m", False))
        add_trade(event_time, recv, price, quote, buy_aggr, "BINANCE")
        with lock:
            state_binance["price"] = price
            state_binance["last_trade_receive_ms"] = recv
            state_binance["trade_events"] += 1
            state_binance["trade_connected"] = True
            state_binance["server_time_ms"] = event_time
            state_binance["timestamp_ms"] = recv
        _fast_on_trade(price, quote, buy_aggr, recv, event_time)
    except Exception:
        pass

def process_book_binance(data):
    global _prev_book, _prev_ofi, _prev_ofi_ts
    try:
        bid = safe_float(data.get("b"))
        bq = safe_float(data.get("B"))
        ask = safe_float(data.get("a"))
        aq = safe_float(data.get("A"))
        if bid <= 0 or ask <= 0 or ask < bid: return
        recv = now_ms()
        mid = (bid + ask) / 2.0
        sp = ask - bid

        with lock:
            pb = _prev_book
            ofi_raw = 0.0
            add_raw = 0.0
            remove_raw = 0.0
            if pb["bid"] > 0 and pb["ask"] > 0:
                # Best-quote OFI: changes in displayed queue at the touch.
                if bid > pb["bid"]:
                    bid_event = bq
                elif bid == pb["bid"]:
                    bid_event = bq - pb["bid_q"]
                else:
                    bid_event = -pb["bid_q"]

                if ask < pb["ask"]:
                    ask_event = -aq
                elif ask == pb["ask"]:
                    ask_event = -(aq - pb["ask_q"])
                else:
                    ask_event = pb["ask_q"]

                ofi_raw = bid_event + ask_event
                add_raw = max(bq - pb["bid_q"], 0.0) + max(aq - pb["ask_q"], 0.0)
                remove_raw = max(pb["bid_q"] - bq, 0.0) + max(pb["ask_q"] - aq, 0.0)

            denom = max(bq + aq + pb.get("bid_q",0.0) + pb.get("ask_q",0.0), 1e-12)
            ofi_norm = clamp(ofi_raw / denom, -1.0, 1.0)
            prev_ofi = state_binance.get("ofi_ema", 0.0)
            state_binance["ofi_prev"] = state_binance.get("ofi", 0.0)
            state_binance["ofi"] = ofi_norm
            state_binance["ofi_ema"] = 0.72 * prev_ofi + 0.28 * ofi_norm

            if _prev_ofi_ts > 0 and recv > _prev_ofi_ts:
                dt = max((recv - _prev_ofi_ts) / 1000.0, 0.05)
                vel = (ofi_norm - _prev_ofi) / dt
                state_binance["ofi_acceleration"] = vel - state_binance.get("ofi_velocity", 0.0)
                state_binance["ofi_velocity"] = 0.72 * state_binance.get("ofi_velocity", 0.0) + 0.28 * vel
            state_binance["ofi_update_ms"] = recv

            state_binance["microprice"] = ((ask * bq) + (bid * aq)) / max(bq + aq, 1e-12)
            state_binance["liquidity_add"] = add_raw
            state_binance["liquidity_remove"] = remove_raw

            # Absorption proxy: meaningful touch-flow pressure with limited price response.
            p0 = state_binance.get("price", mid)
            price_step = abs(mid - p0) / max(sp, 1e-9)
            response_factor = 1.0 - min(price_step / 2.0, 1.0)
            flow_factor = abs(state_binance.get("flow_score", 0.0)) / 100.0
            ofi_factor = abs(ofi_norm)
            state_binance["absorption_score"] = clamp(100.0 * flow_factor * ofi_factor * response_factor, 0.0, 100.0)

            state_binance["best_bid"] = bid
            state_binance["best_ask"] = ask
            state_binance["mid_price"] = mid
            if state_binance["price"] <= 0: state_binance["price"] = mid
            state_binance["spread"] = sp
            state_binance["spread_bps"] = (sp / mid) * 10000.0 if mid > 0 else 0.0
            state_binance["book_imbalance"] = imbalance(bq, aq)
            state_binance["last_book_receive_ms"] = recv
            state_binance["book_connected"] = True
            state_binance["connected"] = True

            _prev_book = {"bid":bid,"bid_q":bq,"ask":ask,"ask_q":aq,"ts":recv}
            _prev_ofi = ofi_norm
            _prev_ofi_ts = recv
        _fast_on_book(mid, recv, sp)
    except Exception:
        pass

def process_depth_binance(data):
    try:
        bids = data.get("bids", [])[:5]
        asks = data.get("asks", [])[:5]
        bq = aq = 0.0
        bn = an = 0.0
        for lv in bids:
            if isinstance(lv, list) and len(lv) >= 2:
                p = safe_float(lv[0]); q = safe_float(lv[1])
                if p > 0 and q >= 0: bq += q; bn += p * q
        for lv in asks:
            if isinstance(lv, list) and len(lv) >= 2:
                p = safe_float(lv[0]); q = safe_float(lv[1])
                if p > 0 and q >= 0: aq += q; an += p * q
        if bq <= 0 and aq <= 0: return
        with lock:
            state_binance["bid5"] = bq
            state_binance["ask5"] = aq
            state_binance["bid5_notional"] = bn
            state_binance["ask5_notional"] = an
            state_binance["depth_imbalance"] = imbalance(bq, aq)
            state_binance["depth_levels"] = {
                "bids": [[safe_float(lv[0]), safe_float(lv[1])] for lv in bids
                         if isinstance(lv, list) and len(lv) >= 2 and safe_float(lv[0]) > 0 and safe_float(lv[1]) >= 0],
                "asks": [[safe_float(lv[0]), safe_float(lv[1])] for lv in asks
                         if isinstance(lv, list) and len(lv) >= 2 and safe_float(lv[0]) > 0 and safe_float(lv[1]) >= 0],
            }
            state_binance["last_depth_receive_ms"] = now_ms()
            state_binance["depth_connected"] = True
    except Exception:
        pass

def process_liquidation(data):
    try:
        o = data.get("o", data)
        if not isinstance(o, dict): return
        if o.get("s", "") != SYMBOL: return
        side = o.get("S", "")
        price = safe_float(o.get("ap", 0.0)) or safe_float(o.get("p", 0.0))
        qty = safe_float(o.get("q", 0.0))
        if price <= 0 or qty <= 0: return
        with lock:
            liq_history.append({"ts": now_ms(), "side": side, "price": price, "quote": price * qty})
            state_binance["last_liq_receive_ms"] = now_ms()
    except Exception:
        pass


# ============================================================
# COINBASE WEBSOCKET HANDLERS (fallback only, non-blocking)
# ============================================================

def process_trade_coinbase(data):
    try:
        price = safe_float(data.get("price"))
        size = safe_float(data.get("size"))
        if price <= 0 or size <= 0: return
        quote = price * size
        maker_side = data.get("side", "")
        buy_aggr = (maker_side == "sell")
        recv = now_ms()
        add_trade(recv, recv, price, quote, buy_aggr, "COINBASE")
        with lock:
            state_coinbase["price"] = price
            state_coinbase["last_trade_receive_ms"] = recv
            state_coinbase["trade_events"] += 1
            state_coinbase["trade_connected"] = True
    except Exception:
        pass

def process_ticker_coinbase(data):
    try:
        bid = safe_float(data.get("best_bid"))
        ask = safe_float(data.get("best_ask"))
        bq = safe_float(data.get("best_bid_size"))
        aq = safe_float(data.get("best_ask_size"))
        if bid <= 0 or ask <= 0 or ask < bid: return
        recv = now_ms()
        mid = (bid + ask) / 2.0
        sp = ask - bid
        with lock:
            state_coinbase["best_bid"] = bid
            state_coinbase["best_ask"] = ask
            state_coinbase["mid_price"] = mid
            if state_coinbase["price"] <= 0: state_coinbase["price"] = mid
            state_coinbase["spread"] = sp
            state_coinbase["spread_bps"] = (sp / mid) * 10000.0 if mid > 0 else 0.0
            state_coinbase["book_imbalance"] = imbalance(bq, aq)
            state_coinbase["last_book_receive_ms"] = recv
            state_coinbase["book_connected"] = True
            state_coinbase["connected"] = True
    except Exception:
        pass


# ============================================================
# WEBSOCKET LOOPS
# ============================================================

def binance_ws_loop(feed, url):
    while True:
        try:
            print(f"[BINANCE {feed}] connecting...")
            ws = websocket.WebSocketApp(
                url,
                on_open=lambda w: print(f"[BINANCE {feed}] CONNECTED"),
                on_message=lambda w, m: handle_binance_msg(feed, m),
                on_error=lambda w, e: None,
                on_close=lambda w, c, m: print(f"[BINANCE {feed}] closed"),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[BINANCE {feed}] loop error: {e}")
        time.sleep(3)

def handle_binance_msg(feed, msg):
    try:
        data = json.loads(msg)
        if "data" in data and "stream" in data: data = data["data"]
        if not isinstance(data, dict): return
        if feed == "TRADE": process_trade_binance(data)
        elif feed == "BOOK": process_book_binance(data)
        elif feed == "DEPTH": process_depth_binance(data)
        elif feed == "LIQ": process_liquidation(data)
    except Exception:
        pass

def coinbase_ws_loop():
    while True:
        try:
            print("[COINBASE] connecting...")
            def on_open(w):
                print("[COINBASE] CONNECTED - subscribing")
                w.send(COINBASE_SUB)
            def on_message(w, m):
                try:
                    data = json.loads(m)
                    t = data.get("type", "")
                    if t == "match": process_trade_coinbase(data)
                    elif t == "ticker": process_ticker_coinbase(data)
                except Exception:
                    pass
            ws = websocket.WebSocketApp(
                COINBASE_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=lambda w, e: None,
                on_close=lambda w, c, m: print("[COINBASE] closed"),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[COINBASE] loop error: {e}")
        time.sleep(3)


# ============================================================
# BACKGROUND LOOPS
# ============================================================

def feature_loop():
    while True:
        try:
            get_master_state()
            build_volume_profile()
            compute_vroc_and_range()
            compute_session_vwap()
            compute_vpin()
            compute_oi_regime()
            compute_naked_pocs()
            compute_vwap_z()
            compute_liquidation_totals()
            compute_cvd_velocity()
        except Exception as e:
            print(f"[FEAT] {e}")
        time.sleep(VP_POLL_SEC)

def flow_loop():
    while True:
        try:
            get_master_state()
            update_flow()
        except Exception as e:
            print(f"[FLOW] {e}")
        time.sleep(FLOW_UPDATE_THROTTLE_MS / 1000.0)


# ============================================================
# SNAPSHOT & SIGNAL
# ============================================================

def snapshot():
    master = get_master_state()
    with lock:
        cur = now_ms()
        data = dict(master)
        if active_feed == "COINBASE":
            # Coinbase is a transport price/trade fallback; preserve fresh Binance
            # depth/derivative analytics instead of replacing them with zeroed state.
            for key in ("ofi","ofi_ema","ofi_velocity","ofi_acceleration","microprice",
                        "liquidity_add","liquidity_remove","absorption_score",
                        "vp_valid","vp_poc","vp_vah","vp_val","vp_width_pct",
                        "vroc","range_exp","atr_m1","vwap","vwap_slope",
                        "rolling_vwap","vwap_stdev","vwap_z","vwap_exhausted",
                        "vpin","vpin_pct","vpin_regime","vpin_pressure",
                        "oi_regime","oi","oi_delta_10s","oi_delta_30s","oi_delta_1m","funding","long_short_ratio",
                        "liquidation_buy","liquidation_sell","liquidation_imbalance",
                        "bid_wall_price","bid_wall_size","ask_wall_price","ask_wall_size"):
                data[key] = state_binance.get(key, data.get(key))
            data["external_quality"] = 0.70 if state_binance.get("trade_connected") else 0.45
        else:
            data["external_quality"] = 1.0
        if data.get("timestamp_ms", 0) <= 0: data["timestamp_ms"] = cur
        if data.get("server_time_ms", 0) <= 0: data["server_time_ms"] = cur
        data["local_time_ms"] = cur
        data["trade_age_ms"] = cur - data["last_trade_receive_ms"] if data["last_trade_receive_ms"] > 0 else 999999
        data["book_age_ms"] = cur - data["last_book_receive_ms"] if data["last_book_receive_ms"] > 0 else 999999
        data["depth_age_ms"] = cur - data["last_depth_receive_ms"] if data["last_depth_receive_ms"] > 0 else 999999
        data["healthy"] = (data["trade_age_ms"] < HEALTH_MAX_AGE_MS and data["book_age_ms"] < HEALTH_MAX_AGE_MS)
        data["active_feed"] = active_feed
        return data

def build_signal():
    s = snapshot()
    action = s["action"] if s["healthy"] else "NO_DATA"
    return {
        "ok": s["healthy"],
        "symbol": SYMBOL,
        "timestamp_ms": s["timestamp_ms"],
        "price": s["price"],
        "bid": s["best_bid"],
        "ask": s["best_ask"],
        "spread": s["spread"],
        "spread_bps": s["spread_bps"],
        "flow_score": s["flow_score"],
        "ofi": s.get("ofi", 0.0),
        "ofi_ema": s.get("ofi_ema", 0.0),
        "ofi_velocity": s.get("ofi_velocity", 0.0),
        "ofi_acceleration": s.get("ofi_acceleration", 0.0),
        "microprice": s.get("microprice", 0.0),
        "liquidity_add": s.get("liquidity_add", 0.0),
        "liquidity_remove": s.get("liquidity_remove", 0.0),
        "absorption_score": s.get("absorption_score", 0.0),
        "buy_score": s["buy_score"],
        "sell_score": s["sell_score"],
        "action": action,
        "cvd": s["cvd"],
        "cvd_velocity": s.get("cvd_velocity", 0.0),
        "cvd_acceleration": s.get("cvd_acceleration", 0.0),
        "delta_1": s.get("delta_1", 0.0),
        "delta_3": s.get("delta_3", 0.0),
        "delta_5": s.get("delta_5", 0.0),
        "delta_15": s.get("delta_15", 0.0),
        "delta_30": s.get("delta_30", 0.0),
        "delta_60": s.get("delta_60", 0.0),
        "book_imbalance": s["book_imbalance"],
        "bid5": s["bid5"],
        "ask5": s["ask5"],
        "depth_imbalance": s["depth_imbalance"],
        "trade_rate": s["trade_rate"],
        "buy_rate": s["buy_rate"],
        "sell_rate": s["sell_rate"],
        "buy_quote_rate": s["buy_quote_rate"],
        "sell_quote_rate": s["sell_quote_rate"],
        "flow_persistence": s["flow_persistence"],
        "flow_persistence_count": s["flow_persistence_count"],
        "velocity": s["velocity"],
        "acceleration": s["acceleration"],
        "price_return_1": s["price_return_1"],
        "price_return_5": s["price_return_5"],
        "price_return_15": s["price_return_15"],
        "trade_age_ms": s["trade_age_ms"],
        "book_age_ms": s["book_age_ms"],
        "depth_age_ms": s["depth_age_ms"],
        "depth_events": s["depth_events"],
        "connected": s["connected"],
        "active_feed": s["active_feed"],
        "external_quality": s.get("external_quality",1.0),
        "vp_valid": s["vp_valid"],
        "vp_poc": s["vp_poc"],
        "vp_vah": s["vp_vah"],
        "vp_val": s["vp_val"],
        "vp_width_pct": s["vp_width_pct"],
        "vp_min_p": s["vp_min_p"],
        "vp_max_p": s["vp_max_p"],
        "vroc": s["vroc"],
        "vroc_sustained": s.get("vroc_sustained", False),
        "range_exp": s["range_exp"],
        "atr_m1": s["atr_m1"],
        "vwap": s["vwap"],
        "vwap_slope": s["vwap_slope"],
        "bid_wall_price": s["bid_wall_price"],
        "bid_wall_size": s["bid_wall_size"],
        "ask_wall_price": s["ask_wall_price"],
        "ask_wall_size": s["ask_wall_size"],
        "funding": s["funding"],
        "oi": s["oi"],
        "oi_delta_10s": s.get("oi_delta_10s",0.0),
        "oi_delta_30s": s.get("oi_delta_30s",0.0),
        "oi_delta_1m": s["oi_delta_1m"],
        "vpin": s["vpin"],
        "vpin_pct": s["vpin_pct"],
        "vpin_regime": s["vpin_regime"],
        "vpin_pressure": s["vpin_pressure"],
        "oi_regime": s["oi_regime"],
        "naked_above": s.get("naked_above", 0.0),
        "naked_below": s.get("naked_below", 0.0),
        "naked_count": s.get("naked_count", 0),
        "rolling_vwap": s.get("rolling_vwap", 0.0),
        "vwap_stdev": s.get("vwap_stdev", 0.0),
        "vwap_z": s.get("vwap_z", 0.0),
        "vwap_exhausted": s.get("vwap_exhausted", False),
        "long_short_ratio": s.get("long_short_ratio", 0.0),
        "ls_ratio": s.get("long_short_ratio", 0.0),
        "long_account": s.get("long_account", 0.0),
        "short_account": s.get("short_account", 0.0),
        "liquidation_buy": s.get("liquidation_buy", 0.0),
        "liquidation_sell": s.get("liquidation_sell", 0.0),
        "liquidation_imbalance": s.get("liquidation_imbalance", 0.0),
        "liq_buy": s.get("liquidation_buy", 0.0),
        "liq_sell": s.get("liquidation_sell", 0.0),
        "liquidation_events": s.get("liquidation_events", 0),
    }


# ============================================================
# HEALTH
# ============================================================

def get_health():
    s = snapshot()
    return {
        "ok": s["healthy"],
        "active_feed": s["active_feed"],
        "binance_connected": state_binance["connected"],
        "coinbase_connected": state_coinbase["connected"],
        "binance_age_ms": (now_ms() - state_binance["last_trade_receive_ms"]) if state_binance["last_trade_receive_ms"] > 0 else 999999,
        "coinbase_age_ms": (now_ms() - state_coinbase["last_trade_receive_ms"]) if state_coinbase["last_trade_receive_ms"] > 0 else 999999,
        "price": s["price"],
        "last_error": s["last_error"],
    }


# ============================================================
# HTTP HANDLER
# ============================================================

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): return

    def send_json(self, obj):
        body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path
        try:
            if path == "/health":
                self.send_json(get_health()); return
            if path == "/state":
                self.send_json(snapshot()); return
            if path == "/signal":
                self.send_json(build_signal()); return
            if path == "/fusion":
                s = snapshot()
                self.send_json({
                    "ok": s["healthy"],
                    "score": s["flow_score"],
                    "active_feed": s["active_feed"],
                }); return
            if path == "/fast":
                self.send_json(fast_snapshot()); return
            if path == "/global-liquidity":
                self.send_json(global_liquidity_snapshot()); return
            if path == "/":
                self.send_json({
                    "status": "running",
                    "active_feed": active_feed,
                    "version": "BGBI-FAST-REACTION-1.0",
                }); return
            if path == "/vp":
                with lock:
                    self.send_json({
                        "ok": state["vp_valid"],
                        "poc": state["vp_poc"], "vah": state["vp_vah"], "val": state["vp_val"],
                        "width_pct": state["vp_width_pct"],
                        "min_p": state["vp_min_p"], "max_p": state["vp_max_p"],
                        "update_ms": state["vp_update_ms"],
                    }); return
            if path == "/vroc":
                with lock:
                    self.send_json({
                        "ok": True,
                        "vroc": state["vroc"],
                        "vroc_sustained": state.get("vroc_sustained", False),
                        "range_exp": state["range_exp"],
                        "atr_m1": state["atr_m1"],
                    }); return
            if path == "/vwap":
                with lock:
                    self.send_json({
                        "ok": state["vwap"] > 0,
                        "vwap": state["vwap"],
                        "slope": state["vwap_slope"],
                        "update_ms": state["vwap_update_ms"],
                    }); return
            if path == "/liquidity-depth":
                with lock:
                    depth = state_binance.get("depth_levels", {"bids": [], "asks": []})
                    update_ms = state_binance.get("last_depth_receive_ms", 0)
                    age_ms = max(0, now_ms() - update_ms) if update_ms else 999999999
                    bids = depth.get("bids", [])
                    asks = depth.get("asks", [])
                    stale_after_ms = max(100, safe_int(os.getenv("BGBI_HEATMAP_STALE_MS", "1500"), 1500))
                    self.send_json({
                        "ok": bool(bids or asks),
                        "source": "BINANCE_DEPTH5",
                        "update_ms": update_ms,
                        "age_ms": age_ms,
                        "stale": (not update_ms) or age_ms > stale_after_ms,
                        "bids": bids,
                        "asks": asks,
                    }); return
            if path == "/walls":
                with lock:
                    self.send_json({
                        "ok": True,
                        "bid_wall_price": state["bid_wall_price"], "bid_wall_size": state["bid_wall_size"],
                        "ask_wall_price": state["ask_wall_price"], "ask_wall_size": state["ask_wall_size"],
                        "update_ms": state["walls_update_ms"],
                    }); return
            if path == "/funding":
                with lock:
                    self.send_json({
                        "ok": True,
                        "funding": state["funding"], "oi": state["oi"],
                        "oi_delta_1m": state["oi_delta_1m"],
                        "funding_update_ms": state["funding_update_ms"],
                        "oi_update_ms": state["oi_update_ms"],
                    }); return
            if path == "/vpin":
                with lock:
                    self.send_json({
                        "ok": True,
                        "vpin": state["vpin"], "vpin_pct": state["vpin_pct"],
                        "regime": state["vpin_regime"], "pressure": state["vpin_pressure"],
                        "history_len": len(state.get("vpin_history", [])),
                    }); return
            if path == "/oiregime":
                with lock:
                    self.send_json({
                        "ok": True,
                        "regime": state["oi_regime"],
                        "oi_delta": state["oi_delta_1m"],
                        "price_chg": state.get("price_return_5", 0.0),
                    }); return
            if path == "/nakedpoc":
                with lock:
                    self.send_json({
                        "ok": True,
                        "nearest_above": state["naked_above"], "nearest_below": state["naked_below"],
                        "count": state["naked_count"], "price": state["price"],
                    }); return
            if path == "/vwapz":
                with lock:
                    self.send_json({
                        "ok": True,
                        "vwap": state["rolling_vwap"], "stdev": state["vwap_stdev"],
                        "z": state["vwap_z"], "exhausted": state["vwap_exhausted"],
                        "update_ms": state["vwap_z_update_ms"],
                    }); return
            if path == "/futures":
                with lock:
                    self.send_json({
                        "ok": True, "symbol": SYMBOL,
                        "long_short_ratio": state["long_short_ratio"],
                        "ls_ratio": state["long_short_ratio"],
                        "long_account": state["long_account"], "short_account": state["short_account"],
                        "funding": state["funding"], "oi": state["oi"], "oi_delta_10s": state.get("oi_delta_10s",0.0), "oi_delta_30s": state.get("oi_delta_30s",0.0), "oi_delta_1m": state["oi_delta_1m"],
                    }); return
            if path == "/liquidations":
                with lock:
                    self.send_json({
                        "ok": True, "symbol": SYMBOL,
                        "liquidation_buy": state["liquidation_buy"],
                        "liquidation_sell": state["liquidation_sell"],
                        "liq_buy": state["liquidation_buy"], "liq_sell": state["liquidation_sell"],
                        "liquidation_imbalance": state["liquidation_imbalance"],
                        "events": state["liquidation_events"],
                        "window_ms": LIQ_WINDOW_MS,
                        "update_ms": state["liq_update_ms"],
                    }); return
            if path == "/cvd/velocity":
                with lock:
                    hist = list(cvd_history)[-CVD_VELOCITY_WINDOW:]
                    self.send_json({
                        "ok": True,
                        "cvd_now": state["cvd"],
                        "velocity": state["cvd_velocity"],
                        "acceleration": state["cvd_acceleration"],
                        "history": [{"ts": h["ts"], "cvd": h["cvd"], "vel": h["vel"]} for h in hist],
                    }); return
            if path == "/orderflow":
                with lock:
                    self.send_json({
                        "ok": True,
                        "ofi": state["ofi"], "ofi_ema": state["ofi_ema"],
                        "ofi_velocity": state.get("ofi_velocity",0.0),
                        "ofi_acceleration": state.get("ofi_acceleration",0.0),
                        "microprice": state.get("microprice",0.0),
                        "liquidity_add": state.get("liquidity_add",0.0),
                        "liquidity_remove": state.get("liquidity_remove",0.0),
                        "absorption_score": state.get("absorption_score",0.0),
                        "book_imbalance": state["book_imbalance"],
                        "depth_imbalance": state["depth_imbalance"],
                        "bid5": state["bid5"], "ask5": state["ask5"],
                    }); return

            self.send_response(404)
            self.end_headers()
        except Exception as e:
            try:
                self.send_json({"error": str(e)})
            except Exception:
                pass


# ============================================================
# DISPLAY
# ============================================================

def display_loop():
    while True:
        try:
            s = snapshot()
            ok = "OK=True" if s["healthy"] else "OK=False"
            active = s.get("active_feed", "?")
            print(
                f"{ok} | [{active}] | "
                f"BTC={s['price']:,.2f} | "
                f"CVD={s['cvd']:+12.0f} v={s.get('cvd_velocity',0.0):+8.1f} | "
                f"Flow={s['flow_score']:+6.1f} {s['action']:<10} | "
                f"VROC={s['vroc']:5.2f} RE={s['range_exp']:4.2f} | "
                f"Z={s['vwap_z']:+5.2f} | "
                f"VPIN={s['vpin']:.2f} {s['vpin_regime']:<9} | "
                f"OI={s['oi_regime']:<17} | "
                f"LS={s['long_short_ratio']:.2f} | "
                f"T/s={s['trade_rate']:5.1f}"
            )
        except Exception as e:
            print(f"[DISPLAY] {e}")
        time.sleep(DISPLAY_INTERVAL)



# ============================================================
# MULTI-VENUE LIQUIDITY CONFIRMATION (read-only)
# Coinbase L2 + Bybit spot orderbook/trades; Binance remains primary.
# Scores are venue-local normalized evidence, NOT a claim of global/exact liquidity.
# ============================================================
GLOBAL_BOOK_KEEP = 40
GLOBAL_STALE_MS = 2500
GLOBAL_LOCK = threading.RLock()

def _new_venue(name, product):
    return {"name": name, "product": product, "connected": False, "last_ms": 0,
            "price": 0.0, "bid": 0.0, "ask": 0.0, "bids": {}, "asks": {},
            "buy_quote": 0.0, "sell_quote": 0.0, "trade_ms": 0, "book_ms": 0,
            "book_ready": False, "last_error": "starting"}

venue_coinbase = _new_venue("COINBASE", COINBASE_PRODUCT)
venue_bybit = _new_venue("BYBIT", SYMBOL)

def _venue_book_score(v):
    bids, asks = v["bids"], v["asks"]
    if not bids or not asks: return 0.0
    mid = (max(bids) + min(asks)) / 2.0
    if mid <= 0: return 0.0
    # Use notional liquidity within +/- 0.25% of mid, weighted toward near-touch levels.
    bnot = anot = 0.0
    for p, q in bids.items():
        d = (mid-p)/mid
        if 0 <= d <= 0.0025: bnot += p*q*(1.0-d/0.0025*0.5)
    for p, q in asks.items():
        d = (p-mid)/mid
        if 0 <= d <= 0.0025: anot += p*q*(1.0-d/0.0025*0.5)
    den = bnot+anot
    return max(-1.0, min(1.0, (bnot-anot)/den)) if den else 0.0

def _venue_trade_score(v):
    b, s = v["buy_quote"], v["sell_quote"]
    den = b+s
    return max(-1.0, min(1.0, (b-s)/den)) if den else 0.0

def global_liquidity_snapshot():
    now = now_ms()
    with GLOBAL_LOCK:
        venues = []
        for v in (venue_coinbase, venue_bybit):
            age = max(0, now-v["book_ms"]) if v["book_ms"] else 999999999
            tage = max(0, now-v["trade_ms"]) if v["trade_ms"] else 999999999
            fresh = bool(v["book_ready"] and age <= GLOBAL_STALE_MS)
            tscore = _venue_trade_score(v)
            bscore = _venue_book_score(v) if fresh else 0.0
            # Keep order-book and executed-flow evidence distinct.
            score = max(-100.0, min(100.0, 65.0*bscore + 35.0*tscore))
            venues.append({"name":v["name"], "product":v["product"], "connected":bool(v["connected"]),
                "price":v["price"], "bid":v["bid"], "ask":v["ask"],
                "book_age_ms":age, "trade_age_ms":tage, "book_fresh":fresh,
                "book_score":round(100*bscore,2), "trade_score":round(100*tscore,2),
                "score":round(score,2), "bid_levels":len(v["bids"]), "ask_levels":len(v["asks"]),
                "last_error":v["last_error"]})
        usable = [v for v in venues if v["book_fresh"]]
        # Equal venue weighting prevents a single exchange from pretending to represent the market.
        score = sum(v["score"] for v in usable)/len(usable) if usable else 0.0
        disagreement = (max(v["score"] for v in usable)-min(v["score"] for v in usable)) if len(usable)>1 else 0.0
        confidence = min(1.0, len(usable)/2.0) * max(0.0, 1.0-disagreement/150.0)
        return {"ok": bool(usable), "mode":"MULTI_VENUE_REFERENCE_NOT_GLOBAL_EXACT",
            "score":round(score,2), "confidence":round(confidence,3), "usable_venues":len(usable),
            "disagreement":round(disagreement,2), "update_ms":now,
            "venues":venues, "warning":"Venue-local visible liquidity only; not all exchanges or hidden liquidity."}

def _set_level(book, price, qty, max_levels=1000):
    p, q = safe_float(price), safe_float(qty)
    if p <= 0 or q < 0: return
    if q == 0: book.pop(p, None)
    else: book[p] = q
    if len(book) > max_levels:
        # Bound memory; retain the levels closest to the current touch.
        keep = sorted(book.keys(), reverse=(book is venue_coinbase["bids"] or book is venue_bybit["bids"]))[:max_levels]
        allowed = set(keep)
        for k in list(book.keys()):
            if k not in allowed: book.pop(k, None)

def coinbase_global_loop():
    while True:
        try:
            def on_open(ws):
                ws.send(json.dumps({"type":"subscribe","product_ids":[COINBASE_PRODUCT],"channels":["level2","matches","ticker"]}))
                with GLOBAL_LOCK: venue_coinbase["connected"] = True; venue_coinbase["last_error"] = ""
            def on_message(ws, raw):
                try: d=json.loads(raw)
                except Exception: return
                typ=d.get("type",""); recv=now_ms()
                with GLOBAL_LOCK:
                    v=venue_coinbase
                    if typ in ("snapshot","l2update") and (d.get("product_id")==COINBASE_PRODUCT or typ=="snapshot"):
                        if typ=="snapshot":
                            v["bids"].clear(); v["asks"].clear()
                            for p,q in d.get("bids",[]): _set_level(v["bids"],p,q)
                            for p,q in d.get("asks",[]): _set_level(v["asks"],p,q)
                        else:
                            for side,p,q in d.get("changes",[]):
                                _set_level(v["bids"] if side=="buy" else v["asks"],p,q)
                        if v["bids"] and v["asks"]:
                            v["bid"]=max(v["bids"]); v["ask"]=min(v["asks"]); v["price"]=(v["bid"]+v["ask"])/2
                            v["book_ms"]=recv; v["book_ready"]=True
                    elif typ=="ticker":
                        v["bid"]=safe_float(d.get("best_bid"),v["bid"]); v["ask"]=safe_float(d.get("best_ask"),v["ask"])
                        v["price"]=safe_float(d.get("price"),v["price"])
                    elif typ=="match":
                        price=safe_float(d.get("price")); size=safe_float(d.get("size")); side=d.get("side","")
                        q=price*size
                        if q>0:
                            if side=="sell": v["buy_quote"]+=q
                            else: v["sell_quote"]+=q
                            v["trade_ms"]=recv
                            # Decay accumulated quote imbalance to keep it responsive.
                            v["buy_quote"]*=0.995; v["sell_quote"]*=0.995
                    v["connected"]=True
            def on_error(ws, err):
                with GLOBAL_LOCK: venue_coinbase["last_error"]=str(err)[:180]; venue_coinbase["connected"]=False
            def on_close(ws, code, msg):
                with GLOBAL_LOCK: venue_coinbase["connected"]=False
            ws=websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with GLOBAL_LOCK: venue_coinbase["connected"]=False; venue_coinbase["last_error"]=str(e)[:180]
        time.sleep(2)

def bybit_global_loop():
    url="wss://stream.bybit.com/v5/public/spot"
    while True:
        try:
            def on_open(ws):
                ws.send(json.dumps({"op":"subscribe","args":[f"orderbook.50.{SYMBOL}",f"publicTrade.{SYMBOL}"]}))
                with GLOBAL_LOCK: venue_bybit["connected"]=True; venue_bybit["last_error"]=""
            def on_message(ws, raw):
                try: d=json.loads(raw)
                except Exception: return
                topic=d.get("topic",""); data=d.get("data",{}); recv=now_ms()
                with GLOBAL_LOCK:
                    v=venue_bybit
                    if topic.startswith("orderbook.") and isinstance(data,dict):
                        typ=d.get("type","")
                        if typ=="snapshot": v["bids"].clear(); v["asks"].clear()
                        for p,q in data.get("b",[]): _set_level(v["bids"],p,q)
                        for p,q in data.get("a",[]): _set_level(v["asks"],p,q)
                        if v["bids"] and v["asks"]:
                            v["bid"]=max(v["bids"]); v["ask"]=min(v["asks"]); v["price"]=(v["bid"]+v["ask"])/2
                            v["book_ms"]=recv; v["book_ready"]=True
                    elif topic.startswith("publicTrade."):
                        rows=data if isinstance(data,list) else [data]
                        for t in rows:
                            price=safe_float(t.get("p")); size=safe_float(t.get("v")); q=price*size
                            if q>0:
                                if t.get("S")=="Buy": v["buy_quote"]+=q
                                else: v["sell_quote"]+=q
                                v["trade_ms"]=recv
                        v["buy_quote"]*=0.995; v["sell_quote"]*=0.995
                    v["connected"]=True
            def on_error(ws, err):
                with GLOBAL_LOCK: venue_bybit["last_error"]=str(err)[:180]; venue_bybit["connected"]=False
            def on_close(ws, code, msg):
                with GLOBAL_LOCK: venue_bybit["connected"]=False
            ws=websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            with GLOBAL_LOCK: venue_bybit["connected"]=False; venue_bybit["last_error"]=str(e)[:180]
        time.sleep(2)


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 82)
    print(" BTC-BGBI-VAYU Enhanced Multi-Exchange Flow Bridge V1.1")
    print(" Binance primary + Coinbase WebSocket failover")
    print("=" * 82)
    print(" READ-ONLY / NO API KEY / NO TRADING")
    print("=" * 82)
    print()

    print(f"Starting HTTP server on port {PORT} for {SYMBOL}...")
    threading.Thread(
        target=lambda: ThreadingHTTPServer((HOST, PORT), Handler).serve_forever(),
        daemon=True
    ).start()

    print("Seeding M1 bars from Binance REST...")
    seed_m1_bars()

    print("Starting Binance SPOT feeds...")
    threading.Thread(target=binance_ws_loop, args=("TRADE", TRADE_WS), daemon=True).start()
    threading.Thread(target=binance_ws_loop, args=("BOOK",  BOOK_WS),  daemon=True).start()
    threading.Thread(target=binance_ws_loop, args=("DEPTH", DEPTH_WS), daemon=True).start()

    if FUTURES_ENABLED:
        print("Starting Binance FUTURES liquidation feed...")
        threading.Thread(target=binance_ws_loop, args=("LIQ", LIQ_WS), daemon=True).start()
    else:
        print(f"[INFO] Futures endpoints disabled for {SYMBOL}; spot flow remains active.")

    if COINBASE_ENABLED:
        print("Starting Coinbase WebSocket feed (failover)...")
        threading.Thread(target=coinbase_ws_loop, daemon=True).start()
        print("Starting Coinbase Level2 confirmation + Bybit spot orderbook/trades...")
        threading.Thread(target=coinbase_global_loop, daemon=True).start()
    else:
        print(f"[INFO] Coinbase disabled for {SYMBOL}; Binance remains primary.")
    # Bybit is a separate read-only venue reference and remains available.
    threading.Thread(target=bybit_global_loop, daemon=True).start()

    print("Starting Binance REST pollers...")
    threading.Thread(target=poll_walls, daemon=True).start()
    if FUTURES_ENABLED:
        threading.Thread(target=poll_funding, daemon=True).start()
        threading.Thread(target=poll_oi,      daemon=True).start()
        threading.Thread(target=poll_ls,      daemon=True).start()

    print("Starting feature loop...")
    threading.Thread(target=feature_loop, daemon=True).start()

    print("Starting flow loop...")
    threading.Thread(target=flow_loop, daemon=True).start()

    print("Starting display loop...")
    threading.Thread(target=display_loop, daemon=True).start()

    print()
    print("=" * 82)
    print(" BTC-BGBI-VAYU ENHANCED MULTI-EXCHANGE BRIDGE RUNNING")
    print("=" * 82)
    print(f" Symbol: {SYMBOL} | Coinbase: {COINBASE_PRODUCT} | Port: {PORT}")
    print(f" Active feed: {active_feed}")
    print(" Endpoints: /health /state /signal /fusion /fast /global-liquidity /vp /vroc /vwap")
    print("            /walls /funding /vpin /oiregime /nakedpoc /vwapz")
    print("            /futures /liquidations /cvd/velocity /orderflow")
    print("=" * 82)
    print()

    while True:
        time.sleep(1)


WORKER_CONFIGS = [
    # V3.2 Loss Guard basket only: each chart receives its matching exchange symbol.
    {"symbol":"BTCUSDT",  "coinbase":"BTC-USD",  "port":8787, "futures":True},
    {"symbol":"SOLUSDT",  "coinbase":"SOL-USD",  "port":8788, "futures":True},
    {"symbol":"XRPUSDT",  "coinbase":"XRP-USD",  "port":8790, "futures":True},
    {"symbol":"ETHUSDT",  "coinbase":"ETH-USD",  "port":8795, "futures":True},
    # Coinbase USD spot failover isn't configured for BNB in this source; Binance streams remain primary.
    {"symbol":"BNBUSDT",  "coinbase":"",          "port":8796, "futures":True},
]

def _stream_worker_output(proc, symbol):
    try:
        for line in proc.stdout:
            print(f"[{symbol}] {line.rstrip()}", flush=True)
    except Exception:
        pass

def manager_main():
    print("=" * 78)
    print("BGBI FIVE-PAIR CRYPTO BRIDGE — SINGLE LAUNCHER")
    print("Exactly five workers: BTC, SOL, XRP, ETH and BNB; no broker orders are sent by Python.")
    print("=" * 78)
    workers = {}
    try:
        while True:
            for cfg in WORKER_CONFIGS:
                sym = cfg["symbol"]
                p = workers.get(sym)
                if p is not None and p.poll() is None:
                    continue
                env = os.environ.copy()
                env["BGBI_SYMBOL"] = cfg["symbol"]
                env["BGBI_COINBASE_PRODUCT"] = cfg["coinbase"]
                env["BGBI_PORT"] = str(cfg["port"])
                env["BGBI_FUTURES_ENABLED"] = "1" if cfg["futures"] else "0"
                p = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), "--worker"],
                    cwd=str(Path(__file__).resolve().parent),
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1
                )
                workers[sym] = p
                threading.Thread(target=_stream_worker_output, args=(p, sym), daemon=True).start()
                print(f"[MANAGER] {sym} -> http://127.0.0.1:{cfg['port']} (pid={p.pid})", flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print("[MANAGER] stopping all symbol workers...", flush=True)
    finally:
        for p in workers.values():
            if p.poll() is None:
                p.terminate()
        for p in workers.values():
            try:
                p.wait(timeout=5)
            except Exception:
                try: p.kill()
                except Exception: pass

if __name__ == "__main__":
    if "--worker" in sys.argv:
        main()
    else:
        manager_main()