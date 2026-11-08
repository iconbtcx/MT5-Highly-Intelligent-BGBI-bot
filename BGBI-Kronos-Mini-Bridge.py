#!/usr/bin/env python3
"""BGBI Kronos-Mini local forecast bridge (read-only; no order execution)."""
from __future__ import annotations
import json, logging, os, sys, time, threading, traceback
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[1]
KRONOS_SOURCE = ROOT / "KronosSource"
sys.path.insert(0, str(KRONOS_SOURCE))

HOST = "127.0.0.1"
PORT = int(os.getenv("BGBI_KRONOS_PORT", "8798"))
LOOKBACK = max(64, min(2048, int(os.getenv("BGBI_KRONOS_LOOKBACK", "256"))))
PRED_LEN = {"M1": 3, "M5": 3, "M15": 2}
# A newly generated forecast on a stale/closed-market candle must not be treated as fresh.
MAX_BAR_AGE_SEC = {"M1": 180, "M5": 900, "M15": 2700}
TF_MAP = {"M1": None, "M5": None, "M15": None}
SYMBOLS = [s.strip() for s in os.getenv(
    "BGBI_KRONOS_SYMBOLS",
    "btcusd,ethusd,bnbusd,solusd,xrpusd,eurusd,gbpusd,usdcad,usdjpy,de40"
).split(",") if s.strip()]
POLL_SECONDS = max(2.0, float(os.getenv("BGBI_KRONOS_WORKER_POLL_SECONDS", "5.0")))
SPREAD_MULTIPLIER = max(1.0, float(os.getenv("BGBI_KRONOS_SPREAD_MULTIPLIER", "2.0")))
SAMPLE_COUNT = max(1, min(5, int(os.getenv("BGBI_KRONOS_SAMPLE_COUNT", "1"))))
LOG_FILE = Path(__file__).resolve().parent / "kronos_bridge.log"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")])
LOG = logging.getLogger("bgbi-kronos")

try:
    import MetaTrader5 as mt5
    import numpy as np
    import pandas as pd
    import torch
    from model import Kronos, KronosTokenizer, KronosPredictor
except Exception as exc:
    LOG.exception("Import failure. Run setup_kronos_bridge.bat first: %s", exc)
    raise

TF_MAP = {"M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15}
CACHE: dict[tuple[str, str], dict] = {}
CACHE_LOCK = threading.RLock()
MT5_LOCK = threading.RLock()
INFERENCE_LOCK = threading.RLock()
MODEL_STATE = {"loaded": False, "error": "not_started", "device": "unknown"}
MT5_STATE = {"connected": False, "error": "not_initialized", "last_ok_epoch": 0}
RESOLVED_SYMBOLS: dict[str, str] = {}
SYMBOL_LIST_CACHE = {"names": [], "updated": 0.0}
STOP_EVENT = threading.Event()
PREDICTOR = None


def norm_symbol(s: str) -> str:
    return "".join(ch for ch in s.casefold() if ch.isalnum())


def utc_iso(epoch: int | float) -> str:
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def initialize_mt5() -> bool:
    with MT5_LOCK:
        if MT5_STATE["connected"]:
            try:
                if mt5.terminal_info() is not None:
                    MT5_STATE["last_ok_epoch"] = int(time.time())
                    return True
            except Exception:
                pass
            MT5_STATE["connected"] = False
        path = os.getenv("BGBI_MT5_PATH", "").strip()
        try:
            ok = mt5.initialize(path=path) if path else mt5.initialize()
            if not ok:
                MT5_STATE["connected"] = False
                MT5_STATE["error"] = f"initialize_failed:{mt5.last_error()}"
                return False
            MT5_STATE["connected"] = True
            MT5_STATE["error"] = ""
            MT5_STATE["last_ok_epoch"] = int(time.time())
            LOG.info("MT5 Python API connected. terminal=%s", mt5.terminal_info())
            return True
        except Exception as exc:
            MT5_STATE["connected"] = False
            MT5_STATE["error"] = repr(exc)
            return False


def resolve_symbol(requested: str) -> str | None:
    if not initialize_mt5():
        return None
    wanted = norm_symbol(requested)
    if not wanted:
        return None
    with MT5_LOCK:
        cached = RESOLVED_SYMBOLS.get(wanted)
        if cached:
            try:
                info = mt5.symbol_info(cached)
                if info is not None:
                    mt5.symbol_select(cached, True)
                    return cached
            except Exception:
                RESOLVED_SYMBOLS.pop(wanted, None)
        now = time.time()
        if (not SYMBOL_LIST_CACHE["names"] or now - SYMBOL_LIST_CACHE["updated"] > 60):
            try:
                all_symbols = mt5.symbols_get()
                SYMBOL_LIST_CACHE["names"] = [x.name for x in all_symbols] if all_symbols else []
                SYMBOL_LIST_CACHE["updated"] = now
            except Exception:
                SYMBOL_LIST_CACHE["names"] = []
        names = SYMBOL_LIST_CACHE["names"]
        exact = [name for name in names if name.casefold() == requested.casefold()]
        if exact:
            name = exact[0]
            RESOLVED_SYMBOLS[wanted] = name
            mt5.symbol_select(name, True)
            return name
        # Permit a broker suffix only if exactly one normalized symbol begins with the requested name.
        candidates = [name for name in names if norm_symbol(name).startswith(wanted)]
        if len(candidates) == 1:
            name = candidates[0]
            RESOLVED_SYMBOLS[wanted] = name
            mt5.symbol_select(name, True)
            return name
        info = mt5.symbol_info(requested)
        if info is not None:
            mt5.symbol_select(info.name, True)
            RESOLVED_SYMBOLS[wanted] = info.name
            return info.name
    return None


def load_predictor() -> None:
    global PREDICTOR
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    try:
        LOG.info("Loading Kronos-Mini + 2k tokenizer on %s (first run may download Hugging Face weights)", device)
        tokenizer = KronosTokenizer.from_pretrained("NeoQuasar/Kronos-Tokenizer-2k")
        model = Kronos.from_pretrained("NeoQuasar/Kronos-mini")
        model.eval()
        tokenizer.eval()
        PREDICTOR = KronosPredictor(model, tokenizer, device=device, max_context=2048)
        MODEL_STATE.update(loaded=True, error="", device=device)
        LOG.info("Kronos-Mini loaded successfully")
    except Exception as exc:
        MODEL_STATE.update(loaded=False, error=repr(exc), device=device)
        LOG.exception("Kronos model load failed")
        raise


def empty_forecast(symbol: str, tf: str, error: str) -> dict:
    return {"symbol": symbol, "tf": tf, "valid": False, "spread_valid": False, "dir": 0,
            "return_pct": 0.0, "range_pct": 0.0, "asof_epoch": 0, "source_age_sec": 999999,
            "updated_epoch": 0, "spread_pct": 0.0, "min_edge_pct": 0.0,
            "error": error}


def get_closed_rates(actual_symbol: str, tf: str):
    if not initialize_mt5():
        raise RuntimeError(f"mt5_not_connected:{MT5_STATE['error']}")
    count = LOOKBACK
    with MT5_LOCK:
        # start_pos=1 deliberately excludes the still-forming candle (position 0).
        rates = mt5.copy_rates_from_pos(actual_symbol, TF_MAP[tf], 1, count)
        if rates is None:
            raise RuntimeError(f"copy_rates_failed:{mt5.last_error()}")
        info = mt5.symbol_info(actual_symbol)
        tick = mt5.symbol_info_tick(actual_symbol)
    if len(rates) < max(64, min(count, 128)):
        raise RuntimeError(f"insufficient_closed_bars:{len(rates)}")
    df = pd.DataFrame(rates).sort_values("time").drop_duplicates("time", keep="last").reset_index(drop=True)
    # MT5 bar timestamps are UTC epoch seconds for the Python API.
    timestamps = pd.to_datetime(df["time"].astype("int64"), unit="s", utc=True)
    volume = df["real_volume"].astype(float) if "real_volume" in df else pd.Series(0.0, index=df.index)
    if not (volume > 0).any() and "tick_volume" in df:
        volume = df["tick_volume"].astype(float)
    inputs = pd.DataFrame({
        "open": df["open"].astype(float), "high": df["high"].astype(float),
        "low": df["low"].astype(float), "close": df["close"].astype(float),
        "volume": volume.clip(lower=0).astype(float),
    })
    # Predictor fills amount when not supplied; do not pretend broker tick-volume is exchange volume.
    latest_close = float(inputs.iloc[-1]["close"])
    asof_epoch = int(df.iloc[-1]["time"])
    bid = float(getattr(tick, "bid", 0.0) or 0.0) if tick else 0.0
    ask = float(getattr(tick, "ask", 0.0) or 0.0) if tick else 0.0
    mid = (bid + ask) / 2 if bid > 0 and ask >= bid else latest_close
    spread_pct = ((ask - bid) / mid * 100.0) if bid > 0 and ask >= bid and mid > 0 else 0.0
    tick_epoch = int(getattr(tick, "time", 0) or 0) if tick else 0
    tick_age = (time.time() - tick_epoch) if tick_epoch > 0 else float("inf")
    spread_valid = bool(bid > 0.0 and ask >= bid and ask > 0.0 and mid > 0.0 and 0.0 <= tick_age <= 180.0)
    interval_mins = {"M1": 1, "M5": 5, "M15": 15}[tf]
    # Kronos' uploaded calc_time_stamps uses the pandas Series .dt accessor.
    # A bare DatetimeIndex has no .dt, so preserve the future timestamps as a Series.
    yts = pd.Series(pd.date_range(start=timestamps.iloc[-1] + pd.Timedelta(minutes=interval_mins),
                                  periods=PRED_LEN[tf], freq=f"{interval_mins}min"), name="timestamp")
    return inputs, timestamps, yts, latest_close, asof_epoch, spread_pct, spread_valid, info


def compute_forecast(requested_symbol: str, tf: str) -> dict:
    key = (norm_symbol(requested_symbol), tf)
    actual = resolve_symbol(requested_symbol)
    if not actual:
        out = empty_forecast(requested_symbol, tf, f"symbol_not_found:{requested_symbol}")
        with CACHE_LOCK:
            CACHE[key] = dict(out)
        return out
    try:
        inputs, xts, yts, last_close, asof_epoch, spread_pct, spread_valid, info = get_closed_rates(actual, tf)
        source_age_sec = int(time.time()) - int(asof_epoch)
        if source_age_sec < -30 or source_age_sec > MAX_BAR_AGE_SEC[tf]:
            stale = empty_forecast(requested_symbol, tf, f"stale_closed_candle:age={source_age_sec}s")
            stale["asof_epoch"] = int(asof_epoch)
            stale["source_age_sec"] = source_age_sec
            with CACHE_LOCK:
                CACHE[key] = dict(stale)
            return stale
        with CACHE_LOCK:
            old = CACHE.get(key)
            if old and int(old.get("asof_epoch", 0)) == asof_epoch and old.get("valid"):
                # Keep one forecast per closed candle, but re-evaluate whether its expected
                # displacement still clears the *current* spread and quote-health check.
                return_pct_cached = float(old.get("return_pct", 0.0))
                min_edge_cached = max(0.0, SPREAD_MULTIPLIER * spread_pct)
                old["spread_pct"] = float(spread_pct)
                old["spread_valid"] = bool(spread_valid)
                old["min_edge_pct"] = float(min_edge_cached)
                old["dir"] = 0
                if spread_valid and abs(return_pct_cached) > min_edge_cached and abs(return_pct_cached) > 0:
                    old["dir"] = 1 if return_pct_cached > 0 else -1
                CACHE[key] = dict(old)
                return dict(old)
        with INFERENCE_LOCK, torch.inference_mode():
            pred = PREDICTOR.predict(df=inputs, x_timestamp=xts, y_timestamp=yts,
                                     pred_len=PRED_LEN[tf], T=1.0, top_p=0.9,
                                     sample_count=SAMPLE_COUNT, verbose=False)
        if pred is None or len(pred) == 0:
            raise RuntimeError("empty_prediction")
        pred = pred.replace([np.inf, -np.inf], np.nan).dropna(subset=["open", "high", "low", "close"])
        if len(pred) == 0:
            raise RuntimeError("prediction_nan")
        last_pred_close = float(pred.iloc[-1]["close"])
        if last_close <= 0 or last_pred_close <= 0:
            raise RuntimeError("invalid_price")
        return_pct = 100.0 * (last_pred_close / last_close - 1.0)
        hi = float(pred["high"].max())
        lo = float(pred["low"].min())
        range_pct = max(0.0, 100.0 * (hi - lo) / last_close)
        min_edge_pct = max(0.0, SPREAD_MULTIPLIER * spread_pct)
        # Direction is neutral unless forecast displacement exceeds a basic round-trip spread proxy.
        direction = 0
        # Fail closed if MT5 cannot provide a valid bid/ask: a zero/unknown spread
        # must never be interpreted as zero trading cost.
        if spread_valid and abs(return_pct) > min_edge_pct and abs(return_pct) > 0:
            direction = 1 if return_pct > 0 else -1
        out = {"symbol": requested_symbol, "tf": tf, "valid": True, "spread_valid": spread_valid,
               "dir": direction, "return_pct": float(return_pct),
               "range_pct": float(range_pct), "asof_epoch": int(asof_epoch),
               "source_age_sec": int(source_age_sec), "updated_epoch": int(time.time()), "spread_pct": float(spread_pct),
               "min_edge_pct": float(min_edge_pct), "error": ""}
        with CACHE_LOCK:
            CACHE[key] = dict(out)
        LOG.info("forecast symbol=%s mt5_symbol=%s tf=%s asof=%s dir=%+d return=%.5f%% range=%.5f%% spread=%.5f%% latency uses samples=%d",
                 requested_symbol, actual, tf, utc_iso(asof_epoch), direction, return_pct, range_pct, spread_pct, SAMPLE_COUNT)
        return out
    except Exception as exc:
        LOG.warning("forecast failed symbol=%s tf=%s: %s", requested_symbol, tf, exc)
        stale = empty_forecast(requested_symbol, tf, repr(exc))
        with CACHE_LOCK:
            CACHE[key] = dict(stale)
        return stale


def worker_loop() -> None:
    LOG.info("forecast worker started; symbols=%s tfs=%s", ",".join(SYMBOLS), ",".join(TF_MAP))
    while not STOP_EVENT.is_set():
        started = time.monotonic()
        if initialize_mt5() and MODEL_STATE["loaded"]:
            for symbol in SYMBOLS:
                if STOP_EVENT.is_set():
                    break
                for tf in ("M1", "M5", "M15"):
                    if STOP_EVENT.is_set():
                        break
                    # compute_forecast returns cached value unless a newer closed bar exists.
                    compute_forecast(symbol, tf)
        else:
            LOG.warning("service not ready: model=%s mt5=%s; will retry", MODEL_STATE["loaded"], MT5_STATE["connected"])
        elapsed = time.monotonic() - started
        STOP_EVENT.wait(max(0.1, POLL_SECONDS - elapsed))


def snapshot(requested_symbol: str) -> dict:
    now = int(time.time())
    rows = {}
    for tf in ("M1", "M5", "M15"):
        with CACHE_LOCK:
            row = dict(CACHE.get((norm_symbol(requested_symbol), tf), empty_forecast(requested_symbol, tf, "not_ready")))
        age = now - int(row.get("updated_epoch", 0)) if int(row.get("updated_epoch", 0)) > 0 else 999999
        row["age_sec"] = age
        # Flat fields keep the MQL5 parser simple; no nested-json parsing is required.
        tag = tf.lower()
        rows[f"{tag}_valid"] = bool(row.get("valid", False))
        rows[f"{tag}_dir"] = int(row.get("dir", 0)) if age >= 0 else 0
        rows[f"{tag}_return_pct"] = float(row.get("return_pct", 0.0))
        rows[f"{tag}_range_pct"] = float(row.get("range_pct", 0.0))
        rows[f"{tag}_asof_epoch"] = int(row.get("asof_epoch", 0))
        rows[f"{tag}_updated_epoch"] = int(row.get("updated_epoch", 0))
        rows[f"{tag}_age_sec"] = int(age)
    # 'ok' means the service and requested symbol route are recognized. Each timeframe has its own valid/fresh flags.
    configured = {norm_symbol(s) for s in SYMBOLS}
    known = norm_symbol(requested_symbol) in configured or norm_symbol(requested_symbol) in RESOLVED_SYMBOLS
    return {"ok": bool(MODEL_STATE["loaded"] and MT5_STATE["connected"] and known), "service": "BGBI-Kronos-Mini",
            "symbol": requested_symbol, "status": "ready" if MODEL_STATE["loaded"] else "model_unavailable",
            "generated_epoch": now, "generated_at_utc": utc_iso(now),
            "model_loaded": bool(MODEL_STATE["loaded"]), "mt5_connected": bool(MT5_STATE["connected"]),
            "device": MODEL_STATE["device"], **rows}


class Handler(BaseHTTPRequestHandler):
    server_version = "BGBI-Kronos-Mini/1.0"
    def log_message(self, fmt, *args):
        LOG.info("http %s - %s", self.client_address[0], fmt % args)
    def _send(self, code: int, payload: dict):
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        parsed = urlparse(self.path)
        args = parse_qs(parsed.query)
        if parsed.path == "/health":
            self._send(200, {"ok": bool(MODEL_STATE["loaded"] and MT5_STATE["connected"]),
                "service": "BGBI-Kronos-Mini", "model_loaded": bool(MODEL_STATE["loaded"]),
                "model_error": MODEL_STATE["error"], "mt5_connected": bool(MT5_STATE["connected"]),
                "mt5_error": MT5_STATE["error"], "device": MODEL_STATE["device"],
                "symbols": SYMBOLS, "timeframes": list(TF_MAP), "time_utc": utc_iso(time.time())})
            return
        if parsed.path == "/snapshot":
            symbol = (args.get("symbol", [""])[0] or "").strip()
            if not symbol or len(symbol) > 80:
                self._send(400, {"ok": False, "error": "symbol_required"}); return
            self._send(200, snapshot(symbol)); return
        self._send(404, {"ok": False, "error": "not_found", "allowed": ["/health", "/snapshot?symbol=..."]})


def main() -> int:
    load_predictor()
    initialize_mt5()
    worker = threading.Thread(target=worker_loop, name="kronos-forecast-worker", daemon=True)
    worker.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    LOG.info("BGBI Kronos-Mini bridge listening on http://%s:%d (forecast-only)", HOST, PORT)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        LOG.info("Ctrl+C: stopping bridge")
    finally:
        STOP_EVENT.set()
        server.shutdown()
        server.server_close()
        with MT5_LOCK:
            try: mt5.shutdown()
            except Exception: pass
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
