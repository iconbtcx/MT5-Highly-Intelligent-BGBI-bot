#!/usr/bin/env python3
"""BGBI USD Live Hybrid Bridge V1.1

Read-only local HTTP bridge for USD-sensitive FX and gold context.
- Market proxy prices: Yahoo Finance chart endpoint (unofficial; may rate-limit/change).
- News: GDELT DOC 2.1 when available, with official Federal Reserve and BLS RSS fallback.
- GDELT HTTP 429 triggers a six-hour cooldown to avoid repeated throttled requests.
- No trading, broker login, API secrets, or order placement.
- Exposes /health, /state, /signal?symbol=EURUSD, /prices, and /news on 127.0.0.1:8797.

The feed is contextual/adaptive-risk input, not a guaranteed forecast. It must not replace
broker prices, account risk controls, stop-loss protection, or an economic calendar.
"""
from __future__ import annotations
import json
import math
import xml.etree.ElementTree as ET
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

HOST = "127.0.0.1"
PORT = 8797
PRICE_POLL_SECONDS = 60
NEWS_POLL_SECONDS = 600
GDELT_COOLDOWN_SECONDS = 6 * 60 * 60
PRICE_FRESH_SECONDS = 1800      # 30 minutes; price provider can be delayed
MAX_ARTICLES = 50
GDelt_COOLDOWN_UNTIL = 0
GDelt_LAST_WARNING = ""

# Official public RSS feeds used only as a fallback when GDELT is unavailable/rate-limited.
# These provide official releases, not a broad all-publisher news firehose.
OFFICIAL_NEWS_RSS = (
    ("Federal Reserve monetary policy", "https://www.federalreserve.gov/feeds/press_monetary.xml"),
    ("Federal Reserve press releases", "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("BLS latest releases", "https://www.bls.gov/feed/bls_latest.rss"),
    ("BLS employment situation", "https://www.bls.gov/feed/empsit.rss"),
    ("BLS CPI", "https://www.bls.gov/feed/cpi.rss"),
)
USER_AGENT = "BGBI-USD-Live-Hybrid-Bridge/1.0 (local read-only research bridge)"

# Unofficial Yahoo Finance chart tickers. Pair quote conventions matter:
# EURUSD/GBPUSD rising means USD weakening; JPY=X and CAD=X are USDJPY/USDCAD quotes.
TICKERS = {
    "DXY": {"ticker": "DX-Y.NYB", "usd_sign": +1.0, "weight": 0.30},
    "EURUSD": {"ticker": "EURUSD=X", "usd_sign": -1.0, "weight": 0.20},
    "GBPUSD": {"ticker": "GBPUSD=X", "usd_sign": -1.0, "weight": 0.15},
    "USDJPY": {"ticker": "JPY=X", "usd_sign": +1.0, "weight": 0.20},
    "USDCAD": {"ticker": "CAD=X", "usd_sign": +1.0, "weight": 0.15},
    "GOLD": {"ticker": "GC=F", "usd_sign": -1.0, "weight": 0.0},
}

LOCK = threading.RLock()
STATE = {
    "version": "1.0", "ok": False, "stale": True, "status": "starting",
    "price_source": "Yahoo Finance chart endpoint (unofficial)",
    "news_source": "GDELT DOC 2.1 + official Fed/BLS RSS fallback", "news_active_source": "none",
    "news_status": "starting", "news_warning": "", "news_cooldown_until": 0,
    "updated_utc": None, "updated_epoch": 0,
    "price_updated_epoch": 0, "news_updated_epoch": 0,
    "price_feed_ok": False, "news_feed_ok": False,
    "usd_strength_score": 0.0, "usd_price_score": 0.0,
    "news_usd_score": 0.0, "news_confidence": 0.0,
    "risk_level": 0.0, "adaptive_risk_multiplier": 1.0,
    "components": {}, "headlines": [], "last_errors": [],
}

POSITIVE_USD_TERMS = (
    "hawkish", "rate hike", "rates higher", "inflation hotter", "hot inflation",
    "higher than expected", "payrolls beat", "strong jobs", "strong employment",
    "yields rise", "yields climb", "dollar gains", "dollar rises", "strong dollar",
    "fed tightening", "federal reserve signals higher", "interest rates remain high",
    "economic growth accelerates", "inflation accelerates", "job growth accelerates",
)
NEGATIVE_USD_TERMS = (
    "dovish", "rate cut", "cuts rates", "inflation cools", "cooler inflation",
    "below expectations", "payrolls miss", "weak jobs", "weak employment",
    "unemployment rises", "yields fall", "dollar falls", "dollar weakens", "weak dollar",
    "fed easing", "rate cuts", "recession risk rises", "growth slows", "disinflation",
)
HIGH_IMPACT_TERMS = (
    "fomc", "federal reserve", "fed chair", "powell", "cpi", "pce", "nonfarm",
    "payrolls", "unemployment rate", "interest rate decision", "rate decision",
    "gdp", "ppi", "retail sales", "jobless claims", "treasury auction",
)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def http_json(url: str, timeout: float = 8.0):
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        payload = resp.read()
    return json.loads(payload.decode("utf-8"))


def yahoo_series(ticker: str) -> dict:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='^=.-')}?range=5d&interval=5m"
    data = http_json(url, timeout=8.0)
    root = ((data or {}).get("chart") or {})
    if root.get("error"):
        raise RuntimeError(str(root["error"]))
    results = root.get("result") or []
    if not results:
        raise RuntimeError("no chart result")
    obj = results[0]
    ts = obj.get("timestamp") or []
    q = (((obj.get("indicators") or {}).get("quote") or [{}])[0])
    closes = q.get("close") or []
    points = [(int(t), float(c)) for t, c in zip(ts, closes) if t and c is not None and math.isfinite(float(c)) and float(c) > 0]
    if len(points) < 4:
        raise RuntimeError("fewer than four valid 5-minute closes")
    epoch, last = points[-1]
    # 15 minutes ~= three 5m intervals; 60 minutes ~= twelve intervals.
    def ret_pct(back: int) -> float:
        idx = max(0, len(points) - 1 - back)
        old = points[idx][1]
        return (last / old - 1.0) * 100.0 if old > 0 else 0.0
    r15 = ret_pct(3)
    r60 = ret_pct(12)
    # 0.08%/15m and 0.20%/1h are just normalizing scales, not trade thresholds.
    s15 = clamp(r15 / 0.08 * 100.0, -100.0, 100.0)
    s60 = clamp(r60 / 0.20 * 100.0, -100.0, 100.0)
    score = clamp(0.65 * s15 + 0.35 * s60, -100.0, 100.0)
    return {
        "ticker": ticker, "price": last, "source_epoch": epoch,
        "age_seconds": max(0, int(time.time()) - epoch),
        "return_15m_pct": r15, "return_60m_pct": r60,
        "movement_score": score, "observations": len(points),
    }


def _score_news_items(items: list[dict], source: str, warning: str = "") -> dict:
    signed_sum = 0.0
    signed_weight = 0.0
    risk_hits = 0
    out = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        link = str(item.get("url", "")).strip()
        if not title or title.lower() in seen:
            continue
        seen.add(title.lower())
        lower = title.lower()
        pos = sum(1 for term in POSITIVE_USD_TERMS if term in lower)
        neg = sum(1 for term in NEGATIVE_USD_TERMS if term in lower)
        impact = any(term in lower for term in HIGH_IMPACT_TERMS)
        tone = 0.0
        if pos or neg:
            tone = clamp((pos - neg) / max(1.0, pos + neg), -1.0, 1.0)
            signed_sum += tone
            signed_weight += 1.0
        if impact:
            risk_hits += 1
        out.append({
            "title": title[:240], "url": link[:800], "domain": str(item.get("domain", source))[:120],
            "seen_date": str(item.get("seen_date", item.get("seendate", "")))[:40],
            "usd_tone_heuristic": round(tone, 3), "high_impact_keyword": bool(impact),
            "source": source,
        })
        if len(out) >= 20:
            break
    score = (signed_sum / signed_weight * 100.0) if signed_weight else 0.0
    confidence = clamp(signed_weight / 8.0, 0.0, 1.0)
    # This is a headline heuristic only. Official RSS headlines are not a scheduled-event calendar.
    news_risk = min(95.0, 15.0 + 18.0 * min(risk_hits, 4)) if risk_hits else 5.0
    return {"score": score, "confidence": confidence, "risk": news_risk,
            "articles_seen": len(out), "risk_keyword_hits": risk_hits, "articles": out,
            "updated_epoch": int(time.time()), "source": source, "warning": warning}


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def fetch_official_rss() -> list[dict]:
    """Fetch a small fixed list of official Fed/BLS RSS feeds, with a request timeout."""
    items = []
    for source_name, url in OFFICIAL_NEWS_RSS:
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
            with urlopen(req, timeout=8.0) as resp:
                raw = resp.read(2_000_000)
            root = ET.fromstring(raw)
            for node in root.iter():
                if _local_name(node.tag) not in ("item", "entry"):
                    continue
                fields = {}
                for child in list(node):
                    name = _local_name(child.tag)
                    value = (child.text or "").strip()
                    if name == "link" and not value:
                        value = str(child.attrib.get("href", "")).strip()
                    if name not in fields and value:
                        fields[name] = value
                title = fields.get("title", "")
                if not title:
                    continue
                link = fields.get("link", fields.get("guid", ""))
                pub = fields.get("pubdate", fields.get("published", fields.get("updated", fields.get("date", ""))))
                host = url.split("/")[2]
                items.append({"title": title, "url": link, "seen_date": pub, "domain": host, "source_feed": source_name})
        except Exception as exc:
            print(f"[USD NEWS RSS WARN] {source_name}: {type(exc).__name__}: {exc}", flush=True)
    # Deduplicate cross-posted releases and keep first occurrence.
    seen = set()
    deduped = []
    for item in items:
        key = item["title"].strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped[:MAX_ARTICLES]


def get_news() -> dict:
    """Use GDELT when available; on failure or HTTP 429, fall back to official RSS."""
    global GDelt_COOLDOWN_UNTIL, GDelt_LAST_WARNING
    warning = ""
    now = int(time.time())
    gdelt_items = []
    if now >= GDelt_COOLDOWN_UNTIL:
        query = '("Federal Reserve" OR FOMC OR "US dollar" OR USD OR inflation OR CPI OR PCE OR payrolls OR Treasury OR "interest rates")'
        from urllib.parse import urlencode
        params = urlencode({"query": query, "mode": "artlist", "format": "json", "sort": "DateDesc", "maxrecords": str(MAX_ARTICLES), "timespan": "6h"})
        url = "https://api.gdeltproject.org/api/v2/doc/doc?" + params
        try:
            data = http_json(url, timeout=10.0)
            articles = data.get("articles", []) if isinstance(data, dict) else []
            for item in articles:
                if isinstance(item, dict) and str(item.get("title", "")).strip():
                    gdelt_items.append(item)
            if gdelt_items:
                GDelt_LAST_WARNING = ""
                return _score_news_items(gdelt_items, "GDELT DOC 2.1")
            warning = "GDELT returned no usable headlines"
        except HTTPError as exc:
            warning = f"GDELT HTTP {exc.code}: {exc.reason}"
            if exc.code == 429:
                GDelt_COOLDOWN_UNTIL = now + GDELT_COOLDOWN_SECONDS
        except Exception as exc:
            warning = f"GDELT {type(exc).__name__}: {exc}"
        GDelt_LAST_WARNING = warning
    else:
        warning = f"GDELT cooldown active until epoch {GDelt_COOLDOWN_UNTIL}"

    rss_items = fetch_official_rss()
    if rss_items:
        return _score_news_items(rss_items, "Official Federal Reserve/BLS RSS", warning)
    raise RuntimeError(f"GDELT unavailable ({warning}); official RSS fallback returned no headlines")


def update_prices_once():
    parts, errors = {}, []
    for key, spec in TICKERS.items():
        try:
            part = yahoo_series(spec["ticker"])
            parts[key] = part
        except Exception as exc:
            errors.append(f"{key}: {type(exc).__name__}: {exc}")
    with LOCK:
        # Keep latest success for diagnostics, but age-check every component for use.
        old_components = STATE.get("components", {})
        for key, value in old_components.items():
            if key not in parts:
                parts[key] = value
        STATE["components"] = parts
        STATE["last_errors"] = (errors + [e for e in STATE.get("last_errors", []) if "news:" in e])[-12:]
        STATE["price_updated_epoch"] = int(time.time()) if parts else 0
        fresh = {}
        for key, spec in TICKERS.items():
            v = parts.get(key)
            if not v:
                continue
            age = max(0, int(time.time()) - int(v.get("source_epoch", 0)))
            if age <= PRICE_FRESH_SECONDS:
                fresh[key] = v
        numer, denom = 0.0, 0.0
        for key, spec in TICKERS.items():
            if key == "GOLD" or key not in fresh:
                continue
            numer += spec["weight"] * spec["usd_sign"] * float(fresh[key]["movement_score"])
            denom += spec["weight"]
        usd_price_score = clamp(numer / denom, -100.0, 100.0) if denom > 0 else 0.0
        STATE["usd_price_score"] = usd_price_score
        STATE["price_feed_ok"] = bool(fresh) and denom >= 0.30
        STATE["price_feed_components_fresh"] = sorted(fresh.keys())
        STATE["price_age_seconds"] = min((v["age_seconds"] for v in fresh.values()), default=999999)
        STATE["last_errors"] = STATE["last_errors"][-12:]
        _recompute_state_locked()


def update_news_once():
    try:
        n = get_news()
        with LOCK:
            STATE["news_usd_score"] = float(n["score"])
            STATE["news_confidence"] = float(n["confidence"])
            STATE["news_risk_level"] = float(n["risk"])
            STATE["news_feed_ok"] = bool(n.get("articles_seen", 0) > 0)
            STATE["news_active_source"] = str(n.get("source", "unknown"))
            STATE["news_status"] = "ok" if STATE["news_feed_ok"] else "no_headlines"
            STATE["news_warning"] = str(n.get("warning", ""))
            STATE["news_cooldown_until"] = int(GDelt_COOLDOWN_UNTIL)
            STATE["news_updated_epoch"] = int(n["updated_epoch"])
            STATE["headlines"] = n["articles"]
            STATE["news_articles_seen"] = int(n["articles_seen"])
            STATE["news_risk_keyword_hits"] = int(n["risk_keyword_hits"])
            STATE["last_errors"] = [e for e in STATE.get("last_errors", []) if "news:" not in e]
            if STATE["news_warning"]:
                print(f"[USD NEWS FALLBACK] source={STATE['news_active_source']} warning={STATE['news_warning']}", flush=True)
            _recompute_state_locked()
    except Exception as exc:
        with LOCK:
            STATE["news_feed_ok"] = False
            STATE["news_active_source"] = "none"
            STATE["news_status"] = "unavailable"
            STATE["news_warning"] = str(exc)
            STATE["news_cooldown_until"] = int(GDelt_COOLDOWN_UNTIL)
            STATE["last_errors"] = ([e for e in STATE.get("last_errors", []) if "news:" not in e] + [f"news: {type(exc).__name__}: {exc}"])[-12:]
            # Fail neutral for news sentiment; preserve last risk estimate only for visibility.
            STATE["news_usd_score"] = 0.0
            STATE["news_confidence"] = 0.0
            STATE["news_risk_level"] = 5.0
            _recompute_state_locked()


def _recompute_state_locked():
    price_score = float(STATE.get("usd_price_score", 0.0))
    news_score = float(STATE.get("news_usd_score", 0.0))
    news_conf = float(STATE.get("news_confidence", 0.0))
    if STATE.get("price_feed_ok"):
        # News contributes only in proportion to heuristic confidence.
        usd_score = clamp(0.78 * price_score + 0.22 * news_score * news_conf, -100.0, 100.0)
    else:
        usd_score = 0.0
    comp = STATE.get("components", {})
    fresh_returns = [abs(float(comp[k].get("return_15m_pct", 0.0))) for k in comp
                     if k != "GOLD" and int(time.time()) - int(comp[k].get("source_epoch", 0)) <= PRICE_FRESH_SECONDS]
    price_shock = clamp((max(fresh_returns, default=0.0) / 0.25) * 100.0, 0.0, 100.0)
    news_risk = float(STATE.get("news_risk_level", 5.0)) if STATE.get("news_feed_ok") else 5.0
    risk_level = max(5.0, min(100.0, 0.55 * price_shock + 0.45 * news_risk))
    # Never raises size; risk level 0..100 maps to 1.0..0.50 multiplier.
    risk_mult = clamp(1.0 - 0.50 * risk_level / 100.0, 0.50, 1.0)
    stale = not bool(STATE.get("price_feed_ok")) or int(time.time()) - int(STATE.get("price_updated_epoch", 0)) > PRICE_FRESH_SECONDS
    STATE["usd_strength_score"] = round(usd_score, 2)
    STATE["risk_level"] = round(risk_level, 2)
    STATE["adaptive_risk_multiplier"] = round(risk_mult, 3)
    STATE["stale"] = stale
    STATE["ok"] = bool(STATE.get("price_feed_ok")) and not stale
    STATE["updated_epoch"] = int(time.time())
    STATE["updated_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if stale:
        STATE["status"] = "stale_or_price_unavailable"
    elif not bool(STATE.get("news_feed_ok")):
        STATE["status"] = "price_ok_news_degraded"
    elif str(STATE.get("news_active_source", "")).startswith("Official"):
        STATE["status"] = "ok_official_rss_fallback"
    else:
        STATE["status"] = "ok"


def canonical_symbol(raw: str) -> str:
    s = "".join(ch for ch in str(raw).upper() if ch.isalnum())
    # Match common broker prefixes/suffixes without requiring exact equality.
    for key in ("EURUSD", "GBPUSD", "USDJPY", "USDCAD", "XAUUSD", "DE40", "BTCUSD"):
        if key in s:
            return key
    return s


def build_signal(symbol: str) -> dict:
    with LOCK:
        data = json.loads(json.dumps(STATE))
    sym = canonical_symbol(symbol)
    comp = data.get("components", {})
    pair_return_score = 0.0
    if sym in ("EURUSD", "GBPUSD", "USDJPY", "USDCAD"):
        p = comp.get(sym)
        if p and int(time.time()) - int(p.get("source_epoch", 0)) <= PRICE_FRESH_SECONDS:
            pair_return_score = float(p.get("movement_score", 0.0))
        expected_from_usd = -float(data.get("usd_strength_score", 0.0)) if sym in ("EURUSD", "GBPUSD") else float(data.get("usd_strength_score", 0.0))
        pair_score = clamp(0.65 * pair_return_score + 0.35 * expected_from_usd, -100.0, 100.0)
    elif sym == "XAUUSD":
        gold = comp.get("GOLD")
        gold_score = float(gold.get("movement_score", 0.0)) if gold and int(time.time()) - int(gold.get("source_epoch", 0)) <= PRICE_FRESH_SECONDS else 0.0
        pair_score = clamp(0.60 * gold_score - 0.40 * float(data.get("usd_strength_score", 0.0)), -100.0, 100.0)
    elif sym == "DE40":
        # No strong directional claim for the index; risk multiplier can still be informational.
        pair_score = 0.0
    else:
        pair_score = 0.0
    result = {
        "ok": bool(data.get("ok")), "stale": bool(data.get("stale")),
        "symbol": sym, "pair_score": round(pair_score, 2),
        "usd_strength_score": float(data.get("usd_strength_score", 0.0)),
        "usd_price_score": float(data.get("usd_price_score", 0.0)),
        "news_usd_score": float(data.get("news_usd_score", 0.0)),
        "news_confidence": float(data.get("news_confidence", 0.0)),
        "risk_level": float(data.get("risk_level", 0.0)),
        "adaptive_risk_multiplier": float(data.get("adaptive_risk_multiplier", 1.0)),
        "price_feed_ok": bool(data.get("price_feed_ok")),
        "news_feed_ok": bool(data.get("news_feed_ok")),
        "updated_epoch": int(data.get("updated_epoch", 0)),
        "updated_utc": data.get("updated_utc"),
        "source": "Yahoo chart price proxies + GDELT or official Fed/BLS RSS headline heuristic; not an official calendar",
    }
    return result


def price_loop():
    while True:
        try:
            update_prices_once()
            print(f"[USD PRICE] ok={STATE.get('price_feed_ok')} USD={STATE.get('usd_price_score'):+.1f} risk={STATE.get('risk_level'):.1f}x{STATE.get('adaptive_risk_multiplier'):.2f}", flush=True)
        except Exception as exc:
            with LOCK:
                STATE["last_errors"] = ([f"price: {type(exc).__name__}: {exc}"] + STATE.get("last_errors", []))[-12:]
                STATE["price_feed_ok"] = False
                _recompute_state_locked()
            print(f"[USD PRICE ERROR] {type(exc).__name__}: {exc}", flush=True)
        time.sleep(PRICE_POLL_SECONDS)


def news_loop():
    while True:
        update_news_once()
        print(f"[USD NEWS] ok={STATE.get('news_feed_ok')} score={STATE.get('news_usd_score'):+.1f} headlines={STATE.get('news_articles_seen', 0)}", flush=True)
        time.sleep(NEWS_POLL_SECONDS)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, obj, status=200):
        body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/health":
            with LOCK:
                self.send_json({"ok": STATE["ok"], "stale": STATE["stale"], "status": STATE["status"],
                                "price_feed_ok": STATE["price_feed_ok"], "news_feed_ok": STATE["news_feed_ok"],
                                "news_status": STATE["news_status"], "news_active_source": STATE["news_active_source"],
                                "news_warning": STATE["news_warning"], "news_cooldown_until": STATE["news_cooldown_until"],
                                "updated_epoch": STATE["updated_epoch"], "last_errors": STATE["last_errors"]})
            return
        if parsed.path == "/state":
            with LOCK:
                self.send_json(STATE)
            return
        if parsed.path == "/signal":
            symbol = query.get("symbol", [""])[0]
            self.send_json(build_signal(symbol))
            return
        if parsed.path == "/prices":
            with LOCK:
                self.send_json({"ok": STATE["price_feed_ok"], "components": STATE["components"], "usd_price_score": STATE["usd_price_score"]})
            return
        if parsed.path == "/news":
            with LOCK:
                self.send_json({"ok": STATE["news_feed_ok"], "source": STATE["news_active_source"],
                                "status": STATE["news_status"], "warning": STATE["news_warning"],
                                "news_usd_score": STATE["news_usd_score"],
                                "news_confidence": STATE["news_confidence"], "risk_level": STATE["risk_level"],
                                "headlines": STATE["headlines"], "news_updated_epoch": STATE["news_updated_epoch"]})
            return
        self.send_json({"error": "not_found", "endpoints": ["/health", "/state", "/signal?symbol=EURUSD", "/prices", "/news"]}, 404)


def main():
    print("=" * 76)
    print("BGBI USD LIVE HYBRID BRIDGE V1.1 — READ ONLY")
    print(f"Local endpoint: http://{HOST}:{PORT}")
    print("Market source: Yahoo Finance chart endpoint (unofficial, may delay/rate-limit)")
    print("News source: GDELT when available; official Federal Reserve/BLS RSS fallback")
    print("GDELT HTTP 429 triggers a 6-hour cooldown; RSS is polled every 10 minutes")
    print("No trading, API keys, broker credentials, or order placement")
    print("=" * 76, flush=True)
    threading.Thread(target=price_loop, daemon=True, name="usd-price-poller").start()
    threading.Thread(target=news_loop, daemon=True, name="usd-news-poller").start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping USD bridge...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
