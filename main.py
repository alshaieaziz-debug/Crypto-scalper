# main.py  —  MEXC-only scanner with Order-Book Imbalance Shift factor
# Python 3.10+  |  aiohttp + uvloop optional
#
# Quick .env (copy, edit, and export as ENV or use docker-compose):
#
# TELEGRAM_BOT_TOKEN=your_bot_token
# TELEGRAM_CHAT_ID=your_chat_id
#
# # --- MEXC (public data only; trading disabled by default) ---
# MEXC_API_KEY=
# MEXC_API_SECRET=
# MEXC_EXECUTE_TRADES=false
#
# # --- Scan & scoring ---
# SCAN_LIMIT=100
# MIN_CONF=6
# MIN_CONF_15M=6
# MIN_CONF_30M=6
# MIN_CONF_1H=7
# LONG_SIDE_PENALTY=0
#
# VOL_SURGE_MIN=1.25
# OI_DELTA_MIN=0.018
# FUNDING_MAX_ABS=0.0006
# SPREAD_MAX_ABS=0.0005
# DEPTH_1PCT_MIN_USD=50000
#
# TAPE_LOOKBACK_SEC=15
# TAPE_REQUIRE_ALWAYS=true
# TAPE_MIN_NOTIONAL=60000
# TAPE_TILT_STRONG=0.15
# TAPE_TILT_WEAK=0.07
#
# # New: order-book imbalance shift factor (real-time)
# IMB_SHIFT_WINDOW_SEC=1.0
# IMB_SHIFT_MIN_DROP=0.25
# IMB_SHIFT_MIN_SHARE_DELTA=0.05
#
# # Risk & limits
# MAX_RISK_PCT=0.007
# RISK_PER_TRADE_PCT=0.01
# MAX_CONCURRENT_TRADES=5
#
# # Dedup & cooldown
# DEDUP_MIN=3
# COOLDOWN_SEC=600
#
# # Timeouts & early-exit
# WIN_TIMEOUT_15M_MIN=200
# WIN_TIMEOUT_30M_MIN=240
# WIN_TIMEOUT_1H_MIN=360
# EARLY_EXIT_CHECK_FRAC=0.25
# EARLY_EXIT_MIN_MFE_R=0.4
#
# # TP/SL trailing after TP1
# TP1_BE_OFFSET_R=0.25
#
# # Stats
# STATS_DAILY_HOUR=22
# STATE_PATH=/data/state.json
#
# Notes:
# - Uses MEXC endpoints:
#   • /api/v1/contract/detail (symbols, scales)
#   • /api/v1/contract/kline/{symbol}?interval=Min15|Min30|Min60
#   • /api/v1/contract/depth/{symbol}
#   • /api/v1/contract/deals/{symbol}
#   • /api/v1/contract/funding_rate/{symbol}
#   • /api/v1/contract/ticker?symbol=...
# - 'holdVol' from /ticker is used as Open Interest.
# - Alerts are short: side, symbol, TF, entry/SL/TP, confidence, and daily WR.
# - Removed noisy filters: BTC/ETH correlation, "session soft", soft-session breakout block.
# - Order execution code path is stubbed and OFF by default to avoid runtime errors.
#
import os, asyncio, time, json, random, math, signal, hmac, hashlib
from collections import deque, defaultdict
import aiohttp
from aiohttp import web
from datetime import datetime, timezone, timedelta

# ========= ENV CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ========= TUNABLES =========
SCAN_LIMIT            = int(os.getenv("SCAN_LIMIT", "100"))

MIN_CONF              = int(os.getenv("MIN_CONF", "6"))
MIN_CONF_15M          = int(os.getenv("MIN_CONF_15M", str(MIN_CONF)))
MIN_CONF_30M          = int(os.getenv("MIN_CONF_30M", str(MIN_CONF)))
MIN_CONF_1H           = int(os.getenv("MIN_CONF_1H",  str(MIN_CONF)))
LONG_SIDE_PENALTY     = int(os.getenv("LONG_SIDE_PENALTY", "0"))

VOL_SURGE_MIN         = float(os.getenv("VOL_SURGE_MIN", "1.25"))
OI_DELTA_MIN_BASE     = float(os.getenv("OI_DELTA_MIN", "0.018"))
FUNDING_MAX_ABS       = float(os.getenv("FUNDING_MAX_ABS", "0.0006"))
SPREAD_MAX_ABS        = float(os.getenv("SPREAD_MAX_ABS", "0.0005"))
DEPTH_1PCT_MIN_USD    = float(os.getenv("DEPTH_1PCT_MIN_USD", "50000"))

# Tape/CVD tilt
TAPE_LOOKBACK_SEC = int(os.getenv("TAPE_LOOKBACK_SEC", "15"))
TAPE_REQUIRE_ALWAYS = os.getenv("TAPE_REQUIRE_ALWAYS","true").lower()=="true"
TAPE_MIN_NOTIONAL = float(os.getenv("TAPE_MIN_NOTIONAL", "60000"))
TAPE_TILT_STRONG  = float(os.getenv("TAPE_TILT_STRONG", "0.15"))
TAPE_TILT_WEAK    = float(os.getenv("TAPE_TILT_WEAK", "0.07"))

# New: Real-time order-book imbalance shift
IMB_SHIFT_WINDOW_SEC      = float(os.getenv("IMB_SHIFT_WINDOW_SEC","1.0"))
IMB_SHIFT_MIN_DROP        = float(os.getenv("IMB_SHIFT_MIN_DROP","0.25"))  # 25% ask/bid drop
IMB_SHIFT_MIN_SHARE_DELTA = float(os.getenv("IMB_SHIFT_MIN_SHARE_DELTA","0.05"))  # +5% share swing

# Risk & limits
MAX_RISK_PCT          = float(os.getenv("MAX_RISK_PCT","0.007"))
RISK_PER_TRADE_PCT    = float(os.getenv("RISK_PER_TRADE_PCT","0.01"))
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES","5"))

# Dedup & cooldown
DEDUP_MIN   = int(os.getenv("DEDUP_MIN","3"))
COOLDOWN_SEC= int(os.getenv("COOLDOWN_SEC","600"))

# Timeouts & early exit
WIN_TIMEOUT_15M_MIN  = int(os.getenv("WIN_TIMEOUT_15M_MIN", "200"))
WIN_TIMEOUT_30M_MIN  = int(os.getenv("WIN_TIMEOUT_30M_MIN", "240"))
WIN_TIMEOUT_1H_MIN   = int(os.getenv("WIN_TIMEOUT_1H_MIN", "360"))
EARLY_EXIT_CHECK_FRAC = float(os.getenv("EARLY_EXIT_CHECK_FRAC","0.25"))
EARLY_EXIT_MIN_MFE_R  = float(os.getenv("EARLY_EXIT_MIN_MFE_R","0.4"))

# TP / SL trailing
TP1_BE_OFFSET_R = float(os.getenv("TP1_BE_OFFSET_R","0.25"))

# Persistence
STATE_PATH = os.getenv("STATE_PATH", "/data/state.json")

# Telegram safety
TELEGRAM_MAX_LEN = 3800

# ========= CONSTANTS =========
MEXC_BASE = "https://contract.mexc.com"
RIYADH_TZ = timezone(timedelta(hours=3))
TIMEFRAMES = ["15m","30m","1h"]
TF_TO_INTERVAL = {"15m":"Min15","30m":"Min30","1h":"Min60"}
TF_SECONDS = {"15m":900,"30m":1800,"1h":3600}

# ========= STATE =========
kbuf = defaultdict(lambda: {
    '15m': {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_ts': None},
    '30m': {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_ts': None},
    '1h':  {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_ts': None},
})
oi_hist = defaultdict(lambda: deque(maxlen=120))  # per-minute OI samples (timestamp, holdVol)
oi_abs_ema = defaultdict(lambda: 0.0)             # EMA of abs 5m OIΔ for adaptive threshold
last_alert_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})

# BE override (reuse structure if needed later)
BE_OVERRIDE_WINDOW_SEC = int(os.getenv("BE_OVERRIDE_WINDOW_SEC","600"))
last_be_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})
be_override_used_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})

# Stats
stats = {"unique": 0, "wins": 0, "losses": 0, "breakevens": 0, "timeouts": 0}
dedup_seen = deque(maxlen=4000)
active_trades = {}
current_day_keys = set()

# Symbol metadata (scales, precision)
symbol_meta = {}  # sym -> dict(priceScale, amountScale, contractSize, minVol, volUnit)

# Caches & metrics
cache_depth   = {}  # sym -> (ts, json)
cache_ticker  = {}  # sym -> (ts, json)
cache_contracts = None
DEPTH_TTL = 1.0
TICKER_TTL = 0.8

# Per-symbol micro profiles
micro_hist = defaultdict(lambda: {
    "spread": deque(maxlen=240),
    "depth":  deque(maxlen=240),
})

# Metrics
error_counters = defaultdict(int)
event_loop_lag_ms = 0.0
_last_heartbeat_monotonic = None

# Token bucket (MEXC docs: 20 req / 2s per module → conservative ~400/min)
BUCKET_CAPACITY = 400
bucket_tokens = BUCKET_CAPACITY
bucket_refill_rate_per_ms = BUCKET_CAPACITY / 60000.0
bucket_last_refill_ms = 0
bucket_lock = asyncio.Lock()

def now_ms(): return int(time.time()*1000)
def riyadh_now(): return datetime.now(RIYADH_TZ)

def seconds_until_riyadh(hour=22, minute=0):
    now = riyadh_now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds(), target

def tf_timeout_minutes(tf:str)->int:
    if tf == "15m": return WIN_TIMEOUT_15M_MIN
    if tf == "30m": return WIN_TIMEOUT_30M_MIN
    return WIN_TIMEOUT_1H_MIN

def sanitize_md(s: str) -> str:
    return s.replace("_", "\\_").replace("-", "\\-")

# ========= PRECISION HELPERS =========
def round_price(sym, px: float) -> float:
    meta = symbol_meta.get(sym, {})
    scale = int(meta.get("priceScale", 4))
    factor = 10 ** scale
    return math.floor(px * factor + 0.5) / factor

def trade_key(sym, tf, direction, entry):
    return f"{sym}|{tf}|{direction}|{round_price(sym, entry):.10f}"

def percentile(arr, p):
    if not arr: return None
    a = sorted(arr)
    k = (len(a)-1) * (p/100.0)
    f = math.floor(k); c = math.ceil(k)
    if f == c: return a[int(k)]
    return a[f]*(c-k) + a[c]*(k-f)

# ========= TOKEN BUCKET =========
async def bucket_acquire(tokens=1):
    global bucket_tokens, bucket_last_refill_ms
    async with bucket_lock:
        now = now_ms()
        if bucket_last_refill_ms == 0:
            bucket_last_refill_ms = now
        else:
            elapsed = max(0, now - bucket_last_refill_ms)
            bucket_tokens = min(BUCKET_CAPACITY, bucket_tokens + elapsed * bucket_refill_rate_per_ms)
            bucket_last_refill_ms = now
        if bucket_tokens >= tokens:
            bucket_tokens -= tokens
            return
        needed = tokens - bucket_tokens
        wait_ms = needed / bucket_refill_rate_per_ms
        await asyncio.sleep(wait_ms/1000.0)
        # refill
        now = now_ms()
        elapsed = max(0, now - bucket_last_refill_ms)
        bucket_tokens = min(BUCKET_CAPACITY, bucket_tokens + elapsed * bucket_refill_rate_per_ms)
        bucket_last_refill_ms = now
        bucket_tokens = max(0, bucket_tokens - tokens)

# ========= HTTP HELPERS =========
async def http_request(session, method, url, params=None, headers=None, timeout=10, weight_cost=1, json_body=None):
    await bucket_acquire(tokens=weight_cost)
    base = 0.25
    attempts = 5
    for attempt in range(attempts):
        try:
            req = session.get if method == "GET" else session.post
            async with req(url, params=params, headers=headers, timeout=timeout, json=json_body) as r:
                if r.status == 200:
                    ctype = r.headers.get("Content-Type","")
                    if "application/json" in ctype or "json" in ctype or ctype == "":
                        return await r.json(content_type=None)
                    return await r.text()
                error_counters[str(r.status)] += 1
                if r.status in (418, 429):
                    await asyncio.sleep(1.0 + random.random()*0.8)
                elif 500 <= r.status < 600:
                    await asyncio.sleep((2**attempt) * base)
                else:
                    try:
                        txt = await r.text()
                        print(f"HTTP {r.status} {url} params={params} resp={txt[:200]}")
                    except Exception:
                        pass
                    return None
        except Exception:
            error_counters["exception"] += 1
            await asyncio.sleep((2**attempt) * base)
    return None

async def http_json(session, url, params=None, headers=None, timeout=10, weight_cost=1):
    return await http_request(session, "GET", url, params=params, headers=headers, timeout=timeout, weight_cost=weight_cost)

async def http_json_post(session, url, payload=None, headers=None, timeout=10, weight_cost=1):
    return await http_request(session, "POST", url, headers=headers, timeout=timeout, weight_cost=weight_cost, json_body=payload)

# ========= TELEGRAM =========
async def tg_send(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": sanitize_md(text), "parse_mode": "Markdown"}
    j = await http_json_post(session, url, payload, timeout=8, weight_cost=1)
    if j is None:
        print("Telegram send failed")

async def tg_send_batched(session, messages):
    if not messages:
        return
    batch = ""
    for msg in messages:
        msg = sanitize_md(msg)
        if len(batch) + len(msg) + 2 > TELEGRAM_MAX_LEN:
            await tg_send(session, batch)
            batch = msg
        else:
            if batch:
                batch += "\n\n"
            batch += msg
    if batch:
        await tg_send(session, batch)

# ========= MEXC WRAPPERS (public market endpoints) =========
async def mx_contracts(session):
    return await http_json(session, f"{MEXC_BASE}/api/v1/contract/detail", weight_cost=1)

async def mx_kline(session, symbol, interval):
    # returns arrays inside "data": time[], open[], close[], high[], low[], vol[], amount[]
    return await http_json(session, f"{MEXC_BASE}/api/v1/contract/kline/{symbol}",
                           params={"interval": interval}, weight_cost=1)

async def mx_depth(session, symbol):
    ts = time.time()
    cached = cache_depth.get(symbol)
    if cached and ts - cached[0] <= DEPTH_TTL:
        return cached[1]
    j = await http_json(session, f"{MEXC_BASE}/api/v1/contract/depth/{symbol}", weight_cost=1)
    if j:
        cache_depth[symbol] = (ts, j)
    return j

async def mx_deals(session, symbol, limit=100):
    return await http_json(session, f"{MEXC_BASE}/api/v1/contract/deals/{symbol}",
                           params={"limit": min(100, max(10, limit))}, weight_cost=1)

async def mx_funding_rate(session, symbol):
    return await http_json(session, f"{MEXC_BASE}/api/v1/contract/funding_rate/{symbol}", weight_cost=1)

async def mx_ticker(session, symbol):
    ts = time.time()
    cached = cache_ticker.get(symbol)
    if cached and ts - cached[0] <= TICKER_TTL:
        return cached[1]
    j = await http_json(session, f"{MEXC_BASE}/api/v1/contract/ticker",
                        params={"symbol": symbol}, weight_cost=1)
    if j:
        cache_ticker[symbol] = (ts, j)
    return j

# ========= DISCOVERY =========
async def discover_top_usdt_perps(session, limit):
    info = await mx_contracts(session)
    if not info or not info.get("data"):
        raise RuntimeError("Failed to load MEXC contracts")
    usdt = []
    for c in info["data"]:
        if c.get("quoteCoin")=="USDT" and c.get("settleCoin")=="USDT" and c.get("state")==0:
            sym = c["symbol"]
            symbol_meta[sym] = {
                "priceScale": c.get("priceScale", 4),
                "amountScale": c.get("amountScale", 0),
                "contractSize": float(c.get("contractSize", 1.0)),
                "minVol": float(c.get("minVol", 1)),
                "volUnit": float(c.get("volUnit", 1))
            }
            usdt.append(sym)
    # rank by 24h amount (needs ticker per symbol)
    sem = asyncio.Semaphore(10)
    rank = []
    async def one(sym):
        async with sem:
            j = await mx_ticker(session, sym)
            if j and j.get("data"):
                d = j["data"]
                amt = float(d.get("amount24", 0))
                rank.append((amt, sym))
    await asyncio.gather(*(one(s) for s in usdt))
    ranked = [s for _, s in sorted(rank, key=lambda x: x[0], reverse=True)]
    return ranked[:limit]

# ========= KLINE HELPERS =========
def parse_kline_arrays(kjson):
    if not kjson or not kjson.get("data"): return []
    d = kjson["data"]
    # ensure equal lengths
    keys = ["time","open","close","high","low","vol"]
    if not all(k in d for k in keys): return []
    n = min(len(d["time"]), len(d["open"]), len(d["close"]), len(d["high"]), len(d["low"]), len(d["vol"]))
    out = []
    for i in range(n):
        out.append({
            "t": int(d["time"][i]) * 1000,  # seconds → ms
            "o": float(d["open"][i]),
            "c": float(d["close"][i]),
            "h": float(d["high"][i]),
            "l": float(d["low"][i]),
            "v": float(d["vol"][i]),
        })
    return out

def next_close_ms(tf):
    s = int(time.time())
    if tf=="15m":  return (s - (s%900)  + 900)*1000
    if tf=="30m":  return (s - (s%1800) + 1800)*1000
    if tf=="1h":   return (s - (s%3600) + 3600)*1000

async def fetch_closed_bar(session, sym, tf):
    arr = parse_kline_arrays(await mx_kline(session, sym, TF_TO_INTERVAL[tf]))
    if len(arr) < 3: return None, None
    # last fully closed is bar where now >= t + tf_sec*1000
    tfms = TF_SECONDS[tf]*1000
    arr_sorted = sorted(arr, key=lambda x: x["t"])
    last = arr_sorted[-1]
    if now_ms() < last["t"] + tfms - 500:  # if last not closed yet, shift back
        return arr_sorted[-3], arr_sorted[-2]
    return arr_sorted[-2], arr_sorted[-1]

# ========= LEVELS / ATR =========
def tf_swing_window(tf:str)->int:
    return {"15m":5, "30m":3, "1h":2}.get(tf, 3)

def pick_swing_levels(tf, highs, lows, closes):
    n = tf_swing_window(tf)
    hi_list = list(highs)[-max(5, n*2):]
    lo_list = list(lows)[-max(5, n*2):]

    filt_hi, filt_lo = [], []
    for i in range(1, len(hi_list)):
        if hi_list[i] <= hi_list[i-1] and lo_list[i] >= lo_list[i-1]:
            continue  # inside bar
        filt_hi.append(hi_list[i]); filt_lo.append(lo_list[i])
    if not filt_hi or not filt_lo:
        filt_hi, filt_lo = hi_list, lo_list

    sl_long = min(filt_lo[-n:]) if len(filt_lo) >= n else (min(filt_lo) if filt_lo else None)
    sl_short= max(filt_hi[-n:]) if len(filt_hi) >= n else (max(filt_hi) if filt_hi else None)
    return sl_long, sl_short

def true_atr(highs, lows, closes, period=14):
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1: return None
    trs = []
    for i in range(n - period, n):
        if i-1 < 0: return None
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1])
        )
        trs.append(tr)
    if not trs: return None
    return sum(trs)/len(trs)

def rr_ok(entry, sl, tp1, min_r=1.0):
    if sl is None: return False
    risk = abs(entry - sl)
    if risk <= 0: return False
    reward = (tp1 - entry) if tp1 > entry else (entry - tp1)
    return (reward / risk) >= min_r

# ========= MICROSTRUCTURE PROFILE =========
def update_micro_profile(sym, spread, depth_usd):
    micro_hist[sym]["spread"].append(max(1e-12, spread))
    micro_hist[sym]["depth"].append(max(0.0, depth_usd))

def adaptive_micro_thresholds(sym):
    s_hist = list(micro_hist[sym]["spread"])
    d_hist = list(micro_hist[sym]["depth"])
    if len(s_hist) < 25 or len(d_hist) < 25:
        return SPREAD_MAX_ABS, DEPTH_1PCT_MIN_USD
    p60_spread = percentile(s_hist, 60) or SPREAD_MAX_ABS
    p40_depth  = percentile(d_hist, 40) or DEPTH_1PCT_MIN_USD
    max_spread = min(max(SPREAD_MAX_ABS, p60_spread * 1.2), SPREAD_MAX_ABS * 2.0)
    min_depth  = max(min(DEPTH_1PCT_MIN_USD, p40_depth * 0.9), DEPTH_1PCT_MIN_USD * 0.6)
    return max_spread, min_depth

def depth_summarize(orderbook):
    # returns spread, bid_usd (≤ +1%), ask_usd (≤ +1%), total_usd
    asks = orderbook.get("asks") or []
    bids = orderbook.get("bids") or []
    if not asks or not bids: return None
    best_ask = float(asks[0][0]); best_bid = float(bids[0][0])
    mid = (best_ask + best_bid)/2
    spread = (best_ask - best_bid)/max(1e-12, mid)
    up_lim  = best_ask * 1.01
    dn_lim  = best_bid * 0.99
    ask_usd = sum(float(p)*float(q) for p,q,*_ in asks if float(p)<=up_lim)
    bid_usd = sum(float(p)*float(q) for p,q,*_ in bids if float(p)>=dn_lim)
    total = ask_usd + bid_usd
    return spread, bid_usd, ask_usd, total

# ========= OI ADAPTIVE THRESHOLD (using holdVol from ticker) =========
def _oi_5m_change_pct_from_queue(q):
    if len(q) < 5: return None
    newest_ts, newest_oi = q[-1]
    target = newest_ts - 5*60*1000 - 3000
    older = None
    for i in range(len(q)-1, -1, -1):
        ts, val = q[i]
        if ts <= target:
            older = (ts, val); break
    if not older: old_ts, old_oi = q[0]
    else:         old_ts, old_oi = older
    if old_oi == 0: return None
    return (newest_oi - old_oi) / old_oi

def adaptive_oi_threshold(sym):
    q = oi_hist[sym]
    newest = _oi_5m_change_pct_from_queue(q)
    if newest is not None:
        a = 0.25
        oi_abs_ema[sym] = a*abs(newest) + (1-a)*oi_abs_ema[sym]
    ema = oi_abs_ema[sym]
    if ema == 0.0 and len(q) < 11:
        return OI_DELTA_MIN_BASE
    return max(OI_DELTA_MIN_BASE, 0.75*ema)

# ========= TAPE / CVD tilt via /deals =========
async def tape_tilt(session, sym):
    j = await mx_deals(session, sym, limit=100)
    if not j or not j.get("data"): return 0.0, 0.0
    nowt = now_ms()
    buy_n = 0.0; sell_n = 0.0
    for t in j["data"]:
        # doc: T int deal type,1:purchase (buy), 2:sell; p price, v quantity, t timestamp ms
        ts  = int(t.get("t", nowt))
        if nowt - ts > TAPE_LOOKBACK_SEC*1000:  # keep within window
            continue
        price = float(t["p"]); qty = float(t["v"])
        notional = price * qty
        if int(t.get("T", 0)) == 1: buy_n += notional
        elif int(t.get("T", 0)) == 2: sell_n += notional
    total = buy_n + sell_n
    if total <= 0: return 0.0, 0.0
    return (buy_n - sell_n)/total, total

# ========= ORDER-BOOK IMBALANCE SHIFT (new factor) =========
async def imbalance_shift(session, sym, direction):
    ob1 = await mx_depth(session, sym)
    if not ob1: return 0.0
    s1 = depth_summarize(ob1)
    if not s1: return 0.0
    _, bid1, ask1, tot1 = s1
    share_bid1 = (bid1 / max(1e-9, (bid1+ask1)))
    await asyncio.sleep(max(0.2, min(2.0, IMB_SHIFT_WINDOW_SEC)))
    ob2 = await mx_depth(session, sym)
    if not ob2: return 0.0
    s2 = depth_summarize(ob2)
    if not s2: return 0.0
    _, bid2, ask2, tot2 = s2
    share_bid2 = (bid2 / max(1e-9, (bid2+ask2)))

    eff = 0.0
    if direction=="Long":
        drop = (ask1 - ask2)/max(1e-9, ask1)
        if drop >= IMB_SHIFT_MIN_DROP or (share_bid2 - share_bid1) >= IMB_SHIFT_MIN_SHARE_DELTA:
            eff = 1.0
    else:
        drop = (bid1 - bid2)/max(1e-9, bid1)
        if drop >= IMB_SHIFT_MIN_DROP or (share_bid1 - share_bid2) >= IMB_SHIFT_MIN_SHARE_DELTA:
            eff = 1.0
    return eff  # 1.0 → strong shift detected, else 0.0

# ========= OI LOOP (ticker.holdVol) =========
async def oi_loop(session, symbols):
    while True:
        sem = asyncio.Semaphore(12)
        t0 = now_ms()
        async def one(sym):
            async with sem:
                j = await mx_ticker(session, sym)
                try:
                    if j and j.get("data") and "holdVol" in j["data"]:
                        oi = float(j["data"]["holdVol"])
                        oi_hist[sym].append((t0, oi))
                except Exception:
                    pass
        await asyncio.gather(*(one(s) for s in symbols))
        # sample roughly each minute
        await asyncio.sleep(60 - (time.time()%60) + 0.02)

# ========= SIGNAL DETECTION =========
def detect_signal(sym, tf, last_closed, prev_row, buf):
    # last_closed / prev_row: dict with keys t,o,c,h,l,v
    if not last_closed: return None
    t = last_closed["t"]; o = last_closed["o"]; c = last_closed["c"]
    h = last_closed["h"]; l = last_closed["l"]; v = last_closed["v"]

    if buf['last_ts'] and t <= buf['last_ts']:
        return None
    if len(buf['vol']) < 20 or len(buf['hi']) < 20 or len(buf['lo']) < 20 or len(buf['cl']) < 20:
        # append and wait until enough history
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_ts']=t
        return None

    hi_hist = list(buf['hi']); lo_hist = list(buf['lo']); cl_hist = list(buf['cl'])
    prev_range_hi = max(hi_hist[-20:]) if hi_hist else None
    prev_range_lo = min(lo_hist[-20:]) if lo_hist else None

    avg_vol = sum(buf['vol'])/len(buf['vol'])
    vol_ok  = v >= avg_vol * VOL_SURGE_MIN
    if not vol_ok or (prev_range_hi is None) or (prev_range_lo is None):
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_ts']=t
        for k in ("hi","lo","cl","vol"):
            if len(buf[k])>64: buf[k].popleft()
        return None

    # ATR pad
    atr = true_atr(hi_hist, lo_hist, cl_hist, period=14)
    if atr is not None:
        scale = {"15m":0.30, "30m":0.24, "1h":0.20}.get(tf, 0.24)
        pad_abs = atr * scale
    else:
        pad_abs = 0.0005 * c  # fallback pad

    long_break  = c > (prev_range_hi + pad_abs)
    short_break = c < (prev_range_lo - pad_abs)

    # Basic engulfing + sweep (keep, but not required)
    bull_engulf = bear_engulf = False
    if prev_row is not None:
        po = prev_row["o"]; pc = prev_row["c"]
        ph = prev_row["h"]; pl = prev_row["l"]
        prev_bear = pc < po
        prev_bull = pc > po
        body_engulf_up   = (c > o) and prev_bear and (c >= po) and (o <= pc)
        body_engulf_down = (c < o) and prev_bull and (c <= po) and (o >= pc)
        lo10 = min(lo_hist[-10:]) if lo_hist else None
        hi10 = max(hi_hist[-10:]) if hi_hist else None
        sweep_down = (lo10 is not None) and (l <= lo10)
        sweep_up   = (hi10 is not None) and (h >= hi10)
        bull_engulf = body_engulf_up and sweep_down
        bear_engulf = body_engulf_down and sweep_up
        # suppress breakouts on pure inside
        if (h <= ph) and (l >= pl):
            long_break = False
            short_break = False

    buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_ts']=t
    for k in ("hi","lo","cl","vol"):
        if len(buf[k])>64: buf[k].popleft()

    if long_break:
        return {"direction":"Long","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,"is_breakout":True,"is_engulf":False,"h":h,"l":l}
    if short_break:
        return {"direction":"Short","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,"is_breakout":True,"is_engulf":False,"h":h,"l":l}
    if bull_engulf:
        return {"direction":"Long","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,"is_breakout":False,"is_engulf":True,"h":h,"l":l}
    if bear_engulf:
        return {"direction":"Short","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,"is_breakout":False,"is_engulf":True,"h":h,"l":l}
    return None

# ========= ALERT FORMAT (short) =========
def leaderboard_line_short():
    total = stats["unique"]; w = stats["wins"]; l = stats["losses"]; be = stats["breakevens"]; to = stats["timeouts"]
    counted = (w + l) if (w + l) > 0 else 0
    wr = (w / counted * 100.0) if counted else 0.0
    return f"W:{w} L:{l} BE:{be} T:{to}  WR:{wr:.0f}%  Alerts:{total}"

def format_alert_short(pair, direction, entry, sl, tp, tf, score, tags):
    return (f"🔔 {direction.upper()} {pair} {tf}\n"
            f"Entry {entry:.6f}  SL {sl:.6f}  TP {tp:.6f}\n"
            f"Conf {score}/10  |  {' '.join(tags)}\n"
            f"📊 {leaderboard_line_short()}")

def format_manage_short(pair, tf, direction, entry, tp2, new_sl):
    return (f"🛡️ Move SL→BE+{TP1_BE_OFFSET_R:.1f}R  {pair} {tf} {direction}\n"
            f"SL {new_sl:.6f}  Target {tp2:.6f}\n"
            f"📊 {leaderboard_line_short()}")

def format_result_short(pair, tf, direction, result, entry, sl, tp1=None, tp2=None):
    emoji = "🟢" if "WIN" in result else ("🔴" if ("LOSS" in result or "TIMEOUT" in result) else "⚪️")
    extra = []
    if tp1 is not None: extra.append(f"TP1 {tp1:.6f}")
    if tp2 is not None: extra.append(f"TP2 {tp2:.6f}")
    exs = ("  " + "  ".join(extra)) if extra else ""
    return f"{emoji} {result}  {pair} {tf} {direction}\nEntry {entry:.6f}  SL {sl:.6f}{exs}\n📊 {leaderboard_line_short()}"

# ========= SCAN LOOP =========
async def scan_loop(session, symbols, tf):
    while True:
        # align to next close
        await asyncio.sleep(max(0, next_close_ms(tf) - now_ms() + 500)/1000 + random.random()*0.35)

        sem = asyncio.Semaphore(6)
        alerts = []

        async def process(sym):
            nonlocal alerts
            async with sem:
                await asyncio.sleep(random.random()*0.15)
                prev, last = await fetch_closed_bar(session, sym, tf)
                if not last: return

                buf = kbuf[sym][tf]
                sig = detect_signal(sym, tf, last, prev, buf)
                if not sig: return

                # Funding bias (soft gate)
                fj = await mx_funding_rate(session, sym)
                fr = 0.0
                try:
                    if fj and fj.get("data") and "fundingRate" in fj["data"]:
                        fr = float(fj["data"]["fundingRate"])
                except Exception:
                    fr = 0.0
                if sig['direction']=="Long" and fr > FUNDING_MAX_ABS*1.2: return
                if sig['direction']=="Short" and fr < -FUNDING_MAX_ABS*1.2: return

                # OI alignment (required)
                oi_pct = _oi_5m_change_pct_from_queue(oi_hist[sym])
                adaptive_thr = adaptive_oi_threshold(sym)
                c, o = sig['close'], sig['open']
                if oi_pct is None:
                    return
                if sig['direction']=="Long":
                    if not (c>o and oi_pct >= +adaptive_thr): return
                else:
                    if not (c<o and oi_pct <= -adaptive_thr): return

                # Orderbook microstructure
                ob = await mx_depth(session, sym)
                if not ob: return
                ssum = depth_summarize(ob)
                if not ssum: return
                spread, bid_usd, ask_usd, depth_usd = ssum
                update_micro_profile(sym, spread, depth_usd)
                max_spread_allowed, min_depth_required = adaptive_micro_thresholds(sym)
                if spread > max_spread_allowed: return
                if (bid_usd + ask_usd) < min_depth_required: return

                # Tape tilt (required if enabled)
                tilt, tnotional = await tape_tilt(session, sym)
                if TAPE_REQUIRE_ALWAYS:
                    if tnotional < TAPE_MIN_NOTIONAL: return
                    if sig['direction']=="Long" and tilt <  TAPE_TILT_WEAK: return
                    if sig['direction']=="Short" and tilt > -TAPE_TILT_WEAK: return

                # SL/TP + RR
                sl_long, sl_short = pick_swing_levels(tf, kbuf[sym][tf]['hi'], kbuf[sym][tf]['lo'], kbuf[sym][tf]['cl'])
                entry = sig['close']
                if sig['direction']=="Long" and sl_long is not None:
                    risk = max(1e-9, entry - sl_long)
                    tp1  = entry + 1.10*risk
                    tp2  = entry + 2.00*risk
                    sl   = sl_long
                elif sig['direction']=="Short" and sl_short is not None:
                    risk = max(1e-9, sl_short - entry)
                    tp1  = entry - 1.10*risk
                    tp2  = entry - 2.00*risk
                    sl   = sl_short
                else:
                    return
                if not rr_ok(entry, sl, tp1, min_r=1.0): return
                risk_pct = abs(entry - sl)/max(1e-12, entry)
                if risk_pct <= 0 or risk_pct > MAX_RISK_PCT: return

                # Cooldown & dedup
                now_s = int(time.time())
                if now_s - last_alert_at[sym][tf] < COOLDOWN_SEC:
                    # allow override if last trade moved to BE recently
                    be_ts = last_be_at[sym][tf]
                    used_ts = be_override_used_at[sym][tf]
                    if be_ts and (now_s - be_ts <= BE_OVERRIDE_WINDOW_SEC) and (now_s - used_ts > BE_OVERRIDE_WINDOW_SEC):
                        be_override_used_at[sym][tf] = now_s
                    else:
                        return
                last_alert_at[sym][tf] = now_s

                # Confidence scoring (0..10)
                score = 0
                tags = []

                # Volume surge (already required, but score it)
                score += 2; tags.append("Vol↑")

                # OI alignment (required)
                score += 2; tags.append(f"OIΔ{oi_pct*100:.2f}%")

                # Tape tilt scoring
                if sig['direction']=="Long":
                    if tilt >= TAPE_TILT_STRONG: score += 2; tags.append("Tape↑↑")
                    elif tilt >= TAPE_TILT_WEAK: score += 1; tags.append("Tape↑")
                    elif tilt <= -TAPE_TILT_WEAK: score -= 1; tags.append("Tape↓")
                else:
                    if tilt <= -TAPE_TILT_STRONG: score += 2; tags.append("Tape↓↓")
                    elif tilt <= -TAPE_TILT_WEAK: score += 1; tags.append("Tape↓")
                    elif tilt >=  TAPE_TILT_WEAK: score -= 1; tags.append("Tape↑")

                # Breakout cleanliness
                h = sig.get("h", entry); l = sig.get("l", entry)
                cr = max(1e-9, h - l)
                break_quality = 0.0
                if sig["is_breakout"]:
                    if sig['direction']=="Long": break_quality = (entry - sig['prev_hi']) / max(1e-9, cr)
                    else:                         break_quality = (sig['prev_lo'] - entry) / max(1e-9, cr)
                if break_quality >= 0.25: score += 1; tags.append("CleanBreak")

                # R:R cleanliness
                R = abs(entry - sl)
                rr = abs(tp2 - entry)/max(1e-9, R)
                if rr >= 2.0: score += 2; tags.append("RR≥2")
                elif rr >= 1.5: score += 1; tags.append("RR≥1.5")

                # New factor: real-time order-book imbalance shift (+2 if strong)
                imb = await imbalance_shift(session, sym, sig['direction'])
                if imb >= 1.0:
                    score += 2
                    tags.append("BookShift↑" if sig['direction']=="Long" else "BookShift↓")

                # Clamp & per-TF thresholds (add long penalty if any)
                final_score = max(1, min(10, score))
                req_by_tf = {"15m": MIN_CONF_15M, "30m": MIN_CONF_30M, "1h": MIN_CONF_1H}
                required = req_by_tf.get(tf, MIN_CONF) + (LONG_SIDE_PENALTY if sig["direction"]=="Long" else 0)
                if final_score < required: return

                # Dedup
                key = trade_key(sym, tf, sig['direction'], entry)
                nowm = now_ms()
                is_unique = True
                while dedup_seen and nowm - dedup_seen[0][1] > DEDUP_MIN*60*1000:
                    dedup_seen.popleft()
                for k, ts in dedup_seen:
                    if k == key:
                        is_unique = False
                        break
                if is_unique:
                    dedup_seen.append((key, nowm))
                    stats["unique"] += 1
                    current_day_keys.add(key)
                    active_trades[key] = {
                        "symbol": sym, "tf": tf, "direction": sig['direction'],
                        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
                        "start_ms": nowm, "be_moved": False,
                        "r": R, "mfe_r": 0.0,
                        "early_check_ms": nowm + int(EARLY_EXIT_CHECK_FRAC * tf_timeout_minutes(tf) * 60 * 1000)
                    }

                alerts.append(format_alert_short(sym, sig['direction'], entry, sl, tp2, tf, final_score, tags))

        await asyncio.gather(*(process(s) for s in symbols))
        await tg_send_batched(session, alerts)

# ========= RESULT RESOLVER =========
async def result_resolver_loop(session):
    while True:
        if not active_trades:
            await asyncio.sleep(3); continue
        keys = list(active_trades.keys())
        sem = asyncio.Semaphore(10)
        async def resolve_one(k):
            async with sem:
                tr = active_trades.get(k)
                if not tr: return
                sym, direction = tr["symbol"], tr["direction"]
                entry, sl, tp1, tp2 = tr["entry"], tr["sl"], tr["tp1"], tr["tp2"]
                tf = tr["tf"]
                start_ms = tr["start_ms"]

                # MFE update via ticker bid1/ask1
                jt0 = await mx_ticker(session, sym)
                if not jt0 or not jt0.get("data"): return
                d0 = jt0["data"]
                bid0 = float(d0.get("bid1", d0.get("lastPrice", entry)))
                ask0 = float(d0.get("ask1", d0.get("lastPrice", entry)))
                R = tr.get("r", max(1e-9, abs(entry - sl)))
                if direction == "Long":
                    tr["mfe_r"] = max(tr.get("mfe_r",0.0), (bid0 - entry)/max(1e-9, R))
                else:
                    tr["mfe_r"] = max(tr.get("mfe_r",0.0), (entry - ask0)/max(1e-9, R))

                # Early exit check
                if now_ms() >= tr.get("early_check_ms", 0):
                    if tr["mfe_r"] < EARLY_EXIT_MIN_MFE_R:
                        stats["losses"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result_short(sym, tf, direction, "EARLY EXIT (LOSS)", entry, sl, tp1, tp2))
                        return

                # Timeout
                if now_ms() - start_ms > tf_timeout_minutes(tf)*60*1000:
                    stats["timeouts"] += 1
                    del active_trades[k]
                    await tg_send(session, format_result_short(sym, tf, direction, "TIMEOUT", entry, sl, tp1, tp2))
                    return

                # Live result using best bid/ask
                jt = await mx_ticker(session, sym)
                if not jt or not jt.get("data"): return
                d = jt["data"]
                bid = float(d.get("bid1", d.get("lastPrice", entry)))
                ask = float(d.get("ask1", d.get("lastPrice", entry)))

                if direction == "Long":
                    if not tr["be_moved"]:
                        if bid >= tp1:
                            trail = entry + (TP1_BE_OFFSET_R * (entry - sl))
                            tr["be_moved"] = True; tr["sl"] = trail
                            last_be_at[sym][tf] = int(time.time())
                            await tg_send(session, format_manage_short(sym, tf, direction, entry, tp2, trail))
                        elif ask <= tr["sl"]:
                            stats["losses"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "LOSS (SL<TP1)", entry, tr["sl"], tp1, tp2))
                    else:
                        if bid >= tp2:
                            stats["wins"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "WIN (TP2)", entry, tr["sl"], tp1, tp2))
                        elif ask <= tr["sl"]:
                            stats["breakevens"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "BREAKEVEN", entry, tr["sl"], tp1, tp2))
                else:
                    if not tr["be_moved"]:
                        if ask <= tp1:
                            trail = entry - (TP1_BE_OFFSET_R * (sl - entry))
                            tr["be_moved"] = True; tr["sl"] = trail
                            last_be_at[sym][tf] = int(time.time())
                            await tg_send(session, format_manage_short(sym, tf, direction, entry, tp2, trail))
                        elif bid >= tr["sl"]:
                            stats["losses"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "LOSS (SL<TP1)", entry, tr["sl"], tp1, tp2))
                    else:
                        if ask <= tp2:
                            stats["wins"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "WIN (TP2)", entry, tr["sl"], tp1, tp2))
                        elif bid >= tr["sl"]:
                            stats["breakevens"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result_short(sym, tf, direction, "BREAKEVEN", entry, tr["sl"], tp1, tp2))
        await asyncio.gather(*(resolve_one(k) for k in keys))
        await asyncio.sleep(3)

# ========= DAILY STATS =========
async def daily_stats_loop(session):
    while True:
        sleep_s, target_dt = seconds_until_riyadh(int(os.getenv("STATS_DAILY_HOUR","22")), 0)
        await asyncio.sleep(sleep_s)
        today_2200 = target_dt
        yesterday_2200 = today_2200 - timedelta(days=1)
        period_str = f"{yesterday_2200.strftime('%Y-%m-%d %H:%M')} → {today_2200.strftime('%Y-%m-%d %H:%M')} (Riyadh)"
        msg = f"🗓️ DAILY\n{period_str}\n📊 {leaderboard_line_short()}"
        await tg_send(session, msg)
        stats["unique"] = 0; stats["wins"] = 0; stats["losses"] = 0; stats["breakevens"] = 0; stats["timeouts"] = 0
        current_day_keys.clear()
        await persist_state()

# ========= METRICS / HEARTBEAT =========
async def heartbeat_loop():
    global event_loop_lag_ms, _last_heartbeat_monotonic
    loop = asyncio.get_running_loop()
    interval = 1.0
    _last_heartbeat_monotonic = loop.time()
    while True:
        await asyncio.sleep(interval)
        nowt = loop.time()
        expected = _last_heartbeat_monotonic + interval
        event_loop_lag_ms = max(0.0, (nowt - expected) * 1000.0)
        _last_heartbeat_monotonic = nowt

async def metrics_handler(_):
    body = {
        "ok": True,
        "event_loop_lag_ms": round(event_loop_lag_ms, 2),
        "errors": dict(error_counters),
        "now_ms": now_ms(),
    }
    return web.json_response(body)

async def health(_): return web.Response(text="ok")

# ========= PERSISTENCE =========
def _serialize_state():
    micro = {}
    for s, v in micro_hist.items():
        micro[s] = {"spread": list(v["spread"])[-60:], "depth": list(v["depth"] )[-60:]}
    return {
        "stats": stats,
        "active_trades": active_trades,
        "dedup_seen": list(dedup_seen),
        "last_alert_at": {s: dict(v) for s,v in last_alert_at.items()},
        "last_be_at": {s: dict(v) for s,v in last_be_at.items()},
        "be_override_used_at": {s: dict(v) for s,v in be_override_used_at.items()},
        "current_day_keys": list(current_day_keys),
        "symbol_meta": symbol_meta,
        "micro_hist": micro,
        "oi_abs_ema": dict(oi_abs_ema),
    }

def _hydrate_state(d):
    global stats, active_trades
    if not d: return
    stats.update(d.get("stats", {}))
    active_trades.clear(); active_trades.update(d.get("active_trades", {}))
    dedup_seen.clear(); dedup_seen.extend(d.get("dedup_seen", []))
    current_day_keys.clear(); current_day_keys.update(d.get("current_day_keys", []))
    for s, v in d.get("last_alert_at", {}).items(): last_alert_at[s] = v
    for s, v in d.get("last_be_at", {}).items():    last_be_at[s] = v
    for s, v in d.get("be_override_used_at", {}).items(): be_override_used_at[s] = v
    symbol_meta.update(d.get("symbol_meta", {}))
    mh = d.get("micro_hist", {})
    for s, obj in mh.items():
        micro_hist[s]["spread"].extend(obj.get("spread", []))
        micro_hist[s]["depth"] .extend(obj.get("depth",  []))
    oi_abs_ema.update(d.get("oi_abs_ema", {}))

async def persist_state():
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_serialize_state(), f)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        print("Persist error:", e)

async def periodic_persist_loop():
    while True:
        await asyncio.sleep(60)
        await persist_state()

def load_state():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                data = json.load(f)
            _hydrate_state(data)
            print("State loaded from", STATE_PATH)
        else:
            print("No prior state.")
    except Exception as e:
        print("Failed to load state:", e)

# ========= APP MAIN =========
async def app_main():
    load_state()
    async with aiohttp.ClientSession() as session:
        symbols = await discover_top_usdt_perps(session, SCAN_LIMIT)
        print("Scanning symbols:", symbols)

        # warmup klines
        sem = asyncio.Semaphore(8)
        async def warm(sym, tf):
            async with sem:
                kj = await mx_kline(session, sym, TF_TO_INTERVAL[tf])
                arr = parse_kline_arrays(kj)
                arr = sorted(arr, key=lambda x: x["t"])
                for row in arr[-64:-1]:  # sync history up to last closed
                    buf = kbuf[sym][tf]
                    buf['hi'].append(row["h"]); buf['lo'].append(row["l"]); buf['vol'].append(row["v"]); buf['cl'].append(row["c"]); buf['last_ts']=row["t"]
        await asyncio.gather(*(warm(s, tf) for s in symbols for tf in TIMEFRAMES))

        tasks = [
            asyncio.create_task(oi_loop(session, symbols)),
            asyncio.create_task(scan_loop(session, symbols, "15m")),
            asyncio.create_task(scan_loop(session, symbols, "30m")),
            asyncio.create_task(scan_loop(session, symbols, "1h")),
            asyncio.create_task(result_resolver_loop(session)),
            asyncio.create_task(daily_stats_loop(session)),
            asyncio.create_task(periodic_persist_loop()),
            asyncio.create_task(heartbeat_loop()),
        ]

        app = web.Application()
        app.router.add_get("/", health)
        app.router.add_get("/metrics", metrics_handler)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT","8080"))); await site.start()
        print("HTTP health/metrics server started.")

        stop_event = asyncio.Event()
        def _exit(*_): stop_event.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_running_loop().add_signal_handler(sig, _exit)
            except NotImplementedError:
                pass

        await stop_event.wait()
        for t in tasks:
            t.cancel()
        for t in tasks:
            try: await t
            except asyncio.CancelledError: pass
            except Exception: pass
        await persist_state()
        try: await site.stop()
        except Exception: pass
        try: await runner.cleanup()
        except Exception: pass

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(app_main())