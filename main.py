# main.py
import os, asyncio, time, json, random, math, signal
from collections import deque, defaultdict
import aiohttp
from aiohttp import web
from datetime import datetime, timezone, timedelta

# ========= ENV CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ========= TUNABLES =========
SCAN_LIMIT            = int(os.getenv("SCAN_LIMIT", "120"))
MIN_CONF              = int(os.getenv("MIN_CONF", "7"))  # fallback default
# NEW (per-TF thresholds + direction penalty)
MIN_CONF_15M          = int(os.getenv("MIN_CONF_15M", str(MIN_CONF)))
MIN_CONF_30M          = int(os.getenv("MIN_CONF_30M", str(MIN_CONF)))
MIN_CONF_1H           = int(os.getenv("MIN_CONF_1H",  str(MIN_CONF)))
LONG_SIDE_PENALTY     = int(os.getenv("LONG_SIDE_PENALTY", "0"))

BREAKOUT_PAD_BPS      = float(os.getenv("BREAKOUT_PAD_BPS", "5"))
VOL_SURGE_MIN         = float(os.getenv("VOL_SURGE_MIN", "1.30"))
OI_DELTA_MIN_BASE     = float(os.getenv("OI_DELTA_MIN", "0.020"))
FUNDING_MAX_ABS       = float(os.getenv("FUNDING_MAX_ABS", "0.0005"))
SPREAD_MAX_ABS        = float(os.getenv("SPREAD_MAX_ABS", "0.0005"))
DEPTH_1PCT_MIN_USD    = float(os.getenv("DEPTH_1PCT_MIN_USD", "40000"))

MACRO_BTC_SHOCK_BP    = float(os.getenv("MACRO_BTC_SHOCK_BP", "150"))
MACRO_COOLDOWN_SEC    = int(os.getenv("MACRO_COOLDOWN_SEC", "900"))
NEWS_PAUSE            = os.getenv("NEWS_PAUSE","false").lower() == "true"

# NEW (hard block breakout in soft sessions)
HARD_BLOCK_SOFT_SESSION_BREAKOUT = os.getenv("HARD_BLOCK_SOFT_SESSION_BREAKOUT","true").lower()=="true"

COINGLASS_API_KEY     = os.getenv("COINGLASS_API_KEY","")  # optional

HARD_REQUIRE_OI   = os.getenv("HARD_REQUIRE_OI","false").lower()=="true"
CORR_HARD_BLOCK   = os.getenv("CORR_HARD_BLOCK","false").lower()=="true"
COOLDOWN_SEC      = int(os.getenv("COOLDOWN_SEC","600"))
MAX_RISK_PCT      = float(os.getenv("MAX_RISK_PCT","0.008"))

# Tape/CVD tilt
TAPE_LOOKBACK_SEC = int(os.getenv("TAPE_LOOKBACK_SEC", "15"))
# NEW: always-on tape requirement (turn on)
TAPE_REQUIRE_ALWAYS = os.getenv("TAPE_REQUIRE_ALWAYS","true").lower()=="true"
TAPE_MIN_NOTIONAL = float(os.getenv("TAPE_MIN_NOTIONAL", "50000"))  # was 10k; now default 50k
TAPE_TILT_STRONG  = float(os.getenv("TAPE_TILT_STRONG", "0.12"))
TAPE_TILT_WEAK    = float(os.getenv("TAPE_TILT_WEAK", "0.05"))

# Stats & reporting
DEDUP_MIN            = int(os.getenv("DEDUP_MIN", "5"))
STATS_DAILY_HOUR     = int(os.getenv("STATS_DAILY_HOUR", "22"))
# NEW: shorter defaults via env (recommend: 120/180/300)
WIN_TIMEOUT_15M_MIN  = int(os.getenv("WIN_TIMEOUT_15M_MIN", "240"))
WIN_TIMEOUT_30M_MIN  = int(os.getenv("WIN_TIMEOUT_30M_MIN", "300"))
WIN_TIMEOUT_1H_MIN   = int(os.getenv("WIN_TIMEOUT_1H_MIN", "480"))
# NEW: early exit
EARLY_EXIT_CHECK_FRAC = float(os.getenv("EARLY_EXIT_CHECK_FRAC","0.25"))  # at 25% of timeout
EARLY_EXIT_MIN_MFE_R  = float(os.getenv("EARLY_EXIT_MIN_MFE_R","0.3"))    # must reach +0.3R by then

# Breakout window
BREAKOUT_LOOKBACK_BARS = int(os.getenv("BREAKOUT_LOOKBACK_BARS", "20"))

# NEW: trailing after TP1 (BE + xR)
TP1_BE_OFFSET_R = float(os.getenv("TP1_BE_OFFSET_R","0.2"))

# Persistence
STATE_PATH = os.getenv("STATE_PATH", "/data/state.json")  # mount a persistent volume to /data on Railway

# Telegram safety
TELEGRAM_MAX_LEN = 3800  # keep margin under 4096

# ========= CONSTANTS =========
BINANCE_BASE = "https://fapi.binance.com"
RIYADH_TZ = timezone(timedelta(hours=3))
TIMEFRAMES = ["15m","30m","1h"]

# ========= STATE =========
kbuf = defaultdict(lambda: {
    '15m': {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_close': None},
    '30m': {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_close': None},
    '1h':  {'vol': deque(maxlen=64), 'hi': deque(maxlen=64), 'lo': deque(maxlen=64), 'cl': deque(maxlen=64), 'last_close': None},
})
oi_hist = defaultdict(lambda: deque(maxlen=120))  # per-minute OI samples
oi_abs_ema = defaultdict(lambda: 0.0)             # EMA of abs 5m OIÎ for adaptive threshold
last_alert_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})
macro_block_until = 0

# BE override
BE_OVERRIDE_WINDOW_SEC = int(os.getenv("BE_OVERRIDE_WINDOW_SEC","600"))
last_be_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})
be_override_used_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})

# Stats (UPDATED: include explicit counters for timeouts & early exits)
stats = {"unique": 0, "wins": 0, "losses": 0, "breakevens": 0, "timeouts": 0, "early_exits": 0}
dedup_seen = deque(maxlen=4000)
active_trades = {}
current_day_keys = set()

# Symbol metadata
symbol_meta = {}  # sym -> dict(tickSize, stepSize, pricePrecision, qtyPrecision)

# Caches
cache_premium = {}  # sym -> (ts, json)   # TTL set below
PREMIUM_TTL = 3.0
cache_book    = {}  # sym -> (ts, json), TTL 3s
cache_depth   = {}  # sym -> (ts, json), TTL 1.5s (LRU-ish via drop-oldest)
DEPTH_TTL = 1.5
DEPTH_LRU_MAX = 300

# Per-symbol micro profiles
micro_hist = defaultdict(lambda: {
    "spread": deque(maxlen=240),
    "depth": deque(maxlen=240),
})

# Per-TF alert counts
alert_counts = {"15m": 0, "30m": 0, "1h": 0}

# Metrics
rate_used_weight_1m = 0
rate_retry_after_ms = 0
error_counters = defaultdict(int)
event_loop_lag_ms = 0.0
_last_heartbeat_monotonic = None

# Token bucket (Binance 1200/min)
BUCKET_CAPACITY = 1200
bucket_tokens = BUCKET_CAPACITY
bucket_refill_rate_per_ms = BUCKET_CAPACITY / 60000.0
bucket_last_refill_ms = 0
bucket_lock = asyncio.Lock()

# Persisted last symbols for startup fallback
last_symbols_persisted = []

# Correlation cache (BTC/ETH 5m)
corr_cache = {"ts": 0.0, "btc": 0.0, "eth": 0.0, "ttl": 10.0}

# OI 5m external cache (Binance OI hist)
oi5m_cache = {}  # sym -> (ts, pct), TTL 60s
OI5M_TTL = 60.0

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

# ========= PRECISION HELPERS =========
def _precision_from_step(step: str) -> int:
    try:
        s = step.rstrip('0')
        if '.' in s:
            return len(s.split('.')[1])
        return 0
    except Exception:
        return 6

def round_price(sym, px: float) -> float:
    meta = symbol_meta.get(sym)
    if not meta: return round(px, 6)
    prec = meta.get("pricePrecision", 6)
    factor = 10 ** prec
    return math.floor(px * factor + 0.5) / factor

def trade_key(sym, tf, direction, entry):
    return f"{sym}|{tf}|{direction}|{round_price(sym, entry):.10f}"

# ========= SMALL UTILS =========
def percentile(arr, p):
    if not arr: return None
    a = sorted(arr)
    k = (len(a)-1) * (p/100.0)
    f = math.floor(k); c = math.ceil(k)
    if f == c: return a[int(k)]
    return a[f]*(c-k) + a[c]*(k-f)

def sanitize_md(s: str) -> str:
    return s.replace("_", "\\_")

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
        now = now_ms()
        elapsed = max(0, now - bucket_last_refill_ms)
        bucket_tokens = min(BUCKET_CAPACITY, bucket_tokens + elapsed * bucket_refill_rate_per_ms)
        bucket_last_refill_ms = now
        bucket_tokens = max(0, bucket_tokens - tokens)

# ========= HTTP HELPERS =========
async def http_request(session, method, url, params=None, headers=None, timeout=10, weight_cost=1, payload=None):
    global rate_used_weight_1m, rate_retry_after_ms
    await bucket_acquire(tokens=weight_cost)
    base = 0.25
    attempts = 6
    for attempt in range(attempts):
        try:
            if rate_used_weight_1m >= 1100:
                await asyncio.sleep(0.8 + random.random()*0.6)
            req = session.get if method == "GET" else session.post
            async with req(url, params=params, headers=headers, timeout=timeout, json=payload) as r:
                try:
                    if "X-MBX-USED-WEIGHT-1m" in r.headers:
                        rate_used_weight_1m = int(r.headers["X-MBX-USED-WEIGHT-1m"])
                    if "Retry-After" in r.headers:
                        rate_retry_after_ms = int(float(r.headers["Retry-After"]) * 1000)
                except Exception:
                    pass

                if r.status == 200:
                    return await r.json(content_type=None)

                error_counters[str(r.status)] += 1

                if r.status in (418, 429):
                    ra = r.headers.get("Retry-After")
                    sleep_s = float(ra) if ra else 1.5*(2**attempt) * random.random()
                    await asyncio.sleep(min(10.0, sleep_s))
                elif 500 <= r.status < 600:
                    sleep = (2**attempt) * base
                    await asyncio.sleep(random.uniform(0, sleep))
                else:
                    try:
                        txt = await r.text()
                        print(f"HTTP {r.status} {url} params={params} resp={txt[:200]}")
                    except Exception:
                        pass
                    return None
        except Exception:
            error_counters["exception"] += 1
            sleep = (2**attempt) * base
            await asyncio.sleep(random.uniform(0, sleep))
    return None

async def http_json(session, url, params=None, headers=None, timeout=10, weight_cost=1):
    return await http_request(session, "GET", url, params=params, headers=headers, timeout=timeout, weight_cost=weight_cost)

async def http_json_post(session, url, payload=None, timeout=8, weight_cost=1):
    return await http_request(session, "POST", url, payload=payload, timeout=timeout, weight_cost=weight_cost)

async def tg_send(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
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

def next_close_ms(tf):
    s = int(time.time())
    if tf=="1m":   return (s - (s%60)   + 60)*1000
    if tf=="5m":   return (s - (s%300)  + 300)*1000
    if tf=="15m":  return (s - (s%900)  + 900)*1000
    if tf=="30m":  return (s - (s%1800) + 1800)*1000
    if tf=="1h":   return (s - (s%3600) + 3600)*1000

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

def leaderboard_line():
    # UPDATED: include timeouts & early exits, WR computed on W/(W+L)
    w = stats.get("wins",0); l = stats.get("losses",0); be = stats.get("breakevens",0)
    to = stats.get("timeouts",0); ee = stats.get("early_exits",0)
    total_msgs = w + l + be + to + ee
    wr = (w / max(1, (w + l)) * 100.0)
    return f"ð *Today*: ð¢ {w}  ð´ {l}  âªï¸ {be}  â³ {to}  ð {ee}  |  WR: {wr:.1f}%  |  Alerts: {total_msgs}"

def format_alert(pair, direction, entry, sl, tp1, tp2, reason, tf, score, extras=None, override_note=None):
    ex = f"\n{extras}" if extras else ""
    ov = f"\nð Cooldown override: {override_note}" if override_note else ""
    return (
f"ð *TRADE ALERT*\n"
f"â¢ Pair: `{pair}`  â¢ TF: *{tf}*\n"
f"â¢ Direction: *{direction}*\n"
f"â¢ Entry: `{entry:.6f}`\n"
f"â¢ SL: `{sl:.6f}`   â¢ TP1 *(move SLâBE)*: `{tp1:.6f}`   â¢ TP2 *(WIN)*: `{tp2:.6f}`\n"
f"â¢ Why: {reason}\n"
f"â¢ Score: *{score}/10*\n"
f"{leaderboard_line()}{ex}{ov}"
)

def format_manage(pair, tf, direction, entry, tp2, new_sl):
    return (
f"ð¡ï¸ *MANAGE*: Move SL â BE+{TP1_BE_OFFSET_R:.1f}R\n"
f"â¢ Pair: `{pair}`  â¢ TF: *{tf}*  â¢ Direction: *{direction}*\n"
f"â¢ New SL: `{new_sl:.6f}`\n"
f"â¢ Targeting TP2: `{tp2:.6f}`\n"
f"{leaderboard_line()}"
)

def format_result(pair, tf, direction, result, entry, sl, tp1=None, tp2=None):
    extra = ""
    if tp2 is not None: extra += f"  â¢ TP2: `{tp2:.6f}`"
    if tp1 is not None: extra = (f"  â¢ TP1: `{tp1:.6f}`") + extra

    # UPDATED: neutral/semantic emojis (timeouts & early exits not red)
    r = result.upper()
    if "WIN" in r:
        emoji = "ð¢"
    elif "LOSS" in r:
        emoji = "ð´"
    elif "BREAKEVEN" in r:
        emoji = "âªï¸"
    elif "TIMEOUT" in r:
        emoji = "â³"
    elif "EARLY EXIT" in r:
        emoji = "ð"
    else:
        emoji = "âªï¸"

    return (
f"{emoji} *RESULT* â {result}\n"
f"â¢ Pair: `{pair}`  â¢ TF: *{tf}*  â¢ Direction: *{direction}*\n"
f"â¢ Entry: `{entry:.6f}`  â¢ SL: `{sl:.6f}`{extra}\n"
f"{leaderboard_line()}"
)

def format_stats_line():
    return f"ð *Daily Stats*\n{leaderboard_line()}"

# ========= BINANCE WRAPPERS (weights tuned) =========
async def bn_exchange_info(session): return await http_json(session, f"{BINANCE_BASE}/fapi/v1/exchangeInfo", weight_cost=10)
async def bn_24h_all(session):       return await http_json(session, f"{BINANCE_BASE}/fapi/v1/ticker/24hr", weight_cost=40)
async def bn_klines(session, symbol, interval, limit=2):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/klines",
                           params={"symbol":symbol,"interval":interval,"limit":limit}, weight_cost=1)
async def bn_open_interest(session, symbol):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/openInterest", params={"symbol":symbol}, weight_cost=1)
async def bn_premium_index(session, symbol, use_cache=True):
    ts = time.time()
    if use_cache:
        cached = cache_premium.get(symbol)
        if cached and ts - cached[0] <= PREMIUM_TTL:
            return cached[1]
    j = await http_json(session, f"{BINANCE_BASE}/fapi/v1/premiumIndex", params={"symbol":symbol}, weight_cost=1)
    if j: cache_premium[symbol] = (ts, j)
    return j
async def bn_depth(session, symbol, limit=100):
    ts = time.time()
    cached = cache_depth.get(symbol)
    if cached and ts - cached[0] <= DEPTH_TTL:
        return cached[1]
    j = await http_json(session, f"{BINANCE_BASE}/fapi/v1/depth", params={"symbol":symbol,"limit":limit}, weight_cost=5)
    if j:
        if len(cache_depth) >= DEPTH_LRU_MAX:
            oldest = min(cache_depth.items(), key=lambda kv: kv[1][0])[0]
            cache_depth.pop(oldest, None)
        cache_depth[symbol] = (ts, j)
    return j
async def bn_book_ticker(session, symbol, use_cache=True):
    ts = time.time()
    if use_cache:
        cached = cache_book.get(symbol)
        if cached and ts - cached[0] <= 3.0:
            return cached[1]
    j = await http_json(session, f"{BINANCE_BASE}/fapi/v1/ticker/bookTicker", params={"symbol":symbol}, weight_cost=1)
    if j: cache_book[symbol] = (ts, j)
    return j
async def bn_agg_trades(session, symbol, start_ms=None, end_ms=None, limit=1000):
    params = {"symbol": symbol, "limit": min(1000, max(50, limit))}
    if start_ms is not None: params["startTime"] = int(start_ms)
    if end_ms is not None:   params["endTime"]   = int(end_ms)
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/aggTrades", params=params, weight_cost=1)

# ========= OI 5m via Binance Open Interest History =========
async def bn_oi5m_change_pct(session, symbol):
    ts = time.time()
    cached = oi5m_cache.get(symbol)
    if cached and ts - cached[0] <= OI5M_TTL:
        return cached[1]
    url = f"{BINANCE_BASE}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "5m", "limit": 2}
    j = await http_json(session, url, params=params, weight_cost=1)
    pct = None
    try:
        if j and isinstance(j, list) and len(j) >= 2:
            old = float(j[-2]["sumOpenInterest"])
            new = float(j[-1]["sumOpenInterest"])
            if old > 0:
                pct = (new - old) / old
    except Exception:
        pct = None
    oi5m_cache[symbol] = (ts, pct)
    return pct

# ========= DISCOVERY / WARMUP (with backoff + fallback) =========
async def discover_top_usdt_perps(session, limit):
    info = None
    base = 0.5
    for attempt in range(6):
        info = await bn_exchange_info(session)
        if info: break
        await asyncio.sleep(random.uniform(0, (2**attempt)*base))
    if not info:
        if last_symbols_persisted:
            print("exchangeInfo failed; falling back to persisted symbols list.")
            return last_symbols_persisted[:limit]
        raise RuntimeError("exchangeInfo failed and no persisted symbols to fall back on")

    for s in info.get('symbols', []):
        if s.get('contractType')=='PERPETUAL' and s.get('quoteAsset')=='USDT':
            sym = s['symbol']
            tick = "0.0001"; step = "0.01"
            pricePrec = s.get("pricePrecision")
            qtyPrec   = s.get("quantityPrecision")
            for f in s.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick = f.get("tickSize", tick)
                if f.get("filterType") == "LOT_SIZE":
                    step = f.get("stepSize", step)
            if pricePrec is None: pricePrec = _precision_from_step(tick)
            if qtyPrec is None:   qtyPrec   = _precision_from_step(step)
            symbol_meta[sym] = {"tickSize": tick, "stepSize": step, "pricePrecision": int(pricePrec), "qtyPrecision": int(qtyPrec)}

    tickers = None
    for attempt in range(6):
        tickers = await bn_24h_all(session)
        if tickers: break
        await asyncio.sleep(random.uniform(0, (2**attempt)*base))
    if not tickers:
        if last_symbols_persisted:
            print("24hr tickers failed; falling back to persisted symbols.")
            return last_symbols_persisted[:limit]
        raise RuntimeError("24hr tickers failed and no persisted symbols")

    perp_symbols = {s['symbol'] for s in info['symbols']
                    if s.get('contractType')=='PERPETUAL' and s.get('quoteAsset')=='USDT'}
    ranked = sorted([t for t in tickers if t['symbol'] in perp_symbols],
                    key=lambda x: float(x.get('quoteVolume','0')), reverse=True)
    syms = [t['symbol'] for t in ranked[:limit]]
    return syms

async def warmup_klines(session, symbols):
    sem = asyncio.Semaphore(8)
    async def load(sym, tf):
        async with sem:
            data = await bn_klines(session, sym, tf, limit=64)
            if not data or len(data)<21: return
            for row in data[:-1]:
                h,l,c,vol,ct = float(row[2]), float(row[3]), float(row[4]), float(row[5]), int(row[6])
                buf = kbuf[sym][tf]
                buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(vol); buf['cl'].append(c); buf['last_close']=ct
    await asyncio.gather(*(load(s, tf) for s in symbols for tf in TIMEFRAMES))

# ========= OI LOOPS & HELPERS =========
async def oi_loop(session, symbols):
    while True:
        t0 = now_ms()
        sem = asyncio.Semaphore(12)
        async def one(sym):
            async with sem:
                j = await bn_open_interest(session, sym)
                if j and 'openInterest' in j:
                    oi_hist[sym].append((t0, float(j['openInterest'])))
        await asyncio.gather(*(one(s) for s in symbols))
        await asyncio.sleep(60 - (time.time()%60) + 0.02)

def _oi_5m_change_pct_from_queue(q, idx_latest=None):
    if len(q) < 5: return None
    if idx_latest is None: newest_ts, newest_oi = q[-1]
    else:                  newest_ts, newest_oi = q[idx_latest]
    target = newest_ts - 5*60*1000 - 5000
    older = None
    r = range(len(q)-1, -1, -1) if idx_latest is None else range(idx_latest, -1, -1)
    for i in r:
        ts, val = q[i]
        if ts <= target:
            older = (ts, val); break
    if not older: old_ts, old_oi = q[0]
    else:         old_ts, old_oi = older
    if old_oi == 0: return None
    return (newest_oi - old_oi) / old_oi

def oi_5min_change_pct_from_queue(sym):
    q = oi_hist[sym]
    return _oi_5m_change_pct_from_queue(q)

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

# ========= MACRO GUARD =========
async def macro_guard_loop(session):
    global macro_block_until
    sym, tf = "BTCUSDT", "1m"
    while True:
        data = await bn_klines(session, sym, tf, limit=21)
        if data and len(data)>=21:
            prev20 = data[-21:-1]
            last   = data[-1]
            avg_vol = sum(float(r[5]) for r in prev20)/20.0
            last_vol = float(last[5])
            o, c = float(last[1]), float(last[4])
            move_bp = abs((c - o)/o)*10000
            vol_surge = last_vol >= avg_vol*1.5
            if move_bp >= MACRO_BTC_SHOCK_BP and vol_surge:
                macro_block_until = now_ms() + MACRO_COOLDOWN_SEC*1000
        await asyncio.sleep(60)

def macro_ok():
    return (not NEWS_PAUSE) and now_ms() >= macro_block_until

# ========= CORRELATION (cache 10s) =========
async def correlation_soft_flag(session, direction):
    nowt = time.time()
    if nowt - corr_cache["ts"] <= corr_cache["ttl"]:
        btc, eth = corr_cache["btc"], corr_cache["eth"]
    else:
        async def ret5m(sym):
            data = await bn_klines(session, sym, "5m", limit=2)
            if not data or len(data)<2: return 0.0
            prev, last = data[-2], data[-1]
            return (float(last[4]) - float(prev[4]))/float(prev[4])
        btc, eth = await asyncio.gather(ret5m("BTCUSDT"), ret5m("ETHUSDT"))
        corr_cache["ts"] = nowt; corr_cache["btc"] = btc; corr_cache["eth"] = eth
    soft = False
    if direction=="Long" and (btc < -0.005 or eth < -0.005): soft = True
    if direction=="Short" and (btc >  0.005 or eth >  0.005): soft = True
    return soft, btc, eth

# ========= SESSION VOL =========
def session_soft_flag(tf, closes):
    if len(closes) < 30:
        return None
    rets = [abs((closes[i]-closes[i-1])/max(1e-12, closes[i-1])) for i in range(1,len(closes))]
    atrp = sum(rets[-20:])/20.0
    th = {"15m":0.0060, "30m":0.0090, "1h":0.0130}.get(tf, 0.009)
    return atrp < th

# ========= MICROSTRUCTURE PROFILING =========
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

def depth_checks_side(orderbook, direction, max_spread_allowed, min_depth_required):
    try:
        best_ask = float(orderbook['asks'][0][0])
        best_bid = float(orderbook['bids'][0][0])
        spread = (best_ask - best_bid) / ((best_ask + best_bid)/2)
        if spread > max_spread_allowed:
            return False, spread, 0.0, 0.0, 0.0
        up_lim  = best_ask * 1.01
        dn_lim  = best_bid * 0.99
        ask_usd = sum(float(p)*float(q) for p,q in orderbook['asks'] if float(p)<=up_lim)
        bid_usd = sum(float(p)*float(q) for p,q in orderbook['bids'] if float(p)>=dn_lim)
        total = ask_usd + bid_usd
        if total < min_depth_required:
            return False, spread, bid_usd, ask_usd, total
        if direction == "Long":
            if bid_usd < min_depth_required * 0.45: return False, spread, bid_usd, ask_usd, total
            imb = bid_usd / max(1e-9, total)
            if imb < 0.52: return False, spread, bid_usd, ask_usd, total
        else:
            if ask_usd < min_depth_required * 0.45: return False, spread, bid_usd, ask_usd, total
            imb = ask_usd / max(1e-9, total)
            if imb < 0.52: return False, spread, bid_usd, ask_usd, total
        return True, spread, bid_usd, ask_usd, total
    except Exception:
        return False, 1.0, 0.0, 0.0, 0.0

# ========= TAPE / CVD (gated) =========
async def tape_tilt(session, sym):
    end_ms = now_ms()
    start_ms = end_ms - TAPE_LOOKBACK_SEC*1000
    j = await bn_agg_trades(session, sym, start_ms, end_ms, limit=1000)
    if not j: return 0.0, 0.0
    buy_n = 0.0; sell_n = 0.0
    for t in j:
        price = float(t["p"]); qty = float(t["q"])
        notional = price * qty
        if t["m"]: sell_n += notional
        else:      buy_n += notional
    total = buy_n + sell_n
    if total <= 0: return 0.0, 0.0
    return (buy_n - sell_n)/total, total

# ========= KLINE CLOSE VALIDATION =========
async def fetch_closed_kline(session, sym, tf):
    data = await bn_klines(session, sym, tf, limit=3)
    if not data or len(data)<3: return None, None
    prev = data[-2]; last = data[-1]
    if int(last[6]) <= now_ms() - 1000:
        return prev, last
    await asyncio.sleep(1.0)
    data2 = await bn_klines(session, sym, tf, limit=3)
    if data2 and len(data2)>=3:
        prev2, last2 = data2[-2], data2[-1]
        if int(last2[6]) <= now_ms() - 1000:
            return prev2, last2
    return data[-3], data[-2]

# ========= SIGNAL DETECTION (FIXED: compute sweeps on history, append after) =========
def detect_signal(sym, tf, last_closed_row, prev_row=None, buf=None):
    close_time = int(last_closed_row[6])
    o = float(last_closed_row[1]); c = float(last_closed_row[4])
    h = float(last_closed_row[2]); l = float(last_closed_row[3])
    v = float(last_closed_row[5])

    po = pc = ph = pl = None
    if prev_row:
        po = float(prev_row[1]); pc = float(prev_row[4])
        ph = float(prev_row[2]); pl = float(prev_row[3])

    if buf is None: return None
    if buf['last_close'] and close_time <= buf['last_close']:
        return None
    if len(buf['vol']) < 20 or len(buf['hi']) < 20 or len(buf['lo']) < 20 or len(buf['cl']) < 20:
        # not enough history â append and exit
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=close_time
        return None

    # Use history snapshots BEFORE appending this bar
    hi_hist = list(buf['hi'])
    lo_hist = list(buf['lo'])
    cl_hist = list(buf['cl'])

    prev_range_hi = max(hi_hist[-BREAKOUT_LOOKBACK_BARS:]) if hi_hist else None
    prev_range_lo = min(lo_hist[-BREAKOUT_LOOKBACK_BARS:]) if lo_hist else None

    avg_vol = sum(buf['vol'])/len(buf['vol'])
    vol_ok  = v >= avg_vol * VOL_SURGE_MIN
    if (not vol_ok) or (prev_range_hi is None) or (prev_range_lo is None):
        # append bar and exit
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=close_time
        for k in ("hi","lo","cl","vol"):
            if len(buf[k])>64: buf[k].popleft()
        return None

    # ATR on history
    atr = true_atr(hi_hist, lo_hist, cl_hist, period=14)
    if atr is not None:
        scale = {"15m":0.30, "30m":0.24, "1h":0.20}.get(tf, 0.24)
        pad_abs = atr * scale
    else:
        pad_abs = (BREAKOUT_PAD_BPS/10000.0) * c

    long_break  = c > (prev_range_hi + pad_abs)
    short_break = c < (prev_range_lo - pad_abs)

    bull_engulf = bear_engulf = False
    if prev_row is not None:
        prev_bear = pc < po
        prev_bull = pc > po
        body_engulf_up   = (c > o) and prev_bear and (c >= po) and (o <= pc)
        body_engulf_down = (c < o) and prev_bull and (c <= po) and (o >= pc)
        lo10 = min(lo_hist[-10:]) if lo_hist else None
        hi10 = max(hi_hist[-10:]) if hi_hist else None
        sweep_down = (l <= lo10) if lo10 is not None else False
        sweep_up   = (h >= hi10) if hi10 is not None else False
        bull_engulf = body_engulf_up and sweep_down
        bear_engulf = body_engulf_down and sweep_up

    if prev_row is not None and (long_break or short_break):
        is_inside = (h <= ph) and (l >= pl)
        if is_inside:
            long_break = False
            short_break = False

    # append current bar AFTER computing signals
    buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=close_time
    for k in ("hi","lo","cl","vol"):
        if len(buf[k])>64: buf[k].popleft()

    if long_break:
        return {"direction":"Long","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,
                "is_breakout":True,"is_engulf":False,"hi":h,"lo":l}
    if short_break:
        return {"direction":"Short","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,
                "is_breakout":True,"is_engulf":False,"hi":h,"lo":l}
    if bull_engulf:
        return {"direction":"Long","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,
                "is_breakout":False,"is_engulf":True,"hi":h,"lo":l}
    if bear_engulf:
        return {"direction":"Short","close":c,"open":o,"prev_hi":prev_range_hi,"prev_lo":prev_range_lo,
                "is_breakout":False,"is_engulf":True,"hi":h,"lo":l}
    return None

# ========= SCAN LOOP PER TF =========
async def scan_loop(session, symbols, tf):
    while True:
        await asyncio.sleep(max(0, next_close_ms(tf) - now_ms() + 500)/1000 + random.random()*0.35)
        if not macro_ok():
            continue

        sem = asyncio.Semaphore(6)
        alerts = []

        async def process(sym):
            nonlocal alerts
            async with sem:
                await asyncio.sleep(random.random()*0.15)
                prev, last_closed = await fetch_closed_kline(session, sym, tf)
                if not last_closed: return

                buf = kbuf[sym][tf]
                sig = detect_signal(sym, tf, last_closed, prev_row=prev, buf=buf)
                if not sig: return

                # Funding bias (premiumIndex cached ~3s)
                finfo = await bn_premium_index(session, sym)
                if not finfo or 'lastFundingRate' not in finfo: return
                fr = float(finfo['lastFundingRate'])
                # keep as a soft-ish gate; can be tuned or extended with basis later
                if sig['direction']=="Long" and fr > FUNDING_MAX_ABS*1.2: return
                if sig['direction']=="Short" and fr < -FUNDING_MAX_ABS*1.2: return

                # OI alignment â queue then 5m hist (cached 60s)
                oi_pct = oi_5min_change_pct_from_queue(sym)
                if oi_pct is None:
                    oi_pct = await bn_oi5m_change_pct(session, sym)
                adaptive_thr = adaptive_oi_threshold(sym)
                oi_soft_fail = False
                c, o = sig['close'], sig['open']
                if oi_pct is None:
                    oi_soft_fail = not HARD_REQUIRE_OI
                else:
                    if sig['direction']=="Long":
                        if not (c>o and oi_pct >= +adaptive_thr): oi_soft_fail = True
                    else:
                        if not (c<o and oi_pct <= -adaptive_thr): oi_soft_fail = True
                if (HARD_REQUIRE_OI and oi_soft_fail): return

                # Orderbook microstructure
                ob = await bn_depth(session, sym, limit=100)
                if not ob or not ob.get('asks') or not ob.get('bids'): return
                try:
                    best_ask = float(ob['asks'][0][0]); best_bid = float(ob['bids'][0][0])
                    spread_raw = (best_ask - best_bid)/((best_ask+best_bid)/2)
                    up_lim  = best_ask * 1.01
                    dn_lim  = best_bid * 0.99
                    ask_usd = sum(float(p)*float(q) for p,q in ob['asks'] if float(p)<=up_lim)
                    bid_usd = sum(float(p)*float(q) for p,q in ob['bids'] if float(p)>=dn_lim)
                    depth_total = ask_usd + bid_usd
                    update_micro_profile(sym, spread_raw, depth_total)
                except Exception:
                    pass

                max_spread_allowed, min_depth_required = adaptive_micro_thresholds(sym)
                ok_depth, spread, bid_usd, ask_usd, depth_usd = depth_checks_side(
                    ob, sig['direction'], max_spread_allowed, min_depth_required
                )
                if not ok_depth: return

                # Correlation (cached 10s)
                corr_soft, btc5m, eth5m = await correlation_soft_flag(session, sig['direction'])
                if CORR_HARD_BLOCK:
                    if sig['direction']=="Long" and (btc5m < -0.007 or eth5m < -0.007): return
                    if sig['direction']=="Short" and (btc5m >  0.007 or eth5m >  0.007): return

                # SL/TP + RR
                buf_local = kbuf[sym][tf]
                sl_long, sl_short = pick_swing_levels(tf, buf_local['hi'], buf_local['lo'], buf_local['cl'])
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

                # Debounce with BE override
                now_s = int(time.time())
                override_note = None
                if now_s - last_alert_at[sym][tf] < COOLDOWN_SEC:
                    be_ts = last_be_at[sym][tf]
                    used_ts = be_override_used_at[sym][tf]
                    if be_ts and (now_s - be_ts <= BE_OVERRIDE_WINDOW_SEC) and (now_s - used_ts > BE_OVERRIDE_WINDOW_SEC):
                        override_note = "last trade hit TP1âBE"
                        be_override_used_at[sym][tf] = now_s
                    else:
                        return
                last_alert_at[sym][tf] = now_s

                # Confidence prelim
                h = sig.get("hi", entry); l = sig.get("lo", entry)
                cr = max(1e-9, h - l)
                clv_curr = (sig['close'] - l)/cr
                break_quality = 0.0
                if sig["is_breakout"]:
                    if sig['direction']=="Long": break_quality = (entry - sig['prev_hi']) / max(1e-9, cr)
                    else:                         break_quality = (sig['prev_lo'] - entry) / max(1e-9, cr)

                score = 6
                info_bits = [
                    f"Volâ¥{VOL_SURGE_MIN:.2f}x",
                    f"Funding {fr*100:.3f}%",
                    f"Spread {spread*100:.3f}% (max {max_spread_allowed*100:.3f}%)",
                    f"Depth1% ${depth_usd:,.0f} (min ${min_depth_required:,.0f})",
                ]
                if oi_pct is None: info_bits.append("OI missing")
                else: info_bits.append(f"OIÎ5m {oi_pct*100:.2f}% (thr {adaptive_thr*100:.2f}%)")
                if oi_soft_fail: score -= 1; info_bits.append("OI-price misalign")
                else:            score += 1
                if corr_soft:    score -= 1; info_bits.append(f"Corr soft: BTC {btc5m*100:.2f}%, ETH {eth5m*100:.2f}%")
                else:            score += 1
                sess_flag = session_soft_flag(tf, list(buf_local['cl']))
                if sess_flag is None: info_bits.append("Session neutral")
                elif sess_flag:       score -= 1; info_bits.append("Session soft")
                else:                 score += 1
                if sig['direction']=="Long" and clv_curr >= 0.65: score += 1; info_bits.append("Strong close")
                if sig['direction']=="Short" and clv_curr <= 0.35: score += 1; info_bits.append("Strong close")
                if break_quality >= 0.25: score += 1; info_bits.append("Clean break")

                # HARD BLOCK: breakout during soft session
                if HARD_BLOCK_SOFT_SESSION_BREAKOUT and sig["is_breakout"] and (sess_flag is True):
                    return

                # Tape tilt â now optionally ALWAYS required
                tape_score = 0
                need_tape = TAPE_REQUIRE_ALWAYS or (score >= MIN_CONF-1 and score < MIN_CONF+2)
                if need_tape:
                    tilt, tnotional = await tape_tilt(session, sym)
                    tape_note = f"Tape tilt {tilt*100:.1f}% on ${tnotional:,.0f}"
                    weak = TAPE_TILT_WEAK
                    strong = TAPE_TILT_STRONG
                    if TAPE_REQUIRE_ALWAYS:
                        if tnotional < TAPE_MIN_NOTIONAL:
                            return
                        if sig['direction']=="Long" and tilt <  weak: return
                        if sig['direction']=="Short" and tilt > -weak: return
                    # scoring (still helpful)
                    if sig['direction']=="Long":
                        if tilt >= strong: tape_score += 2
                        elif tilt >= weak: tape_score += 1
                        elif tilt <= -weak: tape_score -= 1
                    else:
                        if tilt <= -strong: tape_score += 2
                        elif tilt <= -weak: tape_score += 1
                        elif tilt >=  weak:  tape_score -= 1
                    info_bits.append(tape_note)
                    score += tape_score

                final_score = max(1, min(10, score))

                # Per-TF MIN_CONF + long penalty
                req_by_tf = {"15m": MIN_CONF_15M, "30m": MIN_CONF_30M, "1h": MIN_CONF_1H}
                required = req_by_tf.get(tf, MIN_CONF) + (LONG_SIDE_PENALTY if sig["direction"]=="Long" else 0)
                if final_score < required: return

                # Dedup + stats
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
                    # store R + early-exit checkpoint
                    R = abs(entry - sl)
                    early_ms = nowm + int(EARLY_EXIT_CHECK_FRAC * tf_timeout_minutes(tf) * 60 * 1000)
                    active_trades[key] = {
                        "symbol": sym, "tf": tf, "direction": sig['direction'],
                        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
                        "start_ms": nowm, "be_moved": False,
                        "r": R, "mfe_r": 0.0, "early_check_ms": early_ms
                    }
                    alert_counts[tf] = alert_counts.get(tf, 0) + 1

                reason_prefix = "ð Breakout + TRUE-ATR pad" if sig["is_breakout"] else "ð Engulfing reversal + sweep"
                extras = f"CLV:{clv_curr:.2f}  BreakQ:{break_quality:.2f}  BidDepth:${bid_usd:,.0f}  AskDepth:${ask_usd:,.0f}"
                alert_text = format_alert(sym, sig['direction'], entry, sl, tp1, tp2,
                                 reason_prefix + "; " + ", ".join(info_bits), tf, final_score, extras, override_note)
                alerts.append(alert_text)

        await asyncio.gather(*(process(s) for s in symbols))
        await tg_send_batched(session, alerts)

# ========= RESULT RESOLVER (adds early exit + trailing after TP1) =========
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

                # Early exit check (no progress) + MFE update
                R = tr.get("r", max(1e-9, abs(entry - sl)))
                jt0 = await bn_book_ticker(session, sym)
                if not jt0: return
                bid0 = float(jt0["bidPrice"]); ask0 = float(jt0["askPrice"])
                if direction == "Long":
                    tr["mfe_r"] = max(tr.get("mfe_r",0.0), (bid0 - entry)/max(1e-9, R))
                else:
                    tr["mfe_r"] = max(tr.get("mfe_r",0.0), (entry - ask0)/max(1e-9, R))

                if now_ms() >= tr.get("early_check_ms", 0):
                    if tr["mfe_r"] < EARLY_EXIT_MIN_MFE_R:
                        # UPDATED: count as early exit (not a loss)
                        if k in current_day_keys: stats["early_exits"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result(sym, tf, direction, "EARLY EXIT (no progress â not counted)", entry, tr["sl"], tp1, tp2))
                        return

                # Timeout (UPDATED: not a loss)
                if now_ms() - start_ms > tf_timeout_minutes(tf)*60*1000:
                    if k in current_day_keys: stats["timeouts"] += 1
                    del active_trades[k]
                    await tg_send(session, format_result(sym, tf, direction, "TIMEOUT (not counted)", entry, tr["sl"], tp1, tp2))
                    return

                # Live check
                jt = await bn_book_ticker(session, sym)
                if not jt: return
                bid = float(jt["bidPrice"]); ask = float(jt["askPrice"])

                if direction == "Long":
                    if not tr["be_moved"]:
                        if bid >= tp1:
                            # trail to BE + offset*R
                            trail = entry + (TP1_BE_OFFSET_R * (entry - tr["sl"]))
                            tr["be_moved"] = True; tr["sl"] = trail
                            last_be_at[sym][tf] = int(time.time())
                            await tg_send(session, format_manage(sym, tf, direction, entry, tp2, trail))
                        elif ask <= tr["sl"]:
                            if k in current_day_keys: stats["losses"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "LOSS (SL hit before TP1)", entry, tr["sl"], tp1, tp2))
                    else:
                        if bid >= tp2:
                            if k in current_day_keys: stats["wins"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "WIN (TP2 hit)", entry, tr["sl"], tp1, tp2))
                        elif ask <= tr["sl"]:
                            if k in current_day_keys: stats["breakevens"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "BREAKEVEN (SL@BE)", entry, tr["sl"], tp1, tp2))
                else:
                    if not tr["be_moved"]:
                        if ask <= tp1:
                            # trail to BE + offset*R (short side)
                            trail = entry - (TP1_BE_OFFSET_R * (tr["sl"] - entry))
                            tr["be_moved"] = True; tr["sl"] = trail
                            last_be_at[sym][tf] = int(time.time())
                            await tg_send(session, format_manage(sym, tf, direction, entry, tp2, trail))
                        elif bid >= tr["sl"]:
                            if k in current_day_keys: stats["losses"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "LOSS (SL hit before TP1)", entry, tr["sl"], tp1, tp2))
                    else:
                        if ask <= tp2:
                            if k in current_day_keys: stats["wins"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "WIN (TP2 hit)", entry, tr["sl"], tp1, tp2))
                        elif bid >= tr["sl"]:
                            if k in current_day_keys: stats["breakevens"] += 1
                            del active_trades[k]
                            await tg_send(session, format_result(sym, tf, direction, "BREAKEVEN (SL@BE)", entry, tr["sl"], tp1, tp2))
        await asyncio.gather(*(resolve_one(k) for k in keys))
        await asyncio.sleep(3)

# ========= DAILY STATS =========
async def daily_stats_loop(session):
    while True:
        sleep_s, target_dt = seconds_until_riyadh(STATS_DAILY_HOUR, 0)
        await asyncio.sleep(sleep_s)
        today_2200 = target_dt
        yesterday_2200 = today_2200 - timedelta(days=1)
        period_str = f"{yesterday_2200.strftime('%Y-%m-%d %H:%M')} â {today_2200.strftime('%Y-%m-%d %H:%M')} (Riyadh)"
        msg = f"ðï¸ [DAILY STATS]\n{period_str}\n{format_stats_line()}"
        await tg_send(session, msg)
        # UPDATED: reset all counters, including timeouts & early_exits
        stats["unique"] = 0
        stats["wins"] = 0
        stats["losses"] = 0
        stats["breakevens"] = 0
        stats["timeouts"] = 0
        stats["early_exits"] = 0
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
        "used_weight_1m": rate_used_weight_1m,
        "retry_after_ms": rate_retry_after_ms,
        "errors": dict(error_counters),
        "macro_block_until": macro_block_until,
        "alert_counts": alert_counts,
        "now_ms": now_ms(),
    }
    return web.json_response(body)

async def health(_): return web.Response(text="ok")

# ========= PERSISTENCE =========
def _serialize_state():
    micro = {}
    for s, v in micro_hist.items():
        micro[s] = {
            "spread": list(v["spread"])[-60:],
            "depth":  list(v["depth"] )[-60:],
        }
    return {
        "stats": stats,
        "active_trades": active_trades,
        "dedup_seen": list(dedup_seen),
        "last_alert_at": {s: dict(v) for s,v in last_alert_at.items()},
        "last_be_at": {s: dict(v) for s,v in last_be_at.items()},
        "be_override_used_at": {s: dict(v) for s,v in be_override_used_at.items()},
        "macro_block_until": macro_block_until,
        "current_day_keys": list(current_day_keys),
        "symbol_meta": symbol_meta,
        "micro_hist": micro,
        "oi_abs_ema": dict(oi_abs_ema),
        "last_symbols": last_symbols_persisted,
    }

def _hydrate_state(d):
    global stats, active_trades, dedup_seen, macro_block_until, symbol_meta, oi_abs_ema, last_symbols_persisted
    if not d: return
    stats.update(d.get("stats", {}))
    # ensure new keys exist even if loading old state
    stats.setdefault("timeouts", 0)
    stats.setdefault("early_exits", 0)
    active_trades.clear(); active_trades.update(d.get("active_trades", {}))
    dedup_seen.clear(); dedup_seen.extend(d.get("dedup_seen", []))
    macro_block_until = d.get("macro_block_until", 0)
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
    last_symbols_persisted = d.get("last_symbols", [])

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
        # persist a good symbols list for fallback later
        global last_symbols_persisted
        last_symbols_persisted = symbols[:]
        await persist_state()

        await warmup_klines(session, symbols)
        print("Warmup complete.")

        tasks = [
            asyncio.create_task(oi_loop(session, symbols)),
            asyncio.create_task(macro_guard_loop(session)),
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