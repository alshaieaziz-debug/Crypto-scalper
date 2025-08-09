import os, asyncio, time
from collections import deque, defaultdict
import aiohttp
from aiohttp import web
from datetime import datetime, timezone, timedelta

# ========= ENV CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ========= TUNABLES (defaults tuned for 15m/30m/1h) =========
SCAN_LIMIT            = int(os.getenv("SCAN_LIMIT", "120"))      # number of symbols to scan
MIN_CONF              = int(os.getenv("MIN_CONF", "7"))          # min confidence to alert
BREAKOUT_PAD_BPS      = float(os.getenv("BREAKOUT_PAD_BPS", "5"))      # 0.05% pad
VOL_SURGE_MIN         = float(os.getenv("VOL_SURGE_MIN", "1.30"))      # >=30% above 20-c avg
OI_DELTA_MIN          = float(os.getenv("OI_DELTA_MIN", "0.025"))      # >=2.5% last ~5m
FUNDING_MAX_ABS       = float(os.getenv("FUNDING_MAX_ABS", "0.0005"))  # ±0.05%
SPREAD_MAX_ABS        = float(os.getenv("SPREAD_MAX_ABS", "0.0005"))   # 0.05%
DEPTH_1PCT_MIN_USD    = float(os.getenv("DEPTH_1PCT_MIN_USD", "40000"))# combined depth within 1%
MACRO_BTC_SHOCK_BP    = float(os.getenv("MACRO_BTC_SHOCK_BP", "150"))  # 1.5% 1m shock bars
MACRO_COOLDOWN_SEC    = int(os.getenv("MACRO_COOLDOWN_SEC", "900"))    # 15 min pause
NEWS_PAUSE            = os.getenv("NEWS_PAUSE","false").lower() == "true"
COINGLASS_API_KEY     = os.getenv("COINGLASS_API_KEY","")              # optional

# A-tier extras (env-controlled)
HARD_REQUIRE_OI   = os.getenv("HARD_REQUIRE_OI","false").lower()=="true"
CORR_HARD_BLOCK   = os.getenv("CORR_HARD_BLOCK","false").lower()=="true"
COOLDOWN_SEC      = int(os.getenv("COOLDOWN_SEC","600"))     # per pair/tf alert cooldown (10 min)
MAX_RISK_PCT      = float(os.getenv("MAX_RISK_PCT","0.006")) # 0.6% max stop distance

# Stats & reporting
DEDUP_MIN          = int(os.getenv("DEDUP_MIN", "5"))         # treat near-duplicate alerts as one (stats)
WIN_TIMEOUT_MIN    = int(os.getenv("WIN_TIMEOUT_MIN", "240")) # 4h timeout per trade
STATS_DAILY_HOUR   = int(os.getenv("STATS_DAILY_HOUR", "22")) # 22:00 Riyadh (UTC+3)

# ========= CONSTANTS =========
BINANCE_BASE = "https://fapi.binance.com"
RIYADH_TZ = timezone(timedelta(hours=3))  # no DST
TIMEFRAMES = ["15m","30m","1h"]

# ========= STATE =========
kbuf = defaultdict(lambda: {
    '15m': {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
    '30m': {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
    '1h':  {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
})
oi_hist = defaultdict(lambda: deque(maxlen=10))  # symbol -> [(ts_ms, oi_float)]
last_alert_at = defaultdict(lambda: {'15m':0,'30m':0,'1h':0})
macro_block_until = 0

# Stats structures
stats = {"unique": 0, "wins": 0, "losses": 0}
dedup_seen = deque(maxlen=2000)   # (key, ts_ms)
active_trades = {}               # trade_id -> dict(entry, sl, tp1, direction, symbol, tf, start_ms)
current_day_keys = set()         # trade keys created since last daily digest

def trade_key(sym, tf, direction, entry):
    return f"{sym}|{tf}|{direction}|{round(entry,5)}"

# ========= HELPERS =========
def now_ms(): return int(time.time()*1000)

async def http_json(session, url, params=None, headers=None, timeout=10):
    for attempt in range(4):
        try:
            async with session.get(url, params=params, headers=headers, timeout=timeout) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                await asyncio.sleep(0.25*(attempt+1))
        except Exception:
            await asyncio.sleep(0.25*(attempt+1))
    return None

async def http_json_post(session, url, payload=None, timeout=8):
    try:
        async with session.post(url, json=payload, timeout=timeout) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            return None
    except Exception:
        return None

async def tg_send(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    j = await http_json_post(session, url, payload)
    if j is None:
        print("Telegram send failed")

def next_close_ms(tf):
    s = int(time.time())
    if tf=="1m":   return (s - (s%60)   + 60)*1000
    if tf=="5m":   return (s - (s%300)  + 300)*1000
    if tf=="15m":  return (s - (s%900)  + 900)*1000
    if tf=="30m":  return (s - (s%1800) + 1800)*1000
    if tf=="1h":   return (s - (s%3600) + 3600)*1000

def swing_levels(lows, highs):
    sl_long = min(list(lows)[-3:]) if len(lows)>=3 else (min(lows) if lows else None)
    sl_short = max(list(highs)[-3:]) if len(highs)>=3 else (max(highs) if highs else None)
    return sl_long, sl_short

def rr_ok(entry, sl, tp_mult=1.5):
    if sl is None: return False
    risk = abs(entry - sl)
    return risk > 0 and (tp_mult*risk) / risk >= 1.2

def riyadh_now(): return datetime.now(RIYADH_TZ)

def seconds_until_riyadh(hour=22, minute=0):
    now = riyadh_now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds(), target

def format_alert(pair, direction, entry, sl, tp1, tp2, reason, tf, score):
    return (
f"[TRADE ALERT]\n"
f"Pair: {pair}\n"
f"Direction: {direction}\n"
f"Entry: {entry:.6f}\n"
f"Stop Loss: {sl:.6f}\n"
f"Take Profit Targets: {tp1:.6f}, {tp2:.6f}\n"
f"Reason: {reason}\n"
f"Timeframe: {tf}\n"
f"Confidence Score: {score}/10"
)

def format_result(pair, tf, direction, result, entry, sl, tp1):
    return (
f"[RESULT] {result}\n"
f"Pair: {pair}  TF: {tf}\n"
f"Direction: {direction}\n"
f"Entry: {entry:.6f}  SL: {sl:.6f}  TP1: {tp1:.6f}"
)

def format_stats_line():
    total = stats["unique"]; wins = stats["wins"]; losses = stats["losses"]
    wr = (wins / total * 100.0) if total > 0 else 0.0
    return f"Unique Alerts: {total} | Wins: {wins} | Losses: {losses} | Win rate: {wr:.1f}%"

# ========= BINANCE WRAPPERS =========
BINANCE_BASE = "https://fapi.binance.com"

async def bn_exchange_info(session):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/exchangeInfo")

async def bn_24h_all(session):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/ticker/24hr")

async def bn_klines(session, symbol, interval, limit=2):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/klines",
                           params={"symbol":symbol,"interval":interval,"limit":limit})

async def bn_open_interest(session, symbol):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/openInterest", params={"symbol":symbol})

async def bn_premium_index(session, symbol):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/premiumIndex", params={"symbol":symbol})

async def bn_depth(session, symbol, limit=100):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/depth", params={"symbol":symbol,"limit":limit})

async def bn_book_ticker(session, symbol):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/ticker/bookTicker", params={"symbol":symbol})

# ========= OPTIONAL: COINGLASS OI =========
async def cg_open_interest_delta5m(session, symbol, exch="BINANCE"):
    if not COINGLASS_API_KEY:
        return None
    headers = {"coinglassSecret": COINGLASS_API_KEY}
    j = await http_json(session, "https://open-api.coinglass.com/api/futures/openInterest",
                        params={"symbol": symbol.replace("USDT",""), "exchange": exch},
                        headers=headers, timeout=12)
    # TODO: compute 5m %Δ from their response once field names confirmed.
    return None

# ========= DISCOVERY =========
async def discover_top_usdt_perps(session, limit):
    info = await bn_exchange_info(session)
    if not info:
        raise RuntimeError("exchangeInfo failed")
    perp_symbols = {s['symbol'] for s in info['symbols']
                    if s.get('contractType')=='PERPETUAL' and s.get('quoteAsset')=='USDT'}
    tickers = await bn_24h_all(session)
    ranked = sorted([t for t in tickers if t['symbol'] in perp_symbols],
                    key=lambda x: float(x.get('quoteVolume','0')), reverse=True)
    return [t['symbol'] for t in ranked[:limit]]

# ========= WARMUP =========
async def warmup_klines(session, symbols):
    sem = asyncio.Semaphore(6)
    async def load(sym, tf):
        async with sem:
            data = await bn_klines(session, sym, tf, limit=21)
            if not data or len(data)<21: return
            for row in data[:-1]:
                h,l,c,vol,ct = float(row[2]), float(row[3]), float(row[4]), float(row[5]), int(row[6])
                buf = kbuf[sym][tf]
                buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(vol); buf['cl'].append(c); buf['last_close']=ct
    await asyncio.gather(*(load(s, tf) for s in symbols for tf in TIMEFRAMES))

# ========= OI (1/min) =========
async def oi_loop(session, symbols):
    while True:
        t0 = now_ms()
        sem = asyncio.Semaphore(10)
        async def one(sym):
            async with sem:
                j = await bn_open_interest(session, sym)
                if j and 'openInterest' in j:
                    oi_hist[sym].append((t0, float(j['openInterest'])))
        await asyncio.gather(*(one(s) for s in symbols))
        await asyncio.sleep(60 - (time.time()%60) + 0.02)

def oi_5min_change_pct(sym):
    q = oi_hist[sym]
    if len(q) < 5: return None
    newest_ts, newest_oi = q[-1]
    target = newest_ts - 5*60*1000 - 5000
    older = None
    for ts, val in reversed(q):
        if ts <= target:
            older = (ts, val); break
    if not older: older = q[0]
    old_ts, old_oi = older
    if old_oi == 0: return None
    return (newest_oi - old_oi) / old_oi

# ========= MACRO BTC SHOCK =========
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

# ========= CORRELATION (BTC/ETH 5m return) =========
async def correlation_soft_flag(session, direction):
    async def ret5m(sym):
        data = await bn_klines(session, sym, "5m", limit=2)
        if not data or len(data)<2: return 0.0
        prev, last = data[-2], data[-1]
        return (float(last[4]) - float(prev[4]))/float(prev[4])
    btc, eth = await asyncio.gather(ret5m("BTCUSDT"), ret5m("ETHUSDT"))
    soft = False
    if direction=="Long" and (btc < -0.005 or eth < -0.005): soft = True
    if direction=="Short" and (btc >  0.005 or eth >  0.005): soft = True
    return soft, btc, eth

# ========= SESSION VOL (SOFT) =========
def session_soft_flag(tf, closes):
    if len(closes) < 20: return True  # cautious
    rets = [abs((closes[i]-closes[i-1])/closes[i-1]) for i in range(1,len(closes))]
    atrp = sum(rets[-20:])/20.0
    th = {"15m":0.0060, "30m":0.0090, "1h":0.0130}.get(tf, 0.009)
    return atrp < th

# ========= SPREAD & DEPTH (MUST) =========
def depth_checks(orderbook, price):
    try:
        best_ask = float(orderbook['asks'][0][0])
        best_bid = float(orderbook['bids'][0][0])
        spread = (best_ask - best_bid) / ((best_ask + best_bid)/2)
        if spread > SPREAD_MAX_ABS:
            return False, spread, 0.0
        up_lim  = best_ask * 1.01
        dn_lim  = best_bid * 0.99
        ask_usd = sum(float(p)*float(q) for p,q in orderbook['asks'] if float(p)<=up_lim)
        bid_usd = sum(float(p)*float(q) for p,q in orderbook['bids'] if float(p)>=dn_lim)
        depth_usd = ask_usd + bid_usd
        if depth_usd < DEPTH_1PCT_MIN_USD:
            return False, spread, depth_usd
        return True, spread, depth_usd
    except Exception:
        return False, 1.0, 0.0

# ========= PRICE ACTION + VOL (+ REVERSAL) =========
def detect_signal(sym, tf, last_closed_row, prev_row=None):
    close_time = int(last_closed_row[6])
    o = float(last_closed_row[1]); c = float(last_closed_row[4])
    h = float(last_closed_row[2]); l = float(last_closed_row[3])
    v = float(last_closed_row[5])

    po = pc = ph = pl = None
    if prev_row:
        po = float(prev_row[1]); pc = float(prev_row[4])
        ph = float(prev_row[2]); pl = float(prev_row[3])

    buf = kbuf[sym][tf]
    if buf['last_close'] and close_time <= buf['last_close']:
        return None
    if len(buf['vol']) < 20:
        return None

    avg_vol = sum(buf['vol'])/len(buf['vol'])
    vol_ok  = v >= avg_vol * VOL_SURGE_MIN

    range_hi = max(buf['hi']) if buf['hi'] else None
    range_lo = min(buf['lo']) if buf['lo'] else None
    if range_hi is None or range_lo is None:
        return None

    pad = BREAKOUT_PAD_BPS/10000.0
    long_break  = c > range_hi*(1+pad)
    short_break = c < range_lo*(1-pad)

    # Engulfing after sweep (reversal)
    bull_engulf = bear_engulf = False
    if prev_row is not None:
        prev_bear = pc < po
        prev_bull = pc > po
        body_engulf_up   = (c > o) and prev_bear and (c >= po) and (o <= pc)
        body_engulf_down = (c < o) and prev_bull and (c <= po) and (o >= pc)
        lo10 = min(list(buf['lo'])[-10:]) if len(buf['lo']) else None
        hi10 = max(list(buf['hi'])[-10:]) if len(buf['hi']) else None
        sweep_down = (lo10 is not None) and (l <= lo10)
        sweep_up   = (hi10 is not None) and (h >= hi10)
        bull_engulf = body_engulf_up and sweep_down
        bear_engulf = body_engulf_down and sweep_up

    # update buffers after evaluating
    buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=close_time
    if len(buf['cl'])>21: buf['cl'].popleft()

    if not vol_ok:
        return None

    if long_break or short_break:
        direction = "Long" if long_break else "Short"
        return {"direction":direction, "close":c, "open":o}
    if bull_engulf:
        return {"direction":"Long", "close":c, "open":o}
    if bear_engulf:
        return {"direction":"Short", "close":c, "open":o}
    return None

# ========= SCAN LOOP PER TF =========
async def scan_loop(session, symbols, tf):
    while True:
        await asyncio.sleep(max(0, next_close_ms(tf) - now_ms() + 500)/1000)
        if not macro_ok():
            continue

        sem = asyncio.Semaphore(6)
        alerts = []

        async def process(sym):
            nonlocal alerts
            async with sem:
                data = await bn_klines(session, sym, tf, limit=3)
                if not data or len(data)<3:
                    return
                prev = data[-2]
                last_closed = data[-1]
                sig = detect_signal(sym, tf, last_closed, prev_row=prev)
                if not sig:
                    return

                # Funding (must)
                finfo = await bn_premium_index(session, sym)
                if not finfo or 'lastFundingRate' not in finfo: return
                fr = float(finfo['lastFundingRate'])
                if abs(fr) > FUNDING_MAX_ABS: return

                # OI (soft if missing)
                oi_pct = oi_5min_change_pct(sym)
                oi_soft_fail = False
                if oi_pct is None:
                    cg = await cg_open_interest_delta5m(session, sym)
                    oi_pct = cg
                if oi_pct is None or abs(oi_pct) < OI_DELTA_MIN:
                    oi_soft_fail = True
                if HARD_REQUIRE_OI and oi_soft_fail:
                    return

                # Spread & depth (must)
                ob = await bn_depth(session, sym, limit=100)
                price = sig['close']
                ok_depth, spread, depth_usd = depth_checks(ob, price)
                if not ok_depth:
                    return

                # RR & SL sanity (must)
                buf = kbuf[sym][tf]
                sl_long, sl_short = swing_levels(buf['lo'], buf['hi'])
                entry = sig['close']
                if sig['direction']=="Long" and sl_long is not None:
                    if not rr_ok(entry, sl_long): return
                    sl = sl_long
                    risk = max(1e-9, entry - sl)
                    tp1, tp2 = entry + 0.75*risk, entry + 1.50*risk
                    reason_prefix = "Breakout + pad" if entry > max(buf['cl']) else "Engulfing reversal + sweep"
                    risk_pct = (entry - sl)/entry if sl else 999
                elif sig['direction']=="Short" and sl_short is not None:
                    if not rr_ok(entry, sl_short): return
                    sl = sl_short
                    risk = max(1e-9, sl - entry)
                    tp1, tp2 = entry - 0.75*risk, entry - 1.50*risk
                    reason_prefix = "Breakdown + pad" if entry < min(buf['cl']) else "Engulfing reversal + sweep"
                    risk_pct = (sl - entry)/entry if sl else 999
                else:
                    return

                # Oversized stop filter
                if risk_pct <= 0 or risk_pct > MAX_RISK_PCT:
                    return

                # Correlation (soft / hard block option)
                corr_soft, btc5m, eth5m = await correlation_soft_flag(session, sig['direction'])
                if CORR_HARD_BLOCK:
                    if sig['direction']=="Long" and (btc5m < -0.007 or eth5m < -0.007):
                        return
                    if sig['direction']=="Short" and (btc5m >  0.007 or eth5m >  0.007):
                        return

                # Debounce per symbol/tf
                now_s = int(time.time())
                if now_s - last_alert_at[sym][tf] < COOLDOWN_SEC: return
                last_alert_at[sym][tf] = now_s

                # Score
                score = 6
                info_bits = [f"Vol≥{VOL_SURGE_MIN:.2f}x", f"Funding {fr*100:.3f}%", f"Spread {spread*100:.2f}%", f"Depth1% ${depth_usd:,.0f}"]
                if oi_soft_fail:
                    score -= 1; info_bits.append("OI soft-fail")
                else:
                    score += 1; info_bits.append(f"OIΔ5m {oi_pct*100:.2f}%")
                if corr_soft:
                    score -= 1; info_bits.append(f"Corr soft: BTC {btc5m*100:.2f}%, ETH {eth5m*100:.2f}%")
                else:
                    score += 1
                sess_soft = session_soft_flag(tf, list(buf['cl']))
                if sess_soft:
                    score -= 1; info_bits.append("Session soft")
                else:
                    score += 1

                reason = f"{reason_prefix}; " + ", ".join(info_bits)

                final_score = max(1, min(10, score))
                if final_score < MIN_CONF:
                    return

                # Dedup for stats (still send alert)
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
                        "entry": entry, "sl": sl, "tp1": tp1, "start_ms": nowm
                    }

                alerts.append(
                    format_alert(sym, sig['direction'], entry, sl, tp1, tp2, reason, tf, final_score)
                )

        await asyncio.gather(*(process(s) for s in symbols))
        for a in alerts:
            await tg_send(session, a)

# ========= RESULT RESOLVER (TP1 vs SL) =========
async def result_resolver_loop(session):
    while True:
        if not active_trades:
            await asyncio.sleep(5)
            continue
        keys = list(active_trades.keys())
        sem = asyncio.Semaphore(8)
        async def resolve_one(k):
            async with sem:
                tr = active_trades.get(k)
                if not tr: return
                sym, direction = tr["symbol"], tr["direction"]
                entry, sl, tp1 = tr["entry"], tr["sl"], tr["tp1"]
                start_ms = tr["start_ms"]

                if now_ms() - start_ms > WIN_TIMEOUT_MIN*60*1000:
                    if k in current_day_keys:
                        stats["losses"] += 1
                    del active_trades[k]
                    await tg_send(session, format_result(sym, tr["tf"], direction, "TIMEOUT (counted as LOSS)", entry, sl, tp1))
                    return

                jt = await bn_book_ticker(session, sym)
                if not jt: return
                bid = float(jt["bidPrice"]); ask = float(jt["askPrice"])

                if direction == "Long":
                    if bid >= tp1:
                        if k in current_day_keys: stats["wins"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result(sym, tr["tf"], direction, "WIN (TP1 hit)", entry, sl, tp1))
                    elif ask <= sl:
                        if k in current_day_keys: stats["losses"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result(sym, tr["tf"], direction, "LOSS (SL hit)", entry, sl, tp1))
                else:
                    if ask <= tp1:
                        if k in current_day_keys: stats["wins"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result(sym, tr["tf"], direction, "WIN (TP1 hit)", entry, sl, tp1))
                    elif bid >= sl:
                        if k in current_day_keys: stats["losses"] += 1
                        del active_trades[k]
                        await tg_send(session, format_result(sym, tr["tf"], direction, "LOSS (SL hit)", entry, sl, tp1))
        await asyncio.gather(*(resolve_one(k) for k in keys))
        await asyncio.sleep(5)

# ========= DAILY STATS AT 22:00 RIYADH =========
def format_stats_line():
    total = stats["unique"]; wins = stats["wins"]; losses = stats["losses"]
    wr = (wins / total * 100.0) if total > 0 else 0.0
    return f"Unique Alerts: {total} | Wins: {wins} | Losses: {losses} | Win rate: {wr:.1f}%"

async def daily_stats_loop(session):
    while True:
        sleep_s, target_dt = seconds_until_riyadh(STATS_DAILY_HOUR, 0)
        await asyncio.sleep(sleep_s)
        today_2200 = target_dt
        yesterday_2200 = today_2200 - timedelta(days=1)
        period_str = f"{yesterday_2200.strftime('%Y-%m-%d %H:%M')} → {today_2200.strftime('%Y-%m-%d %H:%M')} (Riyadh)"
        msg = f"[DAILY STATS]\nPeriod: {period_str}\n{format_stats_line()}"
        await tg_send(session, msg)
        stats["unique"] = 0; stats["wins"] = 0; stats["losses"] = 0
        current_day_keys.clear()

# ========= WEB HEALTH =========
async def health(_): return web.Response(text="ok")

# ========= APP MAIN =========
async def app_main():
    async with aiohttp.ClientSession() as session:
        symbols = await discover_top_usdt_perps(session, SCAN_LIMIT)
        print("Scanning symbols:", symbols)
        await warmup_klines(session, symbols)
        print("Warmup complete.")

        tasks = [
            asyncio.create_task(oi_loop(session, symbols)),
            asyncio.create_task(macro_guard_loop(session)),       # BTC 1m macro shock
            asyncio.create_task(scan_loop(session, symbols, "15m")),
            asyncio.create_task(scan_loop(session, symbols, "30m")),
            asyncio.create_task(scan_loop(session, symbols, "1h")),
            asyncio.create_task(result_resolver_loop(session)),
            asyncio.create_task(daily_stats_loop(session)),       # daily digest 22:00 Riyadh
        ]

        app = web.Application()
        app.router.add_get("/", health)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT","8080"))); await site.start()
        print("HTTP health server started.")

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except Exception:
        pass
    asyncio.run(app_main())
