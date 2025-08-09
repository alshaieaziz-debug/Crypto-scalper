import os, asyncio, time, math
from collections import deque, defaultdict
import aiohttp
from aiohttp import web

# ========= ENV CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Tunables
SCAN_LIMIT            = int(os.getenv("SCAN_LIMIT", "30"))     # change to 100 after first run
MIN_CONF = int(os.getenv("MIN_CONF", "8"))
BREAKOUT_PAD_BPS      = float(os.getenv("BREAKOUT_PAD_BPS", "5"))     # 0.05% pad
VOL_SURGE_MIN         = float(os.getenv("VOL_SURGE_MIN", "1.25"))      # >=25% above 20-c avg
OI_DELTA_MIN          = float(os.getenv("OI_DELTA_MIN", "0.02"))       # >=2% in last ~5m
FUNDING_MAX_ABS       = float(os.getenv("FUNDING_MAX_ABS", "0.0005"))  # ±0.05%
SPREAD_MAX_ABS        = float(os.getenv("SPREAD_MAX_ABS", "0.0006"))   # 0.06%
DEPTH_1PCT_MIN_USD    = float(os.getenv("DEPTH_1PCT_MIN_USD", "20000"))# combined depth within 1%
MACRO_BTC_SHOCK_BP    = float(os.getenv("MACRO_BTC_SHOCK_BP", "180"))  # 1.8% 1m shock bars
MACRO_COOLDOWN_SEC    = int(os.getenv("MACRO_COOLDOWN_SEC", "600"))    # 10 min pause
NEWS_PAUSE            = os.getenv("NEWS_PAUSE","false").lower() == "true"
COINGLASS_API_KEY     = os.getenv("COINGLASS_API_KEY","")              # optional

# ========= ENDPOINTS (Binance USDⓈ-M) =========
BINANCE_BASE = "https://fapi.binance.com"

# ========= STATE =========
kbuf = defaultdict(lambda: {
    '1m':  {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
    '5m':  {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
    '15m': {'vol': deque(maxlen=20), 'hi': deque(maxlen=20), 'lo': deque(maxlen=20), 'cl': deque(maxlen=21), 'last_close': None},
})
oi_hist = defaultdict(lambda: deque(maxlen=10))  # symbol -> [(ts_ms, oi_float)]
last_alert_at = defaultdict(lambda: {'1m':0,'5m':0,'15m':0})
macro_block_until = 0
TIMEFRAMES = ["1m","5m","15m"]

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

async def tg_send(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with session.post(url, json=payload, timeout=8) as r:
            if r.status != 200:
                print("Telegram error:", r.status, await r.text())
    except Exception as e:
        print("Telegram exception:", e)

def pct(a,b): 
    try: return (a-b)/b
    except ZeroDivisionError: return 0.0

def next_close_ms(tf):
    s = int(time.time())
    if tf=="1m":  return (s - (s%60) + 60)*1000
    if tf=="5m":  return (s - (s%300) + 300)*1000
    if tf=="15m": return (s - (s%900) + 900)*1000

def swing_levels(lows, highs):
    sl_long = min(list(lows)[-3:]) if len(lows)>=3 else (min(lows) if lows else None)
    sl_short = max(list(highs)[-3:]) if len(highs)>=3 else (max(highs) if highs else None)
    return sl_long, sl_short

def rr_ok(entry, sl, tp_mult=1.5):
    if sl is None: return False
    risk = abs(entry - sl)
    return risk > 0 and (tp_mult*risk) / risk >= 1.2  # always true if tp_mult>=1.2, kept for clarity

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

# ========= BINANCE WRAPPERS =========
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

async def bn_depth(session, symbol, limit=50):
    return await http_json(session, f"{BINANCE_BASE}/fapi/v1/depth", params={"symbol":symbol,"limit":limit})

# ========= OPTIONAL: COINGLASS OI =========
async def cg_open_interest_delta5m(session, symbol, exch="BINANCE"):
    if not COINGLASS_API_KEY:
        return None  # no key, soft-fail upstream
    # Placeholder: adjust to your Coinglass endpoint
    # Here we simply return None if call fails; you can wire their exact path once you have a key.
    headers = {"coinglassSecret": COINGLASS_API_KEY}
    # Example (not guaranteed): /api/v2/open_interest?symbol=BTC&exchange=BINANCE
    j = await http_json(session, "https://open-api.coinglass.com/api/futures/openInterest", 
                        params={"symbol": symbol.replace("USDT",""), "exchange": exch}, headers=headers, timeout=12)
    if not j: return None
    # Expect the response to include recent OI history… you’d compute 5m %Δ here.
    return None  # keep None until you plug the exact fields

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
    # rough thresholds
    th = {"1m":0.0015, "5m":0.0035, "15m":0.0060}.get(tf, 0.003)
    return atrp < th

# ========= SPREAD & DEPTH (MUST) =========
def depth_checks(orderbook, price):
    try:
        best_ask = float(orderbook['asks'][0][0])
        best_bid = float(orderbook['bids'][0][0])
        spread = (best_ask - best_bid) / ((best_ask + best_bid)/2)
        if spread > SPREAD_MAX_ABS: 
            return False, spread, 0.0
        # 1% depth (USD)
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

# ========= PRICE ACTION + VOL =========
def detect_signal(sym, tf, last_closed_row):
    close_time = int(last_closed_row[6])
    open_ = float(last_closed_row[1])
    close = float(last_closed_row[4])
    high  = float(last_closed_row[2])
    low   = float(last_closed_row[3])
    vol   = float(last_closed_row[5])

    buf = kbuf[sym][tf]
    if buf['last_close'] and close_time <= buf['last_close']: return None
    if len(buf['vol']) < 20: return None

    avg_vol = sum(buf['vol'])/len(buf['vol'])
    vol_ok  = vol >= avg_vol * VOL_SURGE_MIN

    range_hi = max(buf['hi']) if buf['hi'] else None
    range_lo = min(buf['lo']) if buf['lo'] else None
    if range_hi is None or range_lo is None: return None

    pad = BREAKOUT_PAD_BPS/10000.0
    long_break  = close > range_hi*(1+pad)
    short_break = close < range_lo*(1-pad)

    # Always update buffers
    buf['hi'].append(high); buf['lo'].append(low); buf['vol'].append(vol); buf['cl'].append(close); buf['last_close']=close_time
    if len(buf['cl'])>21: buf['cl'].popleft()

    if not vol_ok: return None
    if not (long_break or short_break): 
        # (Would add reversal/engulfing logic here later if needed)
        return None

    direction = "Long" if long_break else "Short"
    return {"direction":direction, "close":close, "open":open_}

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
                data = await bn_klines(session, sym, tf, limit=2)
                if not data or len(data)<2: return
                last_closed = data[-1]
                sig = detect_signal(sym, tf, last_closed)
                if not sig: return

                # --- Funding (must) ---
                finfo = await bn_premium_index(session, sym)
                if not finfo or 'lastFundingRate' not in finfo: return
                fr = float(finfo['lastFundingRate'])
                if abs(fr) > FUNDING_MAX_ABS: return

                # --- OI (soft if missing) ---
                oi_pct = oi_5min_change_pct(sym)
                oi_soft_fail = False
                if oi_pct is None:
                    # try Coinglass if key present (placeholder returns None until wired)
                    cg = await cg_open_interest_delta5m(session, sym)
                    oi_pct = cg
                if oi_pct is None or abs(oi_pct) < OI_DELTA_MIN:
                    oi_soft_fail = True

                # --- Spread & depth (must) ---
                ob = await bn_depth(session, sym, limit=100)
                price = sig['close']
                ok_depth, spread, depth_usd = depth_checks(ob, price)
                if not ok_depth: 
                    return

                # --- RR check (must) ---
                buf = kbuf[sym][tf]
                sl_long, sl_short = swing_levels(buf['lo'], buf['hi'])
                entry = sig['close']
                if sig['direction']=="Long" and sl_long is not None:
                    if not rr_ok(entry, sl_long): 
                        return
                elif sig['direction']=="Short" and sl_short is not None:
                    if not rr_ok(entry, sl_short):
                        return
                else:
                    return

                # --- Correlation (soft) ---
                corr_soft, btc5m, eth5m = await correlation_soft_flag(session, sig['direction'])

                # --- Session vol (soft) ---
                sess_soft = session_soft_flag(tf, list(buf['cl']))

                # --- Debounce per symbol/tf ---
                now_s = int(time.time())
                if now_s - last_alert_at[sym][tf] < 60: return
                last_alert_at[sym][tf] = now_s

                # --- Build TP/SL + score ---
                if sig['direction']=="Long":
                    sl = sl_long
                    risk = max(1e-9, entry - sl)
                    tp1, tp2 = entry + 0.75*risk, entry + 1.50*risk
                else:
                    sl = sl_short
                    risk = max(1e-9, sl - entry)
                    tp1, tp2 = entry - 0.75*risk, entry - 1.50*risk

                score = 6  # core musts passed to reach here
                info_bits = [f"Vol≥{VOL_SURGE_MIN:.2f}x", f"Funding {fr*100:.3f}%", f"Spread {spread*100:.2f}%", f"Depth1% ${depth_usd:,.0f}"]
                if oi_soft_fail:
                    score -= 1; info_bits.append("OI soft-fail")
                else:
                    score += 1; info_bits.append(f"OIΔ5m {oi_pct*100:.2f}%")
                if corr_soft:
                    score -= 1; info_bits.append(f"Corr soft: BTC {btc5m*100:.2f}%, ETH {eth5m*100:.2f}%")
                else:
                    score += 1
                if sess_soft:
                    score -= 1; info_bits.append("Session soft")
                else:
                    score += 1

                reason = ("Breakout + pad; " if sig['direction']=="Long" else "Breakdown + pad; ") + ", ".join(info_bits)
                # Require high-confidence alerts only
                final_score = max(1, min(10, score))
                if final_score < MIN_CONF:
                   return
alerts.append(
    format_alert(sym, sig['direction'], entry, sl, tp1, tp2, reason, tf, final_score)
)

        await asyncio.gather(*(process(s) for s in symbols))
        for a in alerts:
            await tg_send(session, a)

# ========= WEB HEALTH =========
async def health(_): return web.Response(text="ok")

async def app_main():
    async with aiohttp.ClientSession() as session:
        # Discover & warm
        symbols = await discover_top_usdt_perps(session, SCAN_LIMIT)
        print("Scanning symbols:", symbols)
        await warmup_klines(session, symbols)
        print("Warmup complete.")

        # Background loops
        tasks = [
            asyncio.create_task(oi_loop(session, symbols)),
            asyncio.create_task(macro_guard_loop(session)),
            asyncio.create_task(scan_loop(session, symbols, "1m")),
            asyncio.create_task(scan_loop(session, symbols, "5m")),
            asyncio.create_task(scan_loop(session, symbols, "15m")),
        ]

        # Minimal web server for Railway health
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
