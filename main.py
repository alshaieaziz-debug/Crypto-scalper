import os, asyncio, time, json, random, math, signal, hmac, hashlib
from collections import deque, defaultdict
import aiohttp
from aiohttp import web
from datetime import datetime, timezone, timedelta

# ========= BASIC CONFIG (RELAXED DEFAULTS) =========
RIYADH_TZ = timezone(timedelta(hours=3))
TIMEFRAMES = ["15m","30m","1h"]
PORT = int(os.getenv("PORT","8080"))

# Alerts
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID","")

# Trading (MEXC futures)
MEXC_LIVE          = os.getenv("MEXC_LIVE","false").lower()=="true"
MEXC_API_KEY       = os.getenv("MEXC_API_KEY","")
MEXC_API_SECRET    = os.getenv("MEXC_API_SECRET","")
MEXC_OPEN_TYPE     = int(os.getenv("MEXC_OPEN_TYPE","1"))        # 1=Isolated, 2=Cross
MEXC_POSITION_MODE = int(os.getenv("MEXC_POSITION_MODE","2"))    # 2=One-way
MEXC_LEVERAGE      = int(os.getenv("MEXC_LEVERAGE","5"))
MEXC_RISK_USDT     = float(os.getenv("MEXC_RISK_USDT","3"))
MEXC_RECV_WINDOW   = os.getenv("MEXC_RECV_WINDOW","10000")

# Discovery / scanning
SCAN_LIMIT         = int(os.getenv("SCAN_LIMIT","120"))
STATS_DAILY_HOUR   = int(os.getenv("STATS_DAILY_HOUR","22"))

# Core thresholds (relaxed)
MIN_CONF_15M       = int(os.getenv("MIN_CONF_15M","3"))
MIN_CONF_30M       = int(os.getenv("MIN_CONF_30M","3"))
MIN_CONF_1H        = int(os.getenv("MIN_CONF_1H","3"))
LONG_SIDE_PENALTY  = int(os.getenv("LONG_SIDE_PENALTY","0"))
VOL_SURGE_MIN      = float(os.getenv("VOL_SURGE_MIN","1.10"))
BREAKOUT_PAD_BPS   = float(os.getenv("BREAKOUT_PAD_BPS","5"))
BREAKOUT_LOOKBACK_BARS = int(os.getenv("BREAKOUT_LOOKBACK_BARS","20"))

# Tape / CVD (lenient)
TAPE_REQUIRE_ALWAYS= os.getenv("TAPE_REQUIRE_ALWAYS","false").lower()=="true"
TAPE_LOOKBACK_SEC  = int(os.getenv("TAPE_LOOKBACK_SEC","15"))
TAPE_MIN_NOTIONAL  = float(os.getenv("TAPE_MIN_NOTIONAL","20000"))
TAPE_TILT_WEAK     = float(os.getenv("TAPE_TILT_WEAK","0.03"))
TAPE_TILT_STRONG   = float(os.getenv("TAPE_TILT_STRONG","0.08"))

# Market micro (lenient)
SPREAD_MAX_ABS     = float(os.getenv("SPREAD_MAX_ABS","0.0020"))
DEPTH_1PCT_MIN_USD = float(os.getenv("DEPTH_1PCT_MIN_USD","20000"))

# Funding (lenient)
FUNDING_MAX_ABS    = float(os.getenv("FUNDING_MAX_ABS","0.0010"))

# OI (soft)
HARD_REQUIRE_OI    = os.getenv("HARD_REQUIRE_OI","false").lower()=="true"
OI_DELTA_MIN_BASE  = float(os.getenv("OI_DELTA_MIN","0.01"))

# Correlation & session (no hard blocks)
CORR_HARD_BLOCK    = os.getenv("CORR_HARD_BLOCK","false").lower()=="true"
HARD_BLOCK_SOFT_SESSION_BREAKOUT = os.getenv("HARD_BLOCK_SOFT_SESSION_BREAKOUT","false").lower()=="true"

# Macro guard (easy)
MACRO_BTC_SHOCK_BP = float(os.getenv("MACRO_BTC_SHOCK_BP","400"))
MACRO_COOLDOWN_SEC = int(os.getenv("MACRO_COOLDOWN_SEC","300"))
NEWS_PAUSE         = os.getenv("NEWS_PAUSE","false").lower()=="true"

# Risk & lifecycle
MAX_RISK_PCT       = float(os.getenv("MAX_RISK_PCT","0.012"))
COOLDOWN_SEC       = int(os.getenv("COOLDOWN_SEC","300"))
DEDUP_MIN          = int(os.getenv("DEDUP_MIN","5"))
WIN_TIMEOUT_15M_MIN= int(os.getenv("WIN_TIMEOUT_15M_MIN","120"))
WIN_TIMEOUT_30M_MIN= int(os.getenv("WIN_TIMEOUT_30M_MIN","180"))
WIN_TIMEOUT_1H_MIN = int(os.getenv("WIN_TIMEOUT_1H_MIN","300"))
EARLY_EXIT_CHECK_FRAC = float(os.getenv("EARLY_EXIT_CHECK_FRAC","0.25"))
EARLY_EXIT_MIN_MFE_R  = float(os.getenv("EARLY_EXIT_MIN_MFE_R","0.3"))
TP1_BE_OFFSET_R       = float(os.getenv("TP1_BE_OFFSET_R","0.2"))

# ========= STATE =========
MEXC_BASE="https://contract.mexc.com"
kbuf=defaultdict(lambda:{
    '15m':{'vol':deque(maxlen=64),'hi':deque(maxlen=64),'lo':deque(maxlen=64),'cl':deque(maxlen=64),'last_close':None},
    '30m':{'vol':deque(maxlen=64),'hi':deque(maxlen=64),'lo':deque(maxlen=64),'cl':deque(maxlen=64),'last_close':None},
    '1h': {'vol':deque(maxlen=64),'hi':deque(maxlen=64),'lo':deque(maxlen=64),'cl':deque(maxlen=64),'last_close':None},
})
symbol_meta = {}  # sym -> {pricePrecision, qtyPrecision, contractSize}
oi_hist     = defaultdict(lambda: deque(maxlen=120))
oi_abs_ema  = defaultdict(float)
last_alert_at=defaultdict(lambda:{'15m':0,'30m':0,'1h':0})
last_be_at   =defaultdict(lambda:{'15m':0,'30m':0,'1h':0})
be_override_used_at=defaultdict(lambda:{'15m':0,'30m':0,'1h':0})
macro_block_until=0
stats={"unique":0,"wins":0,"losses":0,"breakevens":0}
dedup_seen=deque(maxlen=4000)
active_trades={}
current_day_keys=set()
micro_hist=defaultdict(lambda:{"spread":deque(maxlen=240),"depth":deque(maxlen=240)})
alert_counts={"15m":0,"30m":0,"1h":0}
drop_counters=defaultdict(int)
event_loop_lag_ms=0.0

# ========= UTILS =========
def now_ms(): return int(time.time()*1000)
def riyadh_now(): return datetime.now(RIYADH_TZ)
def seconds_until_riyadh(h, m):
    n=riyadh_now(); t=n.replace(hour=h,minute=m,second=0,microsecond=0)
    if t<=n: t+=timedelta(days=1); return (t-n).total_seconds(), t
    return (t-n).total_seconds(), t
def tf_timeout_minutes(tf): return WIN_TIMEOUT_15M_MIN if tf=="15m" else (WIN_TIMEOUT_30M_MIN if tf=="30m" else WIN_TIMEOUT_1H_MIN)
def next_close_ms(tf):
    s=int(time.time())
    return (s - (s% (60 if tf=="1m" else (300 if tf=="5m" else (900 if tf=="15m" else (1800 if tf=="30m" else 3600))))) + (60 if tf=="1m" else (300 if tf=="5m" else (900 if tf=="15m" else (1800 if tf=="30m" else 3600)))))*1000
def sanitize_md(s): return s.replace("_","\\_")
def round_price(sym, px):
    p=symbol_meta.get(sym,{}).get("pricePrecision",6); f=10**p; return math.floor(px*f+0.5)/f
def trade_key(sym, tf, direction, entry): return f"{sym}|{tf}|{direction}|{round_price(sym,entry):.10f}"
def percentile(arr,p):
    if not arr: return None
    a=sorted(arr); k=(len(a)-1)*(p/100.0); f=math.floor(k); c=math.ceil(k)
    return a[int(k)] if f==c else a[f]*(c-k)+a[c]*(k-f)

# ========= HTTP + TELEGRAM =========
async def http_get(session, url, params=None, timeout=10):
    try:
        async with session.get(url, params=params, timeout=timeout) as r:
            if r.status!=200: return None
            try: return await r.json(content_type=None)
            except: return json.loads(await r.text())
    except: return None

async def http_post(session, url, payload=None, headers=None, timeout=10):
    try:
        async with session.post(url, json=payload, headers=headers or {}, timeout=timeout) as r:
            if r.status!=200: return None
            try: return await r.json(content_type=None)
            except: return json.loads(await r.text())
    except: return None

async def tg_send(session, text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID): return
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data={"chat_id":TELEGRAM_CHAT_ID,"text":text,"parse_mode":"Markdown"}
    try:
        async with session.post(url, data=json.dumps(data,ensure_ascii=False).encode("utf-8"),
                                headers={"Content-Type":"application/json; charset=utf-8"}, timeout=8) as r:
            if r.status!=200: print("TG send failed", r.status, await r.text())
    except Exception as e: print("TG error", e)

async def tg_send_batched(session, msgs):
    if not msgs: return
    batch=""
    for m in msgs:
        m=sanitize_md(m)
        if len(batch)+len(m)+2>3600:
            await tg_send(session,batch); batch=m
        else:
            batch = m if not batch else batch+"\n\n"+m
    if batch: await tg_send(session,batch)

# ========= MEXC API =========
def to_mexc(sym): return sym if "_" in sym else (sym[:-4]+"_USDT" if sym.endswith("USDT") else sym)
def from_mexc(s): return s.replace("_","")
def tf_to_mexc(tf): return {"1m":"Min1","5m":"Min5","15m":"Min15","30m":"Min30","1h":"Min60"}[tf]

async def mx_detail(session): return await http_get(session, f"{MEXC_BASE}/api/v1/contract/detail")
async def mx_kline(session, sym_u, tf, limit=64):
    j=await http_get(session, f"{MEXC_BASE}/api/v1/contract/kline/{sym_u}", params={"interval":tf_to_mexc(tf)})
    if not j or "data" not in j: return None
    d=j["data"]; ts=d.get("time",[]); o=d.get("open",[]); h=d.get("high",[]); l=d.get("low",[]); c=d.get("close",[]); v=d.get("vol",[])
    sec={"1m":60,"5m":300,"15m":900,"30m":1800,"1h":3600}[tf]; out=[]
    n=min(len(ts),len(o),len(h),len(l),len(c),len(v))
    for i in range(n):
        ot=int(ts[i])*1000; ct=ot+sec*1000
        out.append([ot,float(o[i]),float(h[i]),float(l[i]),float(c[i]),float(v[i]),ct])
    return out[-limit:] if limit and out else out
async def mx_depth(session, sym_u): return await http_get(session, f"{MEXC_BASE}/api/v1/contract/depth/{sym_u}")
async def mx_ticker(session, sym_u): 
    j=await http_get(session, f"{MEXC_BASE}/api/v1/contract/ticker", params={"symbol":sym_u})
    return j.get("data") if isinstance(j,dict) else j
async def mx_deals(session, sym_u, limit=100): return await http_get(session, f"{MEXC_BASE}/api/v1/contract/deals/{sym_u}", params={"limit":min(100,max(50,limit))})
async def mx_funding_rate(session, sym_u):
    j=await http_get(session, f"{MEXC_BASE}/api/v1/contract/funding_rate/{sym_u}")
    try: return float((j.get("data") or j).get("fundingRate"))
    except: return None

# ========= DISCOVERY =========
async def discover(session, limit):
    det=await mx_detail(session); 
    if not det or "data" not in det: raise RuntimeError("MEXC /detail failed")
    metas={}; syms=[]
    for it in det["data"]:
        if it.get("settleCoin")!="USDT" or it.get("state")!=0: continue
        sym_u=it["symbol"]; sym=from_mexc(sym_u)
        metas[sym]={"pricePrecision":int(it.get("priceScale",4)),"qtyPrecision":int(it.get("volScale",0)),"contractSize":float(it.get("contractSize",1.0))}
        syms.append(sym)
    vols={}
    async def one(s):
        t=await mx_ticker(session, to_mexc(s))
        if t: vols[s]=float(t.get("amount24",0.0))
    await asyncio.gather(*(one(s) for s in syms))
    ranked=sorted(syms, key=lambda s:vols.get(s,0.0), reverse=True)[:limit]
    for s in ranked: symbol_meta[s]=metas[s]
    return ranked

async def warmup(session, symbols):
    async def load(sym, tf):
        data=await mx_kline(session, to_mexc(sym), tf, limit=64)
        if not data or len(data)<21: return
        for row in data[:-1]:
            h,l,c,vol,ct=float(row[2]),float(row[3]),float(row[4]),float(row[5]),int(row[6])
            b=kbuf[sym][tf]; b['hi'].append(h); b['lo'].append(l); b['vol'].append(vol); b['cl'].append(c); b['last_close']=ct
    await asyncio.gather(*(load(s,tf) for s in symbols for tf in TIMEFRAMES))

# ========= OI / CORR / SESSION =========
async def oi_loop(session, symbols):
    while True:
        t0=now_ms()
        async def one(sym):
            d=await mx_ticker(session, to_mexc(sym))
            if d and "holdVol" in d:
                try: oi_hist[sym].append((t0,float(d["holdVol"])))
                except: pass
        await asyncio.gather(*(one(s) for s in symbols))
        await asyncio.sleep(60 - (time.time()%60) + 0.05)

def _oi_5m_from_queue(q):
    if len(q)<5: return None
    newest_ts,newest=q[-1]; target=newest_ts-5*60*1000-5000
    older=None
    for i in range(len(q)-1,-1,-1):
        ts,val=q[i]
        if ts<=target: older=(ts,val); break
    old=q[0][1] if not older else older[1]
    return None if old==0 else (newest-old)/old

def adaptive_oi_threshold(sym):
    q=oi_hist[sym]; newest=_oi_5m_from_queue(q)
    if newest is not None: oi_abs_ema[sym]=0.25*abs(newest)+0.75*oi_abs_ema[sym]
    ema=oi_abs_ema[sym]
    return max(OI_DELTA_MIN_BASE, 0.75*ema) if not (ema==0 and len(q)<11) else OI_DELTA_MIN_BASE

async def correlation_soft_flag(session, direction):
    async def ret5m(sym):
        d=await mx_kline(session, to_mexc(sym), "5m", limit=2)
        if not d or len(d)<2: return 0.0
        p,l=d[-2],d[-1]; return (float(l[4])-float(p[4]))/max(1e-12,float(p[4]))
    btc,eth=await asyncio.gather(ret5m("BTCUSDT"), ret5m("ETHUSDT"))
    soft = (direction=="Long" and (btc<-0.005 or eth<-0.005)) or (direction=="Short" and (btc>0.005 or eth>0.005))
    return soft, btc, eth

def session_soft_flag(tf, closes):
    if len(closes)<30: return None
    rets=[abs((closes[i]-closes[i-1])/max(1e-12,closes[i-1])) for i in range(1,len(closes))]
    atrp=sum(rets[-20:])/20.0; th={"15m":0.0060,"30m":0.0090,"1h":0.0130}[tf]
    return atrp < th

# ========= MACRO GUARD =========
async def macro_guard_loop(session):
    global macro_block_until
    while True:
        d=await mx_kline(session, to_mexc("BTCUSDT"), "1m", limit=21)
        if d and len(d)>=21:
            prev20=d[-21:-1]; last=d[-1]
            avg_vol=sum(float(r[5]) for r in prev20)/20.0
            last_vol=float(last[5]); o=float(last[1]); c=float(last[4])
            move_bp=abs((c-o)/max(1e-12,o))*10000
            if move_bp>=MACRO_BTC_SHOCK_BP and last_vol>=avg_vol*1.5:
                macro_block_until=now_ms()+MACRO_COOLDOWN_SEC*1000
        await asyncio.sleep(60)

def macro_ok(): return (not NEWS_PAUSE) and now_ms()>=macro_block_until

# ========= SIGNALS =========
def tf_swing_window(tf): return {"15m":5,"30m":3,"1h":2}[tf]
def true_atr(h,l,c,period=14):
    n=min(len(h),len(l),len(c))
    if n<period+1: return None
    trs=[max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(n-period,n)]
    return sum(trs)/len(trs) if trs else None

def rr_ok(entry, sl, tp1, min_r=1.0):
    if sl is None: return False
    risk=abs(entry-sl); 
    if risk<=0: return False
    reward=(tp1-entry) if tp1>entry else (entry-tp1)
    return (reward/risk)>=min_r

def detect_signal(sym, tf, last_row, prev_row, buf):
    ct=int(last_row[6]); o=float(last_row[1]); c=float(last_row[4]); h=float(last_row[2]); l=float(last_row[3]); v=float(last_row[5])
    if buf['last_close'] and ct<=buf['last_close']: return None
    if min(len(buf['vol']),len(buf['hi']),len(buf['lo']),len(buf['cl']))<20:
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=ct; return None
    hi_hist,lo_hist,cl_hist=list(buf['hi']),list(buf['lo']),list(buf['cl'])
    prev_hi=max(hi_hist[-BREAKOUT_LOOKBACK_BARS:]); prev_lo=min(lo_hist[-BREAKOUT_LOOKBACK_BARS:])
    avg_vol=sum(buf['vol'])/len(buf['vol'])
    if v < avg_vol*VOL_SURGE_MIN:
        buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=ct; return None
    atr=true_atr(hi_hist,lo_hist,cl_hist) or (BREAKOUT_PAD_BPS/10000.0)*c
    pad=(atr*(0.30 if tf=="15m" else (0.24 if tf=="30m" else 0.20))) if isinstance(atr,float) else atr
    long_break=c>(prev_hi+pad); short_break=c<(prev_lo-pad)
    bull=bear=False
    if prev_row:
        po,pc,ph,pl=float(prev_row[1]),float(prev_row[4]),float(prev_row[2]),float(prev_row[3])
        body_up=(c>o) and (pc<po) and (c>=po) and (o<=pc)
        body_dn=(c<o) and (pc>po) and (c<=po) and (o>=pc)
        lo10=min(lo_hist[-10:]); hi10=max(hi_hist[-10:])
        bull=body_up and (l<=lo10); bear=body_dn and (h>=hi10)
        if (long_break or short_break) and (h<=ph and l>=pl): long_break=short_break=False
    buf['hi'].append(h); buf['lo'].append(l); buf['vol'].append(v); buf['cl'].append(c); buf['last_close']=ct
    for k in("hi","lo","cl","vol"):
        if len(buf[k])>64: buf[k].popleft()
    if long_break:  return {"dir":"Long","c":c,"o":o,"prev_hi":prev_hi,"prev_lo":prev_lo,"is_bo":True,"hi":h,"lo":l}
    if short_break: return {"dir":"Short","c":c,"o":o,"prev_hi":prev_hi,"prev_lo":prev_lo,"is_bo":True,"hi":h,"lo":l}
    if bull: return {"dir":"Long","c":c,"o":o,"prev_hi":prev_hi,"prev_lo":prev_lo,"is_bo":False,"hi":h,"lo":l}
    if bear: return {"dir":"Short","c":c,"o":o,"prev_hi":prev_hi,"prev_lo":prev_lo,"is_bo":False,"hi":h,"lo":l}
    return None

def pick_swing_levels(tf, highs, lows, closes):
    n=tf_swing_window(tf)
    hi_list=list(highs)[-max(5,n*2):]; lo_list=list(lows)[-max(5,n*2):]
    fh,fl=[],[]
    for i in range(1,len(hi_list)):
        if hi_list[i]<=hi_list[i-1] and lo_list[i]>=lo_list[i-1]: continue
        fh.append(hi_list[i]); fl.append(lo_list[i])
    if not fh or not fl: fh,fl=hi_list,lo_list
    sl_long=min(fl[-n:]) if len(fl)>=n else min(fl) if fl else None
    sl_short=max(fh[-n:]) if len(fh)>=n else max(fh) if fh else None
    return sl_long, sl_short

# ========= TAPE / MICRO =========
def update_micro(sym, spread, depth_usd):
    micro_hist[sym]["spread"].append(max(1e-12,spread))
    micro_hist[sym]["depth"].append(max(0.0,depth_usd))

def micro_thresholds(sym):
    s=list(micro_hist[sym]["spread"]); d=list(micro_hist[sym]["depth"])
    if len(s)<25 or len(d)<25: return SPREAD_MAX_ABS, DEPTH_1PCT_MIN_USD
    p60=percentile(s,60) or SPREAD_MAX_ABS; p40=percentile(d,40) or DEPTH_1PCT_MIN_USD
    return min(max(SPREAD_MAX_ABS, p60*1.2), SPREAD_MAX_ABS*2.0), max(min(DEPTH_1PCT_MIN_USD, p40*0.9), DEPTH_1PCT_MIN_USD*0.6)

def depth_checks(orderbook, direction, max_spread, min_depth, contract_size):
    try:
        a0=orderbook['asks'][0]; b0=orderbook['bids'][0]
        ask=float(a0[0]); bid=float(b0[0])
        spread=(ask-bid)/max(1e-12,(ask+bid)/2)
        if spread>max_spread: return False,spread,0,0,0
        up=ask*1.01; dn=bid*0.99
        def sum_usd(side,cond):
            s=0.0
            for row in side:
                p=float(row[0]); q=float(row[1])
                if cond(p): s+=p*(q*contract_size)
            return s
        ask_usd=sum_usd(orderbook['asks'], lambda p:p<=up); bid_usd=sum_usd(orderbook['bids'], lambda p:p>=dn)
        total=ask_usd+bid_usd
        if total<min_depth: return False,spread,bid_usd,ask_usd,total
        if direction=="Long":
            if bid_usd<min_depth*0.45: return False,spread,bid_usd,ask_usd,total
            if (bid_usd/max(1e-9,total))<0.52: return False,spread,bid_usd,ask_usd,total
        else:
            if ask_usd<min_depth*0.45: return False,spread,bid_usd,ask_usd,total
            if (ask_usd/max(1e-9,total))<0.52: return False,spread,bid_usd,ask_usd,total
        return True,spread,bid_usd,ask_usd,total
    except: return False,1.0,0,0,0

async def tape_tilt(session, sym):
    d=await mx_deals(session, to_mexc(sym), limit=100)
    data=d.get("data") if isinstance(d,dict) else d
    if not data: return 0.0,0.0
    buy=sell=0.0; cs=symbol_meta.get(sym,{}).get("contractSize",1.0)
    for t in data:
        try:
            price=float(t.get("p",t.get("price",0.0))); qty=float(t.get("v",t.get("qty",0.0))); typ=int(t.get("T",t.get("type",0)))
        except: continue
        notional=price*(qty*cs); 
        if notional<=0: continue
        if typ==1: buy+=notional
        elif typ==2: sell+=notional
    tot=buy+sell
    return ((buy-sell)/tot, tot) if tot>0 else (0.0,0.0)

# ========= FETCH LAST CLOSED BAR =========
async def fetch_closed(session, sym, tf):
    data=await mx_kline(session, to_mexc(sym), tf, limit=3)
    if not data or len(data)<3: return None,None
    prev,last=data[-2],data[-1]
    if int(last[6])<=now_ms()-1000: return prev,last
    await asyncio.sleep(1.0)
    data2=await mx_kline(session, to_mexc(sym), tf, limit=3)
    if data2 and len(data2)>=3 and int(data2[-1][6])<=now_ms()-1000: return data2[-2],data2[-1]
    return data[-3],data[-2]

# ========= FORMATTING =========
def leaderboard_line():
    total=stats["unique"]; w=stats["wins"]; l=stats["losses"]; be=stats["breakevens"]
    wr=(w/total*100.0) if total>0 else 0.0
    return f"ð *Today*: ð¢ {w}  ð´ {l}  âªï¸ {be}  |  WR: {wr:.1f}%  |  Alerts: {total}"

def fmt_alert(pair, direction, entry, sl, tp1, tp2, reason, tf, score, extras=""):
    ex=f"\n{extras}" if extras else ""
    return (f"ð *TRADE ALERT*\nâ¢ Pair: `{pair}`  â¢ TF: *{tf}*\nâ¢ Direction: *{direction}*\n"
            f"â¢ Entry: `{entry:.6f}`\nâ¢ SL: `{sl:.6f}`   â¢ TP1 *(SLâBE)*: `{tp1:.6f}`   â¢ TP2 *(WIN)*: `{tp2:.6f}`\n"
            f"â¢ Why: {reason}\nâ¢ Score: *{score}/10*\n{leaderboard_line()}{ex}")

def fmt_manage(pair, tf, direction, entry, tp2, new_sl):
    return (f"ð¡ï¸ *MANAGE*: Move SL â BE+{TP1_BE_OFFSET_R:.1f}R\nâ¢ Pair: `{pair}`  â¢ TF: *{tf}*  â¢ Direction: *{direction}*\n"
            f"â¢ New SL: `{new_sl:.6f}`\nâ¢ Targeting TP2: `{tp2:.6f}`\n{leaderboard_line()}")

def fmt_result(pair, tf, direction, result, entry, sl, tp1=None, tp2=None):
    extra=("" if tp1 is None else f"  â¢ TP1: `{tp1:.6f}`")+("" if tp2 is None else f"  â¢ TP2: `{tp2:.6f}`")
    emoji="ð¢" if "WIN" in result else ("ð´" if "LOSS" in result else "âªï¸")
    return (f"{emoji} *RESULT* â {result}\nâ¢ Pair: `{pair}`  â¢ TF: *{tf}*  â¢ Direction: *{direction}*\n"
            f"â¢ Entry: `{entry:.6f}`  â¢ SL: `{sl:.6f}`{extra}\n{leaderboard_line()}")

# ========= SCAN LOOP =========
async def scan_loop(session, symbols, tf):
    while True:
        await asyncio.sleep(max(0, next_close_ms(tf)-now_ms()+500)/1000 + random.random()*0.25)
        if not macro_ok(): continue
        alerts=[]

        async def process(sym):
            nonlocal alerts
            await asyncio.sleep(random.random()*0.12)
            prev,last=await fetch_closed(session, sym, tf)
            if not last: return
            sig=detect_signal(sym, tf, last, prev, kbuf[sym][tf])
            if not sig: drop_counters["no_signal"]+=1; return

            # Funding
            fr=await mx_funding_rate(session, to_mexc(sym))
            if fr is None: drop_counters["funding_missing"]+=1; return
            if sig["dir"]=="Long" and fr>FUNDING_MAX_ABS: drop_counters["funding_long_block"]+=1; return
            if sig["dir"]=="Short" and fr<-FUNDING_MAX_ABS: drop_counters["funding_short_block"]+=1; return

            # OI (soft gate)
            oi_pct=_oi_5m_from_queue(oi_hist[sym]); thr=adaptive_oi_threshold(sym)
            oi_soft_fail=False
            if oi_pct is not None:
                if sig["dir"]=="Long" and not (sig["c"]>sig["o"] and oi_pct>=+thr): oi_soft_fail=True
                if sig["dir"]=="Short" and not (sig["c"]<sig["o"] and oi_pct<=-thr): oi_soft_fail=True
            if HARD_REQUIRE_OI and oi_soft_fail: drop_counters["oi_hard_block"]+=1; return

            # Depth / spread
            ob=await mx_depth(session, to_mexc(sym))
            if not ob or not ob.get("asks") or not ob.get("bids"): drop_counters["depth_missing"]+=1; return
            cs=symbol_meta.get(sym,{}).get("contractSize",1.0)
            try:
                ask=float(ob["asks"][0][0]); bid=float(ob["bids"][0][0])
                spread=(ask-bid)/max(1e-12,(ask+bid)/2)
                up, dn=ask*1.01, bid*0.99
                ask_usd=sum(float(p)*float(q)*cs for p,q,*_ in ob["asks"] if float(p)<=up)
                bid_usd=sum(float(p)*float(q)*cs for p,q,*_ in ob["bids"] if float(p)>=dn)
                update_micro(sym, spread, ask_usd+bid_usd)
            except: pass
            max_sp, min_dp = micro_thresholds(sym)
            ok, spread, bid_usd, ask_usd, depth_usd = depth_checks(ob, sig["dir"], max_sp, min_dp, cs)
            if not ok: drop_counters["depth_fail"]+=1; return

            # Correlation (soft)
            corr_soft, btc5m, eth5m = await correlation_soft_flag(session, sig["dir"])
            if CORR_HARD_BLOCK and ((sig["dir"]=="Long" and (btc5m<-0.007 or eth5m<-0.007)) or (sig["dir"]=="Short" and (btc5m>0.007 or eth5m>0.007))):
                drop_counters["corr_hard_block"]+=1; return

            # SL/TP + RR
            sl_long, sl_short = pick_swing_levels(tf, kbuf[sym][tf]['hi'], kbuf[sym][tf]['lo'], kbuf[sym][tf]['cl'])
            entry=sig["c"]
            if sig["dir"]=="Long" and sl_long is not None:
                risk=max(1e-9, entry-sl_long); tp1=entry+1.10*risk; tp2=entry+2.00*risk; sl=sl_long
            elif sig["dir"]=="Short" and sl_short is not None:
                risk=max(1e-9, sl_short-entry); tp1=entry-1.10*risk; tp2=entry-2.00*risk; sl=sl_short
            else: drop_counters["sl_missing"]+=1; return
            if not rr_ok(entry, sl, tp1): drop_counters["rr_fail"]+=1; return
            if abs(entry-sl)/max(1e-12,entry)>MAX_RISK_PCT: drop_counters["risk_pct_high"]+=1; return

            # Cooldown
            now_s=int(time.time())
            if now_s - last_alert_at[sym][tf] < COOLDOWN_SEC:
                be_ts=last_be_at[sym][tf]; used=be_override_used_at[sym][tf]
                if be_ts and (now_s-be_ts<=600) and (now_s-used>600): be_override_used_at[sym][tf]=now_s
                else: drop_counters["cooldown_suppressed"]+=1; return
            last_alert_at[sym][tf]=now_s

            # Scoring (lenient)
            h,l=sig.get("hi",entry),sig.get("lo",entry); cr=max(1e-9,h-l); clv=(sig["c"]-l)/cr
            break_q = (entry - sig["prev_hi"])/max(1e-9,cr) if (sig["is_bo"] and sig["dir"]=="Long") else ((sig["prev_lo"]-entry)/max(1e-9,cr) if sig["is_bo"] else 0.0)
            score = 6
            info=[f"Volâ¥{VOL_SURGE_MIN:.2f}x", f"Funding {fr*100:.3f}%", f"Spread {spread*100:.3f}%", f"Depth1% ${depth_usd:,.0f}"]
            if oi_pct is None: info.append("OI missing")
            else: info.append(f"OIÎ5m {oi_pct*100:.2f}% (thr {adaptive_oi_threshold(sym)*100:.2f}%)")
            if oi_soft_fail: score-=1; info.append("OI misalign")
            else: score+=1
            if corr_soft: score-=1; info.append(f"Corr soft BTC {btc5m*100:.2f}%, ETH {eth5m*100:.2f}%")
            else: score+=1
            if clv>=0.6 and sig["dir"]=="Long": score+=1; info.append("Strong close" if sig["dir"]=="Long" else "")
            if clv<=0.4 and sig["dir"]=="Short": score+=1
            if break_q>=0.2: score+=1; info.append("Clean break")

            if HARD_BLOCK_SOFT_SESSION_BREAKOUT and sig["is_bo"] and (session_soft_flag(tf, list(kbuf[sym][tf]['cl'])) is True):
                drop_counters["soft_session_breakout_block"]+=1; return

            need_tape = TAPE_REQUIRE_ALWAYS or (score >= 2 and score < 6)
            if need_tape:
                tilt,notional=await tape_tilt(session, sym)
                if TAPE_REQUIRE_ALWAYS:
                    if notional<TAPE_MIN_NOTIONAL: drop_counters["tape_notional_low"]+=1; return
                    if sig["dir"]=="Long" and tilt<TAPE_TILT_WEAK: drop_counters["tape_tilt_fail"]+=1; return
                    if sig["dir"]=="Short" and tilt>-TAPE_TILT_WEAK: drop_counters["tape_tilt_fail"]+=1; return
                if sig["dir"]=="Long":
                    score += 2 if tilt>=TAPE_TILT_STRONG else (1 if tilt>=TAPE_TILT_WEAK else (-1 if tilt<=-TAPE_TILT_WEAK else 0))
                else:
                    score += 2 if tilt<=-TAPE_TILT_STRONG else (1 if tilt<=-TAPE_TILT_WEAK else (-1 if tilt>=TAPE_TILT_WEAK else 0))
                info.append(f"Tape {tilt*100:.1f}% on ${notional:,.0f}")

            req={"15m":MIN_CONF_15M,"30m":MIN_CONF_30M,"1h":MIN_CONF_1H}[tf] + (LONG_SIDE_PENALTY if sig["dir"]=="Long" else 0)
            final=max(1,min(10,score))
            if final<req: drop_counters["score_below_min"]+=1; return

            # Dedup + record
            key=trade_key(sym, tf, sig["dir"], entry); nowm=now_ms()
            while dedup_seen and nowm-dedup_seen[0][1]>DEDUP_MIN*60*1000: dedup_seen.popleft()
            if any(k==key for k,_ in dedup_seen): return
            dedup_seen.append((key,nowm)); stats["unique"]+=1; current_day_keys.add(key)
            R=abs(entry-sl); early=nowm+int(EARLY_EXIT_CHECK_FRAC*tf_timeout_minutes(tf)*60*1000)
            active_trades[key]={"symbol":sym,"tf":tf,"direction":sig["dir"],"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,
                                "start_ms":nowm,"be_moved":False,"r":R,"mfe_r":0.0,"early_check_ms":early}
            alert_counts[tf]=alert_counts.get(tf,0)+1
            extras=f"CLV:{clv:.2f} BreakQ:{break_q:.2f} BidDepth:${bid_usd:,.0f} AskDepth:${ask_usd:,.0f}"
            alerts.append(fmt_alert(sym, sig["dir"], entry, sl, tp1, tp2, ("ð Breakout" if sig["is_bo"] else "ð Engulf"), tf, final, extras))

        await asyncio.gather(*(process(s) for s in symbols))
        await tg_send_batched(session, alerts)

# ========= RESULT RESOLVER =========
async def md_book_ticker(session, sym):
    d=await mx_ticker(session, to_mexc(sym))
    if not d: return None
    return {"bidPrice":str(d.get("bid1", d.get("lastPrice",0))), "askPrice":str(d.get("ask1", d.get("lastPrice",0)))}

async def result_resolver(session):
    while True:
        if not active_trades: await asyncio.sleep(3); continue
        keys=list(active_trades.keys())

        async def one(k):
            tr=active_trades.get(k); if not tr: return
            sym=tr["symbol"]; direction=tr["direction"]; entry,sl,tp1,tp2=tr["entry"],tr["sl"],tr["tp1"],tr["tp2"]; tf=tr["tf"]
            R=tr.get("r",max(1e-9,abs(entry-sl)))
            jt0=await md_book_ticker(session, sym); if not jt0: return
            bid0=float(jt0["bidPrice"]); ask0=float(jt0["askPrice"])
            if direction=="Long": tr["mfe_r"]=max(tr.get("mfe_r",0.0),(bid0-entry)/max(1e-9,R))
            else: tr["mfe_r"]=max(tr.get("mfe_r",0.0),(entry-ask0)/max(1e-9,R))

            if now_ms()>=tr.get("early_check_ms",0) and tr["mfe_r"]<EARLY_EXIT_MIN_MFE_R:
                del active_trades[k]; await tg_send(session, fmt_result(sym, tf, direction, "EARLY EXIT (no progress)", entry, tr["sl"], tp1, tp2)); return

            if now_ms()-tr["start_ms"]>tf_timeout_minutes(tf)*60*1000:
                del active_trades[k]; await tg_send(session, fmt_result(sym, tf, direction, "TIMEOUT (no follow-through)", entry, tr["sl"], tp1, tp2)); return

            jt=await md_book_ticker(session, sym); if not jt: return
            bid=float(jt["bidPrice"]); ask=float(jt["askPrice"])

            if direction=="Long":
                if not tr["be_moved"]:
                    if bid>=tp1:
                        trail=entry+(TP1_BE_OFFSET_R*(entry-tr["sl"])); tr["be_moved"]=True; tr["sl"]=trail
                        last_be_at[sym][tf]=int(time.time()); await tg_send(session, fmt_manage(sym, tf, direction, entry, tp2, trail))
                    elif ask<=tr["sl"]:
                        stats["losses"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "LOSS (SL hit before TP1)", entry, tr["sl"], tp1, tp2))
                else:
                    if bid>=tp2:
                        stats["wins"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "WIN (TP2 hit)", entry, tr["sl"], tp1, tp2))
                    elif ask<=tr["sl"]:
                        stats["breakevens"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "BREAKEVEN (SL@BE)", entry, tr["sl"], tp1, tp2))
            else:
                if not tr["be_moved"]:
                    if ask<=tp1:
                        trail=entry-(TP1_BE_OFFSET_R*(tr["sl"]-entry)); tr["be_moved"]=True; tr["sl"]=trail
                        last_be_at[sym][tf]=int(time.time()); await tg_send(session, fmt_manage(sym, tf, direction, entry, tp2, trail))
                    elif bid>=tr["sl"]:
                        stats["losses"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "LOSS (SL hit before TP1)", entry, tr["sl"], tp1, tp2))
                else:
                    if ask<=tp2:
                        stats["wins"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "WIN (TP2 hit)", entry, tr["sl"], tp1, tp2))
                    elif bid>=tr["sl"]:
                        stats["breakevens"]+=1 if k in current_day_keys else 0; del active_trades[k]
                        await tg_send(session, fmt_result(sym, tf, direction, "BREAKEVEN (SL@BE)", entry, tr["sl"], tp1, tp2))

        await asyncio.gather(*(one(k) for k in keys))
        await asyncio.sleep(3)

# ========= DAILY STATS / METRICS =========
def _reset_daily():
    stats["unique"]=stats["wins"]=stats["losses"]=stats["breakevens"]=0; current_day_keys.clear()

async def daily_stats_loop(session):
    while True:
        sleep_s, tgt = seconds_until_riyadh(STATS_DAILY_HOUR,0); await asyncio.sleep(sleep_s)
        y=tgt - timedelta(days=1); period=f"{y.strftime('%Y-%m-%d %H:%M')} â {tgt.strftime('%Y-%m-%d %H:%M')} (Riyadh)"
        await tg_send(session, f"ð [DAILY STATS]\n{period}\n{leaderboard_line()}")
        _reset_daily()

async def heartbeat_loop():
    global event_loop_lag_ms
    loop=asyncio.get_running_loop(); prev=loop.time()
    while True:
        await asyncio.sleep(1.0)
        nowt=loop.time(); event_loop_lag_ms=max(0.0,(nowt-(prev+1.0))*1000.0); prev=nowt

async def metrics_handler(_):
    return web.json_response({
        "ok": True,
        "alert_counts": alert_counts,
        "drop_counters": dict(drop_counters),
        "stats": stats,
        "event_loop_lag_ms": round(event_loop_lag_ms,2),
        "macro_block_until": macro_block_until,
        "now_ms": now_ms(),
        "scan_limit": SCAN_LIMIT,
        "relaxed": True
    })

async def health(_): return web.Response(text="ok")

# ========= MEXC EXECUTION (minimal) =========
def _mexc_sign(ts, body_or_params):
    return hmac.new(MEXC_API_SECRET.encode(), f"{MEXC_API_KEY}{ts}{body_or_params or ''}".encode(), hashlib.sha256).hexdigest()

async def mexc_private_post(session, path, payload):
    if not (MEXC_API_KEY and MEXC_API_SECRET): raise RuntimeError("MEXC keys missing")
    ts=str(int(time.time()*1000)); body=json.dumps(payload, separators=(',',':'))
    headers={"ApiKey":MEXC_API_KEY,"Request-Time":ts,"Recv-Window":MEXC_RECV_WINDOW,"Signature":_mexc_sign(ts,body),"Content-Type":"application/json"}
    return await http_post(session, f"{MEXC_BASE}{path}", payload, headers=headers)

async def mexc_set_position_mode(session):
    try: await mexc_private_post(session,"/api/v1/private/position/change-position-mode",{"positionMode":MEXC_POSITION_MODE})
    except Exception as e: print("mexc_set_position_mode err:", e)

async def mexc_set_leverage(session, sym_u):
    try: await mexc_private_post(session,"/api/v1/private/position/change-leverage",{"symbol":sym_u,"leverage":MEXC_LEVERAGE,"openType":MEXC_OPEN_TYPE})
    except Exception as e: print("mexc_set_leverage err:", e)

async def mexc_market_order(session, symbol, direction, entry, sl, tp1, tp2):
    if not MEXC_LIVE: return {"live": False}
    sym_u=to_mexc(symbol); side=1 if direction=="Long" else 2
    cs=symbol_meta.get(symbol,{}).get("contractSize",1.0)
    qty=max(1.0, math.floor(MEXC_RISK_USDT / max(1e-9, abs(entry-sl)*cs)))
    payload={"symbol":sym_u,"price":entry,"vol":qty,"side":side,"type":1,"openType":MEXC_OPEN_TYPE,"positionId":0,"leverage":MEXC_LEVERAGE,"externalOid":f"sig-{int(time.time())}"}
    try:
        r=await mexc_private_post(session,"/api/v1/private/order/submit",payload)
        if r:
            await mexc_private_post(session,"/api/v1/private/planorder/submit",{"symbol":sym_u,"vol":qty,"side":(3 if direction=="Long" else 4),"openType":MEXC_OPEN_TYPE,"type":1,"triggerPrice":tp2,"executePrice":tp2,"positionId":0})
            await mexc_private_post(session,"/api/v1/private/planorder/submit",{"symbol":sym_u,"vol":qty,"side":(4 if direction=="Long" else 3),"openType":MEXC_OPEN_TYPE,"type":2,"triggerPrice":sl,"executePrice":sl,"positionId":0})
        return r
    except Exception as e:
        print("mexc_market_order err:", e); return None

# ========= APP =========
async def app_main():
    async with aiohttp.ClientSession() as session:
        symbols=await discover(session, SCAN_LIMIT)
        print("Scanning", len(symbols), "symbols:", symbols[:10], "...")

        await warmup(session, symbols)
        print("Warmup complete.")

        # optional: set global position mode (safe even if not live)
        await mexc_set_position_mode(session)

        tasks=[
            asyncio.create_task(oi_loop(session, symbols)),
            asyncio.create_task(macro_guard_loop(session)),
            asyncio.create_task(scan_loop(session, symbols, "15m")),
            asyncio.create_task(scan_loop(session, symbols, "30m")),
            asyncio.create_task(scan_loop(session, symbols, "1h")),
            asyncio.create_task(result_resolver(session)),
            asyncio.create_task(daily_stats_loop(session)),
            asyncio.create_task(heartbeat_loop()),
        ]

        app=web.Application()
        app.router.add_get("/", health); app.router.add_get("/metrics", metrics_handler)
        runner=web.AppRunner(app); await runner.setup()
        site=web.TCPSite(runner, "0.0.0.0", PORT); await site.start()
        print("HTTP up on", PORT)

        stop_event=asyncio.Event()
        def _exit(*_): stop_event.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: asyncio.get_running_loop().add_signal_handler(sig, _exit)
            except NotImplementedError: pass

        await stop_event.wait()
        for t in tasks: t.cancel()
        for t in tasks:
            try: await t
            except asyncio.CancelledError: pass
            except Exception: pass
        try: await site.stop()
        except: pass
        try: await runner.cleanup()
        except: pass

if __name__=="__main__":
    try:
        import uvloop; uvloop.install()
    except Exception: pass
    asyncio.run(app_main())
