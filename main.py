# app.py — Binance USDT-M Perps Breakout Bot (Selective + BTC-Regime + Pre-Entry)
# © You. MIT-style feel free.

import sys, subprocess, os
def _ensure(pkgs):
    import importlib
    miss=[]
    for p in pkgs:
        base=p.split("==")[0].split(">=")[0].split("[")[0].replace("-","_")
        try: importlib.import_module(base)
        except Exception: miss.append(p)
    if miss:
        print("Installing:", miss, flush=True)
        subprocess.check_call([sys.executable,"-m","pip","install","--upgrade",*miss])
_ensure([
    "aiohttp>=3.9.5","uvloop>=0.19.0","pydantic>=2.7.0","pydantic-settings>=2.2.1",
    "structlog>=24.1.0","tzdata>=2024.1","orjson>=3.10.7",
])

import asyncio, aiohttp, orjson, time, math, statistics
from typing import List, Dict, Any, Optional, Deque, Tuple
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ====================== Settings ======================
class Settings(BaseSettings):
    # Universe
    SYMBOLS: str | List[str] = "ALL"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 180
    BACKFILL_LIMIT: int = 60
    WS_CHUNK_SIZE: int = 60

    # ---------- Tier 0: Eligibility ----------
    MIN_24H_QVOL_USDT: float = 25_000_000.0   # /fapi/v1/ticker/24hr quoteVolume
    ELIG_24H_REFRESH_SEC: int = 300
    MIN_1M_NOTIONAL_USDT: float = 100_000.0   # live Σ(price*qty) over 60s window
    MIN_TOB_QTY: float = 2500.0               # EMA of (best bid qty + best ask qty)
    MIN_DEPTH10BP_USDT: float = 150_000.0     # sum depth within ±10bp around mid
    MUTE_LOW_24H_SEC: int = 7200
    MUTE_LOW_1M_SEC: int = 1800

    # ---------- BTC Regime ----------
    BTC_SYMBOL: str = "BTCUSDT"
    BTC_ATRZ_SHOCK: float = 1.5               # shock if ATR% z >= this
    MIN_CORR_WITH_BTC: float = 0.25           # only in Trend/Shock
    CORR_WIN_BARS: int = 60

    # ---------- HTF Bias ----------
    HTF_SMA_LEN: int = 50

    # ---------- Pre-Entry (3A) ----------
    PRE_ENABLE: bool = True
    PRE_TF: str = "5m"
    PRE_RANGE_LOOKBACK: int = 20
    PRE_PROX_BP: int = 12
    PRE_TOB_EMA_MIN: float = 0.12
    PRE_CVD_MIN: float = 0.0
    PRE_OI_Z_MIN: float = 0.8
    PRE_OI_DELTA_MIN: float = 0.003
    PRE_ATR7_PCT_MAX: float = 0.006
    PRE_MIN_SIGNALS_TREND: int = 3
    PRE_MIN_SIGNALS_SHOCK: int = 3
    PRE_USE_HTF_BIAS: bool = True
    PRE_AVOID_WALL: bool = True
    PRE_ENTRY_MODE: str = "MARKET"
    PRE_COOLDOWN_SEC: int = 1200
    DRIFT_BARS: int = 3  # higher-lows/higher-highs persistence

    # ---------- Confirmed Breakout (3B) ----------
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    WICK_SIDE_MAX: float = 0.35
    VOL_Z_MIN: float = 1.5
    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 1.0
    OI_DELTA_MIN: float = 0.005

    # Walls
    ENABLE_WALLS: bool = True
    WALL_BAND_BP: int = 15
    WALL_MULT: float = 2.5
    WALL_MEDIAN_WINDOW: int = 200
    WALL_BAND_EXTRA_SHOCK_BP: int = 5

    # Risk / Mgmt
    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5
    TRAIL_ATR_MULT: float = 1.0
    ENTRY_MODE: str = "MARKET"
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    COOLDOWN_AFTER_TRADE_SEC: int = 14400
    CHURN_COOLDOWN_BOOST: float = 1.5  # if SL <10m

    # Time-of-day
    ENABLE_TOD: bool = True
    TOD_WINDOWS: str = "11:00-15:30,16:30-21:00"

    # Ops
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8080"))
    TZ: str = "Asia/Riyadh"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)
    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def _split_symbols(cls, v):
        if isinstance(v, str) and v.strip().upper() != "ALL":
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v
    @field_validator("TIMEFRAMES", mode="before")
    @classmethod
    def _split_tfs(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

S = Settings()

def setup_logging(level="INFO"):
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps)],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger, cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
log = setup_logging(S.LOG_LEVEL)

# ====================== Utils / TA ======================
def now_utc() -> datetime: return datetime.now(timezone.utc)
def to_tz(dt: datetime, tz: str) -> datetime: return dt.astimezone(ZoneInfo(tz))
def riyadh_now() -> datetime: return to_tz(now_utc(), S.TZ)
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
def atr(highs,lows,closes,length:int)->Optional[float]:
    if len(closes) < length+1: return None
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < length: return None
    return sum(trs[-length:])/length
def pct_z(values: List[float], cur: float) -> float:
    if len(values) < 10: return 0.0
    m = statistics.mean(values); s = statistics.pstdev(values)
    return 0.0 if s==0 else (cur - m)/s
def corr(x: List[float], y: List[float]) -> float:
    n=min(len(x),len(y))
    if n<10: return 0.0
    x=x[-n:]; y=y[-n:]
    mx=sum(x)/n; my=sum(y)/n
    num=sum((a-mx)*(b-my) for a,b in zip(x,y))
    den=(sum((a-mx)**2 for a in x)*sum((b-my)**2 for b in y))**0.5
    return 0.0 if den==0 else num/den
def _fmt_pct(a: float, b: float) -> float:
    return 0.0 if b==0 else (a/b - 1.0)*100.0

# ====================== Data Types ======================
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct
class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty","imb_ema","tob_ema_qty")
    def __init__(self, bp,bq,ap,aq):
        self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq
        self.imb_ema=0.0; self.tob_ema_qty=0.0
class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ====================== Binance REST/WS ======================
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"
REST_SESSION: Optional[aiohttp.ClientSession] = None
BINANCE_CLIENT = None

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int)->List[str]:
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status(); data = await r.json()
    out=[]
    for s in data.get("symbols", []):
        try:
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING":
                out.append(s["symbol"])
        except: continue
    out=sorted(out)
    if max_symbols>0: out=out[:max_symbols]
    return out

class BinanceClient:
    def __init__(self, session:aiohttp.ClientSession): self.session=session
    async def klines(self, symbol:str, interval:str, limit:int=200):
        p={"symbol":symbol.upper(),"interval":interval,"limit":limit}
        async with self.session.get(f"{BASE_REST}/fapi/v1/klines", params=p) as r:
            r.raise_for_status(); return await r.json()
    async def open_interest_hist(self, symbol:str, period:str="5m", limit:int=30)->Optional[List[float]]:
        p={"symbol":symbol.upper(),"period":period,"limit":limit}
        async with self.session.get(f"{BASE_REST}/futures/data/openInterestHist", params=p) as r:
            if r.status!=200: return None
            d=await r.json()
            if not d: return None
            return [float(x["sumOpenInterest"]) for x in d]
    async def open_interest(self, symbol:str)->Optional[float]:
        async with self.session.get(f"{BASE_REST}/fapi/v1/openInterest", params={"symbol":symbol.upper()}) as r:
            if r.status!=200: return None
            d=await r.json(); return float(d.get("openInterest", 0))
    async def ticker24h(self)->Dict[str,float]:
        async with self.session.get(f"{BASE_REST}/fapi/v1/ticker/24hr") as r:
            r.raise_for_status(); arr=await r.json()
        out={}
        for x in arr:
            try:
                if x.get("symbol","").endswith("USDT"):
                    out[x["symbol"]]=float(x.get("quoteVolume",0))
            except: pass
        return out

# ====================== Streams ======================
class WSStream:
    def __init__(self, symbols:List[str], timeframes:List[str], on_kline, on_bookticker, on_aggtrade, on_depth):
        self.symbols=[s.lower() for s in symbols]; self.timeframes=timeframes
        self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    def _streams(self)->str:
        parts=[]
        for s in self.symbols:
            for tf in self.timeframes: parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker"); parts.append(f"{s}@aggTrade"); parts.append(f"{s}@depth5@100ms")
        return f"{BASE_WS}?streams={'/'.join(parts)}"
    async def run(self):
        url=self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30, autoping=True, compress=15) as ws:
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        payload=msg.json(); stream=payload.get("stream",""); data=payload.get("data",{})
                        if "kline" in stream and data.get("k",{}).get("x"): await self.on_kline(data["s"], data["k"])
                        elif "bookTicker" in stream: await self.on_bookticker(data["s"], data)
                        elif "aggTrade" in stream: await self.on_aggtrade(data["s"], data)
                        elif "depth5" in stream: await self.on_depth(data["s"], data)
                    elif msg.type==aiohttp.WSMsgType.ERROR:
                        break

class WSStreamMulti:
    def __init__(self, all_symbols:List[str], timeframes:List[str], on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
        self.on_aggtrade=on_aggtrade; self.on_depth=on_depth
    async def run(self):
        tasks=[asyncio.create_task(WSStream(c,self.timeframes,self.on_kline,self.on_bookticker,self.on_aggtrade,self.on_depth).run()) for c in self.chunks]
        await asyncio.gather(*tasks)

# ====================== Trackers ======================
class OrderBookTracker:
    def __init__(self):
        self.tops:Dict[str,BookTop]={}
        self.depth_last: Dict[str, Dict[str, List[Tuple[float,float]]]] = {}
        self.depth_sizes: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=S.WALL_MEDIAN_WINDOW))
    def update_book_ticker(self, sym:str, d:dict):
        bp, bq, ap, aq = float(d.get("b",0)), float(d.get("B",0)), float(d.get("a",0)), float(d.get("A",0))
        t = self.tops.get(sym)
        if not t: t=BookTop(bp,bq,ap,aq)
        else:
            t.bid_price=bp; t.bid_qty=bq; t.ask_price=ap; t.ask_qty=aq
        # EMA updates
        s_qty = max(bq+aq, 0.0)
        alpha = 0.2
        t.tob_ema_qty = (1-alpha)*t.tob_ema_qty + alpha*s_qty if t.tob_ema_qty>0 else s_qty
        imb = 0.0 if (bq+aq)<=0 else (bq-aq)/(bq+aq)
        t.imb_ema = (1-alpha)*t.imb_ema + alpha*imb
        self.tops[sym]=t
    def on_depth5(self, sym:str, data:dict):
        bids=[(float(p),float(q)) for p,q in data.get("b",[])]
        asks=[(float(p),float(q)) for p,q in data.get("a",[])]
        self.depth_last[sym]={"bids":bids,"asks":asks}
        if bids: self.depth_sizes[sym].append(bids[0][1])
        if asks: self.depth_sizes[sym].append(asks[0][1])
    def median_top(self, sym:str)->float:
        arr=self.depth_sizes.get(sym)
        if not arr: return 0.0
        s=sorted(arr); n=len(s)
        return s[n//2] if n%2==1 else 0.5*(s[n//2-1]+s[n//2])
    def sum_depth_around_mid_10bp_usdt(self, sym:str)->float:
        top=self.tops.get(sym); d=self.depth_last.get(sym)
        if not top or not d: return 0.0
        mid=(top.bid_price+top.ask_price)/2
        band=mid*0.001  # 10bp
        val=0.0
        for p,q in d["bids"]:
            if mid-band <= p <= mid: val+=p*q
        for p,q in d["asks"]:
            if mid <= p <= mid+band: val+=p*q
        return val
    def opposite_wall_near(self, sym:str, side:str, ref_price:float, band_bp:int, mult:float)->bool:
        d=self.depth_last.get(sym)
        if not d or ref_price<=0: return False
        med=self.median_top(sym)
        if med<=0: return False
        band=ref_price*(band_bp/10_000)
        if side=="LONG":
            wall_sz=sum(q for p,q in d["asks"] if ref_price < p <= ref_price+band)
        else:
            wall_sz=sum(q for p,q in d["bids"] if ref_price-band <= p < ref_price)
        return wall_sz > mult*med
OB=OrderBookTracker()

class CVDTracker:
    def __init__(self, window_sec:int=300):
        self.window_sec=window_sec
        self.buff:Dict[str,Deque[Tuple[float,float]]] = defaultdict(lambda: deque())
    def on_agg(self, sym:str, price:float, qty:float, buyer_is_maker:bool):
        sign = -1.0 if buyer_is_maker else +1.0
        ts = time.time(); dq = sign*qty
        q = self.buff[sym]; q.append((ts,dq))
        cutoff = ts - self.window_sec
        while q and q[0][0] < cutoff: q.popleft()
    def slope(self, sym:str)->float:
        q=self.buff.get(sym)
        if not q or len(q)<2: return 0.0
        total=sum(d for _,d in q); span=q[-1][0]-q[0][0]
        return total/max(span,1e-6)
CVD = CVDTracker(300) if True else None

# Rolling agg notional 60s
AGG_NOTIONAL: Dict[str, Deque[Tuple[float,float]]] = defaultdict(lambda: deque())  # (ts, notional)
def add_notional(sym:str, price:float, qty:float):
    ts=time.time()
    dq=price*qty
    q=AGG_NOTIONAL[sym]; q.append((ts,dq))
    cutoff=ts-60.0
    while q and q[0][0]<cutoff: q.popleft()

def notional_60s(sym:str)->float:
    q=AGG_NOTIONAL.get(sym)
    return sum(v for _,v in q) if q else 0.0

# ====================== Buffers / Engines ======================
class BreakoutEngine:
    def __init__(self):
        self.buffers:Dict[Tuple[str,str],Dict[str,Deque[float]]]={}
        self.retests:Dict[Tuple[str,str],List["RetestState"]]={}
    def _buf(self, sym, tf):
        k=(sym,tf)
        if k not in self.buffers:
            self.buffers[k]={"o":deque(maxlen=600),"h":deque(maxlen=600),"l":deque(maxlen=600),"c":deque(maxlen=600),"v":deque(maxlen=600)}
        return self.buffers[k]
    def add_candle(self, sym, tf, c:Candle):
        b=self._buf(sym,tf)
        b["o"].append(c.open); b["h"].append(c.high); b["l"].append(c.low); b["c"].append(c.close); b["v"].append(c.volume)
        # progress retests
        for r in list(self.retests.get((sym,tf),[])): r.step()
        self.retests[(sym,tf)]=[r for r in self.retests.get((sym,tf),[]) if r.remaining>0]
    def prior_high_low(self, sym, tf, N=20):
        b=self.buffers.get((sym,tf))
        if not b or len(b["h"])<N+1: return None,None
        hs,ls=list(b["h"]),list(b["l"])
        return max(hs[-N-1:-1]), min(ls[-N-1:-1])
    def sweep_reclaim(self, sym, tf, side:str)->bool:
        b=self.buffers.get((sym,tf))
        if not b or len(b["h"])<S.SWEEP_LOOKBACK+1: return False
        hs,ls,cs=list(b["h"]),list(b["l"]),list(b["c"])
        prev_high=max(hs[-S.SWEEP_LOOKBACK-1:-1]); prev_low=min(ls[-S.SWEEP_LOOKBACK-1:-1])
        c,h,l = cs[-1], hs[-1], ls[-1]
        if side=="LONG":
            swept=h>prev_high; back_inside=c<=prev_high*(1+S.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (S.RECLAIM_BODY_RATIO_MIN<=1.0)
        else:
            swept=l<prev_low; back_inside=c>=prev_low*(1-S.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (S.RECLAIM_BODY_RATIO_MIN<=1.0)
    def on_closed_bar(self, sym, tf)->Optional[Signal]:
        b=self.buffers.get((sym,tf))
        if not b or len(b["c"])<max(25,S.ATR_LEN+1): return None
        o,h,l,c,v=list(b["o"]),list(b["h"]),list(b["l"]),list(b["c"]),list(b["v"])
        ph,pl=self.prior_high_low(sym,tf,20)
        if ph is None: return None
        br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        pad=S.BREAKOUT_PAD_BP/10_000
        side=None; level=None
        if c[-1]>ph*(1+pad) and br>=S.BODY_RATIO_MIN: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=S.BODY_RATIO_MIN: side,level="SHORT",pl
        if not side: return None
        # wick guard
        rng=max(h[-1]-l[-1],1e-12)
        if side=="LONG":
            if (h[-1]-max(c[-1],o[-1]))/rng > S.WICK_SIDE_MAX: return None
        else:
            if (min(c[-1],o[-1])-l[-1])/rng > S.WICK_SIDE_MAX: return None
        atr_val = atr(h,l,c,S.ATR_LEN)
        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br,
                      sweep_reclaim=self.sweep_reclaim(sym,tf,side), atr_val=atr_val, htf_bias_ok=True)
class RetestState:
    def __init__(self, level:float, direction:str, expire_bars:int, pad_bp:int):
        self.level=level; self.direction=direction; self.remaining=expire_bars
        self.touched=False; self.pad_bp=pad_bp; self.await_next_close=False
    def step(self): self.remaining-=1; return self.remaining>0
    def in_band(self, price:float)->bool:
        band=self.level*(self.pad_bp/10_000); return abs(price-self.level)<=band
BE=BreakoutEngine()

# ====================== Global State ======================
STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
REST_SESSION: aiohttp.ClientSession
BINANCE_CLIENT: BinanceClient

OPEN_TRADES: Dict[str, Dict[str, Any]] = {}
LAST_TRADE_TS: Dict[str, int] = {}
PRE_LAST_TS: Dict[str, int] = {}
MUTED_UNTIL: Dict[str, int] = {}

DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "count": 0, "wins_tp2": 0, "wins_ts": 0, "losses_sl": 0,
    "sum_R": 0.0, "best": None, "worst": None
})

# BTC Regime cache
BTC_REGIME = {"state":"Chop","side":None,"atrz":0.0,"slope":0.0,"cvd":0.0}
SYSTEM_SIDE_LOCK: Optional[str] = None   # "LONG" / "SHORT" during Shock

# ====================== Helpers ======================
def local_day_key(dt: Optional[datetime]=None) -> str:
    d = riyadh_now() if dt is None else to_tz(dt, S.TZ)
    return d.strftime("%Y-%m-%d")

def stats_add_trade_result(symbol: str, R: float, outcome: str):
    key = local_day_key(); st = DAILY_STATS[key]
    st["count"] += 1; st["sum_R"] += R
    if outcome == "TP2": st["wins_tp2"] += 1
    elif outcome == "TS": st["wins_ts"] += 1
    elif outcome == "SL": st["losses_sl"] += 1
    if st["best"] is None or R > st["best"][1]: st["best"] = (symbol, R)
    if st["worst"] is None or R < st["worst"][1]: st["worst"] = (symbol, R)

def compose_daily_summary() -> str:
    key = local_day_key(); st = DAILY_STATS.get(key, {})
    if not st or st["count"] == 0: return f"📅 Daily Stats – {key}\nNo trades today."
    wins = st["wins_tp2"] + st["wins_ts"]; losses = st["losses_sl"]
    hit = (wins / st["count"]) * 100.0 if st["count"] > 0 else 0.0
    avg_R = (st["sum_R"] / st["count"]) if st["count"] > 0 else 0.0
    best = f"{st['best'][0]} {st['best'][1]:+.2f}R" if st.get("best") else "—"
    worst = f"{st['worst'][0]} {st['worst'][1]:+.2f}R" if st.get("worst") else "—"
    return (f"📅 Daily Stats – {key}\nTrades: {st['count']}\n"
            f"Wins: {wins} (TP2 {st['wins_tp2']}, TS {st['wins_ts']}) | Losses: {losses}\n"
            f"Hit rate: {hit:.1f}% | Net: {st['sum_R']:+.2f}R | Avg: {avg_R:+.2f}R\n"
            f"Best: {best}   Worst: {worst}")

def _log_event(obj: Dict[str, Any]):
    os.makedirs(S.DATA_DIR, exist_ok=True)
    with open(os.path.join(S.DATA_DIR,"events.jsonl"),"ab") as f:
        f.write(orjson.dumps(obj)+b"\n")

def _parse_windows(w: str):
    parts=[p.strip() for p in w.split(",") if p.strip()]
    out=[]
    for p in parts:
        try:
            a,b=p.split("-"); ah,am=[int(x) for x in a.split(":")]; bh,bm=[int(x) for x in b.split(":")]
            out.append(((ah,am),(bh,bm)))
        except: continue
    return out
TOD_PARSED = _parse_windows(S.TOD_WINDOWS) if S.ENABLE_TOD else []

def is_in_tod(now_local: datetime) -> bool:
    if not S.ENABLE_TOD or not TOD_PARSED: return True
    h,m = now_local.hour, now_local.minute
    cur=h*60+m
    for (ah,am),(bh,bm) in TOD_PARSED:
        if ah*60+am <= cur <= bh*60+bm: return True
    return False

# ====================== OI helpers ======================
async def oi_ok(symbol: str, side: str, z_min:float, d_min:float) -> bool:
    try:
        hist = await BINANCE_CLIENT.open_interest_hist(symbol, period=S.OI_LOOKBACK, limit=30)
        cur  = await BINANCE_CLIENT.open_interest(symbol)
        if not hist or cur is None: return True
        prev = hist[:-1] if len(hist)>1 else hist
        if len(prev) < 5: return True
        mean = sum(prev)/len(prev)
        var  = sum((x-mean)**2 for x in prev)/len(prev)
        std  = math.sqrt(var)
        z = 0.0 if std==0 else (cur - mean)/std
        delta_pct = (cur-mean)/mean if mean>0 else 0.0
        dir_ok = (cur >= mean) if side=="LONG" else (cur <= mean)
        return dir_ok and ((z >= z_min) or (delta_pct >= d_min))
    except Exception as e:
        log.warning("oi_check_error", symbol=symbol, error=str(e)); return True

# ====================== Eligibility (Tier 0) ======================
ELIG_QVOL: Dict[str, float] = {}
async def refresh_eligibility_24h():
    while True:
        try:
            tv = await BINANCE_CLIENT.ticker24h()
            ELIG_QVOL.clear(); ELIG_QVOL.update(tv)
        except Exception as e:
            log.warning("elig_24h_err", error=str(e))
        await asyncio.sleep(S.ELIG_24H_REFRESH_SEC)

def eligible_symbol(sym:str)->bool:
    now=int(time.time())
    if MUTED_UNTIL.get(sym,0) > now: return False
    # 24h notional
    qv=ELIG_QVOL.get(sym, 0.0)
    if qv < S.MIN_24H_QVOL_USDT:
        MUTED_UNTIL[sym] = now + S.MUTE_LOW_24H_SEC
        return False
    # live 60s notional
    if notional_60s(sym) < S.MIN_1M_NOTIONAL_USDT:
        MUTED_UNTIL[sym] = now + S.MUTE_LOW_1M_SEC
        return False
    # TOB EMA qty floor
    t=OB.tops.get(sym)
    if not t or t.tob_ema_qty < S.MIN_TOB_QTY: return False
    # depth ±10bp floor
    if OB.sum_depth_around_mid_10bp_usdt(sym) < S.MIN_DEPTH10BP_USDT: return False
    return True

# ====================== BTC Regime (Tier 1) ======================
def btc_regime_update():
    b5=BE.buffers.get((S.BTC_SYMBOL,"5m")); b15=BE.buffers.get((S.BTC_SYMBOL,"15m"))
    if not b5 or not b15 or len(b5["c"])<S.CORR_WIN_BARS+1 or len(b15["c"])<S.HTF_SMA_LEN+1: return
    c5=list(b5["c"]); h5=list(b5["h"]); l5=list(b5["l"])
    c15=list(b15["c"])
    # ATR% and z
    a5 = atr(h5,l5,c5,14); atrp = (a5/c5[-1]) if (a5 and c5[-1]>0) else 0.0
    z = pct_z([((atr(h5,l5,c5,i) or 0)/c5[-1]) for i in range(5, min(60,len(c5)-1))], atrp)
    # Trend via 15m SMA slope and price vs SMA
    sma15 = sum(c15[-S.HTF_SMA_LEN:])/S.HTF_SMA_LEN
    slope = (c15[-1]-c15[-S.HTF_SMA_LEN])/S.HTF_SMA_LEN
    price_vs = c15[-1]-sma15
    # CVD slope
    cvd = CVD.slope(S.BTC_SYMBOL) if CVD else 0.0
    # Decide
    state="Chop"; side=None
    if z >= S.BTC_ATRZ_SHOCK:
        state="Shock"; side = "LONG" if (price_vs>0 and slope>0 and cvd>=0) else ("SHORT" if (price_vs<0 and slope<0 and cvd<=0) else None)
    else:
        if price_vs>0 and slope>0: state="TrendUp"; side="LONG"
        elif price_vs<0 and slope<0: state="TrendDown"; side="SHORT"
        else: state="Chop"; side=None
    BTC_REGIME.update({"state":state,"side":side,"atrz":z,"slope":slope,"cvd":cvd})

def regime_allows(side:str)->bool:
    st=BTC_REGIME["state"]; btc_side=BTC_REGIME["side"]
    if st=="TrendUp": return side=="LONG"
    if st=="TrendDown": return side=="SHORT"
    if st=="Shock":
        if btc_side and side!=btc_side: return False
        return True
    return True  # Chop: let lower tiers decide (but we disable PRE later)

def corr_allows(sym:str, side:str)->bool:
    st=BTC_REGIME["state"]
    if st not in ("TrendUp","TrendDown","Shock"): return True
    # Need correlation with BTC
    b_alt=BE.buffers.get((sym,"5m")); b_btc=BE.buffers.get((S.BTC_SYMBOL,"5m"))
    if not b_alt or not b_btc: return False
    x=list(b_alt["c"]); y=list(b_btc["c"])
    c = corr(x[-S.CORR_WIN_BARS:], y[-S.CORR_WIN_BARS:])
    return c >= S.MIN_CORR_WITH_BTC

# ====================== Telegram / Health ======================
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            await s.post(f"{self.api}/sendMessage", json={
                "chat_id": self.chat_id, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True
            })

class HealthServer:
    def __init__(self, state:dict):
        self.app=web.Application(); self.state=state
        self.app.add_routes([web.get("/healthz", self.health)])
        self.runner=None; self.site=None
    async def start(self, host:str, port:int):
        self.runner=web.AppRunner(self.app); await self.runner.setup()
        self.site=web.TCPSite(self.runner, host=host, port=port); await self.site.start()
    async def health(self, req):
        return web.json_response({
            "ok":True, "uptime_sec":int(time.time()-self.state.get("start_time",time.time())),
            "last_kline":self.state.get("last_kline",{}),"last_alert":self.state.get("last_alert",{}),
            "symbols":self.state.get("symbols",[]),"timeframes":self.state.get("timeframes",[]),
            "open_trades": list(OPEN_TRADES.keys()),"today_stats": DAILY_STATS.get(local_day_key(), {}),
            "btc_regime": BTC_REGIME, "muted": {k:v-int(time.time()) for k,v in MUTED_UNTIL.items() if v>int(time.time())}
        })

async def alert(text:str, symbol:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text); return
    try:
        tg=Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
        await tg.send(text)
    except Exception as e:
        log.warning("telegram_err", error=str(e))
    STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}

# ====================== Daily Stats ======================
def seconds_until_next_2200_riyadh() -> float:
    now = riyadh_now(); target = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if now >= target: target = target + timedelta(days=1)
    return (target - now).total_seconds()
async def daily_stats_loop():
    await asyncio.sleep(max(1.0, seconds_until_next_2200_riyadh()))
    while True:
        try: await alert(f"📊 {compose_daily_summary()}", "DAILY")
        except Exception as e: log.warning("daily_stats_err", error=str(e))
        await asyncio.sleep(24*3600)

# ====================== Trade lifecycle ======================
def position_allowed(sym:str, side:str)->bool:
    now=int(time.time())
    if MUTED_UNTIL.get(sym,0) > now: return False
    if sym in OPEN_TRADES: return False
    if now - LAST_TRADE_TS.get(sym,0) < S.COOLDOWN_AFTER_TRADE_SEC: return False
    if BTC_REGIME["state"]=="Shock":
        global SYSTEM_SIDE_LOCK
        if SYSTEM_SIDE_LOCK and side!=SYSTEM_SIDE_LOCK: return False
    return True

def trade_plan(entry:float, side:str, atr_val:float):
    sl = entry - atr_val*S.ATR_SL_MULT if side=="LONG" else entry + atr_val*S.ATR_SL_MULT
    risk=abs(entry-sl)
    tp1 = entry + risk if side=="LONG" else entry - risk
    tp2 = entry + 2*risk if side=="LONG" else entry - 2*risk
    return sl, risk, tp1, tp2

async def _close_trade(symbol: str, outcome: str, exit_price: float):
    trade=OPEN_TRADES.pop(symbol, None)
    if not trade: return
    entry=trade["entry"]; risk=trade["risk"]; side=trade["side"]
    R=0.0 if risk<=0 else ((exit_price-entry)/risk if side=="LONG" else (entry-exit_price)/risk)
    if outcome=="SL": R=-1.0
    stats_add_trade_result(symbol, R, outcome)
    pl_pct=_fmt_pct(exit_price, entry); dur_min=(int(time.time())-trade["opened_ts"])/60.0
    msg=(f"📌 <b>RESULT</b> {symbol} [{trade.get('entry_type')}] {outcome}\n"
         f"{entry:.6f} → {exit_price:.6f}  (PnL {pl_pct:+.2f}% | {R:+.2f}R)\n"
         f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'} | Dur {dur_min:.1f}m")
    await alert(msg, symbol)
    _log_event({"ts":int(time.time()),"type":"result","symbol":symbol,"outcome":outcome,"entry":entry,"exit":exit_price,"R":R})
    # churn guard: if SL within 10m, boost cooldown
    if outcome=="SL" and dur_min <= 10.0:
        LAST_TRADE_TS[symbol] = int(time.time() - S.COOLDOWN_AFTER_TRADE_SEC*(1-1/S.CHURN_COOLDOWN_BOOST))

async def manage_on_book(sym:str):
    trade=OPEN_TRADES.get(sym)
    if not trade: return
    top=OB.tops.get(sym)
    if not top: return
    mid=(top.ask_price+top.bid_price)/2
    if mid<=0: return
    side=trade["side"]; entry=trade["entry"]; sl=trade["sl"]; tp1=trade["tp1"]; tp2=trade["tp2"]
    # before TP1
    if not trade["tp1_hit"]:
        if (side=="LONG" and mid<=sl) or (side=="SHORT" and mid>=sl):
            await _close_trade(sym,"SL", mid); return
        if (side=="LONG" and mid>=tp1) or (side=="SHORT" and mid<=tp1):
            trade["tp1_hit"]=True; trade["trail_active"]=True; trade["trail_peak"]=mid
        return
    # after TP1
    if side=="LONG":
        trade["trail_peak"]=max(trade["trail_peak"], mid)
        trail_stop=trade["trail_peak"]-trade["trail_dist"]
        if mid<=trail_stop: await _close_trade(sym,"TS", trail_stop); return
        if mid>=tp2: await _close_trade(sym,"TP2", tp2); return
    else:
        trade["trail_peak"]=min(trade["trail_peak"], mid) if trade["trail_peak"] is not None else mid
        trail_stop=trade["trail_peak"]+trade["trail_dist"]
        if mid>=trail_stop: await _close_trade(sym,"TS", trail_stop); return
        if mid<=tp2: await _close_trade(sym,"TP2", tp2); return

async def open_trade(sym:str, tf:str, side:str, entry:float, entry_type:str, atr_val:float):
    sl, risk, tp1, tp2 = trade_plan(entry, side, atr_val)
    if risk<=0: return
    qty_txt="—"
    if S.ACCOUNT_EQUITY_USDT and S.ACCOUNT_EQUITY_USDT>0:
        qty=(S.ACCOUNT_EQUITY_USDT*(S.MAX_RISK_PCT/100.0))/risk
        qty_txt=f"{qty:.4f}"
    OPEN_TRADES[sym]={
        "symbol":sym,"side":side,"tf":tf,"entry_type":entry_type,
        "entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"risk":risk,
        "opened_ts":int(time.time()),"tp1_hit":False,"trail_active":False,"trail_peak":None,
        "trail_dist":atr_val*S.TRAIL_ATR_MULT,"atr_at_entry":atr_val
    }
    LAST_TRADE_TS[sym]=int(time.time())
    # Shock lock
    if BTC_REGIME["state"]=="Shock" and BTC_REGIME["side"] in ("LONG","SHORT"):
        global SYSTEM_SIDE_LOCK
        SYSTEM_SIDE_LOCK=BTC_REGIME["side"]
    sl_pct=_fmt_pct(sl, entry); tp1_pct=_fmt_pct(tp1, entry); tp2_pct=_fmt_pct(tp2, entry)
    await alert((f"{'⚡ PRE' if entry_type=='PRE' else '✅ CONF'} {side} <b>{sym}</b> <code>{tf}</code>\n"
                 f"Entry {entry:.6f} | SL {sl:.6f} ({sl_pct:+.2f}%) | TP1 {tp1:.6f} ({tp1_pct:+.2f}%) | TP2 {tp2:.6f} ({tp2_pct:+.2f}%)\n"
                 f"Trail after TP1 (ATR×{S.TRAIL_ATR_MULT:.2f})  Qty {qty_txt}"), sym)
    _log_event({"ts":int(time.time()),"type":"open","symbol":sym,"side":side,"entry_type":entry_type,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2})

# ====================== Pre-Entry (Tier 3A) ======================
def range_edges(sym:str, tf:str, lookback:int)->Tuple[Optional[float],Optional[float]]:
    b=BE.buffers.get((sym,tf))
    if not b or len(b["h"])<lookback+1: return None,None
    hs,ls=list(b["h"]),list(b["l"])
    return max(hs[-lookback-1:-1]), min(ls[-lookback-1:-1])

def atr_pct(sym:str, tf:str, length:int=7)->Optional[float]:
    b=BE.buffers.get((sym, tf))
    if not b or len(b["c"])<length+1: return None
    a=atr(list(b["h"]), list(b["l"]), list(b["c"]), length)
    c=list(b["c"])[-1]
    return (a/c) if a and c>0 else None

def drift_ok(sym:str, tf:str, side:str, bars:int)->bool:
    b=BE.buffers.get((sym, tf))
    if not b or len(b["h"])<bars+1: return False
    hs,ls=list(b["h"]),list(b["l"])
    if side=="LONG":
        lows=ls[-bars:]
        return all(lows[i] > lows[i-1] for i in range(1,len(lows)))
    else:
        highs=hs[-bars:]
        return all(highs[i] < highs[i-1] for i in range(1,len(highs)))

async def try_pre_entry(sym:str):
    if not S.PRE_ENABLE or S.PRE_TF not in S.TIMEFRAMES: return
    if not is_in_tod(riyadh_now()): return
    if int(time.time()) - PRE_LAST_TS.get(sym,0) < S.PRE_COOLDOWN_SEC: return
    # eligibility first
    if not eligible_symbol(sym): return
    # regime gates
    if BTC_REGIME["state"]=="Chop": return  # disable PRE in Chop
    side_req=BTC_REGIME["side"] or "LONG"
    # we only allow pre-entries in the regime side
    side=side_req
    if not regime_allows(side): return
    if not corr_allows(sym, side): return
    if not position_allowed(sym, side): return

    top=OB.tops.get(sym)
    if not top or top.ask_price<=0 or top.bid_price<=0: return
    mid=(top.ask_price+top.bid_price)/2

    hi,lo=range_edges(sym, S.PRE_TF, S.PRE_RANGE_LOOKBACK)
    if hi is None: return
    prox=S.PRE_PROX_BP/10_000
    near_hi = abs(mid-hi) <= hi*prox
    near_lo = abs(mid-lo) <= lo*prox
    # lock to regime side only
    if side=="LONG" and not near_hi: return
    if side=="SHORT" and not near_lo: return

    # HTF bias (optional)
    if S.PRE_USE_HTF_BIAS and S.PRE_TF=="5m":
        buf15=BE.buffers.get((sym,"15m"))
        if buf15 and len(buf15["c"])>=S.HTF_SMA_LEN:
            htf=sum(list(buf15["c"])[-S.HTF_SMA_LEN:])/S.HTF_SMA_LEN
            if (side=="LONG" and mid<=htf) or (side=="SHORT" and mid>=htf): return

    # Predictive signals (need N)
    signals=0; details=[]
    t=OB.tops.get(sym)
    imb=t.imb_ema if t else 0.0
    if side=="LONG" and imb>=S.PRE_TOB_EMA_MIN: signals+=1; details.append(f"TOB_EMA {imb:+.2f}")
    if side=="SHORT" and -imb>=S.PRE_TOB_EMA_MIN: signals+=1; details.append(f"TOB_EMA {imb:+.2f}")
    slope=CVD.slope(sym) if CVD else 0.0
    if (side=="LONG" and slope>S.PRE_CVD_MIN) or (side=="SHORT" and slope<-S.PRE_CVD_MIN):
        signals+=1; details.append(f"CVD {slope:+.3f}")
    if await oi_ok(sym, side, S.PRE_OI_Z_MIN, S.PRE_OI_DELTA_MIN):
        signals+=1; details.append("OI ✓")
    atrp=atr_pct(sym, S.PRE_TF, 7)
    if atrp is not None and atrp <= S.PRE_ATR7_PCT_MAX:
        signals+=1; details.append(f"Squeeze {atrp*100:.2f}%")
    need = S.PRE_MIN_SIGNALS_SHOCK if BTC_REGIME["state"]=="Shock" else S.PRE_MIN_SIGNALS_TREND
    if signals < need: return
    # drift
    if not drift_ok(sym, S.PRE_TF, side, S.DRIFT_BARS): return

    # wall guard
    band_bp=S.WALL_BAND_BP + (S.WALL_BAND_EXTRA_SHOCK_BP if BTC_REGIME["state"]=="Shock" else 0)
    ref = hi*(1+S.BREAKOUT_PAD_BP/10_000) if side=="LONG" else lo*(1-S.BREAKOUT_PAD_BP/10_000)
    if S.PRE_AVOID_WALL and S.ENABLE_WALLS and OB.opposite_wall_near(sym, side, ref, band_bp, S.WALL_MULT): return

    # open trade
    # atr for risk
    b=BE.buffers.get((sym,S.PRE_TF)); atr_val=None
    if b and len(b["c"])>=S.ATR_LEN+1:
        atr_val=atr(list(b["h"]),list(b["l"]),list(b["c"]),S.ATR_LEN)
    if not atr_val: atr_val = mid*0.003
    entry = mid if S.PRE_ENTRY_MODE.upper()=="MARKET" else (hi if side=="LONG" else lo)
    await open_trade(sym, S.PRE_TF, side, entry, "PRE", atr_val)
    PRE_LAST_TS[sym]=int(time.time())

# ====================== Confirmed (Tier 3B) ======================
async def on_closed_bar_try_confirmed(sym:str, tf:str, sig:Signal):
    # TOD
    if not is_in_tod(riyadh_now()): return
    # eligibility
    if not eligible_symbol(sym): return
    # regime + corr
    if not regime_allows(sig.side): return
    if not corr_allows(sym, sig.side): return
    # HTF bias for 5m
    if tf=="5m":
        buf15=BE.buffers.get((sym,"15m"))
        if buf15 and len(buf15["c"])>=S.HTF_SMA_LEN:
            htf=sum(list(buf15["c"])[-S.HTF_SMA_LEN:])/S.HTF_SMA_LEN
            if (sig.side=="LONG" and sig.price<=htf) or (sig.side=="SHORT" and sig.price>=htf): return
    # Vol Z (30)
    b=BE.buffers.get((sym,tf)); vol_z=0.0
    if b and len(b["v"])>=30:
        vols=list(b["v"]); win=vols[-30:-1]
        if win:
            m=sum(win)/len(win); var=sum((x-m)**2 for x in win)/len(win); std=math.sqrt(var) if var>0 else 1.0
            vol_z=(vols[-1]-m)/std
    if vol_z < S.VOL_Z_MIN: return
    # OI eased
    if not await oi_ok(sym, sig.side, S.OI_Z_MIN, S.OI_DELTA_MIN): return
    # wall guard
    ref = sig.level*(1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000)
    if S.ENABLE_WALLS and OB.opposite_wall_near(sym, sig.side, ref, S.WALL_BAND_BP, S.WALL_MULT): return
    # position allowed?
    if not position_allowed(sym, sig.side): return
    # entry mode
    entry = sig.price if S.ENTRY_MODE.upper()=="MARKET" else (sig.level*(1+S.BREAKOUT_PAD_BP/10_000) if sig.side=="LONG" else sig.level*(1-S.BREAKOUT_PAD_BP/10_000))
    atr_val = sig.atr_val if (sig.atr_val and sig.atr_val>0) else (entry*0.003)
    await open_trade(sym, tf, sig.side, entry, "CONF", atr_val)

# ====================== Handlers ======================
async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    def _f(x): 
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), _f(k["o"]), _f(k["h"]), _f(k["l"]), _f(k["c"]), _f(k.get("q", k.get("Q", 0))), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"]={"symbol":sym,"tf":tf,"t":k["T"]}

    # update BTC regime periodically on BTC 5m closes
    if sym==S.BTC_SYMBOL and tf=="5m": btc_regime_update()
    # pre-entry quick check only for pre-TF
    if tf==S.PRE_TF:
        try: await try_pre_entry(sym)
        except Exception as e: log.warning("pre_entry_err", symbol=sym, error=str(e))
    # confirmed path
    sig=BE.on_closed_bar(sym, tf)
    if sig:
        try: await on_closed_bar_try_confirmed(sym, tf, sig)
        except Exception as e: log.warning("confirmed_err", symbol=sym, tf=tf, error=str(e))

async def on_bookticker(symbol:str, data:dict):
    sym=symbol.upper()
    OB.update_book_ticker(sym, data)
    await manage_on_book(sym)
    # Opportunistic pre check if no position
    if sym not in OPEN_TRADES:
        await try_pre_entry(sym)

async def on_aggtrade(symbol:str, data:dict):
    sym=symbol.upper()
    try:
        p=float(data.get("p",0)); q=float(data.get("q",0)); m=bool(data.get("m",False))
    except: return
    if q<=0: return
    add_notional(sym, p, q)
    if CVD: CVD.on_agg(sym, p, q, m)

async def on_depth(symbol:str, data:dict):
    sym=symbol.upper()
    if not S.ENABLE_WALLS: return
    OB.on_depth5(sym, data)

# ====================== WS reliability ======================
WS_TASK = None
def _last_kline_epoch() -> int:
    lk = STATE.get("last_kline", {})
    return int(lk.get("t", 0)) // 1000 if lk else 0

async def start_streams(symbols):
    global WS_TASK
    ws_multi = WSStreamMulti(symbols, S.TIMEFRAMES, on_kline, on_bookticker, on_aggtrade, on_depth, chunk_size=S.WS_CHUNK_SIZE)
    while True:
        try:
            WS_TASK = asyncio.create_task(ws_multi.run())
            await WS_TASK
        except Exception as e:
            log.warning("ws_crashed_restart", error=str(e))
        await asyncio.sleep(5)

async def ws_watchdog():
    longest_tf_minutes = 15 if ("15m" in S.TIMEFRAMES) else 5
    threshold = longest_tf_minutes * 60 * 3
    while True:
        try:
            now = int(time.time()); last = _last_kline_epoch()
            if last and now - last > threshold:
                log.warning("watchdog_no_kline_restarting", idle_sec=now-last)
                global WS_TASK
                if WS_TASK:
                    WS_TASK.cancel()
                    try: await WS_TASK
                    except: pass
                await start_streams(STATE["symbols"])
        except Exception as e:
            log.warning("watchdog_err", error=str(e))
        await asyncio.sleep(60)

# ====================== HTTP & Main ======================
async def start_http_server(state:dict):
    server=HealthServer(state); await server.start(S.HOST, S.PORT)
    log.info("http_started", host=S.HOST, port=S.PORT)

async def small_backfill(client:"BinanceClient", sym:str, tf:str):
    data=await client.klines(sym, tf, limit=S.BACKFILL_LIMIT)
    for k in data[:-1]:
        o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
        q = float(k[5]) if len(k)>5 else 0.0
        BE.add_candle(sym, tf, Candle(int(k[0]), o,h,l,c,q,int(k[6])))

async def main():
    global REST_SESSION, BINANCE_CLIENT, SYSTEM_SIDE_LOCK
    SYSTEM_SIDE_LOCK=None
    log.info("boot", tiers="elig->regime->htf->pre/confirmed->exec", rr="1:2", trail_after_tp1=True)
    asyncio.create_task(start_http_server(STATE))
    asyncio.create_task(daily_stats_loop())

    REST_SESSION=aiohttp.ClientSession()
    BINANCE_CLIENT=BinanceClient(REST_SESSION)
    asyncio.create_task(refresh_eligibility_24h())

    # Symbols
    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS,(list,tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols
    log.info("symbols_selected", count=len(symbols))

    # Backfill
    for i, sym in enumerate(symbols+[S.BTC_SYMBOL] if S.BTC_SYMBOL not in symbols else symbols):
        for tf in S.TIMEFRAMES:
            try: await small_backfill(BINANCE_CLIENT, sym, tf)
            except Exception as e: log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))
        if i % 10 == 0: await asyncio.sleep(0.2)

    # initialize BTC regime once buffers exist
    btc_regime_update()

    # Streams + watchdog
    asyncio.create_task(ws_watchdog())
    await start_streams(symbols)

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass