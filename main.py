# app.py — Binance USDT-M PERPS scanner
# R:R=1:2 (TP1, TP2); trailing stop only after TP1; STRICT OI rule (z-score & delta);
# extra light volume filter; fake-move wick filter; 4h cooldown AFTER a trade;
# ALL perps auto-scan; daily stats 22:00 Asia/Riyadh; Telegram alerts; /healthz.
# Fix: HTTP server now uses AppRunner/TCPSite (no thread/signal crash).

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

import asyncio, aiohttp, orjson, time
from typing import List, Dict, Any, Optional, Deque, Tuple
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ---------------- Settings ----------------
class Settings(BaseSettings):
    SYMBOLS: str | List[str] = "ALL"
    TIMEFRAMES: List[str] = ["5m","15m"]
    MAX_SYMBOLS: int = 200
    WS_CHUNK_SIZE: int = 80
    BACKFILL_LIMIT: int = 60

    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    TOB_IMB_MIN: float = 0.15
    SPREAD_MAX_BP: int = 5

    VOL_SMA_LEN: int = 20
    VOL_SMA_RATIO_MIN: float = 1.10

    OI_LOOKBACK: str = "5m"
    OI_Z_MIN: float = 1.0
    OI_DELTA_MIN: float = 0.005

    WICK_SIDE_MAX: float = 0.35

    ATR_LEN: int = 14
    ATR_SL_MULT: float = 1.5
    ENTRY_MODE: str = "MARKET"
    MAX_RISK_PCT: float = 1.0
    ACCOUNT_EQUITY_USDT: Optional[float] = None
    SLIPPAGE_BP: int = 3
    DRY_RUN: bool = True

    TRAIL_ATR_MULT: float = 1.0

    COOLDOWN_AFTER_TRADE_SEC: int = 14400  # 4h

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

def setup_logging(level: str = "INFO"):
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()
log = setup_logging(S.LOG_LEVEL)

# ---------------- Utils/TA ----------------
def now_utc() -> datetime: return datetime.now(timezone.utc)
def to_tz(dt: datetime, tz: str) -> datetime: return dt.astimezone(ZoneInfo(tz))
def riyadh_now() -> datetime: return to_tz(now_utc(), S.TZ)
def percent_bp(a: float, b: float) -> float: return 0.0 if b==0 else (a-b)/b*10_000
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
def sma(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    return sum(vals[-n:])/n
def atr(highs: List[float], lows: List[float], closes: List[float], length: int) -> Optional[float]:
    if len(closes) < length + 1: return None
    trs=[]
    for i in range(1,len(closes)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < length: return None
    return sum(trs[-length:]) / length

# --------------- Data ----------------
class Candle:
    __slots__=("open_time","open","high","low","close","volume","close_time")
    def __init__(self, ot,o,h,l,c,v,ct):
        self.open_time=ot; self.open=o; self.high=h; self.low=l; self.close=c; self.volume=v; self.close_time=ct
class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bp,bq,ap,aq):
        self.bid_price=bp; self.bid_qty=bq; self.ask_price=ap; self.ask_qty=aq
class Signal:
    def __init__(self, **kw): self.__dict__.update(kw)

# ------------- Binance REST/WS -------------
BASE_REST = "https://fapi.binance.com"
BASE_WS   = "wss://fstream.binance.com/stream"
REST_SESSION: Optional[aiohttp.ClientSession] = None
BINANCE_CLIENT = None

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int) -> List[str]:
    async with session.get(f"{BASE_REST}/fapi/v1/exchangeInfo") as r:
        r.raise_for_status()
        data = await r.json()
    out=[]
    for s in data.get("symbols", []):
        try:
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING":
                out.append(s["symbol"])
        except Exception:
            continue
    out = sorted(out)
    if max_symbols>0: out = out[:max_symbols]
    return out

class BinanceClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
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
            d=await r.json()
            return float(d.get("openInterest", 0))
    async def funding_rate(self, symbol:str)->Optional[float]:
        p={"symbol":symbol.upper(),"limit":1}
        async with self.session.get(f"{BASE_REST}/fapi/v1/fundingRate", params=p) as r:
            if r.status!=200: return None
            d=await r.json()
            return float(d[-1]["fundingRate"]) if d else None

class WSStream:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker):
        self.symbols=[s.lower() for s in symbols]; self.timeframes=timeframes
        self.on_kline=on_kline; self.on_bookticker=on_bookticker
    def _streams(self)->str:
        parts=[]
        for s in self.symbols:
            for tf in self.timeframes: parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker")
        return f"{BASE_WS}?streams={'/'.join(parts)}"
    async def run(self):
        url=self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        payload=msg.json(); stream=payload.get("stream",""); data=payload.get("data",{})
                        if "kline" in stream and data.get("k",{}).get("x"): await self.on_kline(data["s"], data["k"])
                        elif "bookTicker" in stream: await self.on_bookticker(data["s"], data)
                    elif msg.type==aiohttp.WSMsgType.ERROR:
                        break

class WSStreamMulti:
    def __init__(self, all_symbols: List[str], timeframes: List[str], on_kline, on_bookticker, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
    async def run(self):
        tasks=[asyncio.create_task(WSStream(c,self.timeframes,self.on_kline,self.on_bookticker).run()) for c in self.chunks]
        await asyncio.gather(*tasks)

# ------------- OB & Breakout -------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update_book_ticker(self, sym:str, d:dict):
        self.tops[sym.upper()] = BookTop(float(d.get("b",0)),float(d.get("B",0)),float(d.get("a",0)),float(d.get("A",0)))
    def top(self, sym:str)->Optional[BookTop]: return self.tops.get(sym.upper())

class RetestState:
    def __init__(self, level:float, direction:str, expire_bars:int, pad_bp:int):
        self.level=level; self.direction=direction; self.remaining=expire_bars
        self.touched=False; self.pad_bp=pad_bp; self.await_next_close=False
    def step(self): self.remaining-=1; return self.remaining>0
    def in_band(self, price:float)->bool:
        band=self.level*(self.pad_bp/10_000); return abs(price-self.level)<=band

class BreakoutEngine:
    def __init__(self, settings: Settings):
        self.s=settings
        self.buffers:Dict[Tuple[str,str],Dict[str,Deque[float]]]={}
        self.retests:Dict[Tuple[str,str],List[RetestState]]={}
    def _buf(self, sym, tf):
        k=(sym,tf)
        if k not in self.buffers:
            self.buffers[k]={"o":deque(maxlen=600),"h":deque(maxlen=600),"l":deque(maxlen=600),"c":deque(maxlen=600),"v":deque(maxlen=600)}
        return self.buffers[k]
    def _ret(self, sym, tf):
        k=(sym,tf)
        if k not in self.retests: self.retests[k]=[]
        return self.retests[k]
    def add_candle(self, sym, tf, cndl:Candle):
        b=self._buf(sym,tf)
        b["o"].append(cndl.open); b["h"].append(cndl.high); b["l"].append(cndl.low); b["c"].append(cndl.close); b["v"].append(cndl.volume)
        for r in list(self._ret(sym,tf)): r.step()
        self.retests[(sym,tf)]=[r for r in self._ret(sym,tf) if r.remaining>0]
    def _prior_high_low(self, hs, ls, N=20):
        if len(hs)<N+1: return None,None
        return max(list(hs)[-N-1:-1]), min(list(ls)[-N-1:-1])
    def _sweep_reclaim(self, hs, ls, cs, side:str)->bool:
        if len(hs)<self.s.SWEEP_LOOKBACK+1: return False
        prev_high=max(list(hs)[-self.s.SWEEP_LOOKBACK-1:-1]); prev_low=min(list(ls)[-self.s.SWEEP_LOOKBACK-1:-1])
        c, h, l = cs[-1], hs[-1], ls[-1]
        if side=="LONG":
            swept=h>prev_high; back_inside=c<=prev_high*(1+self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN<=1.0)
        else:
            swept=l<prev_low; back_inside=c>=prev_low*(1-self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN<=1.0)
    def _progress_retests(self, sym, tf, o,h,l,c):
        lst=self._ret(sym,tf)
        if not lst: return None
        r=lst[-1]
        if r.direction=="LONG":
            touched=r.in_band(l[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]>r.level*(1+self.s.BREAKOUT_PAD_BP/10_000):
                r.await_next_close=False; return True
        else:
            touched=r.in_band(h[-1])
            if touched and not r.touched: r.touched=True; r.await_next_close=True; return None
            if r.touched and r.await_next_close and c[-1]<r.level*(1-self.s.BREAKOUT_PAD_BP/10_000):
                r.await_next_close=False; return True
        return None
    def on_closed_bar(self, sym, tf)->Optional[Signal]:
        b=self._buf(sym,tf); o,h,l,c,v=list(b["o"]),list(b["h"]),list(b["l"]),list(b["c"]),list(b["v"])
        if len(c)<max(25, S.ATR_LEN+1): return None
        ph,pl=self._prior_high_low(h,l,20)
        if ph is None: return None
        pad=self.s.BREAKOUT_PAD_BP/10_000; br=body_ratio(o[-1],h[-1],l[-1],c[-1])
        side=None; level=None
        if c[-1]>ph*(1+pad) and br>=self.s.BODY_RATIO_MIN: side,level="LONG",ph
        elif c[-1]<pl*(1-pad) and br>=self.s.BODY_RATIO_MIN: side,level="SHORT",pl
        self._progress_retests(sym,tf,o,h,l,c)
        if not side: return None
        sr=self._sweep_reclaim(h,l,c,side)
        self._ret(sym,tf).append(RetestState(level,side,self.s.RETEST_BARS,self.s.RETEST_MAX_BP))
        atr_val = atr(list(h), list(l), list(c), S.ATR_LEN)
        rng=max(h[-1]-l[-1],1e-12)
        if side=="LONG":
            upper_wick=(h[-1]-max(c[-1],o[-1]))/rng
            if upper_wick > S.WICK_SIDE_MAX: return None
        else:
            lower_wick=(min(c[-1],o[-1])-l[-1])/rng
            if lower_wick > S.WICK_SIDE_MAX: return None
        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br,
                      pad_bp=self.s.BREAKOUT_PAD_BP, retest_ok=False, sweep_reclaim=sr,
                      htf_bias_ok=True, tob_imbalance=0.0, vol_z=0.0, spread_bp=None,
                      atr_val=atr_val)

# ------------- Telegram -------------
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.api}/sendMessage", json={
                "chat_id": self.chat_id, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True
            }) as r:
                if r.status!=200: print("Telegram error:", r.status, await r.text())

# ------------- HTTP server (no signal handlers, no threads) -------------
class HealthServer:
    def __init__(self, state:dict):
        self.app=web.Application()
        self.state=state
        self.app.add_routes([web.get("/healthz", self.health)])
        self.runner=None
        self.site=None
    async def start(self, host:str, port:int):
        self.runner=web.AppRunner(self.app)
        await self.runner.setup()
        self.site=web.TCPSite(self.runner, host=host, port=port)
        await self.site.start()
    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
    async def health(self, req):
        return web.json_response({
            "ok":True,
            "uptime_sec":int(time.time()-self.state.get("start_time",time.time())),
            "last_kline":self.state.get("last_kline",{}),
            "last_alert":self.state.get("last_alert",{}),
            "symbols":self.state.get("symbols",[]),
            "timeframes":self.state.get("timeframes",[]),
            "open_trades": list(OPEN_TRADES.keys()),
            "today_stats": DAILY_STATS.get(local_day_key(), {}),
        })

# ------------- Helpers & Stats -------------
def _fmt_pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a/b - 1.0) * 100.0

def local_day_key(dt: Optional[datetime]=None) -> str:
    d = riyadh_now() if dt is None else to_tz(dt, S.TZ)
    return d.strftime("%Y-%m-%d")

STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
OB=OrderBookTracker()
BE=BreakoutEngine(S)

LAST_TRADE_TS: Dict[str, int] = {}
OPEN_TRADES: Dict[str, Dict[str, Any]] = {}

DAILY_STATS: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
    "count": 0, "wins_tp2": 0, "wins_ts": 0, "losses_sl": 0,
    "sum_R": 0.0, "best": None, "worst": None
})

def stats_add_trade_result(symbol: str, R: float, outcome: str):
    key = local_day_key()
    st = DAILY_STATS[key]
    st["count"] += 1
    st["sum_R"] += R
    if outcome == "TP2": st["wins_tp2"] += 1
    elif outcome == "TS": st["wins_ts"] += 1
    elif outcome == "SL": st["losses_sl"] += 1
    if st["best"] is None or R > st["best"][1]: st["best"] = (symbol, R)
    if st["worst"] is None or R < st["worst"][1]: st["worst"] = (symbol, R)

def compose_daily_summary() -> str:
    key = local_day_key()
    st = DAILY_STATS.get(key, {})
    if not st or st["count"] == 0:
        return f"📅 Daily Stats – {key}\nNo trades today."
    wins = st["wins_tp2"] + st["wins_ts"]
    losses = st["losses_sl"]
    hit = (wins / st["count"]) * 100.0 if st["count"] > 0 else 0.0
    avg_R = (st["sum_R"] / st["count"]) if st["count"] > 0 else 0.0
    best = f"{st['best'][0]} {st['best'][1]:+.2f}R" if st.get("best") else "—"
    worst = f"{st['worst'][0]} {st['worst'][1]:+.2f}R" if st.get("worst") else "—"
    return (
        f"📅 Daily Stats – {key}\n"
        f"Trades: {st['count']}\n"
        f"Wins: {wins}  (TP2: {st['wins_tp2']}, TS: {st['wins_ts']}) | Losses (SL): {losses}\n"
        f"Hit rate: {hit:.1f}%  |  Net: {st['sum_R']:+.2f}R  |  Avg: {avg_R:+.2f}R\n"
        f"Best: {best}   Worst: {worst}"
    )

def seconds_until_next_2200_riyadh() -> float:
    now = riyadh_now()
    target = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if now >= target: target = target + timedelta(days=1)
    return (target - now).total_seconds()

async def daily_stats_loop():
    await asyncio.sleep(max(1.0, seconds_until_next_2200_riyadh()))
    while True:
        try:
            summary = compose_daily_summary()
            await alert(summary, "DAILY")
        except Exception as e:
            log.warning("daily_stats_err", error=str(e))
        await asyncio.sleep(24 * 3600)

def _log_event(obj: Dict[str, Any]):
    os.makedirs(S.DATA_DIR, exist_ok=True)
    path=os.path.join(S.DATA_DIR,"events.jsonl")
    with open(path,"ab") as f: f.write(orjson.dumps(obj)+b"\n")

# ----------- OI (STRICT: z & delta must BOTH pass) -----------
async def fetch_oi_strict_ok(symbol: str) -> bool:
    try:
        hist = await BINANCE_CLIENT.open_interest_hist(symbol, period=S.OI_LOOKBACK, limit=30)
        cur  = await BINANCE_CLIENT.open_interest(symbol)
        if not hist or cur is None: return True  # don't block on missing OI
        prev = hist[:-1] if len(hist)>1 else hist
        if len(prev) < 5: return True
        mean = sum(prev)/len(prev)
        var  = sum((x-mean)**2 for x in prev)/len(prev)
        std  = var**0.5
        z = 0.0 if std==0 else (cur - mean)/std
        delta_pct = (cur-mean)/mean if mean>0 else 0.0
        return (z >= S.OI_Z_MIN) and (delta_pct >= S.OI_DELTA_MIN)
    except Exception as e:
        log.warning("oi_check_error", symbol=symbol, error=str(e))
        return True

# ----------- Signal handling -----------
async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    # robust parsing: values may be strings
    def _f(x): 
        try: return float(x)
        except: return 0.0
    cndl=Candle(int(k["t"]), _f(k["o"]), _f(k["h"]), _f(k["l"]), _f(k["c"]), _f(k.get("q", k.get("Q", 0))), int(k["T"]))
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"]={"symbol":sym,"tf":tf,"t":k["T"]}
    sig=BE.on_closed_bar(sym, tf)
    if not sig: return

    # HTF SMA gate for 5m using 15m buffer
    if tf=="5m":
        buf=BE.buffers.get((sym,"15m"))
        if buf and len(buf["c"])>=S.HTF_SMA_LEN:
            htf=sum(list(buf["c"])[-S.HTF_SMA_LEN:])/S.HTF_SMA_LEN
            sig.htf_bias_ok = not ((sig.side=="LONG" and sig.price<=htf) or (sig.side=="SHORT" and sig.price>=htf))
        else:
            sig.htf_bias_ok=True

    # TOB & spread
    top=OB.top(sym)
    if top:
        imb=(top.bid_qty-top.ask_qty)/max(top.bid_qty+top.ask_qty,1e-9)
        sig.tob_imbalance=imb
        mid=(top.ask_price+top.bid_price)/2
        mid = mid if mid > 0 else max(top.ask_price, top.bid_price)
        sig.spread_bp=percent_bp(max(top.ask_price-top.bid_price,0.0), mid)

    # Volume filters
    buf=BE.buffers.get((sym,tf))
    vol_ratio = 0.0
    if buf and len(buf["v"])>=max(30, S.VOL_SMA_LEN+1):
        vols=list(buf["v"])
        # Z-score
        win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=var**0.5 if var>0 else 1.0
            sig.vol_z=(vols[-1]-mean)/std
        else:
            sig.vol_z=0.0
        # Extra light filter: vol/SMA20
        sma20 = sma(vols, S.VOL_SMA_LEN) or 0.0
        vol_ratio = (vols[-1] / sma20) if sma20 > 0 else 0.0
    else:
        sig.vol_z = getattr(sig,"vol_z",0.0)

    # Basic filters
    if not getattr(sig,"htf_bias_ok",True): return
    if abs(getattr(sig,"tob_imbalance",0.0))<S.TOB_IMB_MIN: return
    if (sig.spread_bp or 0)>S.SPREAD_MAX_BP: return
    if getattr(sig,"vol_z",0.0)<1.5: return
    if vol_ratio < S.VOL_SMA_RATIO_MIN: return
    if not await fetch_oi_strict_ok(sym): return

    # 4h cooldown AFTER trade open
    now_ts = int(time.time())
    if now_ts - LAST_TRADE_TS.get(sym, 0) < S.COOLDOWN_AFTER_TRADE_SEC: return
    if sym in OPEN_TRADES: return

    # Compute Entry/SL, RR 1:2
    if not sig.atr_val or sig.atr_val <= 0:
        sig.atr_val = sig.price * 0.003
    pad = S.BREAKOUT_PAD_BP / 10_000
    if S.ENTRY_MODE.upper() == "RETEST":
        entry = sig.level * (1 + pad) if sig.side == "LONG" else sig.level * (1 - pad)
        entry_note = "retest"
    else:
        entry = sig.price
        entry_note = "market"
    sl = entry - sig.atr_val * S.ATR_SL_MULT if sig.side=="LONG" else entry + sig.atr_val * S.ATR_SL_MULT
    risk = abs(entry - sl)
    if risk <= 0: return
    tp1 = entry + risk if sig.side=="LONG" else entry - risk
    tp2 = entry + 2*risk if sig.side=="LONG" else entry - 2*risk

    qty_txt = "—"
    if S.ACCOUNT_EQUITY_USDT and S.ACCOUNT_EQUITY_USDT > 0:
        qty = (S.ACCOUNT_EQUITY_USDT * (S.MAX_RISK_PCT / 100.0)) / risk
        qty_txt = f"{qty:.4f}"

    OPEN_TRADES[sym] = {
        "symbol": sym, "side": sig.side, "tf": tf,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "risk": risk,
        "opened_ts": now_ts,
        "tp1_hit": False, "trail_active": False, "trail_peak": None,
        "trail_dist": sig.atr_val * S.TRAIL_ATR_MULT, "atr_at_entry": sig.atr_val
    }
    LAST_TRADE_TS[sym] = now_ts

    direction="✅ LONG" if sig.side=="LONG" else "❌ SHORT"
    tz_dt=to_tz(now_utc(), S.TZ)
    sl_pct=_fmt_pct(sl, entry); tp1_pct=_fmt_pct(tp1, entry); tp2_pct=_fmt_pct(tp2, entry)
    text=(f"{direction} <b>{sym}</b> <code>{tf}</code>\n"
          f"Price: <b>{sig.price:.6f}</b>  Level: {sig.level:.6f}  Body: {sig.body_ratio:.2f}\n"
          f"TOB: {getattr(sig,'tob_imbalance',0):.2f}  VolZ: {getattr(sig,'vol_z',0):.2f}  "
          f"Vol/SMA{S.VOL_SMA_LEN}: {vol_ratio:.2f}  Spread(bp): {sig.spread_bp or 0:.1f}\n"
          f"HTF bias: {'✅' if sig.htf_bias_ok else '❌'}  Sweep/Reclaim: {'✅' if sig.sweep_reclaim else '—'}\n"
          f"Time: {tz_dt.isoformat()}\n"
          f"\n<b>Plan</b> ({entry_note}, R:R = 1:2):\n"
          f"Entry: <b>{entry:.6f}</b>\n"
          f"SL: <b>{sl:.6f}</b>  ({sl_pct:+.2f}%)\n"
          f"TP1: <b>{tp1:.6f}</b>  ({tp1_pct:+.2f}%)\n"
          f"TP2: <b>{tp2:.6f}</b>  ({tp2_pct:+.2f}%)\n"
          f"Trailing: starts after TP1 (ATR×{S.TRAIL_ATR_MULT:.2f})\n"
          f"Risk%: {S.MAX_RISK_PCT:.2f}%   Qty: {qty_txt}")
    await alert(text, sym)
    _log_event({"ts": now_ts, "type": "signal", "symbol": sym, "side": sig.side, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2})

# ----------- Trade management via bookTicker -----------
async def on_bookticker(symbol:str, data:dict):
    OB.update_book_ticker(symbol, data)
    sym = symbol.upper()
    trade = OPEN_TRADES.get(sym)
    if not trade: return
    top = OB.top(sym)
    if not top: return
    mid = (top.ask_price + top.bid_price) / 2
    if mid <= 0: return

    side = trade["side"]
    entry = trade["entry"]; sl = trade["sl"]; tp1 = trade["tp1"]; tp2 = trade["tp2"]
    trail_dist = trade["trail_dist"]

    if not trade["tp1_hit"]:
        if side == "LONG":
            if mid <= sl: await _close_trade(sym, "SL", exit_price=sl, entry=entry, risk=trade["risk"]); return
            if mid >= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        else:
            if mid >= sl: await _close_trade(sym, "SL", exit_price=sl, entry=entry, risk=trade["risk"]); return
            if mid <= tp1:
                trade["tp1_hit"] = True; trade["trail_active"] = True; trade["trail_peak"] = mid
        return

    if side == "LONG":
        if trade["trail_active"]:
            trade["trail_peak"] = max(trade["trail_peak"], mid)
            trail_stop = trade["trail_peak"] - trail_dist
            if mid <= trail_stop:
                await _close_trade(sym, "TS", exit_price=trail_stop, entry=entry, risk=trade["risk"]); return
        if mid >= tp2:
            await _close_trade(sym, "TP2", exit_price=tp2, entry=entry, risk=trade["risk"]); return
    else:
        if trade["trail_active"]:
            trade["trail_peak"] = min(trade["trail_peak"], mid) if trade["trail_peak"] is not None else mid
            trail_stop = trade["trail_peak"] + trail_dist
            if mid >= trail_stop:
                await _close_trade(sym, "TS", exit_price=trail_stop, entry=entry, risk=trade["risk"]); return
        if mid <= tp2:
            await _close_trade(sym, "TP2", exit_price=tp2, entry=entry, risk=trade["risk"]); return

async def _close_trade(symbol: str, outcome: str, exit_price: float, entry: float, risk: float):
    trade = OPEN_TRADES.pop(symbol, None)
    if not trade: return
    if risk <= 0: R = 0.0
    else:
        if outcome in ("TP2", "TS"):
            R = (exit_price - entry)/risk if trade["side"]=="LONG" else (entry - exit_price)/risk
        else:
            R = -1.0
    stats_add_trade_result(symbol, R, outcome)
    pl_pct = _fmt_pct(exit_price, entry)
    dur_min = (int(time.time()) - trade["opened_ts"]) / 60.0
    msg = (f"📌 <b>RESULT</b> {symbol}\n"
           f"Outcome: <b>{outcome}</b>\n"
           f"Entry: {entry:.6f} → Exit: {exit_price:.6f}  (PnL: {pl_pct:+.2f}% | {R:+.2f}R)\n"
           f"TP1 hit: {'✅' if trade.get('tp1_hit') else '—'}\n"
           f"Duration: {dur_min:.1f} min  |  Trail ATR: {trade.get('atr_at_entry'):.6f} × {S.TRAIL_ATR_MULT:.2f}")
    await alert(msg, symbol)
    _log_event({"ts": int(time.time()), "type": "result", "symbol": symbol, "outcome": outcome, "entry": entry, "exit": exit_price, "pnl_pct": pl_pct, "R": R, "tp1_hit": trade.get("tp1_hit")})

# ------------- Alert + Main -------------
async def alert(text:str, symbol:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text); return
    tg=Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
    await tg.send(text)
    STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}

async def start_http_server(state:dict):
    server = HealthServer(state)
    await server.start(S.HOST, S.PORT)
    log.info("http_started", host=S.HOST, port=S.PORT)

async def main():
    global REST_SESSION, BINANCE_CLIENT
    log.info("boot", tfs=S.TIMEFRAMES, rr="1:2", trailing_after_tp1=True, cooldown_after_trade_sec=S.COOLDOWN_AFTER_TRADE_SEC)

    # Start HTTP server safely in-loop
    asyncio.create_task(start_http_server(STATE))

    # Daily stats loop
    asyncio.create_task(daily_stats_loop())

    REST_SESSION = aiohttp.ClientSession()
    BINANCE_CLIENT = BinanceClient(REST_SESSION)

    if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
        symbols=await discover_perp_usdt_symbols(REST_SESSION, S.MAX_SYMBOLS)
    elif isinstance(S.SYMBOLS, (list, tuple)):
        symbols=list(S.SYMBOLS)
    else:
        symbols=[str(S.SYMBOLS)]
    STATE["symbols"]=symbols
    log.info("symbols_selected", count=len(symbols))

    for i, sym in enumerate(symbols):
        for tf in S.TIMEFRAMES:
            try:
                await small_backfill(BINANCE_CLIENT, sym, tf)
            except Exception as e:
                log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))
        if i % 10 == 0:
            await asyncio.sleep(0.2)

    ws_multi=WSStreamMulti(symbols, S.TIMEFRAMES, on_kline, on_bookticker, chunk_size=S.WS_CHUNK_SIZE)
    await ws_multi.run()

async def small_backfill(client:"BinanceClient", sym:str, tf:str):
    data=await client.klines(sym, tf, limit=S.BACKFILL_LIMIT)
    for k in data[:-1]:
        o,h,l,c = float(k[1]),float(k[2]),float(k[3]),float(k[4])
        q = float(k[5]) if len(k)>5 else 0.0
        BE.add_candle(sym, tf, Candle(int(k[0]), o, h, l, c, q, int(k[6])))

async def on_shutdown():
    try:
        if REST_SESSION and not REST_SESSION.closed:
            await REST_SESSION.close()
    except Exception: pass

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass