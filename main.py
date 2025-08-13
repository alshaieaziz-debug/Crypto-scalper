# app.py — ALL Binance USDT-M PERPS auto-scan edition
# (single-file async bot: aiohttp+uvloop, /healthz, Telegram alerts)
# See ENV knobs at the top of Settings below.

import sys, subprocess, os
def _ensure(pkgs):
    import importlib
    miss=[]
    for p in pkgs:
        m=p.split("==")[0].split(">=")[0].split("[")[0]
        try: importlib.import_module(m.replace("-","_"))
        except Exception: miss.append(p)
    if miss:
        print("Installing:", miss, flush=True)
        subprocess.check_call([sys.executable,"-m","pip","install","--upgrade",*miss])

_ensure([
    "aiohttp>=3.9.5", "uvloop>=0.19.0", "pydantic>=2.7.0",
    "pydantic-settings>=2.2.1", "structlog>=24.1.0", "tzdata>=2024.1", "orjson>=3.10.7",
])

import asyncio, aiohttp, orjson, time
from typing import List, Dict, Any, Optional, Deque, Tuple
from collections import deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import structlog, uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ---------------- Settings ----------------
class Settings(BaseSettings):
    # Core scan config
    SYMBOLS: str | List[str] = "ALL"   # "ALL" = auto-discover all USDT-M perps; or "BTCUSDT,ETHUSDT"
    TIMEFRAMES: List[str] = ["5m","15m"]

    # Safety/perf knobs for ALL mode
    MAX_SYMBOLS: int = 200            # upper cap in ALL mode
    WS_CHUNK_SIZE: int = 80           # symbols per WS connection (Binance allows multiplexing; chunk sensibly)
    BACKFILL_LIMIT: int = 50          # small REST backfill per TF to warm indicators

    # Confirmations & filters (same as before)
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    TOB_IMB_MIN: float = 0.15
    TOB_STABILITY_CHECKS: int = 1
    VOL_Z_MIN: float = 1.5
    FUNDING_MAX: float = 0.01
    OI_Z_MIN: float = 1.0
    BETA_MAX_DIVERGENCE: float = 0.7
    SPREAD_MAX_BP: int = 5
    ATR_PAUSE_MULT: float = 2.0

    MAX_RISK_PCT: float = 0.5
    DD_HALT_PCT: float = 3.0
    COOLDOWN_SEC: int = 900
    MIN_CONF_BASE: int = 3
    SLIPPAGE_BP: int = 3
    DRY_RUN: bool = True
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
def percent_bp(a: float, b: float) -> float: return 0.0 if b==0 else (a-b)/b*10_000
def body_ratio(o,h,l,c): rng=max(h-l,1e-12); return abs(c-o)/rng
def sma(vals: List[float], n:int)->Optional[float]:
    if n<=0 or len(vals)<n: return None
    return sum(vals[-n:])/n

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
BASE_WS = "wss://fstream.binance.com/stream"

async def discover_perp_usdt_symbols(session: aiohttp.ClientSession, max_symbols:int) -> List[str]:
    """USDT-M exchangeInfo → PERPETUAL + quote=USDT + TRADING"""
    url = f"{BASE_REST}/fapi/v1/exchangeInfo"
    async with session.get(url) as r:
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
    if max_symbols>0:
        out = out[:max_symbols]
    return out

class BinanceClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    async def klines(self, symbol:str, interval:str, limit:int=200):
        p={"symbol":symbol.upper(),"interval":interval,"limit":limit}
        async with self.session.get(f"{BASE_REST}/fapi/v1/klines", params=p) as r:
            r.raise_for_status(); return await r.json()
    async def funding_rate(self, symbol:str)->Optional[float]:
        p={"symbol":symbol.upper(),"limit":1}
        async with self.session.get(f"{BASE_REST}/fapi/v1/fundingRate", params=p) as r:
            r.raise_for_status(); d=await r.json()
            return float(d[-1]["fundingRate"]) if d else None
    async def open_interest_z(self, symbol:str, period:str="5m")->Optional[float]:
        p={"symbol":symbol.upper(),"period":period,"limit":30}
        async with self.session.get(f"{BASE_REST}/futures/data/openInterestHist", params=p) as r:
            if r.status!=200: return None
            d=await r.json()
            if not d or len(d)<5: return None
            vals=[float(x["sumOpenInterest"]) for x in d]
            m=sum(vals[:-1])/(len(vals)-1)
            var=sum((v-m)**2 for v in vals[:-1])/(len(vals)-1)
            std=var**0.5
            return 0.0 if std==0 else (vals[-1]-m)/std
    async def mark_price(self, symbol:str)->Optional[float]:
        async with self.session.get(f"{BASE_REST}/fapi/v1/premiumIndex", params={"symbol":symbol.upper()}) as r:
            r.raise_for_status(); d=await r.json(); return float(d["markPrice"])

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
    """Run multiple WS connections (chunked symbol lists) concurrently."""
    def __init__(self, all_symbols: List[str], timeframes: List[str], on_kline, on_bookticker, chunk_size:int):
        self.chunks=[all_symbols[i:i+chunk_size] for i in range(0,len(all_symbols),chunk_size)]
        self.timeframes=timeframes; self.on_kline=on_kline; self.on_bookticker=on_bookticker
    async def run(self):
        tasks=[]
        for chunk in self.chunks:
            tasks.append(asyncio.create_task(WSStream(chunk,self.timeframes,self.on_kline,self.on_bookticker).run()))
        await asyncio.gather(*tasks)

# ------------- OB & Breakout -------------
class OrderBookTracker:
    def __init__(self): self.tops:Dict[str,BookTop]={}
    def update_book_ticker(self, sym:str, d:dict):
        self.tops[sym.upper()] = BookTop(float(d["b"]),float(d["B"]),float(d["a"]),float(d["A"]))
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
        if len(c)<25: return None
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
        return Signal(symbol=sym, tf=tf, side=side, price=c[-1], level=level, body_ratio=br,
                      pad_bp=self.s.BREAKOUT_PAD_BP, retest_ok=False, sweep_reclaim=sr,
                      htf_bias_ok=True, tob_imbalance=0.0, vol_z=0.0, spread_bp=None)

# ------------- Telegram & Health -------------
class Telegram:
    def __init__(self, token:str, chat_id:str):
        self.api=f"https://api.telegram.org/bot{token}"; self.chat_id=chat_id
    async def send(self, text:str):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.api}/sendMessage", json={
                "chat_id": self.chat_id, "text": text, "parse_mode":"HTML", "disable_web_page_preview": True
            }) as r:
                if r.status!=200: print("Telegram error:", r.status, await r.text())

class HealthServer:
    def __init__(self, host:str, port:int, state:dict):
        self.app=web.Application(); self.state=state
        self.app.add_routes([web.get("/healthz", self.health)])
        self.host=host; self.port=port
    async def health(self, req): 
        return web.json_response({"ok":True,"uptime_sec":int(time.time()-self.state.get("start_time",time.time())),
                                  "last_kline":self.state.get("last_kline",{}),"last_alert":self.state.get("last_alert",{}),
                                  "symbols":self.state.get("symbols",[]),"timeframes":self.state.get("timeframes",[])})
    def run(self): web.run_app(self.app, host=self.host, port=self.port)

# ------------- App wiring -------------
STATE={"start_time":int(time.time()),"last_kline":{},"last_alert":{},"symbols":[], "timeframes":S.TIMEFRAMES}
OB=OrderBookTracker()
BE=BreakoutEngine(S)

async def small_backfill(client:BinanceClient, sym:str, tf:str):
    try:
        data=await client.klines(sym, tf, limit=S.BACKFILL_LIMIT)
        for k in data[:-1]:
            BE.add_candle(sym, tf, Candle(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), int(k[6])))
    except Exception as e:
        log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))

async def periodic_derivs(client:BinanceClient, symbols:List[str]):
    while True:
        try:
            for sym in symbols[:50]:  # sample subset to reduce load
                fr=await client.funding_rate(sym)
                oi=await client.open_interest_z(sym,"5m")
                log.info("derivs", symbol=sym, funding=fr, oi_z=oi)
        except Exception as e:
            log.warning("derivs_poll_err", error=str(e))
        await asyncio.sleep(60)

def _log_event(symbol:str, text:str):
    os.makedirs(S.DATA_DIR, exist_ok=True)
    path=os.path.join(S.DATA_DIR,"events.jsonl")
    with open(path,"ab") as f: f.write(orjson.dumps({"ts":int(time.time()),"symbol":symbol,"text":text})+b"\n")

async def on_kline(symbol:str, k:dict):
    sym=symbol.upper(); tf=k["i"]
    cndl=Candle(int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]), float(k["q"]), int(k["T"]))
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
        sig.spread_bp=percent_bp(top.ask_price-top.bid_price, mid)
    # Vol z (30)
    buf=BE.buffers.get((sym,tf))
    if buf and len(buf["v"])>=30:
        vols=list(buf["v"]); win=vols[-30:-1]
        if win:
            mean=sum(win)/len(win); var=sum((x-mean)**2 for x in win)/len(win); std=var**0.5 if var>0 else 1.0
            sig.vol_z=(vols[-1]-mean)/std
    # Filters
    if not getattr(sig,"htf_bias_ok",True): return
    if abs(getattr(sig,"tob_imbalance",0.0))<S.TOB_IMB_MIN: return
    if (sig.spread_bp or 0)>S.SPREAD_MAX_BP: return
    if getattr(sig,"vol_z",0.0)<S.VOL_Z_MIN: return
    # Alert
    direction="✅ LONG" if sig.side=="LONG" else "❌ SHORT"
    tz_dt=to_tz(now_utc(), S.TZ)
    text=(f"{direction} <b>{sym}</b> <code>{tf}</code>\n"
          f"Price: <b>{sig.price:.4f}</b>  Level: {sig.level:.4f}  Body: {sig.body_ratio:.2f}\n"
          f"TOB: {sig.tob_imbalance:.2f}  VolZ: {getattr(sig,'vol_z',0):.2f}  Spread(bp): {sig.spread_bp or 0:.1f}\n"
          f"HTF bias: {'✅' if sig.htf_bias_ok else '❌'}  Sweep/Reclaim: {'✅' if sig.sweep_reclaim else '—'}\n"
          f"Time: {tz_dt.isoformat()}")
    await alert(text, sym)

async def on_bookticker(symbol:str, data:dict):
    OB.update_book_ticker(symbol, data)

async def alert(text:str, symbol:str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text); return
    tg=Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
    await tg.send(text)
    STATE["last_alert"]={"symbol":symbol,"text":text,"ts":int(time.time())}
    _log_event(symbol, text)

async def main():
    log.info("boot", tfs=S.TIMEFRAMES)
    # Health server
    hs=HealthServer(S.HOST, S.PORT, STATE)
    asyncio.create_task(asyncio.to_thread(hs.run))
    async with aiohttp.ClientSession() as sess:
        client=BinanceClient(sess)
        # Discover symbols if ALL
        if S.SYMBOLS=="ALL" or (isinstance(S.SYMBOLS,str) and S.SYMBOLS.upper()=="ALL"):
            symbols=await discover_perp_usdt_symbols(sess, S.MAX_SYMBOLS)
        elif isinstance(S.SYMBOLS, list):
            symbols=S.SYMBOLS
        else:
            symbols=[str(S.SYMBOLS)]
        STATE["symbols"]=symbols
        log.info("symbols_selected", count=len(symbols))
        # Small backfill (throttled)
        for sym in symbols:
            for tf in S.TIMEFRAMES:
                await small_backfill(client, sym, tf)
            await asyncio.sleep(0.02)  # minor spacing to be polite
        # Derivatives poll (sample subset)
        asyncio.create_task(periodic_derivs(client, symbols))
    # WS streams in chunks
    ws_multi=WSStreamMulti(STATE["symbols"], S.TIMEFRAMES, on_kline, on_bookticker, chunk_size=S.WS_CHUNK_SIZE)
    await ws_multi.run()

if __name__=="__main__":
    try: uvloop.install()
    except Exception: pass
    try: asyncio.run(main())
    except KeyboardInterrupt: pass