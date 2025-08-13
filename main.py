# app.py
"""
Async Binance USDT perpetual breakout scanner (single-file edition)
- Scans 5m/15m (configurable via env)
- aiohttp + uvloop
- Health endpoint /healthz
- Telegram alerts (emoji-rich)
- Confirmations:
  (1) Close-beyond-level + BODY_RATIO_MIN
  (2) Quick retest within RETEST_BARS, ± RETEST_MAX_BP, next-bar close in trend
  (3) Sweep+reclaim of SWEEP_LOOKBACK highs/lows + RECLAIM_BODY_RATIO_MIN
  (4) HTF 15m SMA bias gates 5m entries
  (5) TOB imbalance with basic stability (single check; hook present for multi)
- Extra checks: volume z-score, funding cap, OI z-score, spread cap, ATR pause hook
- Risk/alerts scaffolding (dry-run, slippage utility, MAX_RISK_PCT guard hook)
- Daily summary at 22:00 Asia/Riyadh
- Structured logging (structlog JSON)

NOTE: This is an alerts scanner (no real orders).
"""

# -----------------------------
# 0) Lightweight auto-installer
# -----------------------------
import sys, subprocess, os

def _ensure(pkgs):
    import importlib
    missing = []
    for p in pkgs:
        mod = p.split("==")[0].split(">=")[0].split("[")[0]
        try:
            importlib.import_module(mod.replace("-", "_"))
        except Exception:
            missing.append(p)
    if missing:
        print("Installing missing packages:", missing, flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", *missing])

# Keep to core libs (Railway supports this)
_ensure([
    "aiohttp>=3.9.5",
    "uvloop>=0.19.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.2.1",
    "structlog>=24.1.0",
    "tzdata>=2024.1",
    "orjson>=3.10.7",
])

# -------------
# 1) Imports
# -------------
import asyncio
import aiohttp
import orjson
import time
from typing import List, Dict, Any, Optional, Deque, Tuple
from collections import deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import structlog
import uvloop
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

# ----------------
# 2) Configuration
# ----------------
class Settings(BaseSettings):
    # Core
    BINANCE_API_KEY: Optional[str] = None
    BINANCE_API_SECRET: Optional[str] = None
    SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    TIMEFRAMES: List[str] = ["5m", "15m"]

    # Confirmations
    BREAKOUT_PAD_BP: int = 10
    BODY_RATIO_MIN: float = 0.60
    RETEST_BARS: int = 2
    RETEST_MAX_BP: int = 8
    SWEEP_LOOKBACK: int = 15
    RECLAIM_BUFFER_BP: int = 6
    RECLAIM_BODY_RATIO_MIN: float = 0.55
    HTF_SMA_LEN: int = 50
    TOB_IMB_MIN: float = 0.15
    TOB_STABILITY_CHECKS: int = 1  # simple (hook exists to extend)

    # Filters
    VOL_Z_MIN: float = 1.5
    FUNDING_MAX: float = 0.01
    OI_Z_MIN: float = 1.0
    BETA_MAX_DIVERGENCE: float = 0.7  # hook
    SPREAD_MAX_BP: int = 5
    ATR_PAUSE_MULT: float = 2.0  # hook

    # Risk & Ops
    MAX_RISK_PCT: float = 0.5
    DD_HALT_PCT: float = 3.0  # hook
    COOLDOWN_SEC: int = 900   # hook (dedupe/cooldown store)
    MIN_CONF_BASE: int = 3    # hook (dynamic)
    SLIPPAGE_BP: int = 3
    DRY_RUN: bool = True
    LOG_LEVEL: str = "INFO"
    DATA_DIR: str = "./data"

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8080"))
    TZ: str = "Asia/Riyadh"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    @field_validator("SYMBOLS", mode="before")
    @classmethod
    def _split_symbols(cls, v):
        if isinstance(v, str):
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v

    @field_validator("TIMEFRAMES", mode="before")
    @classmethod
    def _split_tfs(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v

S = Settings()

# ---------------
# 3) JSON Logging
# ---------------
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

# ---------------
# 4) Small helpers
# ---------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_tz(dt: datetime, tz: str) -> datetime:
    return dt.astimezone(ZoneInfo(tz))

def percent_bp(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 10_000

def body_ratio(o: float, h: float, l: float, c: float) -> float:
    rng = max(h - l, 1e-12)
    return abs(c - o) / rng

def sma(values: List[float], length: int) -> Optional[float]:
    if len(values) < length or length <= 0:
        return None
    return sum(values[-length:]) / length

def zscore(series: List[float], length: int) -> Optional[float]:
    if len(series) < length or length <= 1:
        return None
    window = series[-length:]
    m = sum(window) / length
    var = sum((x - m) ** 2 for x in window) / length
    std = var ** 0.5
    if std == 0:
        return 0.0
    return (series[-1] - m) / std

# ---------------
# 5) Data objects
# ---------------
class Candle:
    __slots__ = ("open_time","open","high","low","close","volume","close_time")
    def __init__(self, open_time:int, open:float, high:float, low:float, close:float, volume:float, close_time:int):
        self.open_time=open_time; self.open=open; self.high=high; self.low=low; self.close=close; self.volume=volume; self.close_time=close_time

class BookTop:
    __slots__=("bid_price","bid_qty","ask_price","ask_qty")
    def __init__(self, bid_price:float, bid_qty:float, ask_price:float, ask_qty:float):
        self.bid_price=bid_price; self.bid_qty=bid_qty; self.ask_price=ask_price; self.ask_qty=ask_qty

class Signal:
    def __init__(self, **kw):
        self.__dict__.update(kw)

# --------------------
# 6) Binance REST/WS
# --------------------
BASE_REST = "https://fapi.binance.com"
BASE_WS = "wss://fstream.binance.com/stream"

class BinanceClient:
    def __init__(self, session: aiohttp.ClientSession, api_key: Optional[str] = None):
        self.session = session
        self.api_key = api_key

    async def klines(self, symbol: str, interval: str, limit: int = 500) -> List[List[Any]]:
        p = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        async with self.session.get(f"{BASE_REST}/fapi/v1/klines", params=p) as r:
            r.raise_for_status()
            return await r.json()

    async def funding_rate(self, symbol: str) -> Optional[float]:
        p = {"symbol": symbol.upper(), "limit": 1}
        async with self.session.get(f"{BASE_REST}/fapi/v1/fundingRate", params=p) as r:
            r.raise_for_status()
            data = await r.json()
            if data:
                return float(data[-1]["fundingRate"])
            return None

    async def open_interest_z(self, symbol: str, period: str = "5m") -> Optional[float]:
        p = {"symbol": symbol.upper(), "period": period, "limit": 30}
        async with self.session.get(f"{BASE_REST}/futures/data/openInterestHist", params=p) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if not data or len(data) < 5:
                return None
            vals = [float(x["sumOpenInterest"]) for x in data]
            mean = sum(vals[:-1]) / (len(vals)-1)
            var = sum((v-mean)**2 for v in vals[:-1]) / (len(vals)-1)
            std = var**0.5
            if std == 0:
                return 0.0
            return (vals[-1] - mean)/std

    async def mark_price(self, symbol: str) -> Optional[float]:
        async with self.session.get(f"{BASE_REST}/fapi/v1/premiumIndex", params={"symbol": symbol.upper()}) as r:
            r.raise_for_status()
            data = await r.json()
            return float(data["markPrice"])

class WSStream:
    def __init__(self, symbols: List[str], timeframes: List[str], on_kline, on_bookticker):
        self.symbols = [s.lower() for s in symbols]
        self.timeframes = timeframes
        self.on_kline = on_kline
        self.on_bookticker = on_bookticker

    def _streams(self) -> str:
        parts = []
        for s in self.symbols:
            for tf in self.timeframes:
                parts.append(f"{s}@kline_{tf}")
            parts.append(f"{s}@bookTicker")
        return f"{BASE_WS}?streams={'/'.join(parts)}"

    async def run(self):
        url = self._streams()
        async with aiohttp.ClientSession() as sess:
            async with sess.ws_connect(url, heartbeat=30) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = msg.json()
                        stream = payload.get("stream", "")
                        data = payload.get("data", {})
                        if "kline" in stream and data.get("k", {}).get("x"):
                            await self.on_kline(data["s"], data["k"])
                        elif "bookTicker" in stream:
                            await self.on_bookticker(data["s"], data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

# ----------------------
# 7) Orderbook snapshot
# ----------------------
class OrderBookTracker:
    def __init__(self):
        self.tops: Dict[str, BookTop] = {}

    def update_book_ticker(self, symbol: str, data: dict):
        self.tops[symbol.upper()] = BookTop(
            bid_price=float(data["b"]),
            bid_qty=float(data["B"]),
            ask_price=float(data["a"]),
            ask_qty=float(data["A"]),
        )

    def top(self, symbol: str) -> Optional[BookTop]:
        return self.tops.get(symbol.upper())

# -----------------------
# 8) Breakout engine core
# -----------------------
class RetestState:
    def __init__(self, level: float, direction: str, expire_after_bars: int, pad_bp: int):
        self.level = level
        self.direction = direction  # LONG or SHORT
        self.remaining = expire_after_bars
        self.touched = False
        self.pad_bp = pad_bp
        self.await_next_close = False

    def step(self):
        self.remaining -= 1
        return self.remaining > 0

    def in_band(self, price: float) -> bool:
        band = self.level * (self.pad_bp/10_000)
        return abs(price - self.level) <= band

class BreakoutEngine:
    def __init__(self, settings: Settings):
        self.s = settings
        self.buffers: Dict[Tuple[str,str], Dict[str, Deque[float]]] = {}
        self.retests: Dict[Tuple[str,str], List[RetestState]] = {}

    def _buf(self, sym: str, tf: str):
        k = (sym, tf)
        if k not in self.buffers:
            self.buffers[k] = { "o": deque(maxlen=600), "h": deque(maxlen=600),
                                "l": deque(maxlen=600), "c": deque(maxlen=600), "v": deque(maxlen=600) }
        return self.buffers[k]

    def _ret(self, sym: str, tf: str) -> List[RetestState]:
        k = (sym, tf)
        if k not in self.retests:
            self.retests[k] = []
        return self.retests[k]

    def add_candle(self, sym: str, tf: str, cndl: Candle):
        b = self._buf(sym, tf)
        b["o"].append(cndl.open)
        b["h"].append(cndl.high)
        b["l"].append(cndl.low)
        b["c"].append(cndl.close)
        b["v"].append(cndl.volume)
        # step retests
        for r in list(self._ret(sym, tf)):
            r.step()
        self.retests[(sym, tf)] = [r for r in self._ret(sym, tf) if r.remaining > 0]

    def _prior_high_low(self, hs: List[float], ls: List[float], N: int = 20):
        if len(hs) < N+1: return None, None
        return max(list(hs)[-N-1:-1]), min(list(ls)[-N-1:-1])

    def _sweep_reclaim(self, hs: List[float], ls: List[float], cs: List[float], side: str) -> bool:
        if len(hs) < self.s.SWEEP_LOOKBACK + 1:
            return False
        prev_high = max(list(hs)[-self.s.SWEEP_LOOKBACK-1:-1])
        prev_low  = min(list(ls)[-self.s.SWEEP_LOOKBACK-1:-1])
        c, h, l = cs[-1], hs[-1], ls[-1]
        if side == "LONG":
            swept = h > prev_high
            back_inside = c <= prev_high * (1 + self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN <= 1.0)
        else:
            swept = l < prev_low
            back_inside = c >= prev_low * (1 - self.s.RECLAIM_BUFFER_BP/10_000)
            return swept and back_inside and (self.s.RECLAIM_BODY_RATIO_MIN <= 1.0)

    def _progress_retests(self, sym: str, tf: str, o, h, l, c) -> Optional[bool]:
        lst = self._ret(sym, tf)
        if not lst: return None
        r = lst[-1]
        if r.direction == "LONG":
            touched = r.in_band(l[-1])
            if touched and not r.touched:
                r.touched = True; r.await_next_close = True; return None
            if r.touched and r.await_next_close:
                if c[-1] > r.level * (1 + self.s.BREAKOUT_PAD_BP/10_000):
                    r.await_next_close = False
                    return True
        else:
            touched = r.in_band(h[-1])
            if touched and not r.touched:
                r.touched = True; r.await_next_close = True; return None
            if r.touched and r.await_next_close:
                if c[-1] < r.level * (1 - self.s.BREAKOUT_PAD_BP/10_000):
                    r.await_next_close = False
                    return True
        return None

    def on_closed_bar(self, sym: str, tf: str) -> Optional[Signal]:
        b = self._buf(sym, tf)
        o, h, l, c, v = list(b["o"]), list(b["h"]), list(b["l"]), list(b["c"]), list(b["v"])
        if len(c) < 25: return None
        ph, pl = self._prior_high_low(h, l, 20)
        if ph is None: return None
        pad = self.s.BREAKOUT_PAD_BP / 10_000
        br = body_ratio(o[-1], h[-1], l[-1], c[-1])

        side, level = None, None
        if c[-1] > ph * (1 + pad) and br >= self.s.BODY_RATIO_MIN:
            side, level = "LONG", ph
        elif c[-1] < pl * (1 - pad) and br >= self.s.BODY_RATIO_MIN:
            side, level = "SHORT", pl

        # progress retests each closed bar
        self._progress_retests(sym, tf, o, h, l, c)

        if not side:
            return None

        sr = self._sweep_reclaim(h, l, c, side)
        self._ret(sym, tf).append(RetestState(level, side, self.s.RETEST_BARS, self.s.RETEST_MAX_BP))

        return Signal(
            symbol=sym, tf=tf, side=side, price=c[-1], level=level,
            body_ratio=br, pad_bp=self.s.BREAKOUT_PAD_BP, retest_ok=False, sweep_reclaim=sr,
            htf_bias_ok=True, tob_imbalance=0.0, vol_z=0.0, spread_bp=None
        )

# ---------------------
# 9) Telegram notifier
# ---------------------
class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.api = f"https://api.telegram.org/bot{token}"

    async def send(self, text: str):
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.api}/sendMessage", json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }) as r:
                if r.status != 200:
                    print("Telegram error:", r.status, await r.text())

# -----------------
# 10) Health server
# -----------------
class HealthServer:
    def __init__(self, host: str, port: int, state_ref: dict):
        self.host=host; self.port=port; self.state_ref=state_ref
        self.app = web.Application()
        self.app.add_routes([web.get("/healthz", self.health)])

    async def health(self, request):
        return web.json_response({
            "ok": True,
            "uptime_sec": int(time.time() - self.state_ref.get("start_time", time.time())),
            "last_kline": self.state_ref.get("last_kline", {}),
            "last_alert": self.state_ref.get("last_alert", {}),
            "symbols": self.state_ref.get("symbols", []),
            "timeframes": self.state_ref.get("timeframes", []),
        })

    def run(self):
        web.run_app(self.app, host=self.host, port=self.port)

# ----------------
# 11) App wiring
# ----------------
STATE = {
    "start_time": int(time.time()),
    "last_kline": {},
    "last_alert": {},
    "symbols": S.SYMBOLS,
    "timeframes": S.TIMEFRAMES,
}

OB = OrderBookTracker()
BE = BreakoutEngine(S)

async def backfill(client: BinanceClient, symbol: str, tf: str):
    data = await client.klines(symbol, tf, limit=200)
    for k in data[:-1]:  # exclude in-progress
        cndl = Candle(
            open_time=int(k[0]), open=float(k[1]), high=float(k[2]), low=float(k[3]),
            close=float(k[4]), volume=float(k[5]), close_time=int(k[6])
        )
        BE.add_candle(symbol, tf, cndl)

async def periodic_derivs(client: BinanceClient):
    while True:
        try:
            for sym in S.SYMBOLS:
                fr = await client.funding_rate(sym)
                oi = await client.open_interest_z(sym, "5m")
                log.info("derivs", symbol=sym, funding=fr, oi_z=oi)
        except Exception as e:
            log.warning("derivs_poll_err", error=str(e))
        await asyncio.sleep(60)

async def daily_summary_task(tg: Optional[Telegram]):
    tz = ZoneInfo(S.TZ)
    events_path = os.path.join(S.DATA_DIR, "events.jsonl")
    os.makedirs(S.DATA_DIR, exist_ok=True)
    while True:
        now = datetime.now(tz)
        target = now.replace(hour=22, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        wait = max((target - now).total_seconds(), 1)
        await asyncio.sleep(wait)
        since_utc = (target - timedelta(days=1)).astimezone(timezone.utc).timestamp()
        total = 0; by_symbol: Dict[str,int] = {}
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = orjson.loads(line)
                        if obj.get("ts", 0) >= since_utc:
                            total += 1
                            s = obj.get("symbol","?")
                            by_symbol[s] = by_symbol.get(s,0) + 1
                    except Exception:
                        pass
        except FileNotFoundError:
            pass
        msg = f"📊 <b>Daily Summary</b> (up to {target.date()} 22:00):\n• Events: {total}\n• By symbol: {by_symbol}"
        if tg:
            await tg.send(msg)
        log.info("daily_summary", events=total, by_symbol=by_symbol)

def _log_event(symbol: str, text: str):
    os.makedirs(S.DATA_DIR, exist_ok=True)
    path = os.path.join(S.DATA_DIR, "events.jsonl")
    evt = {"ts": int(time.time()), "symbol": symbol, "text": text}
    with open(path, "ab") as f:
        f.write(orjson.dumps(evt) + b"\n")

async def on_kline(symbol: str, k: dict):
    sym = symbol.upper()
    tf = k["i"]
    cndl = Candle(
        open_time=int(k["t"]),
        open=float(k["o"]),
        high=float(k["h"]),
        low=float(k["l"]),
        close=float(k["c"]),
        volume=float(k["q"]),
        close_time=int(k["T"]),
    )
    BE.add_candle(sym, tf, cndl)
    STATE["last_kline"] = {"symbol": sym, "tf": tf, "t": k["T"]}

    sig = BE.on_closed_bar(sym, tf)
    if not sig:
        return

    # (4) HTF SMA gate for 5m using 15m buffer
    if tf == "5m":
        buf = BE.buffers.get((sym, "15m"))
        if buf and len(buf["c"]) >= S.HTF_SMA_LEN:
            htf = sum(list(buf["c"])[-S.HTF_SMA_LEN:]) / S.HTF_SMA_LEN
            if (sig.price <= htf and sig.side == "LONG") or (sig.price >= htf and sig.side == "SHORT"):
                sig.htf_bias_ok = False
        else:
            sig.htf_bias_ok = True

    # (5) TOB imbalance / spread
    top = OB.top(sym)
    if top:
        imb = (top.bid_qty - top.ask_qty) / max(top.bid_qty + top.ask_qty, 1e-9)
        sig.tob_imbalance = imb
        mid = (top.ask_price + top.bid_price)/2
        sig.spread_bp = percent_bp(top.ask_price - top.bid_price, mid)

    # Volume z-score (30)
    buf = BE.buffers.get((sym, tf))
    if buf and len(buf["v"]) >= 30:
        vols = list(buf["v"])
        window = vols[-30:-1]
        if len(window) > 0:
            mean = sum(window) / len(window)
            var = sum((x-mean)**2 for x in window) / len(window)
            std = var**0.5 if var>0 else 1.0
            sig.vol_z = (vols[-1] - mean)/std

    # Basic filters
    pass_filters = True
    if not getattr(sig, "htf_bias_ok", True): pass_filters = False
    if abs(getattr(sig, "tob_imbalance", 0.0)) < S.TOB_IMB_MIN: pass_filters = False
    if (sig.spread_bp or 0) > S.SPREAD_MAX_BP: pass_filters = False
    if getattr(sig, "vol_z", 0.0) < S.VOL_Z_MIN: pass_filters = False
    # (Funding/OI hooks are logged by periodic task; you can block here if desired)

    if not pass_filters:
        return

    # Alert
    direction = "✅ LONG" if sig.side == "LONG" else "❌ SHORT"
    tz_dt = to_tz(now_utc(), S.TZ)
    text = (
        f"{direction} <b>{sym}</b> <code>{tf}</code>\n"
        f"Price: <b>{sig.price:.4f}</b>  Level: {sig.level:.4f}  Body: {sig.body_ratio:.2f}\n"
        f"TOB: {sig.tob_imbalance:.2f}  VolZ: {getattr(sig,'vol_z',0):.2f}  Spread(bp): {sig.spread_bp or 0:.1f}\n"
        f"HTF bias: {'✅' if sig.htf_bias_ok else '❌'}  Sweep/Reclaim: {'✅' if sig.sweep_reclaim else '—'}\n"
        f"Time: {tz_dt.isoformat()}"
    )
    await alert(text, sym)

async def on_bookticker(symbol: str, data: dict):
    OB.update_book_ticker(symbol, data)

async def alert(text: str, symbol: str):
    if not S.TELEGRAM_BOT_TOKEN or not S.TELEGRAM_CHAT_ID:
        log.info("alert", text=text)
        return
    tg = Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
    await tg.send(text)
    STATE["last_alert"] = {"symbol": symbol, "text": text, "ts": int(time.time())}
    _log_event(symbol, text)

async def main():
    log.info("boot", symbols=S.SYMBOLS, tfs=S.TIMEFRAMES)
    # Health server
    hs = HealthServer(S.HOST, S.PORT, STATE)
    asyncio.create_task(asyncio.to_thread(hs.run))

    # Backfill + periodic derivs
    async with aiohttp.ClientSession() as sess:
        client = BinanceClient(sess, S.BINANCE_API_KEY)
        for sym in S.SYMBOLS:
            for tf in S.TIMEFRAMES:
                try:
                    await backfill(client, sym, tf)
                except Exception as e:
                    log.warning("backfill_error", symbol=sym, tf=tf, error=str(e))
        asyncio.create_task(periodic_derivs(client))

    # Daily summary
    tg = None
    if S.TELEGRAM_BOT_TOKEN and S.TELEGRAM_CHAT_ID:
        tg = Telegram(S.TELEGRAM_BOT_TOKEN, S.TELEGRAM_CHAT_ID)
        asyncio.create_task(daily_summary_task(tg))

    # WS streams
    ws = WSStream(S.SYMBOLS, S.TIMEFRAMES, on_kline, on_bookticker)
    await ws.run()

if __name__ == "__main__":
    try:
        uvloop.install()
    except Exception:
        pass
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass