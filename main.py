import ccxt
import numpy as np
import pandas as pd
import telegram
import logging
import asyncio
from datetime import datetime, timedelta
import time
import pytz
import sys
import os
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler
import json
from collections import Counter

# ================== Sabit Değerler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN ve CHAT_ID ortam değişkenlerini ayarla.")
TEST_MODE = False
VERBOSE_LOG = False
HEARTBEAT_ENABLED = False
STARTUP_MSG_ENABLED = True
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05
ADX_PERIOD = 14
ADX_THRESHOLD = 15
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = float(os.getenv("FROTH_K_ATR", 1.1))
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)
VOL_WIN = 60
VOL_Q = 0.60
VOL_MA_RATIO_MIN = 1.05
VOL_Z_MIN = 1.0
FF_BODY_MIN = 0.45
FF_UPWICK_MAX = 0.35
FF_DNWICK_MAX = 0.35
FF_BB_MIN = 0.20
OBV_SLOPE_WIN = 3
NTX_PERIOD = 14
NTX_K_EFF = 10
NTX_VOL_WIN = 60
NTX_THR_LO, NTX_THR_HI = 44.0, 54.0
NTX_ATRZ_LO, NTX_ATRZ_HI = -1.0, 1.5
NTX_MIN_FOR_HYBRID = 44.0
NTX_RISE_K_STRICT = 5
NTX_RISE_MIN_NET = 1.0
NTX_RISE_POS_RATIO = 0.6
NTX_RISE_EPS = 0.05
NTX_RISE_K_HYBRID = 3
NTX_FROTH_K = 1.0
EMA_FAST = 10
EMA_MID = 30
EMA_SLOW = 90
ADX_SOFT = 21
MIN_BARS = 80
NEW_SYMBOL_COOLDOWN_MIN = 180
ADX_RISE_K = 5
ADX_RISE_MIN_NET = 1.0
ADX_RISE_POS_RATIO = 0.6
ADX_RISE_EPS = 0.0
ADX_RISE_USE_HYBRID = True
# --- RSI-EMA divergence ayarları ---
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 0.5
DIVERGENCE_LOOKBACK = 40
DIVERGENCE_MIN_DISTANCE = 5
# --- Gate skor ağırlıkları (trend↓, yön↑) ---
GATE_WEIGHTS = dict(
    TREND=float(os.getenv("W_TREND", 0.40)),  # 0.45 -> 0.40
    DIR=float(os.getenv("W_DIR", 0.40)),      # 0.35 -> 0.40
    NTX=float(os.getenv("W_NTX", 0.20))       # sabit
)
FF_ACTIVE_PROFILE = os.getenv("FF_PROFILE", "normal")
CHOP_TREND_MAX = 38.0
CHOP_RANGE_MIN = 61.0
RSI_BAND_TREND = (46, 54)
RSI_BAND_FLAT = (47, 57)
RSI_BAND_NORM = (45, 55)
FF_PROFILES = {
    "agresif": dict(FF_BODY_MIN=0.38, FF_UPWICK_MAX=0.42, FF_DNWICK_MAX=0.42, FF_BB_MIN=0.15,
                    VOL_MA_RATIO_MIN=1.00, VOL_Z_MIN=0.80, OBV_SLOPE_WIN=3, MIN_DVOL_USD=100_000),
    "normal": dict(FF_BODY_MIN=0.45, FF_UPWICK_MAX=0.35, FF_DNWICK_MAX=0.35, FF_BB_MIN=0.20,
                   VOL_MA_RATIO_MIN=1.05, VOL_Z_MIN=1.00, OBV_SLOPE_WIN=3, MIN_DVOL_USD=150_000),
    "garantici": dict(FF_BODY_MIN=0.52, FF_UPWICK_MAX=0.28, FF_DNWICK_MAX=0.28, FF_BB_MIN=0.30,
                      VOL_MA_RATIO_MIN=1.10, VOL_Z_MIN=1.20, OBV_SLOPE_WIN=4, MIN_DVOL_USD=250_000)
}

THR_TREND = float(os.getenv("THR_TREND", 0.55))
THR_NORM = float(os.getenv("THR_NORM", 0.61))
THR_FLAT = float(os.getenv("THR_FLAT", 0.67))

# ================== Logging ==================
logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    file_handler = RotatingFileHandler('bot.log', maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== Borsa & Bot ==================
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
    'timeout': 60000
})
MARKETS = {}
new_symbol_until = {}
_stats_lock = asyncio.Lock()
scan_status = {}
crit_false_counts = Counter()
crit_total_counts = Counter()

async def load_markets():
    global MARKETS
    MARKETS = await asyncio.to_thread(exchange.load_markets)

def configure_exchange_session(exchange, pool=50):
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool,
        pool_maxsize=pool,
        max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    )
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    exchange.session = s

configure_exchange_session(exchange, pool=50)
telegram_bot = telegram.Bot(
    token=BOT_TOKEN,
    request=telegram.request.HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
)

# ================== Global State ==================
signal_cache = {}
message_queue = asyncio.Queue(maxsize=1000)
_fetch_sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
_rate_lock = asyncio.Lock()
_last_call_ts = 0.0
STATE_FILE = 'positions.json'
DT_KEYS = {"last_signal_time", "entry_time", "last_bar_time", "last_regime_bar"}

def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)

def _parse_dt(val):
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except Exception:
            return val
    return val

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            for k, v in data.items():
                if isinstance(v, dict):
                    for dk in DT_KEYS:
                        if dk in v:
                            v[dk] = _parse_dt(v[dk])
            return data
        except Exception as e:
            logger.warning(f"State yüklenemedi: {e}")
            return {}
    return {}

def save_state():
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(signal_cache, f, default=_json_default)
        os.replace(tmp, STATE_FILE)  # atomic on POSIX & modern Windows
    except Exception as e:
        logger.warning(f"State kaydedilemedi: {e}")

signal_cache = load_state()

# ================== Util ==================
def timeframe_to_minutes(tf: str) -> int:
    tf = tf.lower().strip()
    if tf.endswith('h'): return int(float(tf[:-1]) * 60)
    if tf.endswith('m'): return int(float(tf[:-1]))
    if tf.endswith('d'): return int(float(tf[:-1]) * 1440)
    return 240  # default 4h

def cooldown_for_new_symbol(tf: str) -> int:
    m = timeframe_to_minutes(tf)
    # 30m -> 60 dk, 1h -> 120 dk, 4h -> 180 dk, 1d -> 360 dk gibi basit ölçek
    return int(clamp(m * 2, 60, 360))

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def pct_rank(series: pd.Series, value: float) -> float:
    s = series.dropna()
    if not np.isfinite(value) or s.empty:
        return 0.0
    return float((s < value).mean())

def rolling_z(series: pd.Series, win: int) -> float:
    s = series.tail(win).astype(float)
    if s.size < 5 or s.std(ddof=0) == 0 or not np.isfinite(s.iloc[-1]):
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-12))

def fmt_sym(symbol, x):
    try:
        prec = MARKETS.get(symbol, {}).get('precision', {}).get('price', 5)
        return f"{float(x):.{prec}f}"
    except Exception:
        return str(x)

def bars_since(mask: pd.Series, idx: int = -2) -> int:
    s = mask.iloc[: idx + 1]
    rev = s.values[::-1]
    return int(np.argmax(rev)) if rev.any() else len(rev)

def get_regime(df: pd.DataFrame, use_adx=True, adx_th=ADX_SOFT) -> str:
    e30, e90, adx_last = df['ema30'].iloc[-2], df['ema90'].iloc[-2], df['adx'].iloc[-2]
    if use_adx and not pd.isna(adx_last) and adx_last < adx_th:
        return "neutral"
    if e30 > e90: return "bull"
    if e30 < e90: return "bear"
    return "neutral"

def fmt_date(ts, tz_str='Europe/Istanbul'):
    tz = pytz.timezone(tz_str)
    if isinstance(ts, pd.Timestamp):
        ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts
        ts = ts.astimezone(tz)
    elif isinstance(ts, datetime):
        ts = ts if ts.tzinfo else tz.localize(ts)
        ts = ts.astimezone(tz)
    return ts.strftime('%d.%m.%Y')

def retest_meta_human(df, meta, tz_str='Europe/Istanbul'):
    t = lambda i: fmt_date(df.index[i], tz_str)
    return f"Retest zamanları → touch: {t(meta['touch_idx'])}, reclaim: {t(meta['reclaim_idx'])}, confirm: {t(meta['confirm_idx'])}"

async def mark_status(symbol: str, code: str, detail: str = ""):
    async with _stats_lock:
        scan_status[symbol] = {'code': code, 'detail': detail}

async def record_crit_batch(items):
    async with _stats_lock:
        for name, passed in items:
            crit_total_counts[name] += 1
            if not passed:
                crit_false_counts[name] += 1

# --- RSI-EMA divergence yardımcıları ---
def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi, dtype=np.float64)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    alpha = 2 / (ema_length + 1)
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * alpha) + (ema[i-1] * (1 - alpha))
    return ema

def find_local_extrema(arr, order=3):
    highs, lows = [], []
    for i in range(order, len(arr) - order):
        left = arr[i-order:i]
        right = arr[i+1:i+order+1]
        if arr[i] > np.max(np.concatenate((left, right))):
            highs.append(i)
        if arr[i] < np.min(np.concatenate((left, right))):
            lows.append(i)
    return np.array(highs), np.array(lows)

def detect_rsiema_divergence_core(price_slice: np.ndarray,
                                  rsiema_slice: np.ndarray,
                                  min_distance: int = DIVERGENCE_MIN_DISTANCE):
    """Küçük koddaki koşullar birebir:
       bullish: price LL & rsiema HL & rsiema[last_low] < RSI_LOW
       bearish: price HH & rsiema LH & rsiema[last_high] > RSI_HIGH
    """
    price_highs, price_lows = find_local_extrema(price_slice, order=3)
    bullish = False
    bearish = False

    if len(price_lows) >= 2:
        last_low = price_lows[-1]
        prev_low = price_lows[-2]
        if (last_low - prev_low) >= min_distance:
            if price_slice[last_low] < price_slice[prev_low] and \
               rsiema_slice[last_low] > (rsiema_slice[prev_low] + EMA_THRESHOLD) and \
               rsiema_slice[last_low] < RSI_LOW:
                bullish = True

    if len(price_highs) >= 2:
        last_high = price_highs[-1]
        prev_high = price_highs[-2]
        if (last_high - prev_high) >= min_distance:
            if price_slice[last_high] > price_slice[prev_high] and \
               rsiema_slice[last_high] < (rsiema_slice[prev_high] - EMA_THRESHOLD) and \
               rsiema_slice[last_high] > RSI_HIGH:
                bearish = True

    return bullish, bearish

# --- Dip–Tepe 4H v2.0 ---
def dip_tepe_signals(
    df: pd.DataFrame,
    atr_len: int = 14,
    kATR: float = 1.25,
    minSepBars: int = 8,
    brkLen: int = 4,
    brkBufferATR: float = 0.12,
    seqWinBars: int = 3,
    bodyMinFrac: float = 0.30
) -> tuple[bool, bool]:
    """Pine 'Dip–Tepe 4H v2.0' ile uyumlu: majorBottomSignal, majorTopSignal (son kapalı bar için)"""
    if len(df) < max(atr_len, brkLen) + 5:
        return False, False

    # Türevler
    atr = df['atr'] if 'atr' in df.columns else ensure_atr(df.copy(), period=atr_len)['atr']
    rng = (df['high'] - df['low']).astype(float).clip(lower=1e-10)
    body = (df['close'] - df['open']).abs().astype(float)
    bodyFrac = (body / rng)

    # State
    dir_ = None
    extremePx = np.nan
    extremeBar = None
    lastSwingBar = None

    majorTop = np.zeros(len(df), dtype=bool)
    majorBot = np.zeros(len(df), dtype=bool)

    # init
    if np.isnan(extremePx):
        dir_ = +1 if df['close'].iloc[1] >= df['close'].iloc[0] else -1
        extremePx = df['high'].iloc[1] if dir_ == +1 else df['low'].iloc[1]
        extremeBar = 1

    # yürüt
    for i in range(2, len(df)):
        h, l, c, o = float(df['high'].iloc[i]), float(df['low'].iloc[i]), float(df['close'].iloc[i]), float(df['open'].iloc[i])
        # extreme canlı güncelle
        if dir_ == +1 and h > extremePx:
            extremePx, extremeBar = h, i
        if dir_ == -1 and l < extremePx:
            extremePx, extremeBar = l, i

        # Mod A (ATR dönüş + gövde filtresi)
        bearRevA_raw = (dir_ == +1) and (c <= (extremePx - kATR * float(atr.iloc[i])))
        bullRevA_raw = (dir_ == -1) and (c >= (extremePx + kATR * float(atr.iloc[i])))
        bearRevA = bearRevA_raw and (c < o) and (float(bodyFrac.iloc[i]) >= bodyMinFrac)
        bullRevA = bullRevA_raw and (c > o) and (float(bodyFrac.iloc[i]) >= bodyMinFrac)

        # Mod B (mevcut bar hariç) → i'yi dışlayarak bak
        if i - 1 >= 1 + brkLen:
            lowestPrev  = np.min(df['low'].iloc[i-1-brkLen+1:i].values)
            highestPrev = np.max(df['high'].iloc[i-1-brkLen+1:i].values)
        else:
            lowestPrev = np.min(df['low'].iloc[:i].values)
            highestPrev = np.max(df['high'].iloc[:i].values)

        downBuf = lowestPrev  - brkBufferATR * float(atr.iloc[i])
        upBuf   = highestPrev + brkBufferATR * float(atr.iloc[i])
        strDown = c <= downBuf
        strUp   = c >= upBuf

        bearA, bearB = bearRevA, strDown
        bullA, bullB = bullRevA, strUp

        # barssince taklidi
        def bars_since(mask_series_end_values: list[bool], win: int) -> int:
            # sondan geriye maske içinde ilk True'a kadar
            for j in range(1, win+2):
                if j <= len(mask_series_end_values) and mask_series_end_values[-j]:
                    return j-1
            return 10_000

        # seqWinBars penceresinde A→B veya B→A
        # basitçe: i içindeki durum & son win kadar önceki karşı olay olmuş mu
        bs_bearA = bars_since([bearA], seqWinBars)
        bs_bearB = bars_since([bearB], seqWinBars)
        bs_bullA = bars_since([bullA], seqWinBars)
        bs_bullB = bars_since([bullB], seqWinBars)

        bearAB_seq = (bearA and (bs_bearB <= seqWinBars)) or (bearB and (bs_bearA <= seqWinBars))
        bullAB_seq = (bullA and (bs_bullB <= seqWinBars)) or (bullB and (bs_bullA <= seqWinBars))

        bearFinal = bearAB_seq
        bullFinal = bullAB_seq

        enoughSep = (lastSwingBar is None) or ((i - (lastSwingBar or 0)) >= minSepBars)
        majorTopSignal = bool(bearFinal and enoughSep)
        majorBottomSignal = bool(bullFinal and enoughSep)

        if majorTopSignal:
            lastSwingBar = i
            dir_ = -1
            extremePx = l
            extremeBar = i
            majorTop[i] = True
        elif majorBottomSignal:
            lastSwingBar = i
            dir_ = +1
            extremePx = h
            extremeBar = i
            majorBot[i] = True

    # son kapalı bar (i = len-2)
    return bool(majorBot[-2]), bool(majorTop[-2])

# ================== Mesaj Kuyruğu ==================
async def enqueue_message(text: str):
    try:
        message_queue.put_nowait(text)
    except asyncio.QueueFull:
        logger.warning("Mesaj kuyruğu dolu, mesaj düşürüldü.")

async def message_sender():
    while True:
        message = await message_queue.get()
        try:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            await asyncio.sleep(1)
        except (telegram.error.RetryAfter, telegram.error.TimedOut) as e:
            wait_time = getattr(e, 'retry_after', 5) + 2
            logger.warning(f"Telegram: RetryAfter, {wait_time-2}s bekle")
            await asyncio.sleep(wait_time)
            await enqueue_message(message)
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {str(e)}")
        message_queue.task_done()

# ================== Rate-limit Dostu Fetch ==================
async def fetch_ohlcv_async(symbol, timeframe, limit):
    global _last_call_ts
    for attempt in range(4):
        try:
            async with _fetch_sem:
                async with _rate_lock:
                    now = asyncio.get_event_loop().time()
                    wait = max(0.0, (_last_call_ts + RATE_LIMIT_MS/1000.0) - now)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    _last_call_ts = asyncio.get_event_loop().time()
                return await asyncio.to_thread(exchange.fetch_ohlcv, symbol, timeframe, None, limit)
        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
            backoff = (2 ** attempt) * 1.5 * (0.85 + 0.30 * random.random())
            logger.warning(f"Rate limit {symbol} {timeframe}, backoff {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            backoff = 1.0 + attempt
            logger.warning(f"Network/Timeout {symbol} {timeframe}, retry in {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
    raise ccxt.NetworkError(f"fetch_ohlcv failed after retries: {symbol} {timeframe}")

# ================== Sembol Keşfi ==================
async def discover_bybit_symbols(linear_only=True, quote_whitelist=("USDT",)):
    markets = await asyncio.to_thread(exchange.load_markets)
    syms = []
    for s, m in markets.items():
        if not m.get('active', True): continue
        if not m.get('swap', False): continue
        if linear_only and not m.get('linear', False): continue
        if m.get('quote') not in quote_whitelist: continue
        syms.append(s)
    syms = sorted(set(syms))
    logger.info(f"Keşfedilen sembol sayısı: {len(syms)} (linear={linear_only}, quotes={quote_whitelist})")
    return syms

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros_like(closes, dtype=np.float64)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = (closes[i] * k) + (ema[i-1] * (1 - k))
    return ema

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return np.zeros(len(closes), dtype=np.float64)
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = (up / down) if down != 0 else (float('inf') if up > 0 else 0)
    rsi = np.zeros_like(closes, dtype=np.float64)
    rsi[:period] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    for i in range(period, len(closes)):
        delta = deltas[i-1]
        upval = max(delta, 0.)
        downval = max(-delta, 0.)
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = (up / down) if down != 0 else (float('inf') if up > 0 else 0)
        rsi[i] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    return rsi

def choppiness_index(df: pd.DataFrame, n=14, smooth=3):
    tr1 = (df['high'] - df['low']).astype(float)
    tr2 = (df['high'] - df['close'].shift()).abs().astype(float)
    tr3 = (df['low'] - df['close'].shift()).abs().astype(float)
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_n = tr.rolling(n).sum().replace(0, np.nan)
    hh = df['high'].rolling(n).max()
    ll = df['low'].rolling(n).min()
    rng = (hh - ll).replace(0, np.nan)
    chop_raw = 100 * np.log10(atr_n / rng) / np.log10(n)
    df['chop'] = chop_raw.ewm(alpha=1.0/smooth, adjust=False).mean().bfill().ffill()
    return df

def calculate_adx(df, symbol, period=ADX_PERIOD):
    df['high_diff'] = df['high'] - df['high'].shift(1)
    df['low_diff'] = df['low'].shift(1) - df['low']
    df['+DM'] = np.where((df['high_diff'] > df['low_diff']) & (df['high_diff'] > 0), df['high_diff'], 0)
    df['-DM'] = np.where((df['low_diff'] > df['high_diff']) & (df['low_diff'] > 0), df['low_diff'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = np.maximum(high_low, np.maximum(high_close, low_close))
    alpha = 1.0 / period
    tr_ema = df['TR'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    df['di_plus'] = 100 * (df['+DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    df['di_minus'] = 100 * (df['-DM'].ewm(alpha=alpha, adjust=False).mean() / tr_ema.replace(0, np.nan)).fillna(0)
    denom = (df['di_plus'] + df['di_minus']).clip(lower=1e-9)
    df['DX'] = (100 * (df['di_plus'] - df['di_minus']).abs() / denom).fillna(0)
    df['adx'] = df['DX'].ewm(alpha=alpha, adjust=False).mean().fillna(0)
    adx_condition = df['adx'].iloc[-2] >= ADX_THRESHOLD if pd.notna(df['adx'].iloc[-2]) else False
    di_condition_long = df['di_plus'].iloc[-2] > df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    di_condition_short = df['di_plus'].iloc[-2] < df['di_minus'].iloc[-2] if pd.notna(df['di_plus'].iloc[-2]) and pd.notna(df['di_minus'].iloc[-2]) else False
    if VERBOSE_LOG:
        logger.info(f"ADX calculated: {df['adx'].iloc[-2]:.2f} for {symbol} at {df.index[-2]}")
    return df, adx_condition, di_condition_long, di_condition_short

def calculate_bb(df, period=20, mult=2.0):
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['bb_mid'] + mult * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - mult * df['bb_std']
    return df

def calc_sqzmom_lb(df: pd.DataFrame, length=20, mult_bb=2.0, lengthKC=20, multKC=1.5, use_true_range=True):
    src = df['close'].astype(float)
    basis = src.rolling(length).mean()
    dev = src.rolling(length).std(ddof=0) * mult_bb
    upperBB = basis + dev
    lowerBB = basis - dev
    ma = src.rolling(lengthKC).mean()
    if use_true_range:
        tr1 = (df['high'] - df['low']).astype(float)
        tr2 = (df['high'] - df['close'].shift()).abs().astype(float)
        tr3 = (df['low'] - df['close'].shift()).abs().astype(float)
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    else:
        tr = (df['high'] - df['low']).astype(float)
    rangema = tr.rolling(lengthKC).mean()
    upperKC = ma + rangema * multKC
    lowerKC = ma - rangema * multKC
    sqz_on = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqz_off = (lowerBB < lowerKC) & (upperBB > upperKC)
    no_sqz = (~sqz_on) & (~sqz_off)
    highest = df['high'].rolling(lengthKC).max()
    lowest = df['low'].rolling(lengthKC).min()
    mid1 = (highest + lowest) / 2.0
    mid2 = src.rolling(lengthKC).mean()
    center = (mid1 + mid2) / 2.0
    series = (src - center)
    val = pd.Series(index=df.index, dtype='float64')
    for i in range(lengthKC-1, len(series)):
        y = series.iloc[i-lengthKC+1:i+1].values
        x = np.arange(lengthKC, dtype=float)
        if np.isfinite(y).sum() >= 2:
            m, b = np.polyfit(x, y, 1)
            val.iloc[i] = m*(lengthKC-1) + b
        else:
            val.iloc[i] = np.nan
    df['lb_sqz_val'] = val
    df['lb_sqz_on'] = sqz_on
    df['lb_sqz_off'] = sqz_off
    df['lb_sqz_no'] = no_sqz
    val_prev = df['lb_sqz_val'].shift(1)
    df['lb_open_green'] = (df['lb_sqz_val'] > 0) & (df['lb_sqz_val'] > val_prev)
    df['lb_open_red'] = (df['lb_sqz_val'] < 0) & (df['lb_sqz_val'] < val_prev)
    return df

def ensure_atr(df, period=14):
    if 'atr' in df.columns:
        return df
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=period).mean()
    return df

def calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60):
    close = df['close'].values
    vol = df['volume'].values
    obv = np.zeros_like(close, dtype=float)
    for i in range(1, len(close)):
        if close[i] > close[i-1]:
            obv[i] = obv[i-1] + vol[i]
        elif close[i] < close[i-1]:
            obv[i] = obv[i-1] - vol[i]
        else:
            obv[i] = obv[i-1]
    df['obv'] = obv
    df['vol_ma'] = pd.Series(vol, index=df.index, dtype="float64").rolling(vol_ma_window).mean()
    vol_s = pd.Series(vol, index=df.index, dtype="float64")
    df['vol_med'] = vol_s.rolling(spike_window).median()
    df['vol_mad'] = vol_s.rolling(spike_window).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True
    )
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan)
    df['vol_z'] = (vol_s - df['vol_med']) / denom
    return df

def get_atr_values(df, lookback_atr=LOOKBACK_ATR):
    df = ensure_atr(df, period=14)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1].dropna()
    close_last = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
    if atr_series.empty or not np.isfinite(close_last) or close_last == 0:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2]) if pd.notna(df['atr'].iloc[-2]) else np.nan
    avg_atr_ratio = float(atr_series.mean() / close_last)
    return atr_value, avg_atr_ratio

def calculate_indicators(df, symbol, timeframe):
    if len(df) < MIN_BARS:
        logger.info(f"{symbol}: Yetersiz veri ({len(df)} mum), skip.")
        return None, None, None, None
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)
    closes = df['close'].values.astype(np.float64)
    df['ema10'] = calculate_ema(closes, EMA_FAST)
    df['ema30'] = calculate_ema(closes, EMA_MID)
    df['ema90'] = calculate_ema(closes, EMA_SLOW)
    df = calculate_bb(df)
    df = calc_sqzmom_lb(df, length=20, mult_bb=2.0, lengthKC=20, multKC=1.5, use_true_range=True)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    df = calc_ntx(df, period=NTX_PERIOD, k_eff=NTX_K_EFF)
    df['rsi'] = pd.Series(calculate_rsi(df['close'].values.astype(np.float64), period=14), index=df.index)
    df['rsi_ema'] = pd.Series(
        calculate_rsi_ema(df['rsi'].values.astype(np.float64), ema_length=14),
        index=df.index
    )
    df = choppiness_index(df, n=14, smooth=3)
    return df, adx_condition, di_condition_long, di_condition_short

# --- EMA eğimi (birleştirilmiş) ---
def ema_slope_ok(df: pd.DataFrame, side: str, k: int = NTX_K_EFF, min_norm: float = 0.08, use_atr_norm: bool = True) -> bool:
    if len(df) < k + 3:
        return False
    e10_now = float(df['ema10'].iloc[-2])
    e10_k = float(df['ema10'].iloc[-2-k])
    if not (np.isfinite(e10_now) and np.isfinite(e10_k)):
        return False
    if not use_atr_norm:
        ok = (e10_now > e10_k) if side == "long" else (e10_now < e10_k)
        return ok
    atr = float(df['atr'].iloc[-2])
    if not np.isfinite(atr) or atr <= 0:
        return False
    slope_norm = (e10_now - e10_k) / (atr * max(1, k))
    return slope_norm >= min_norm if side == "long" else slope_norm <= -min_norm

# --- 2 bar teyit (flip-flop'ı azalt) ---
def two_bar_confirm(df: pd.DataFrame, side: str) -> bool:
    try:
        c0, c1 = float(df['close'].iloc[-2]), float(df['close'].iloc[-3])
        e10_0, e10_1 = float(df['ema10'].iloc[-2]), float(df['ema10'].iloc[-3])
    except Exception:
        return False
    if side == "long":
        return (c0 > e10_0) and (c1 > e10_1)
    else:
        return (c0 < e10_0) and (c1 < e10_1)

# --- Retest için sığ penetrasyon ve fitil yönü (touch barı için) ---
def retest_micro_checks_row(row, e30_val, side, atr_val, max_pen_k=0.25):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    if side == "long":
        pen = max(0.0, e30_val - l)
        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)
        wick_dir_ok = lower_wick >= upper_wick * 0.8
        return (pen <= max_pen_k * atr_val) and wick_dir_ok
    else:
        pen = max(0.0, h - e30_val)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        wick_dir_ok = upper_wick >= lower_wick * 0.8
        return (pen <= max_pen_k * atr_val) and wick_dir_ok

# --- Yeni Yardımcı Fonksiyonlar ---
def retest_chain_ok(
    df: pd.DataFrame,
    side: str,
    touch_win: int = 6,
    reclaim_win: int = 6,
    cross_idx: int | None = None,
    confirm_two_bars: bool = True,
    use_shallow_pen: bool = False  # senin tercihine göre kapalı
) -> (bool, dict):
    """
    Şartlar (LONG):
      1) cross_idx sonrası, t - cross_idx 8..RETEST_MAX_K_ADAPT aralığında
      2) TOUCH: low <= ema30 <= high (cross'tan sonra)
      3) RECLAIM: TOUCH'tan sonra ilk bar: yeşil & close>ema10
      4) CONFIRM: RECLAIM'ten sonra ardışık 2 bar yeşil & close>ema10
         (confirm fiyatı reclaim fiyatından büyük: c_now > c_reclaim)
    SHORT için tam tersi.
    """
    t = len(df) - 2
    if t < 10 or cross_idx is None:
        return False, {"reason": "no_cross_or_short_data"}

    close = df['close'].astype(float)
    open_ = df['open'].astype(float)
    high  = df['high'].astype(float)
    low   = df['low'].astype(float)
    e10   = df['ema10'].astype(float)
    e30   = df['ema30'].astype(float)

    # -- 1) pencere: cross sonrasında olmalı
    if not (cross_idx < t):
        return False, {"reason": "confirm_before_cross"}
    k_since = t - cross_idx

    # Üst sınırı check_signals'ta zaten tuttuk; yine de savunmacı olalım
    if k_since < 8:
        return False, {"reason": "too_early"}

    # -- 4) CONFIRM: son 2 barın ikisi de yeşil (short: kırmızı) ve ema10 üstü/altı
    c0, o0 = close.iloc[t], open_.iloc[t]
    c1, o1 = close.iloc[t-1], open_.iloc[t-1]
    e10_0, e10_1 = e10.iloc[t], e10.iloc[t-1]
    e30_0, e30_1 = e30.iloc[t], e30.iloc[t-1]

    if side == "long":
        confirm_ok = (
            (c1 > o1 and c1 > e10_1 and c1 > e30_1) and
            (c0 > o0 and c0 > e10_0 and c0 > e30_0)
        ) if confirm_two_bars else (c0 > o0 and c0 > e10_0 and c0 > e30_0)
    else:
        confirm_ok = (
            (c1 < o1 and c1 < e10_1 and c1 < e30_1) and
            (c0 < o0 and c0 < e10_0 and c0 < e30_0)
        ) if confirm_two_bars else (c0 < o0 and c0 < e10_0 and c0 < e30_0)
    if not confirm_ok:
        return False, {"reason": "confirm_fail"}

    # -- 3) RECLAIM: confirm'den bir bar önce
    r = t-1
    c_r, o_r, e10_r, e30_r = close.iloc[r], open_.iloc[r], e10.iloc[r], e30.iloc[r]
    if side == "long":
        if not (c_r > o_r and c_r > e10_r):
            return False, {"reason": "reclaim_fail"}
    else:
        if not (c_r < o_r and c_r < e10_r):
            return False, {"reason": "reclaim_fail"}

    # -- 2) TOUCH: reclaim'den önceki 'touch_win' pencerede ema30'a temas, cross'tan sonra olmalı
    u_lo = max(cross_idx + 1, r - touch_win)
    u_hi = r - 1
    touch_idx = None
    for u in range(u_hi, u_lo - 1, -1):
        e30_u = e30.iloc[u]
        if low.iloc[u] <= e30_u <= high.iloc[u]:
            if use_shallow_pen:
                # (şu an kapalı, ama istenirse açılır)
                atr_u = float(df['atr'].iloc[u]) if 'atr' in df.columns and pd.notna(df['atr'].iloc[u]) else np.nan
                if not np.isfinite(atr_u) or not retest_micro_checks_row(df.iloc[u], e30_u, side, atr_u):
                    continue
            touch_idx = u
            break
    if touch_idx is None:
        return False, {"reason": "no_touch"}

    # -- Güç şartı: confirm fiyatı reclaim'den büyük/küçük (senin istediğin “strict” kural)
    if side == "long" and not (c0 > c_r):
        return False, {"reason": "confirm_not_stronger"}
    if side == "short" and not (c0 < c_r):
        return False, {"reason": "confirm_not_stronger"}

    return True, {"touch_idx": touch_idx, "reclaim_idx": r, "confirm_idx": t, "k_since_cross": int(k_since)}

# ========= ADX Rising =========
def adx_rising_strict(df_adx: pd.Series) -> bool:
    if df_adx is None or len(df_adx) < ADX_RISE_K + 1:
        return False
    window = df_adx.iloc[-(ADX_RISE_K+1):-1].astype(float)
    if window.isna().any():
        return False
    x = np.arange(len(window))
    slope, _ = np.polyfit(x, window.values, 1)
    diffs = np.diff(window.values)
    pos_ratio = (diffs > ADX_RISE_EPS).mean() if diffs.size > 0 else 0.0
    net = window.iloc[-1] - window.iloc[0]
    return (slope > 0) and (net >= ADX_RISE_MIN_NET) and (pos_ratio >= ADX_RISE_POS_RATIO)

def adx_rising_hybrid(df_adx: pd.Series) -> bool:
    if not ADX_RISE_USE_HYBRID:
        return False
    if df_adx is None or len(df_adx) < 4:
        return False
    window = df_adx.iloc[-4:-1].astype(float)
    if window.isna().any():
        return False
    x = np.arange(len(window))
    slope, _ = np.polyfit(x, window.values, 1)
    last_diff = window.values[-1] - window.values[-2]
    return (slope > 0) and (last_diff > ADX_RISE_EPS)

def adx_rising(df: pd.DataFrame) -> bool:
    if 'adx' not in df.columns:
        return False
    return adx_rising_strict(df['adx']) or adx_rising_hybrid(df['adx'])

# ==== NTX ====
def calc_ntx(df: pd.DataFrame, period: int = NTX_PERIOD, k_eff: int = NTX_K_EFF) -> pd.DataFrame:
    close = df['close'].astype(float)
    atr = df['atr'].astype(float).replace(0, np.nan)
    ema10 = df['ema10'].astype(float)
    num = (close - close.shift(k_eff)).abs()
    den = close.diff().abs().rolling(k_eff).sum()
    er = (num / (den + 1e-12)).clip(0, 1).fillna(0)
    slope_norm = (ema10 - ema10.shift(k_eff)) / ((atr * k_eff) + 1e-12)
    slope_mag = slope_norm.abs().clip(0, 3) / 3.0
    slope_mag = slope_mag.fillna(0)
    dif = close.diff()
    sign_price = np.sign(dif)
    sign_slope = np.sign(slope_norm.shift(1)).replace(0, np.nan)
    same_dir = (sign_price == sign_slope).astype(float)
    pos_ratio = same_dir.rolling(k_eff).mean().fillna(0)
    vol_ratio = (df['volume'] / df['vol_ma'].replace(0, np.nan)).clip(lower=0).fillna(0)
    vol_sig = np.tanh(np.maximum(0.0, vol_ratio - 1.0)).fillna(0)
    base = (
        0.35 * er +
        0.35 * slope_mag +
        0.15 * pos_ratio +
        0.15 * vol_sig
    ).clip(0, 1)
    df['ntx_raw'] = base
    df['ntx'] = df['ntx_raw'].ewm(alpha=1.0/period, adjust=False).mean() * 100.0
    return df

def ntx_threshold(atr_z: float) -> float:
    a = clamp((atr_z - NTX_ATRZ_LO) / (NTX_ATRZ_HI - NTX_ATRZ_LO + 1e-12), 0.0, 1.0)
    return NTX_THR_LO + a * (NTX_THR_HI - NTX_THR_LO)

def ntx_rising_strict(s: pd.Series, k: int = NTX_RISE_K_STRICT, min_net: float = NTX_RISE_MIN_NET, pos_ratio_th: float = NTX_RISE_POS_RATIO, eps: float = NTX_RISE_EPS) -> bool:
    if s is None or len(s) < k + 1: return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any(): return False
    x = np.arange(len(w)); slope, _ = np.polyfit(x, w.values, 1)
    diffs = np.diff(w.values)
    posr = (diffs > eps).mean() if diffs.size else 0.0
    net = w.iloc[-1] - w.iloc[0]
    return (slope > 0) and (net >= min_net) and (posr >= pos_ratio_th)

def ntx_rising_hybrid_guarded(df: pd.DataFrame, side: str, eps: float = NTX_RISE_EPS, min_ntx: float = NTX_MIN_FOR_HYBRID, k: int = NTX_RISE_K_HYBRID, froth_k: float = NTX_FROTH_K) -> bool:
    s = df['ntx'] if 'ntx' in df.columns else None
    if s is None or len(s) < k + 1: return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any(): return False
    x = np.arange(len(w)); slope, _ = np.polyfit(x, w.values, 1)
    last_diff = w.values[-1] - w.values[-2]
    if not (slope > 0 and last_diff > eps): return False
    if w.iloc[-1] < min_ntx: return False
    close_last = float(df['close'].iloc[-2])
    ema10_last = float(df['ema10'].iloc[-2])
    atr_value = float(df['atr'].iloc[-2])
    if not (np.isfinite(close_last) and np.isfinite(ema10_last) and np.isfinite(atr_value) and atr_value > 0):
        return False
    if abs(close_last - ema10_last) > froth_k * atr_value:
        return False
    return True

def ntx_vote(df: pd.DataFrame, ntx_thr: float) -> bool:
    if 'ntx' not in df.columns or 'ema10' not in df.columns:
        return False
    ntx_last = float(df['ntx'].iloc[-2]) if pd.notna(df['ntx'].iloc[-2]) else np.nan
    level_ok = (np.isfinite(ntx_last) and ntx_last >= ntx_thr)
    mom_ok = ntx_rising_strict(df['ntx']) or ntx_rising_hybrid_guarded(df, side="long") or ntx_rising_hybrid_guarded(df, side="short")
    k = NTX_K_EFF
    if len(df) >= k + 3 and pd.notna(df['ema10'].iloc[-2]) and pd.notna(df['ema10'].iloc[-2-k]):
        slope_ok = (df['ema10'].iloc[-2] > df['ema10'].iloc[-2-k])
    else:
        slope_ok = False
    votes = int(level_ok) + int(mom_ok) + int(slope_ok)
    return votes >= 2

def compute_retest_cap(adx_last: float, atr_z: float) -> int:
    adx_norm = clamp(((adx_last or 0) - 21.0) / 15.0, 0.0, 1.0)
    atr_norm = clamp(((atr_z or 0) - (-1.0)) / (1.5 - (-1.0)), 0.0, 1.0)
    rmax = 12 + 12 * (1 - adx_norm) + 8 * atr_norm
    return int(round(clamp(rmax, 12, 32)))

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
        await mark_status(symbol, "started")
        now = datetime.now(tz)
        until = new_symbol_until.get(symbol)
        if until and now < until:
            await mark_status(symbol, "cooldown", "new_symbol_cooldown")
            return
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            limit_need = max(150, LOOKBACK_ATR + 80, ADX_PERIOD + 40)
            ohlcv = await fetch_ohlcv_async(symbol, timeframe, limit=limit_need)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or df.empty or len(df) < MIN_BARS:
                new_symbol_until[symbol] = now + timedelta(minutes=cooldown_for_new_symbol(timeframe))
                await mark_status(symbol, "min_bars", f"bars={len(df) if df is not None else 0}")
                logger.info(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), cooldown.")
                return
        calc = calculate_indicators(df, symbol, timeframe)
        if not calc or calc[0] is None:
            await mark_status(symbol, "skip", "indicators_failed")
            return
        df, adx_condition, di_condition_long, di_condition_short = calc
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            await mark_status(symbol, "skip", "invalid_atr")
            logger.warning(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
        _adx = float(adx_last) if np.isfinite(adx_last) else 0.0
        _atrz = float(atr_z) if np.isfinite(atr_z) else 0.0
        RETEST_MAX_K_ADAPT = compute_retest_cap(_adx, _atrz)
        retest_limit_long = retest_limit_short = RETEST_MAX_K_ADAPT
        if VERBOSE_LOG:
            logger.info(
                f"{symbol} {timeframe} ADAPT k: retest≤{retest_limit_long}, "
                f"adx={_adx:.1f} adx_n={clamp((_adx - 21.0) / 15.0, 0.0, 1.0):.2f} "
                f"atr_z={_atrz:.2f} atr_n={clamp((_atrz - (-1.0)) / (1.5 - (-1.0)), 0.0, 1.0):.2f}"
            )
        # --- 30/90 kesişmeleri
        e10, e30, e90 = df['ema10'], df['ema30'], df['ema90']
        cross_up_3090 = (e30.shift(1) <= e90.shift(1)) & (e30 > e90)
        cross_dn_3090 = (e30.shift(1) >= e90.shift(1)) & (e30 < e90)

        def _last_true_idx(mask: pd.Series):
            arr = mask.values
            idxs = np.flatnonzero(arr)
            return int(idxs[-1]) if idxs.size else None

        last_up_idx  = _last_true_idx(cross_up_3090)
        last_dn_idx  = _last_true_idx(cross_dn_3090)
        t = len(df) - 2  # kapalı bar

        in_window_long  = last_up_idx is not None and (8 <= (t - last_up_idx) <= RETEST_MAX_K_ADAPT)
        in_window_short = last_dn_idx is not None and (8 <= (t - last_dn_idx) <= RETEST_MAX_K_ADAPT)

        # Retest zinciri
        retest_ok_L, retL_meta = False, {"reason": "no_cross"}
        retest_ok_S, retS_meta = False, {"reason": "no_cross"}
        if in_window_long:
            retest_ok_L, retL_meta = retest_chain_ok(
                df, side="long", touch_win=6, reclaim_win=6,
                cross_idx=last_up_idx, confirm_two_bars=True, use_shallow_pen=False
            )
        if in_window_short:
            retest_ok_S, retS_meta = retest_chain_ok(
                df, side="short", touch_win=6, reclaim_win=6,
                cross_idx=last_dn_idx, confirm_two_bars=True, use_shallow_pen=False
            )

        # LazyBear Squeeze Momentum
        smi_open_green = bool(df['lb_open_green'].iloc[-2]) if 'lb_open_green' in df.columns and pd.notna(df['lb_open_green'].iloc[-2]) else False
        smi_open_red   = bool(df['lb_open_red'].iloc[-2])   if 'lb_open_red'   in df.columns and pd.notna(df['lb_open_red'].iloc[-2])   else False

        # Dip–Tepe sinyalleri
        dt_bottom, dt_top = dip_tepe_signals(
            df,
            atr_len=14, kATR=1.25, minSepBars=8,
            brkLen=4, brkBufferATR=0.12, seqWinBars=3, bodyMinFrac=0.30
        )

        # Diverjans
        lookback_div = DIVERGENCE_LOOKBACK
        if len(df) < lookback_div + 5:
            div_bullish = div_bearish = False
        else:
            price_slice = df['close'].astype(float).values[-lookback_div-1:-1]
            rsiema_slice = df['rsi_ema'].astype(float).values[-lookback_div-1:-1]
            div_bullish, div_bearish = detect_rsiema_divergence_core(
                price_slice, rsiema_slice, min_distance=DIVERGENCE_MIN_DISTANCE
            )
        close_last = float(df['close'].iloc[-2])
        e10_last = float(df['ema10'].iloc[-2])
        e30_last = float(df['ema30'].iloc[-2])
        is_green = (pd.notna(close_last) and pd.notna(df['open'].iloc[-2]) and (close_last > df['open'].iloc[-2]))
        is_red = (pd.notna(close_last) and pd.notna(df['open'].iloc[-2]) and (close_last < df['open'].iloc[-2]))
        div_long_ok  = (div_bullish and (close_last > e10_last) and (close_last > e30_last)
                        and smi_open_green and dt_bottom and is_green)
        div_short_ok = (div_bearish and (close_last < e10_last) and (close_last < e30_last)
                        and smi_open_red and dt_top and is_red)

        # Sinyal koşulları
        buy_condition  = (retest_ok_L and smi_open_green and dt_bottom) or div_long_ok
        sell_condition = (retest_ok_S and smi_open_red   and dt_top)    or div_short_ok

        reason = ""
        ret_meta_str = ""
        if buy_condition:
            if retest_ok_L:
                reason = "Retest-Zincir"
                ret_meta_str = retest_meta_human(df, retL_meta)
            else:
                reason = "RSI-EMA Divergence (BULLISH)"
        elif sell_condition:
            if retest_ok_S:
                reason = "Retest-Zincir"
                ret_meta_str = retest_meta_human(df, retS_meta)
            else:
                reason = "RSI-EMA Divergence (BEARISH)"

        criteria = [
            ("retest_chain_L", retest_ok_L),
            ("retest_chain_S", retest_ok_S),
            ("div_long_ok", div_long_ok),
            ("div_short_ok", div_short_ok),
            ("k_window_L", in_window_long),
            ("k_window_S", in_window_short),
            ("smi_green", smi_open_green),
            ("smi_red", smi_open_red),
            ("dt_bottom", dt_bottom),
            ("dt_top", dt_top)
        ]
        await record_crit_batch(criteria)
        if VERBOSE_LOG:
            logger.info(f"{symbol} {timeframe} RETEST_META L:{retL_meta} S:{retS_meta}")
            logger.info(
                f"{symbol} {timeframe} | "
                f"buy:{buy_condition}({ 'retest' if retest_ok_L else 'divergence' if div_long_ok else '-' }) "
                f"| sell:{sell_condition}({ 'retest' if retest_ok_S else 'divergence' if div_short_ok else '-' }) "
                f"| smi_green={smi_open_green} smi_red={smi_open_red} dt_bottom={dt_bottom} dt_top={dt_top}"
            )
        if buy_condition and sell_condition:
            await mark_status(symbol, "skip", "conflicting_signals")
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return
        now = datetime.now(tz)
        bar_time = df.index[-2]
        if not isinstance(bar_time, (pd.Timestamp, datetime)):
            bar_time = pd.to_datetime(bar_time, errors="ignore")
        current_pos = signal_cache.get(f"{symbol}_{timeframe}", {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
            'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
            'regime_dir': None, 'last_regime_bar': None
        })
        e10_prev, e30_prev, e90_prev = df['ema10'].iloc[-3], df['ema30'].iloc[-3], df['ema90'].iloc[-3]
        e10_last, e30_last, e90_last = df['ema10'].iloc[-2], df['ema30'].iloc[-2], df['ema90'].iloc[-2]
        exit_cross_long = (pd.notna(e10_prev) and pd.notna(e30_prev) and pd.notna(e10_last) and pd.notna(e30_last)
                           and (e10_prev >= e30_prev) and (e10_last < e30_last))
        exit_cross_short = (pd.notna(e10_prev) and pd.notna(e30_prev) and pd.notna(e10_last) and pd.notna(e30_last)
                            and (e10_prev <= e30_prev) and (e10_last > e30_last))
        regime_break_long = e30_last < e90_last
        regime_break_short = e30_last > e90_last
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: REVERSAL CLOSE 🔁",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Price: {fmt_sym(symbol, current_price)}",
                    f"P/L: {profit_percent:+.2f}%",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': current_pos['signal'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                current_pos = signal_cache[f"{symbol}_{timeframe}"]
        if buy_condition and current_pos['signal'] != 'buy':
            tf_minutes = timeframe_to_minutes(timeframe)
            cooldown_min = max(COOLDOWN_MINUTES, tf_minutes)
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=cooldown_min) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.info(f"{symbol} {timeframe}: BUY atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price - sl_atr_abs
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    if VERBOSE_LOG:
                        logger.info(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': entry_price,
                        'lowest_price': None, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                    await enqueue_message("\n".join([
                        f"{symbol} {timeframe}: BUY (LONG) 🚀",
                        f"Sebep: {reason}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
                        f"Tarih: {fmt_date(bar_time)}"
                    ]))
                    save_state()
        elif sell_condition and current_pos['signal'] != 'sell':
            tf_minutes = timeframe_to_minutes(timeframe)
            cooldown_min = max(COOLDOWN_MINUTES, tf_minutes)
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=cooldown_min) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.info(f"{symbol} {timeframe}: SELL atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price + sl_atr_abs
                current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    await mark_status(symbol, "skip", "invalid_entry_sl")
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    if VERBOSE_LOG:
                        logger.info(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                    await mark_status(symbol, "skip", "instant_sl_risk")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': None,
                        'lowest_price': entry_price, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[f"{symbol}_{timeframe}"] = current_pos
                    await enqueue_message("\n".join([
                        f"{symbol} {timeframe}: SELL (SHORT) 📉",
                        f"Sebep: {reason}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
                        f"Tarih: {fmt_date(bar_time)}"
                    ]))
                    save_state()
        if current_pos['signal'] == 'buy':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: TP1 Hit 🎯",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])}",
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi.",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                save_state()
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['tp2_hit'] = True
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])}",
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, kalan %40 açık.",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                save_state()
            if exit_cross_long or regime_break_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: EMA EXIT (LONG) 🔁",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Price: {fmt_sym(symbol, current_price)}",
                    f"P/L: {profit_percent:+.2f}%",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: STOP LONG ⛔",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Price: {fmt_sym(symbol, current_price)}",
                    f"P/L: {profit_percent:+.2f}%",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[f"{symbol}_{timeframe}"] = current_pos
        elif current_pos['signal'] == 'sell':
            current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: TP1 Hit 🎯",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])}",
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi.",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                save_state()
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                current_pos['tp2_hit'] = True
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])}",
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, kalan %40 açık.",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                save_state()
            if exit_cross_short or regime_break_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: EMA EXIT (SHORT) 🔁",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Price: {fmt_sym(symbol, current_price)}",
                    f"P/L: {profit_percent:+.2f}%",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] = float(max(0.0, min(1.0, current_pos['remaining_ratio'])))
                await enqueue_message("\n".join([
                    f"{symbol} {timeframe}: STOP SHORT ⛔",
                    f"Bar Zamanı: {fmt_date(bar_time)}",
                    f"Gönderim: {fmt_date(now)}",
                    f"Price: {fmt_sym(symbol, current_price)}",
                    f"P/L: {profit_percent:+.2f}%",
                    f"Kalan: %{current_pos['remaining_ratio']*100:.0f}"
                ]))
                signal_cache[f"{symbol}_{timeframe}"] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[f"{symbol}_{timeframe}"] = current_pos
        await mark_status(symbol, "ok")
    except ccxt.NetworkError as e:
        await mark_status(symbol, "error", f"network:{str(e)[:120]}")
        logger.exception(f"Ağ hatası ({symbol} {timeframe}): {e}, 10s bekle")
        await asyncio.sleep(10)
        return
    except Exception as e:
        await mark_status(symbol, "error", f"exception:{str(e)[:120]}")
        logger.exception(f"Hata ({symbol} {timeframe}): {e}")
        return

# ================== Main ==================
async def main():
    await load_markets()
    tz = pytz.timezone('Europe/Istanbul')
    if STARTUP_MSG_ENABLED:
        try:
            await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı 🟢 " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            logger.warning(f"Açılış mesajı gönderilemedi: {e}")
    asyncio.create_task(message_sender())
    timeframes = ['4h']
    symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
    if not symbols:
        raise RuntimeError("Uygun sembol bulunamadı. Permissions/region?")
    logger.info(f"Toplam sembol: {len(symbols)} | N_SHARDS={N_SHARDS} | BATCH_SIZE={BATCH_SIZE}")
    loop_count = 0
    while True:
        loop_start = time.time()
        async with _stats_lock:
            scan_status.clear()
            crit_false_counts.clear()
            crit_total_counts.clear()
        total_scanned = 0
        for shard_index in range(N_SHARDS):
            shard_symbols = [s for i, s in enumerate(symbols) if (i % N_SHARDS) == shard_index]
            total_scanned += len(shard_symbols)
            logger.info(f"Shard {shard_index+1}/{N_SHARDS} -> {len(shard_symbols)} sembol taranacak")
            tasks = [check_signals(sym, tf) for tf in timeframes for sym in shard_symbols]
            for i in range(0, len(tasks), BATCH_SIZE):
                await asyncio.gather(*tasks[i:i+BATCH_SIZE])
                await asyncio.sleep(INTER_BATCH_SLEEP + random.random()*0.5)
        elapsed = time.time() - loop_start
        sleep_sec = max(0.0, 120.0 - elapsed)
        async with _stats_lock:
            codes = Counter(v['code'] for v in scan_status.values())
            total_symbols = len(symbols)
            missing = [s for s in symbols if s not in scan_status]
            crit_lines = []
            for name, tot in crit_total_counts.items():
                f = crit_false_counts.get(name, 0)
                pct = (f / tot * 100.0) if tot else 0.0
                crit_lines.append((pct, name, f, tot))
            crit_lines.sort(reverse=True)
        logger.info(
            "Coverage: total=%d | ok=%d | cooldown=%d | min_bars=%d | skip=%d | error=%d | missing=%d",
            total_symbols,
            codes.get('ok', 0),
            codes.get('cooldown', 0),
            codes.get('min_bars', 0),
            codes.get('skip', 0),
            codes.get('error', 0),
            len(missing)
        )
        if missing:
            logger.warning("Missing (ilk 15): %s", ", ".join(missing[:15]))
        if crit_lines:
            logger.info("Kriter FALSE dökümü (yüksekten düşüğe):")
            for pct, name, f, tot in crit_lines:
                logger.info(" - %s: %d/%d (%.1f%%)", name, f, tot, pct)
        logger.info(
            "Tur bitti | total=%d | ok=%d | cooldown=%d | min_bars=%d | skip=%d | error=%d | elapsed=%.1fs | bekle=%.1fs",
            total_symbols, codes.get('ok',0), codes.get('cooldown',0), codes.get('min_bars',0),
            codes.get('skip',0), codes.get('error',0), elapsed, sleep_sec
        )
        loop_count += 1
        if HEARTBEAT_ENABLED and (loop_count % 5 == 0):
            await enqueue_message(
                f"Heartbeat: ok={codes.get('ok',0)}/{total_symbols}, "
                f"cooldown={codes.get('cooldown',0)}, "
                f"min_bars={codes.get('min_bars',0)}, "
                f"error={codes.get('error',0)}, "
                f"missing={len(missing)}"
            )
        await asyncio.sleep(sleep_sec)
        save_state()

if __name__ == "__main__":
    asyncio.run(main())
