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
FROTH_GUARD_K_ATR = 1.1
# SIGNAL_MODE = "2of3" # Unused
# REQUIRE_DIRECTION = False # Unused
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
USE_2BAR_LATE = True  # Late girişte two_bar_confirm kullanımı
# Yeni Config (entry_gate ve FakeFilter için)
GATE_WEIGHTS = dict(
    TREND=float(os.getenv("W_TREND", 0.45)),
    DIR=float(os.getenv("W_DIR", 0.35)),
    NTX=float(os.getenv("W_NTX", 0.20))
)
FF_ACTIVE_PROFILE = os.getenv("FF_PROFILE", "normal")  # agresif|normal|garantici
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
        with open(STATE_FILE, 'w') as f:
            json.dump(signal_cache, f, default=_json_default)
    except Exception as e:
        logger.warning(f"State kaydedilemedi: {e}")

signal_cache = load_state()

# ================== Util ==================
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

def fmt_dt(ts, tz_str='Europe/Istanbul', with_tz=True):
    tz = pytz.timezone(tz_str)
    if isinstance(ts, pd.Timestamp):
        ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts
        ts = ts.astimezone(tz)
    elif isinstance(ts, datetime):
        ts = ts.astimezone(tz) if ts.tzinfo else tz.localize(ts)
    else:
        return str(ts)
    return ts.strftime('%Y-%m-%d %H:%M:%S %Z' if with_tz else '%Y-%m-%d %H:%M:%S')

def retest_meta_human(df, meta, tz_str='Europe/Istanbul'):
    t = lambda i: fmt_dt(df.index[i], tz_str)
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
            backoff = (2 ** attempt) * 1.5
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
def retest_chain_ok(df: pd.DataFrame, side: str, touch_win: int = 4, reclaim_win: int = 4, use_shallow_pen: bool = True) -> (bool, dict):
    """
    Confirm = şu anki kapalı mum (t=-2).
    Long:
      1) Touch: E30'a temas (fitil/gövde) — confirm'den ≤4 bar önce
      2) Reclaim: yeşil ve EMA10 üstü kapanış — touch'tan ≤4 bar sonra ve confirm'den önce
      3) Confirm: yeşil, EMA10 üstü ve kapanış > reclaim kapanışı
    Short için tersine.
    """
    t = len(df) - 2
    if t < 10:
        return False, {"reason": "short_data"}
    close = df['close'].astype(float)
    open_ = df['open'].astype(float)
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    e10 = df['ema10'].astype(float)
    e30 = df['ema30'].astype(float)
    atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
    atr_norm = clamp((atr_z - (-1.0)) / (1.5 - (-1.0)), 0.0, 1.0)
    max_pen_k = 0.25 * (1.0 + 0.2 * atr_norm)  # Dynamic max_pen_k: looser in trend, tighter in flat
    c_now, o_now = close.iloc[t], open_.iloc[t]
    e10_now = e10.iloc[t]
    if side == "long":
        # Confirm: yeşil + EMA10 üstü
        if not (np.isfinite(c_now) and np.isfinite(o_now) and c_now > o_now and c_now > e10_now):
            return False, {"reason": "confirm_fail"}
        # Reclaim aralığı: confirm'den geriye 1..4 bar
        r_lo = max(0, t - reclaim_win); r_hi = t - 1
        for r in range(r_hi, r_lo - 1, -1):
            c_r, o_r, e10_r = close.iloc[r], open_.iloc[r], e10.iloc[r]
            if not (c_r > o_r and c_r > e10_r):  # yeşil + EMA10 üstü
                continue
            # Touch: reclaim'den 1..4 bar önce E30'a temas
            u_lo = max(0, r - touch_win); u_hi = r - 1
            for u in range(u_hi, u_lo - 1, -1):
                e30_u = e30.iloc[u]
                if low.iloc[u] <= e30_u <= high.iloc[u]:
                    # u barında kontrol (ATR de u barından)
                    atr_u = float(df['atr'].iloc[u]) if pd.notna(df['atr'].iloc[u]) else float('nan')
                    if use_shallow_pen and np.isfinite(atr_u):
                        if not retest_micro_checks_row(df.iloc[u], e30_u, side, atr_u, max_pen_k=max_pen_k):
                            continue
                    # Confirm, reclaim'den yüksek olmalı
                    if c_now > c_r:
                        return True, {"touch_idx": u, "reclaim_idx": r, "confirm_idx": t}
        return False, {"reason": "chain_not_found"}
    else:
        # Short confirm: kırmızı + EMA10 altı
        if not (np.isfinite(c_now) and np.isfinite(o_now) and c_now < o_now and c_now < e10_now):
            return False, {"reason": "confirm_fail"}
        r_lo = max(0, t - reclaim_win); r_hi = t - 1
        for r in range(r_hi, r_lo - 1, -1):
            c_r, o_r, e10_r = close.iloc[r], open_.iloc[r], e10.iloc[r]
            if not (c_r < o_r and c_r < e10_r):  # kırmızı + EMA10 altı
                continue
            u_lo = max(0, r - touch_win); u_hi = r - 1
            for u in range(u_hi, u_lo - 1, -1):
                e30_u = e30.iloc[u]
                if low.iloc[u] <= e30_u <= high.iloc[u]:
                    atr_u = float(df['atr'].iloc[u]) if pd.notna(df['atr'].iloc[u]) else float('nan')
                    if use_shallow_pen and np.isfinite(atr_u):
                        if not retest_micro_checks_row(df.iloc[u], e30_u, side, atr_u, max_pen_k=max_pen_k):
                            continue
                    if c_now < c_r:
                        return True, {"touch_idx": u, "reclaim_idx": r, "confirm_idx": t}
        return False, {"reason": "chain_not_found"}

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

# ==== Yeni Entry Gate Fonksiyonları ====
def _obv_slope_recent(df: pd.DataFrame, win=OBV_SLOPE_WIN) -> float:
    s = df['obv'].iloc[-(win+1):-1].astype(float)
    if s.size < 3 or s.isna().any(): return 0.0
    x = np.arange(len(s)); m, _ = np.polyfit(x, s.values, 1)
    return float(m)

def trend_score_from_adx_chop(adx_last: float, chop_last: float) -> float:
    s_adx = clamp((adx_last - 18.0) / (35.0 - 18.0 + 1e-12), 0.0, 1.0) if np.isfinite(adx_last) else 0.0
    if not np.isfinite(chop_last): s_chop = 0.0
    elif chop_last <= CHOP_TREND_MAX: s_chop = 1.0
    elif chop_last >= CHOP_RANGE_MIN: s_chop = 0.0
    else:
        frac = (chop_last - CHOP_TREND_MAX) / (CHOP_RANGE_MIN - CHOP_TREND_MAX + 1e-12)
        s_chop = 1.0 - 0.85*frac
    return 0.6*s_adx + 0.4*s_chop

def direction_score(df: pd.DataFrame, side: str) -> float:
    k = NTX_K_EFF
    if len(df) < k + 3: return 0.0
    e10_now, e10_prev = df['ema10'].iloc[-2], df['ema10'].iloc[-2-k]
    obv_slope = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
    s_ema = 1.0 if ((e10_now > e10_prev and side=='long') or (e10_now < e10_prev and side=='short')) else 0.0
    s_obv = 1.0 if ((obv_slope > 0 and side=='long') or (obv_slope < 0 and side=='short')) else 0.0
    return 0.6*s_ema + 0.4*s_obv

def ntx_soft_score(df: pd.DataFrame, atr_z: float) -> float:
    if 'ntx' not in df.columns or pd.isna(df['ntx'].iloc[-2]): return 0.0
    thr = ntx_threshold(atr_z); val = float(df['ntx'].iloc[-2])
    if val <= thr: return clamp((val/(thr+1e-9))*0.5, 0.0, 0.5)
    else: return 0.5 + clamp(((val-thr)/(100-thr+1e-9))*0.5, 0.0, 0.5)

def compute_gate_threshold(adx_last: float, chop_last: float) -> float:
    if (np.isfinite(adx_last) and adx_last >= 28.0) or (np.isfinite(chop_last) and chop_last <= CHOP_TREND_MAX):
        return float(os.getenv("THR_TREND", 0.60))
    if (np.isfinite(adx_last) and adx_last <= 20.0) or (np.isfinite(chop_last) and chop_last >= CHOP_RANGE_MIN):
        return float(os.getenv("THR_FLAT", 0.72))
    return float(os.getenv("THR_NORM", 0.66))

def rsi_band_for_regime(adx_last: float, chop_last: float):
    if (np.isfinite(adx_last) and adx_last >= 28.0) or (np.isfinite(chop_last) and chop_last <= CHOP_TREND_MAX):
        return RSI_BAND_TREND
    if (np.isfinite(adx_last) and adx_last <= 20.0) or (np.isfinite(chop_last) and chop_last >= CHOP_RANGE_MIN):
        return RSI_BAND_FLAT
    return RSI_BAND_NORM

def momentum_pass(df: pd.DataFrame, side: str, band: tuple[int,int]) -> bool:
    rsi = float(df['rsi'].iloc[-2]) if 'rsi' in df.columns and pd.notna(df['rsi'].iloc[-2]) else np.nan
    if not np.isfinite(rsi): return True
    low, high = band
    if low <= rsi <= high: return False
    if side == "long" and rsi < 40: return False
    if side == "short" and rsi > 60: return False
    return True

def froth_ok_blended(df: pd.DataFrame, base_K: float, atr_z: float) -> bool:
    ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema10'].iloc[-2]))
    atr_val = float(df['atr'].iloc[-2])
    if not np.isfinite(ema_gap) or not np.isfinite(atr_val) or atr_val <= 0: return False
    atr_norm = clamp((atr_z - (-1.0)) / (1.5 - (-1.0)), 0.0, 1.0)
    K_dyn = clamp(base_K * (1.10 - 0.20*atr_norm), 0.9*base_K, 1.25*base_K)
    if adx_rising(df): K_dyn = min(K_dyn * 1.07, base_K * 1.25)
    return ema_gap <= K_dyn * atr_val

def regime_ok_strict(df: pd.DataFrame, side: str) -> bool:
    e30 = float(df['ema30'].iloc[-2]); e90 = float(df['ema90'].iloc[-2])
    return (e30 > e90) if side=="long" else (e30 < e90)

def ntx_fatigue(df: pd.DataFrame, k: int = 7) -> bool:
    if 'ntx' not in df.columns or len(df) < k+2: return False
    w = df['ntx'].iloc[-(k+1):-1].astype(float)
    if w.isna().any(): return False
    diffs = np.diff(w.values)
    return (diffs > 0).mean() >= 0.8

def entry_gate(df: pd.DataFrame, side: str, atr_z: float, in_retest_window: bool) -> (bool, dict):
    adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
    chop_last = float(df['chop'].iloc[-2]) if 'chop' in df.columns and pd.notna(df['chop'].iloc[-2]) else np.nan
    s_trend = trend_score_from_adx_chop(adx_last, chop_last)
    s_dir = direction_score(df, side)
    s_ntx = ntx_soft_score(df, atr_z)
    score = (GATE_WEIGHTS["TREND"]*s_trend +
             GATE_WEIGHTS["DIR"]*s_dir +
             GATE_WEIGHTS["NTX"]*s_ntx)
    if ema_slope_ok(df, side, use_atr_norm=True, min_norm=0.08):
        score += 0.05  # Bonus for ATR-normalized EMA slope
    thr_gate = compute_gate_threshold(adx_last, chop_last)
    pass_soft = (score >= thr_gate)
    if np.isfinite(adx_last) and 18.0 <= adx_last <= 25.0:
        di_plus = float(df['di_plus'].iloc[-2]) if pd.notna(df['di_plus'].iloc[-2]) else np.nan
        di_minus = float(df['di_minus'].iloc[-2]) if pd.notna(df['di_minus'].iloc[-2]) else np.nan
        if side=="long" and np.isfinite(di_plus) and np.isfinite(di_minus) and di_plus>di_minus:
            score += 0.05
        if side=="short" and np.isfinite(di_plus) and np.isfinite(di_minus) and di_plus<di_minus:
            score += 0.05
        pass_soft = (score >= thr_gate)
    borderline = (0.65 <= score < 0.72)
    if borderline and regime_ok_strict(df, side):
        score += 0.02
        pass_soft = (score >= thr_gate)
    fk_ok, fk_dbg = fake_filter_v2(df, side=side)
    froth_ok = froth_ok_blended(df, base_K=FROTH_GUARD_K_ATR, atr_z=atr_z)
    rsi_band = rsi_band_for_regime(adx_last, chop_last)
    mom_ok = momentum_pass(df, side=side, band=rsi_band)
    if borderline and not regime_ok_strict(df, side):
        pass_soft = False
    if pass_soft and ntx_fatigue(df) and not in_retest_window:
        pass_soft = False
    passed = pass_soft and fk_ok and froth_ok and mom_ok
    meta = dict(
        score=round(score,3), thr=round(thr_gate,2),
        s_trend=round(s_trend,3), s_dir=round(s_dir,3), s_ntx=round(s_ntx,3),
        fk_ok=fk_ok, froth_ok=froth_ok, mom_ok=mom_ok, fk_dbg=fk_dbg,
        adx=adx_last, chop=chop_last, rsi_band=rsi_band,
        borderline=borderline, regime_ok=regime_ok_strict(df, side),
        ntx_fatigue=ntx_fatigue(df)
    )
    return passed, meta

# ================== Candle Body/Wicks (FakeFilter için) ==================
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

# === FakeFilter v2 (Güncellenmiş) ===
def _ff_params():
    p = FF_PROFILES.get(FF_ACTIVE_PROFILE, FF_PROFILES["normal"])
    return (p["FF_BODY_MIN"], p["FF_UPWICK_MAX"], p["FF_DNWICK_MAX"],
            p["FF_BB_MIN"], p["VOL_MA_RATIO_MIN"], p["VOL_Z_MIN"],
            p["OBV_SLOPE_WIN"], p["MIN_DVOL_USD"])

def _bb_prox(last, side="long"):
    if side == "long":
        num = float(last['close'] - last['bb_mid']); den = float(last['bb_upper'] - last['bb_mid'])
    else:
        num = float(last['bb_mid'] - last['close']); den = float(last['bb_mid'] - last['bb_lower'])
    if not (np.isfinite(num) and np.isfinite(den)) or den <= 0: return 0.0
    return clamp(num/den, 0.0, 1.0)

def simple_volume_ok(df: pd.DataFrame, side: str) -> (bool, str):
    FF_BODY_MIN_, FF_UPWICK_MAX_, FF_DNWICK_MAX_, FF_BB_MIN_, VOL_MA_RATIO_MIN_, VOL_Z_MIN_, OBV_WIN_, MIN_DVOL_USD_ = _ff_params()
    if len(df) < VOL_WIN + 2:
        return False, "data_short"
    last = df.iloc[-2]
    vol = float(last['volume'])
    close = float(last['close'])
    vol_ma = float(last['vol_ma'])
    vol_z = float(last.get('vol_z', np.nan))
    dvol_usd = vol * close
    dvol_ref = float((df['close'] * df['volume']).rolling(VOL_WIN).quantile(VOL_Q).iloc[-2])
    vol_ratio = vol / vol_ma if np.isfinite(vol_ma) and vol_ma > 0 else 1.0
    if not np.isfinite(vol_z) or not np.isfinite(vol_ratio):
        return False, "vol_data_invalid"
    liq_ok = dvol_usd >= max(MIN_DVOL_USD_, 0.85*dvol_ref)
    vol_ok = (vol_ratio >= VOL_MA_RATIO_MIN_) and (vol_z >= VOL_Z_MIN_)
    return liq_ok and vol_ok, f"dvol_usd={dvol_usd:.0f}, vol_ratio={vol_ratio:.2f}, vol_z={vol_z:.2f}, ref={dvol_ref:.0f}"

def fake_filter_v2(df: pd.DataFrame, side: str) -> (bool, str):
    FF_BODY_MIN_, FF_UPWICK_MAX_, FF_DNWICK_MAX_, FF_BB_MIN_, VOL_MA_RATIO_MIN_, VOL_Z_MIN_, OBV_WIN_, MIN_DVOL_USD_ = _ff_params()
    if len(df) < max(VOL_WIN + 2, OBV_WIN_ + 2):
        return False, "data_short"
    last = df.iloc[-2]
    vol_ok, vol_reason = simple_volume_ok(df, side)
    body, up, low = candle_body_wicks(last)
    bb_prox = _bb_prox(last, side=side)
    obv_slope = _obv_slope_recent(df, win=OBV_WIN_)
    body_ok = body >= FF_BODY_MIN_
    wick_ok = (up <= FF_UPWICK_MAX_) if side == "long" else (low <= FF_DNWICK_MAX_)
    bb_ok = bb_prox >= FF_BB_MIN_
    obv_ok = (obv_slope > 0) if side == "long" else (obv_slope < 0)
    score = int(vol_ok) + int(body_ok) + int(wick_ok) + int(bb_ok) + int(obv_ok)
    all_ok = score >= 3
    debug = (
        f"vol={'OK' if vol_ok else 'FAIL'} ({vol_reason}), "
        f"body={body:.2f} ({'OK' if body_ok else 'FAIL'}), "
        f"wick={(up if side=='long' else low):.2f} ({'OK' if wick_ok else 'FAIL'}), "
        f"bb_prox={bb_prox:.2f} ({'OK' if bb_ok else 'FAIL'}), "
        f"obv_slope={obv_slope:.2f} ({'OK' if obv_ok else 'FAIL'})"
    )
    return all_ok, debug

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
                new_symbol_until[symbol] = now + timedelta(minutes=NEW_SYMBOL_COOLDOWN_MIN)
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
        # --- ADAPTİF PENCERE AYARI (ADX & ATR_z) ---
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
        _adx = float(adx_last) if np.isfinite(adx_last) else 0.0
        _atrz = float(atr_z) if np.isfinite(atr_z) else 0.0
        adx_norm = clamp((_adx - 21.0) / 15.0, 0.0, 1.0)
        atr_norm = clamp((_atrz - (-1.0)) / (1.5 - (-1.0)), 0.0, 1.0)
        _rmax = 12 + 6*(1 - adx_norm) + 2*atr_norm
        _emin = _rmax + 1
        _emax = _emin + 6 + 2*atr_norm
        RETEST_MAX_K_ADAPT = int(round(clamp(_rmax, 8, 24)))
        EMA_LATE_MIN_K_ADAPT = int(round(clamp(_emin, 9, 26)))
        EMA_LATE_MAX_K_ADAPT = int(round(clamp(_emax, 12, 32)))
        retest_limit_long = retest_limit_short = RETEST_MAX_K_ADAPT
        ema_min_k = EMA_LATE_MIN_K_ADAPT
        ema_max_k = EMA_LATE_MAX_K_ADAPT
        if VERBOSE_LOG:
            logger.info(
                f"{symbol} {timeframe} ADAPT k: retest≤{retest_limit_long}, "
                f"ema=[{ema_min_k},{ema_max_k}] adx={_adx:.1f} adx_n={adx_norm:.2f} "
                f"atr_z={_atrz:.2f} atr_n={atr_norm:.2f}"
            )
        # --- 30/90 pencere sayaçları (k) ---
        e10 = df['ema10']; e30 = df['ema30']; e90 = df['ema90']
        cross_up_3090 = (e30.shift(1) <= e90.shift(1)) & (e30 > e90)
        cross_dn_3090 = (e30.shift(1) >= e90.shift(1)) & (e30 < e90)
        k_long = bars_since(cross_up_3090, idx=-2)
        k_short = bars_since(cross_dn_3090, idx=-2)
        in_retest_long = (k_long <= retest_limit_long)
        in_retest_short = (k_short <= retest_limit_short)
        # --- Yeni Entry Gate (skor+veto) ---
        ok_L, meta_L = entry_gate(df, side="long", atr_z=atr_z, in_retest_window=in_retest_long)
        ok_S, meta_S = entry_gate(df, side="short", atr_z=atr_z, in_retest_window=in_retest_short)
        gate_long, gate_short = ok_L, ok_S
        # --- Yapısal ortak şartlar (kapanış rengi) ---
        closed_candle = df.iloc[-2]
        is_green = (pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] > closed_candle['open']))
        is_red = (pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] < closed_candle['open']))
        smi_open_green = df['lb_open_green'].iloc[-2] if 'lb_open_green' in df.columns and pd.notna(df['lb_open_green'].iloc[-2]) else False
        smi_open_red = df['lb_open_red'].iloc[-2] if 'lb_open_red' in df.columns and pd.notna(df['lb_open_red'].iloc[-2]) else False
        chop_last = float(df['chop'].iloc[-2]) if 'chop' in df.columns and pd.notna(df['chop'].iloc[-2]) else np.nan
        strong_trend = (np.isfinite(adx_last) and adx_last >= 30) or (np.isfinite(chop_last) and chop_last <= 35)
        cond_lb_L = smi_open_green if not strong_trend else True
        cond_lb_S = smi_open_red if not strong_trend else True
        # --- Yeni Retest Zinciri (4 bar pencereleri) ---
        retest_ok_L, retL_meta = retest_chain_ok(df, side="long", touch_win=4, reclaim_win=4, use_shallow_pen=True)
        retest_ok_S, retS_meta = retest_chain_ok(df, side="short", touch_win=4, reclaim_win=4, use_shallow_pen=True)
        # --- Late (EMA geç) (isim standardizasyonu) ---
        window_ema_long = (ema_min_k <= k_long <= ema_max_k)
        window_ema_short = (ema_min_k <= k_short <= ema_max_k)
        ema_late_L = (
            window_ema_long
            and (e10.iloc[-2] > e30.iloc[-2] > e90.iloc[-2])
            and (closed_candle['close'] > e30.iloc[-2])
            and is_green
            and cond_lb_L
            and gate_long
            and (two_bar_confirm(df, side="long") if USE_2BAR_LATE else True)
        )
        ema_late_S = (
            window_ema_short
            and (e10.iloc[-2] < e30.iloc[-2] < e90.iloc[-2])
            and (closed_candle['close'] < e30.iloc[-2])
            and is_red
            and cond_lb_S
            and gate_short
            and (two_bar_confirm(df, side="short") if USE_2BAR_LATE else True)
        )
        # --- Nihai giriş tetikleyicileri ---
        buy_condition = gate_long and (retest_ok_L or ema_late_L)
        sell_condition = gate_short and (retest_ok_S or ema_late_S)
        reason = ""
        ret_meta_str = ""
        if buy_condition:
            if retest_ok_L:
                reason = "Retest-Zincir"
                ret_meta_str = retest_meta_human(df, retL_meta)
            else:
                reason = "Late (EMA)"
        elif sell_condition:
            if retest_ok_S:
                reason = "Retest-Zincir"
                ret_meta_str = retest_meta_human(df, retS_meta)
            else:
                reason = "Late (EMA)"
        # --- Kriter Sayacı ---
        criteria = [
            ("gate_long", gate_long),
            ("gate_short", gate_short),
            ("retest_chain_L", retest_ok_L),
            ("retest_chain_S", retest_ok_S),
            ("ema_late_L", ema_late_L),
            ("ema_late_S", ema_late_S),
            ("fk_long", meta_L['fk_ok']),
            ("fk_short", meta_S['fk_ok']),
            ("froth_long", meta_L['froth_ok']),
            ("froth_short", meta_S['froth_ok']),
            ("mom_long", meta_L['mom_ok']),
            ("mom_short", meta_S['mom_ok']),
            ("k_window_L", in_retest_long or window_ema_long),
            ("k_window_S", in_retest_short or window_ema_short),
        ]
        await record_crit_batch(criteria)
        if VERBOSE_LOG:
            logger.info(f"{symbol} {timeframe} RETEST_META L:{retL_meta} S:{retS_meta}")
            logger.info(
                f"{symbol} {timeframe} | GATE L:{gate_long} S:{gate_short} "
                f"| buy:{buy_condition}({ 'retest' if retest_ok_L else 'late' if ema_late_L else '-' }) "
                f"| sell:{sell_condition}({ 'retest' if retest_ok_S else 'late' if ema_late_S else '-' }) "
                f"| scoreL={meta_L['score']:.3f}/{meta_L['thr']:.2f} fk={meta_L['fk_ok']} froth={meta_L['froth_ok']} mom={meta_L['mom_ok']}"
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.info(f"{symbol} {timeframe}: BUY atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
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
                    await enqueue_message("\n".join([s for s in [
                        f"{symbol} {timeframe}: BUY (LONG) 🚀",
                        f"Sebep: {reason}",
                        (ret_meta_str if ret_meta_str else None),
                        f"Bar Zamanı: {fmt_dt(bar_time)}",
                        f"Gönderim: {fmt_dt(now)}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
                    ] if s is not None]))
                    save_state()
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                if VERBOSE_LOG:
                    logger.info(f"{symbol} {timeframe}: SELL atlandı (cooldown veya aynı bar) 🚫")
                await mark_status(symbol, "skip", "cooldown_or_same_bar")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
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
                    await enqueue_message("\n".join([s for s in [
                        f"{symbol} {timeframe}: SELL (SHORT) 📉",
                        f"Sebep: {reason}",
                        (ret_meta_str if ret_meta_str else None),
                        f"Bar Zamanı: {fmt_dt(bar_time)}",
                        f"Gönderim: {fmt_dt(now)}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
                    ] if s is not None]))
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
                    f"Bar Zamanı: {fmt_dt(bar_time)}",
                    f"Gönderim: {fmt_dt(now)}",
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
