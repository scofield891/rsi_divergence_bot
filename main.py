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
LOOKBACK_SMI = 20
ADX_PERIOD = 14
ADX_THRESHOLD = 15
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
RETEST_CONFIRM_BARS = 7
LATE_WAIT_BARS = 15
CROSS_1030_CONFIRM_BARS = 8
SMI_LIGHT_NORM_MAX = 0.75
SMI_LIGHT_ADAPTIVE = True
SMI_LIGHT_PCTL = 0.75
SMI_LIGHT_MAX_MIN = 0.60
SMI_LIGHT_MAX_MAX = 1.30
SMI_LIGHT_REQUIRE_SQUEEZE = False
USE_SMI_SLOPE_CONFIRM = False
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = 1.1
SIGNAL_MODE = "2of3"
REQUIRE_DIRECTION = False
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
ADX_SOFT = 21  # Kapı-ADX eşiği 2-of-3 için
MIN_BARS = 80
NEW_SYMBOL_COOLDOWN_MIN = 180
ADX_RISE_K = 5
ADX_RISE_MIN_NET = 1.0
ADX_RISE_POS_RATIO = 0.6
ADX_RISE_EPS = 0.0
ADX_RISE_USE_HYBRID = True
RETEST_K_ATR = 0.40  # Added missing constant

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

def _bars_since_last_true(mask: pd.Series) -> int:
    rev = mask.values[::-1]
    if not rev.any():
        return len(rev)
    return int(np.argmax(rev))

def get_regime(df: pd.DataFrame, use_adx=True, adx_th=ADX_THRESHOLD) -> str:
    e30, e90, adx_last = df['ema30'].iloc[-2], df['ema90'].iloc[-2], df['adx'].iloc[-2]
    if use_adx and not pd.isna(adx_last) and adx_last < adx_th:
        return "neutral"
    if e30 > e90: return "bull"
    if e30 < e90: return "bear"
    return "neutral"

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

def calculate_kc(df, period=20, atr_period=20, mult=1.5):
    df['kc_mid'] = pd.Series(calculate_ema(df['close'].values, period), index=df.index)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = np.maximum(high_low, np.maximum(high_close, low_close))
    df['atr_kc'] = tr.rolling(atr_period).mean()
    df['kc_upper'] = df['kc_mid'] + mult * df['atr_kc']
    df['kc_lower'] = df['kc_mid'] - mult * df['atr_kc']
    return df

def calculate_squeeze(df):
    df['squeeze_on'] = (df['bb_lower'] > df['kc_lower']) & (df['bb_upper'] < df['kc_upper'])
    df['squeeze_off'] = (df['bb_lower'] < df['kc_lower']) & (df['bb_upper'] > df['kc_upper'])
    return df

def calculate_smi_momentum(df, length=LOOKBACK_SMI):
    highest = df['high'].rolling(length).max()
    lowest = df['low'].rolling(length).min()
    avg1 = (highest + lowest) / 2
    avg2 = df['close'].rolling(length).mean()
    avg = (avg1 + avg2) / 2
    diff = df['close'] - avg
    smi = pd.Series(np.nan, index=df.index)
    for i in range(length-1, len(df)):
        y = diff.iloc[i-length+1:i+1].values
        x = np.arange(length)
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            smi.iloc[i] = np.nan
            continue
        slope, intercept = np.polyfit(x[mask], y[mask], 1)
        smi.iloc[i] = slope * (length - 1) + intercept
    df['smi'] = smi
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
        return None, None, None, None, None, None, None
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)
    closes = df['close'].values.astype(np.float64)
    df['ema10'] = calculate_ema(closes, EMA_FAST)
    df['ema30'] = calculate_ema(closes, EMA_MID)
    df['ema90'] = calculate_ema(closes, EMA_SLOW)
    df['rsi'] = calculate_rsi(closes, 14)
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_squeeze(df)
    df = calculate_smi_momentum(df)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    df = calc_ntx(df, period=NTX_PERIOD, k_eff=NTX_K_EFF)
    return df, df['squeeze_off'].iloc[-2], df['smi'].iloc[-2], 'green' if df['smi'].iloc[-2] > 0 else 'red' if df['smi'].iloc[-2] < 0 else 'gray', adx_condition, di_condition_long, di_condition_short

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
    """
    NTX oyu = (Seviye, Momentum, Eğim) 3 alt şarttan en az 2'si True
    """
    if 'ntx' not in df.columns or 'ema10' not in df.columns:
        return False
    ntx_last = float(df['ntx'].iloc[-2]) if pd.notna(df['ntx'].iloc[-2]) else np.nan
    # 1) Seviye
    level_ok = (np.isfinite(ntx_last) and ntx_last >= ntx_thr)
    # 2) Momentum (mevcut fonksiyonları kullan)
    mom_ok = ntx_rising_strict(df['ntx']) or ntx_rising_hybrid_guarded(df, side="long") or ntx_rising_hybrid_guarded(df, side="short")
    # 3) Eğim (ema10 ileri eğimli)
    k = NTX_K_EFF
    if len(df) >= k + 3 and pd.notna(df['ema10'].iloc[-2]) and pd.notna(df['ema10'].iloc[-2-k]):
        slope_ok = (df['ema10'].iloc[-2] > df['ema10'].iloc[-2-k])
    else:
        slope_ok = False
    votes = int(level_ok) + int(mom_ok) + int(slope_ok)
    return votes >= 2

# ================== Candle Body/Wicks (FakeFilter için) ==================
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

# === FakeFilter v2 ===
def _bb_prox(last, side="long"):
    if side == "long":
        num = float(last['close'] - last['bb_mid']); den = float(last['bb_upper'] - last['bb_mid'])
    else:
        num = float(last['bb_mid'] - last['close']); den = float(last['bb_mid'] - last['bb_lower'])
    if not (np.isfinite(num) and np.isfinite(den)) or den <= 0: return 0.0
    return clamp(num/den, 0.0, 1.0)

def _obv_slope_recent(df: pd.DataFrame, win=OBV_SLOPE_WIN) -> float:
    s = df['obv'].iloc[-(win+1):-1].astype(float)
    if s.size < 3 or s.isna().any(): return 0.0
    x = np.arange(len(s)); m,_ = np.polyfit(x, s.values, 1)
    return float(m)

def simple_volume_ok(df: pd.DataFrame, side: str) -> (bool, str):
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
    vol_ok = (vol_ratio >= VOL_MA_RATIO_MIN) and (vol_z >= VOL_Z_MIN)
    return vol_ok, f"dvol_usd={dvol_usd:.0f}, vol_ratio={vol_ratio:.2f}, vol_z={vol_z:.2f}, ref={dvol_ref:.0f}"

def fake_filter_v2(df: pd.DataFrame, side: str) -> (bool, str):
    if len(df) < max(VOL_WIN + 2, OBV_SLOPE_WIN + 2):
        return False, "data_short"
    last = df.iloc[-2]
    vol_ok, vol_reason = simple_volume_ok(df, side) # erken return YOK
    body, up, low = candle_body_wicks(last)
    bb_prox = _bb_prox(last, side=side)
    obv_slope = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
    body_ok = body >= FF_BODY_MIN
    wick_ok = (up <= FF_UPWICK_MAX) if side == "long" else (low <= FF_DNWICK_MAX)
    bb_ok = bb_prox >= FF_BB_MIN
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
            limit_need = max(150, LOOKBACK_ATR + 80, LOOKBACK_SMI + 40, ADX_PERIOD + 40)
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
        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calc
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            await mark_status(symbol, "skip", "invalid_atr")
            logger.warning(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
        smi_raw = smi_histogram
        atr_for_norm = max(atr_value, 1e-9)
        smi_norm = (smi_raw / atr_for_norm) if np.isfinite(smi_raw) else np.nan
        if SMI_LIGHT_ADAPTIVE:
            smi_norm_series = (df['smi'] / df['atr']).replace([np.inf, -np.inf], np.nan)
            ref = smi_norm_series.iloc[-(LOOKBACK_SMI+1):-1].abs() if len(df) >= LOOKBACK_SMI else smi_norm_series.abs()
            if ref.notna().sum() >= 30:
                pct_val = float(np.nanpercentile(ref, SMI_LIGHT_PCTL * 100))
                SMI_LIGHT_NORM_MAX_EFF = clamp(pct_val, SMI_LIGHT_MAX_MIN, SMI_LIGHT_MAX_MAX)
            else:
                SMI_LIGHT_NORM_MAX_EFF = SMI_LIGHT_NORM_MAX
        else:
            SMI_LIGHT_NORM_MAX_EFF = SMI_LIGHT_NORM_MAX
        if SMI_LIGHT_REQUIRE_SQUEEZE:
            base_long = smi_squeeze_off and (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = smi_squeeze_off and (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
        else:
            base_long = (smi_norm > 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
            base_short = (smi_norm < 0) and (abs(smi_norm) < SMI_LIGHT_NORM_MAX_EFF)
        if USE_SMI_SLOPE_CONFIRM and pd.notna(df['smi'].iloc[-3]) and pd.notna(df['smi'].iloc[-2]):
            smi_slope = float(df['smi'].iloc[-2] - df['smi'].iloc[-3])
            slope_ok_long = smi_slope > 0
            slope_ok_short = smi_slope < 0
        else:
            slope_ok_long = slope_ok_short = True
        smi_green = (pd.notna(df['smi'].iloc[-2]) and df['smi'].iloc[-2] > 0)
        smi_red = (pd.notna(df['smi'].iloc[-2]) and df['smi'].iloc[-2] < 0)
        fk_ok_L, fk_dbg_L = fake_filter_v2(df, side="long")
        fk_ok_S, fk_dbg_S = fake_filter_v2(df, side="short")
        if VERBOSE_LOG:
            logger.info(f"{symbol} {timeframe} FK_LONG {fk_ok_L} | {fk_dbg_L}")
            logger.info(f"{symbol} {timeframe} FK_SHORT {fk_ok_S} | {fk_dbg_S}")
        # --- (froth'tan ÖNCE) 2-of-3 Kapı Oyları ---
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else np.nan
        rising_adx = adx_rising(df)
        # DI hizası
        di_long = di_condition_long
        di_short = di_condition_short
        # Oylar
        vote_adx = (np.isfinite(adx_last) and adx_last >= ADX_SOFT)
        vote_dir_long = bool(rising_adx or di_long)
        vote_dir_short = bool(rising_adx or di_short)
        atr_z = rolling_z(df['atr'], LOOKBACK_ATR) if 'atr' in df else 0.0
        ntx_thr = ntx_threshold(atr_z)
        vote_ntx = ntx_vote(df, ntx_thr)
        gate_long = (int(vote_adx) + int(vote_dir_long) + int(vote_ntx)) >= 2
        gate_short = (int(vote_adx) + int(vote_dir_short) + int(vote_ntx)) >= 2
        logger.info(f"{symbol} {timeframe} GATE L/S -> ADX:{'✓' if vote_adx else '×'} "
                    f"DIR(L:{'✓' if vote_dir_long else '×'},S:{'✓' if vote_dir_short else '×'}) "
                    f"NTX:{'✓' if vote_ntx else '×'}")
        # --- (froth İÇİN) trend_strong bayrakları (artık adx_last/rising_adx hazır) ---
        base_K = FROTH_GUARD_K_ATR
        trend_strong_long = (vote_adx and (di_long or rising_adx))
        trend_strong_short = (vote_adx and (di_short or rising_adx))
        K_long = min(base_K * 1.2, 1.3) if trend_strong_long else base_K
        K_short = min(base_K * 1.2, 1.3) if trend_strong_short else base_K
        ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema10'].iloc[-2]))
        froth_ok_long = (ema_gap <= K_long * atr_value) or trend_strong_long
        froth_ok_short = (ema_gap <= K_short * atr_value) or trend_strong_short
        # --- Rejim ek koşulu: ema90 eğimi ---
        e90_prev = df['ema90'].iloc[-3]
        e90_last = df['ema90'].iloc[-2]
        ema90_slope_up = (pd.notna(e90_prev) and pd.notna(e90_last) and e90_last > e90_prev)
        ema90_slope_down = (pd.notna(e90_prev) and pd.notna(e90_last) and e90_last < e90_prev)
        # --- Yapısal ortak şartlar (long/short ayrı) ---
        closed_candle = df.iloc[-2]
        is_green = (pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] > closed_candle['open']))
        is_red = (pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] < closed_candle['open']))
        # Yapısal long
        struct_long = (
            (closed_candle['close'] > closed_candle['ema30'] > closed_candle['ema90']) and
            fk_ok_L and froth_ok_long and
            ema90_slope_up
        )
        # Yapısal short
        struct_short = (
            (closed_candle['close'] < closed_candle['ema30'] < closed_candle['ema90']) and
            fk_ok_S and froth_ok_short and
            ema90_slope_down
        )
        # --- Setup 1: Retest ---
        retest_long = abs(float(df['low'].iloc[-2]) - float(df['ema30'].iloc[-2])) <= RETEST_K_ATR * atr_value
        retest_short = abs(float(df['high'].iloc[-2]) - float(df['ema30'].iloc[-2])) <= RETEST_K_ATR * atr_value
        entry_retest_long = gate_long and struct_long and retest_long and is_green
        entry_retest_short = gate_short and struct_short and retest_short and is_red
        # --- Setup 2: 10-30 yeni kesişim (≤8 bar) + SMI rengi zorunlu ---
        e10 = df['ema10']; e30 = df['ema30']; e90 = df['ema90']
        cross_up_1030 = (e10.shift(1) <= e30.shift(1)) & (e10 > e30)
        cross_dn_1030 = (e10.shift(1) >= e30.shift(1)) & (e10 < e30)
        bars_since_1030_up = _bars_since_last_true(cross_up_1030)
        bars_since_1030_dn = _bars_since_last_true(cross_dn_1030)
        entry_1030_long = gate_long and struct_long and (bars_since_1030_up <= CROSS_1030_CONFIRM_BARS) and (e10.iloc[-2] > e30.iloc[-2] > e90.iloc[-2]) and is_green and smi_green
        entry_1030_short = gate_short and struct_short and (bars_since_1030_dn <= CROSS_1030_CONFIRM_BARS) and (e10.iloc[-2] < e30.iloc[-2] < e90.iloc[-2]) and is_red and smi_red
        # --- Setup 3: Late (≤15 bar; retest yoksa) + SMI rengi zorunlu ---
        bars_since_retest_L = _bars_since_last_true((abs(df['low'] - df['ema30']) <= RETEST_K_ATR * df['atr']))
        bars_since_retest_S = _bars_since_last_true((abs(df['high'] - df['ema30']) <= RETEST_K_ATR * df['atr']))
        no_retest_recent_long = (bars_since_retest_L > RETEST_CONFIRM_BARS)
        no_retest_recent_short = (bars_since_retest_S > RETEST_CONFIRM_BARS)
        entry_late_long = gate_long and struct_long and no_retest_recent_long and (bars_since_1030_up <= LATE_WAIT_BARS) and is_green and smi_green
        entry_late_short = gate_short and struct_short and no_retest_recent_short and (bars_since_1030_dn <= LATE_WAIT_BARS) and is_red and smi_red
        # Son birleşik tetikler
        buy_condition = entry_retest_long or entry_1030_long or entry_late_long
        sell_condition = entry_retest_short or entry_1030_short or entry_late_short
        # --- Sinyal Sebebi ---
        if buy_condition:
            reason = "Erken (Retest)" if entry_retest_long else "10/30 Onay" if entry_1030_long else "Geç (Retestsiz)"
        elif sell_condition:
            reason = "Erken (Retest)" if entry_retest_short else "10/30 Onay" if entry_1030_short else "Geç (Retestsiz)"
        else:
            reason = ""
        # --- Kriter Sayacı ---
        criteria = [
            ("gate_long", gate_long),
            ("gate_short", gate_short),
            ("struct_long", struct_long),
            ("struct_short", struct_short),
            ("retest_long", entry_retest_long),
            ("retest_short", entry_retest_short),
            ("1030_long", entry_1030_long),
            ("1030_short", entry_1030_short),
            ("late_long", entry_late_long),
            ("late_short", entry_late_short),
            ("smi_green", smi_green),
            ("smi_red", smi_red),
            ("fk_long", fk_ok_L),
            ("fk_short", fk_ok_S),
            ("froth_long", froth_ok_long),
            ("froth_short", froth_ok_short),
            ("adx15_ok", adx_condition),  # Updated from adx_ok to adx15_ok
            ("ntx_vote", vote_ntx),
        ]
        await record_crit_batch(criteria)
        if VERBOSE_LOG:
            logger.info(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition} reason:{reason}")
        if buy_condition and sell_condition:
            await mark_status(symbol, "skip", "conflicting_signals")
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return
        now = datetime.now(tz)
        bar_time = df.index[-2]
        if not isinstance(bar_time, (pd.Timestamp, datetime)):
            bar_time = pd.to_datetime(bar_time, errors="ignore")
        regime_now = get_regime(df)
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
                    await enqueue_message("\n".join([
                        f"{symbol} {timeframe}: BUY (LONG) 🚀",
                        f"Sebep: EMA {reason}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
                    ]))
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
                    await enqueue_message("\n".join([
                        f"{symbol} {timeframe}: SELL (SHORT) 📉",
                        f"Sebep: EMA {reason}",
                        f"Entry: {fmt_sym(symbol, entry_price)}",
                        f"SL: {fmt_sym(symbol, sl_price)}",
                        f"TP1: {fmt_sym(symbol, tp1_price)}",
                        f"TP2: {fmt_sym(symbol, tp2_price)}",
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
