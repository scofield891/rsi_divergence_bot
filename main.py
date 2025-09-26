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
# ================== Sabit Değerler ==================
# Güvenlik: ENV zorunlu
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN ve CHAT_ID ortam değişkenlerini ayarla.")
TEST_MODE = False
VERBOSE_LOG = False
# ---- Sinyal / Risk Parametreleri ----
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
ADX_NORMAL_HIGH = 21
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
# Retest sonrası onay ve 10/30 onay pencereleri
RETEST_CONFIRM_BARS = 5
LATE_WAIT_BARS = 15
CROSS_1030_CONFIRM_BARS = 5
# ==== SMI Light ====
SMI_LIGHT_NORM_MAX = 0.75
SMI_LIGHT_ADAPTIVE = True
SMI_LIGHT_PCTL = 0.65
SMI_LIGHT_MAX_MIN = 0.60
SMI_LIGHT_MAX_MAX = 1.10
SMI_LIGHT_REQUIRE_SQUEEZE = False
USE_SMI_SLOPE_CONFIRM = True
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = 1.1
# === ADX sinyal modu ===
SIGNAL_MODE = "2of3"
REQUIRE_DIRECTION = False
# ---- Rate-limit & tarama pacing ----
MAX_CONCURRENT_FETCHES = 4
RATE_LIMIT_MS = 200
N_SHARDS = 5
BATCH_SIZE = 10
INTER_BATCH_SLEEP = 5.0
# ---- Sembol keşif ----
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)
# ================== TRAP SKORLAMA ==================
USE_TRAP_SCORING = True
SCORING_CTX_BARS = 3
SCORING_WIN = 120
W_WICK = 20.0
W_VOL = 20.0
W_BBPROX = 15.0
W_ATRZ = 15.0
W_RSI = 15.0
W_MISC = 5.0
W_FB = 10.0
RSI_LONG_EXCESS = 70.0
RSI_SHORT_EXCESS = 30.0
TRAP_ONLY_LOW = True
TRAP_MAX_SCORE = 39.0
TRAP_TIGHT_MAX = 35.0
TRAP_DYN_USE = False
TRAP_BASE_MAX = 39.0
# ==== Basit Hacim + FakeFilter v2 ====
VOL_WIN = 60
VOL_Q = 0.60
VOL_MA_RATIO_MIN = 1.10
VOL_Z_MIN = 1.5
FF_BODY_MIN = 0.55
FF_UPWICK_MAX = 0.25
FF_DNWICK_MAX = 0.25
FF_BB_MIN = 0.30
# OBV eğimi penceresi (FakeFilter v2 için lazım)
OBV_SLOPE_WIN = 5
# ==== NTX ====
NTX_PERIOD = 14
NTX_K_EFF = 10
NTX_VOL_WIN = 60
NTX_THR_LO, NTX_THR_HI = 52.0, 60.0
NTX_ATRZ_LO, NTX_ATRZ_HI = -1.0, 1.5
NTX_MIN_FOR_HYBRID = 50.0
NTX_RISE_K_STRICT = 5
NTX_RISE_MIN_NET = 1.0
NTX_RISE_POS_RATIO = 0.6
NTX_RISE_EPS = 0.05
NTX_RISE_K_HYBRID = 3
NTX_FROTH_K = 1.0
NTX_HYBRID_TRAP_MARGIN = 3.0
# ==== EMA 10/30/90 Parametreleri ====
EMA_FAST = 10
EMA_MID = 30
EMA_SLOW = 90
CROSS_FRESH_BARS = 4
SLOPE_WINDOW = 5
CONFIRM_K_ATR = 0.20
RETEST_K_ATR = 0.30
D_K_ATR = 0.10
USE_STACK_ONLY = False
USE_STACK_FRESH = True
# ==== Debounce ayarları ====
DEB_WEAK = 3
DEB_MED = 2
DEB_STR = 1
ADX_SOFT = 28
ADX_HARD = 35
REQUIRE_RISING_FOR_SOFT = True
REQUIRE_DI_FOR_HARD = True
REQUIRE_NTX_FOR_HARD = True
EARLY_EXTRA_FILTER = True
EARLY_TRAP_MARGIN = 3.0
def _risk_label(score: float) -> str:
    if score < 20: return "Çok düşük risk 🟢"
    if score < 40: return "Düşük risk 🟢"
    if score < 60: return "Orta risk ⚠️"
    if score < 80: return "Yüksek risk 🟠"
    return "Aşırı risk 🔴"
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
    """Mask'teki son True'dan beri kaç bar geçtiğini hesapla (ters indexle)"""
    s = mask.replace(False, np.nan).iloc[::-1]
    first_true = s.first_valid_index()
    return len(s) if first_true is None else s.index.get_loc(first_true)
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
    logger.info(f"ADX calculated: {df['adx'].iloc[-2]:.2f} for {symbol} at {df.index[-2]}")
    return df, adx_condition, di_condition_long, di_condition_short
def calculate_bb(df, period=20, mult=2.0):
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['bb_mid'] + mult * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - mult * df['bb_std']
    return df
def calculate_kc(df, period=20, atr_period=20, mult=1.5):
    df['kc_mid'] = pd.Series(calculate_ema(df['close'].values, period))
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
    if len(df) < 80:
        logger.warning(f"DF çok kısa ({len(df)}), indikatör hesaplanamadı.")
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
ADX_RISE_K = 5
ADX_RISE_MIN_NET = 1.0
ADX_RISE_POS_RATIO = 0.6
ADX_RISE_EPS = 0.0
ADX_RISE_USE_HYBRID = True
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
def _debounce_required(side: str, adx_last: float, rising: bool, di_align: bool, ntx_rise: bool) -> int:
    if adx_last >= ADX_HARD:
        hard_ok = True
        if REQUIRE_DI_FOR_HARD:
            hard_ok = hard_ok and di_align
        if REQUIRE_NTX_FOR_HARD:
            hard_ok = hard_ok and ntx_rise
        if REQUIRE_RISING_FOR_SOFT:
            hard_ok = hard_ok and rising
        if hard_ok:
            return 0
    if adx_last >= ADX_SOFT:
        if (not REQUIRE_RISING_FOR_SOFT) or rising:
            return DEB_STR
    if adx_last >= 20:
        return DEB_MED
    return DEB_WEAK
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
def ntx_rising_hybrid_guarded(df: pd.DataFrame, side: str, eps: float = NTX_RISE_EPS, min_ntx: float = NTX_MIN_FOR_HYBRID, k: int = NTX_RISE_K_HYBRID, froth_k: float = NTX_FROTH_K, trap_margin: float = NTX_HYBRID_TRAP_MARGIN, eff_trap_max: float = 39.0, trap_score_current: float | None = None) -> bool:
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
    if trap_score_current is None:
        trap_score_current = compute_trap_scores(df, side=side)["score"]
    if trap_score_current >= (eff_trap_max - trap_margin):
        return False
    return True
# ================== TRAP SKORLAMA HESABI ==================
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng
def _fitil_bias_sig(last_row, ctx_u_median, ctx_l_median, side="long", deadband=0.05, scale=0.50):
    body, u, l = candle_body_wicks(last_row)
    u_ref = max(ctx_u_median, 1e-6)
    l_ref = max(ctx_l_median, 1e-6)
    u_adj = u / u_ref
    l_adj = l / l_ref
    if side == "long":
        diff = (u_adj - l_adj)
    else:
        diff = (l_adj - u_adj)
    if abs(diff) <= deadband:
        return 0.0
    if diff < 0:
        return 0.0
    return clamp(diff / scale, 0.0, 1.0)
def compute_trap_scores(df: pd.DataFrame, side: str = "long") -> dict:
    try:
        ctx = df.iloc[-(SCORING_CTX_BARS+1):-1]
        last = df.iloc[-2]
        body_u_l = ctx.apply(candle_body_wicks, axis=1, result_type='expand')
        body_ctx = float(body_u_l[0].median()) if not body_u_l.empty else 0.0
        upper_ctx = float(body_u_l[1].median()) if not body_u_l.empty else 0.0
        lower_ctx = float(body_u_l[2].median()) if not body_u_l.empty else 0.0
        wick_ctx = upper_ctx if side == "long" else lower_ctx
        vol_z_ctx = float(ctx['vol_z'].median()) if 'vol_z' in ctx else 0.0
        vol_ma = float(last.get('vol_ma', np.nan))
        vol_now = float(last['volume'])
        vol_ratio = (vol_now / vol_ma) if (np.isfinite(vol_ma) and vol_ma > 0) else 1.0
        vol_sig = np.tanh(max(0.0, vol_z_ctx) / 3.0)
        vol_sig = max(vol_sig, np.tanh(max(0.0, vol_ratio - 1.0)))
        if side == "long":
            num = float(last['close'] - last['bb_mid'])
            den = float(last['bb_upper'] - last['bb_mid'])
        else:
            num = float(last['bb_mid'] - last['close'])
            den = float(last['bb_mid'] - last['bb_lower'])
        bb_prox = clamp(num / (den + 1e-12), 0.0, 1.0) if np.isfinite(num) and np.isfinite(den) else 0.0
        atr_z = rolling_z(df['atr'], SCORING_WIN) if 'atr' in df else 0.0
        atr_sig = clamp((atr_z + 1.0) / 3.0, 0.0, 1.0)
        rsi_ctx = float(ctx['rsi'].median()) if 'rsi' in ctx else 50.0
        if side == "long":
            rsi_sig = clamp((rsi_ctx - RSI_LONG_EXCESS) / 20.0, 0.0, 1.0)
        else:
            rsi_sig = clamp((RSI_SHORT_EXCESS - rsi_ctx) / 20.0, 0.0, 1.0)
        misc = 0.0
        if 'adx' in last and np.isfinite(last['adx']) and last['adx'] < ADX_THRESHOLD:
            misc += 0.5
        if 'squeeze_on' in last and bool(last['squeeze_on']):
            misc += 0.5
        misc_sig = clamp(misc / 1.0, 0.0, 1.0)
        wick_sig = clamp((wick_ctx - 0.25) / 0.5, 0.0, 1.0)
        if len(df) >= SCORING_WIN:
            wick_ref = df.iloc[-(SCORING_WIN+1):-1].apply(candle_body_wicks, axis=1, result_type='expand')
            if not wick_ref.empty:
                uref = float(wick_ref[1].median()) if side == "long" else float(wick_ref[2].median())
                if np.isfinite(uref) and uref > 0:
                    wick_sig *= clamp(0.8 + 0.4 * (0.5 / (uref + 1e-9)), 0.5, 1.2)
        fb_sig = _fitil_bias_sig(last, upper_ctx, lower_ctx, side=side, deadband=0.05, scale=0.50)
        score = (
            W_WICK * wick_sig +
            W_VOL * vol_sig +
            W_BBPROX * bb_prox +
            W_ATRZ * atr_sig +
            W_RSI * rsi_sig +
            W_MISC * misc_sig +
            W_FB * fb_sig
        )
        score = float(clamp(score, 0.0, 100.0))
        return {"score": score, "label": _risk_label(score)}
    except Exception as e:
        logger.warning(f"compute_trap_scores hata: {e}")
        return {"score": 0.0, "label": _risk_label(0.0)}
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
    vol_ok, vol_reason = simple_volume_ok(df, side)
    if not vol_ok:
        return False, f"vol_fail: {vol_reason}"
    body, up, low = candle_body_wicks(last)
    bb_prox = _bb_prox(last, side=side)
    obv_slope = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
    body_ok = body >= FF_BODY_MIN
    wick_ok = up <= FF_UPWICK_MAX if side == "long" else low <= FF_DNWICK_MAX
    bb_ok = bb_prox >= FF_BB_MIN
    obv_ok = (obv_slope > 0) if side == "long" else (obv_slope < 0)
    all_ok = body_ok and wick_ok and bb_ok and obv_ok
    debug = (f"body={body:.2f} ({'OK' if body_ok else 'FAIL'}), "
             f"wick={up if side=='long' else low:.2f} ({'OK' if wick_ok else 'FAIL'}), "
             f"bb_prox={bb_prox:.2f} ({'OK' if bb_ok else 'FAIL'}), "
             f"obv_slope={obv_slope:.2f} ({'OK' if obv_ok else 'FAIL'}), {vol_reason}")
    return all_ok, debug
# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
        # Veri
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            limit_need = max(150, LOOKBACK_ATR + 80, LOOKBACK_SMI + 40, ADX_PERIOD + 40, SCORING_WIN + 5)
            ohlcv = await fetch_ohlcv_async(symbol, timeframe, limit=limit_need)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if df is None or df.empty or len(df) < 80:
                logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
                return
        # İndikatörler
        calc = calculate_indicators(df, symbol, timeframe)
        if not calc or calc[0] is None:
            return
        df, smi_squeeze_off, smi_histogram, smi_color, adx_condition, di_condition_long, di_condition_short = calc
        # ATR
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
        # SMI Light
        smi_raw = smi_histogram
        atr_for_norm = max(atr_value, 1e-9)
        smi_norm = (smi_raw / atr_for_norm) if np.isfinite(smi_raw) else np.nan
        if SMI_LIGHT_ADAPTIVE:
            smi_norm_series = (df['smi'] / df['atr']).replace([np.inf, -np.inf], np.nan)
            ref = smi_norm_series.iloc[-(SCORING_WIN+1):-1].abs() if len(df) >= SCORING_WIN else smi_norm_series.abs()
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
        smi_condition_long = base_long and slope_ok_long
        smi_condition_short = base_short and slope_ok_short
        # ADX ve NTX
        adx_ok = adx_condition
        rising_adx = adx_rising(df)
        atr_z = rolling_z(df['atr'], SCORING_WIN) if 'atr' in df else 0.0
        ntx_thr = ntx_threshold(atr_z)
        ntx_last = float(df['ntx'].iloc[-2]) if pd.notna(df['ntx'].iloc[-2]) else np.nan
        ntx_ok = (np.isfinite(ntx_last) and ntx_last >= ntx_thr)
        strength_ok = adx_ok or ntx_ok
        ntx_rising_str = ntx_rising_strict(df['ntx']) if 'ntx' in df else False
        ntx_rising_hyb_long = ntx_rising_hybrid_guarded(df, side="long") if 'ntx' in df else False
        ntx_rising_hyb_short = ntx_rising_hybrid_guarded(df, side="short") if 'ntx' in df else False
        rising_long = rising_adx or ntx_rising_str or ntx_rising_hyb_long
        rising_short = rising_adx or ntx_rising_str or ntx_rising_hyb_short
        di_long = di_condition_long
        di_short = di_condition_short
        if SIGNAL_MODE == "2of3":
            dir_long_ok = (int(strength_ok) + int(rising_long) + int(di_long)) >= 2
            dir_short_ok = (int(strength_ok) + int(rising_short) + int(di_short)) >= 2
            if REQUIRE_DIRECTION:
                dir_long_ok = dir_long_ok and di_long
                dir_short_ok = dir_short_ok and di_short
            str_ok = True
        else:
            dir_long_ok, dir_short_ok = di_long, di_short
            str_ok = strength_ok
        logger.info(f"{symbol} {timeframe} NTX_last:{ntx_last:.2f} thr:{ntx_thr:.2f} ntx_ok:{ntx_ok} ntx_rising_str:{ntx_rising_str} rising_long:{rising_long} rising_short:{rising_short}")
        trend_strong_long = dir_long_ok and rising_long
        trend_strong_short = dir_short_ok and rising_short
        # FakeFilter v2
        fk_ok_L, fk_dbg_L = fake_filter_v2(df, side="long")
        fk_ok_S, fk_dbg_S = fake_filter_v2(df, side="short")
        logger.info(f"{symbol} {timeframe} FK_LONG {fk_ok_L} | {fk_dbg_L}")
        logger.info(f"{symbol} {timeframe} FK_SHORT {fk_ok_S} | {fk_dbg_S}")
        # Trap
        bull_score = compute_trap_scores(df, side="long") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        bear_score = compute_trap_scores(df, side="short") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        eff_trap_max = TRAP_BASE_MAX
        trap_ok_long = (bull_score["score"] <= eff_trap_max)
        trap_ok_short = (bear_score["score"] <= eff_trap_max)
        logger.info(f"{symbol} {timeframe} trap_thr:{eff_trap_max:.2f}")
        # Froth
        base_K = FROTH_GUARD_K_ATR
        K_long = min(base_K * 1.2, 1.3) if trend_strong_long else base_K
        K_short = min(base_K * 1.2, 1.3) if trend_strong_short else base_K
        ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema10'].iloc[-2]))
        froth_ok_long = (ema_gap <= K_long * atr_value) or trend_strong_long
        froth_ok_short = (ema_gap <= K_short * atr_value) or trend_strong_short
        # Mum rengi
        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        is_green = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] > closed_candle['open'])
        is_red = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] < closed_candle['open'])
        # EMA Tetik
        e10 = df['ema10']
        e30 = df['ema30']
        e90 = df['ema90']
        regime_long = (e30.iloc[-2] > e90.iloc[-2])
        regime_short = (e30.iloc[-2] < e90.iloc[-2])
        cross_up_1030 = (e10.shift(1) <= e30.shift(1)) & (e10 > e30)
        cross_dn_1030 = (e10.shift(1) >= e30.shift(1)) & (e10 < e30)
        bars_since_1030_up = _bars_since_last_true(cross_up_1030)
        bars_since_1030_dn = _bars_since_last_true(cross_dn_1030)
        confirm_1030_L = (
            (bars_since_1030_up <= CROSS_1030_CONFIRM_BARS)
            and (e10.iloc[-2] > e30.iloc[-2] > e90.iloc[-2])
            and smi_condition_long
            and is_green
        )
        confirm_1030_S = (
            (bars_since_1030_dn <= CROSS_1030_CONFIRM_BARS)
            and (e10.iloc[-2] < e30.iloc[-2] < e90.iloc[-2])
            and smi_condition_short
            and is_red
        )
        retest_long = abs(float(df['low'].iloc[-2]) - e30.iloc[-2]) <= RETEST_K_ATR * atr_value
        retest_short = abs(float(df['high'].iloc[-2]) - e30.iloc[-2]) <= RETEST_K_ATR * atr_value
        bars_since_retest_L = _bars_since_last_true(
            (abs(df['low'] - df['ema30']) <= RETEST_K_ATR * df['atr']).replace(False, np.nan)
        )
        bars_since_retest_S = _bars_since_last_true(
            (abs(df['high'] - df['ema30']) <= RETEST_K_ATR * df['atr']).replace(False, np.nan)
        )
        retest_confirm_ok_L = (bars_since_retest_L <= RETEST_CONFIRM_BARS) and retest_long
        retest_confirm_ok_S = (bars_since_retest_S <= RETEST_CONFIRM_BARS) and retest_short
        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
            'remaining_ratio': 1.0, 'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
            'regime_dir': None, 'bars_since_flip': 9999, 'last_regime_bar': None
        })
        state_bars_flip = current_pos.get('bars_since_flip', 9999)
        retest_after_flip_L = (bars_since_retest_L <= state_bars_flip)
        retest_after_flip_S = (bars_since_retest_S <= state_bars_flip)
        late_long_ok = (regime_long and state_bars_flip >= LATE_WAIT_BARS and not retest_after_flip_L)
        late_short_ok = (regime_short and state_bars_flip >= LATE_WAIT_BARS and not retest_after_flip_S)
        # Tetikleyiciler
        buy_early = (
            (state_bars_flip <= LATE_WAIT_BARS)
            and retest_confirm_ok_L
            and is_green
            and dir_long_ok
            and fk_ok_L
            and trap_ok_long
            and froth_ok_long
            and (closed_candle['close'] > closed_candle['ema30'] > closed_candle['ema90'])
        )
        sell_early = (
            (state_bars_flip <= LATE_WAIT_BARS)
            and retest_confirm_ok_S
            and is_red
            and dir_short_ok
            and fk_ok_S
            and trap_ok_short
            and froth_ok_short
            and (closed_candle['close'] < closed_candle['ema30'] < closed_candle['ema90'])
        )
        buy_1030 = (
            confirm_1030_L
            and dir_long_ok
            and fk_ok_L
            and trap_ok_long
            and froth_ok_long
            and (closed_candle['close'] > closed_candle['ema30'] > closed_candle['ema90'])
        )
        sell_1030 = (
            confirm_1030_S
            and dir_short_ok
            and fk_ok_S
            and trap_ok_short
            and froth_ok_short
            and (closed_candle['close'] < closed_candle['ema30'] < closed_candle['ema90'])
        )
        buy_late = (
            late_long_ok
            and smi_condition_long
            and is_green
            and dir_long_ok
            and fk_ok_L
            and trap_ok_long
            and froth_ok_long
            and (closed_candle['close'] > closed_candle['ema30'] > closed_candle['ema90'])
        )
        sell_late = (
            late_short_ok
            and smi_condition_short
            and is_red
            and dir_short_ok
            and fk_ok_S
            and trap_ok_short
            and froth_ok_short
            and (closed_candle['close'] < closed_candle['ema30'] < closed_candle['ema90'])
        )
        buy_condition_ema = buy_early or buy_1030 or buy_late
        sell_condition_ema = sell_early or sell_1030 or sell_late
        # Debounce
        adx_last = df['adx'].iloc[-2]
        ntx_rise_long = (ntx_rising_str or ntx_rising_hyb_long)
        ntx_rise_short = (ntx_rising_str or ntx_rising_hyb_short)
        req_long = _debounce_required("long", adx_last, rising_long, di_long, ntx_rise_long)
        req_short = _debounce_required("short", adx_last, rising_short, di_short, ntx_rise_short)
        bars_flip = int(current_pos.get('bars_since_flip', 9999))
        reg_dir = current_pos.get('regime_dir')
        debounce_ok_long = not (reg_dir == 'long' and bars_flip < req_long)
        debounce_ok_short = not (reg_dir == 'short' and bars_flip < req_short)
        buy_condition = buy_condition_ema and str_ok and debounce_ok_long
        sell_condition = sell_condition_ema and str_ok and debounce_ok_short
        # Flip sonrası ekstra filtre
        if EARLY_EXTRA_FILTER:
            if reg_dir == 'long' and not debounce_ok_long:
                buy_condition = buy_condition and retest_long and di_long and (bull_score["score"] <= TRAP_MAX_SCORE - EARLY_TRAP_MARGIN)
            if reg_dir == 'short' and not debounce_ok_short:
                sell_condition = sell_condition and retest_short and di_short and (bear_score["score"] <= TRAP_MAX_SCORE - EARLY_TRAP_MARGIN)
        # Sebep
        if buy_condition:
            reason = "Erken (Retest)" if buy_early else "10/30 Onay" if buy_1030 else "Geç (Retestsiz)"
        elif sell_condition:
            reason = "Erken (Retest)" if sell_early else "10/30 Onay" if sell_1030 else "Geç (Retestsiz)"
        else:
            reason = ""
        logger.info(f"{symbol} {timeframe} buy_early:{buy_early} buy_1030:{buy_1030} buy_late:{buy_late} sell_early:{sell_early} sell_1030:{sell_1030} sell_late:{sell_late}")
        logger.info(f"{symbol} {timeframe} buy:{buy_condition} sell:{sell_condition} riskL:{bull_score['label']} riskS:{bear_score['label']}")
        if buy_condition and sell_condition:
            logger.warning(f"{symbol} {timeframe}: Çakışan sinyaller, işlem yapılmadı.")
            return
        now = datetime.now(tz)
        # Rejim güncelle
        bar_time = df.index[-2]
        if not isinstance(bar_time, (pd.Timestamp, datetime)):
            bar_time = pd.to_datetime(bar_time, errors="ignore")
        e90 = df['ema90']
        slope_val_90 = (e90.iloc[-2] - e90.iloc[-(SLOPE_WINDOW+2)]) if len(e90) >= SLOPE_WINDOW+2 else 0.0
        reg_now = 'long' if slope_val_90 > 0 else 'short' if slope_val_90 < 0 else current_pos.get('regime_dir')
        if current_pos.get('last_regime_bar') != bar_time:
            if current_pos.get('regime_dir') is None:
                current_pos['bars_since_flip'] = 9999
            elif reg_now is not None and reg_now != current_pos.get('regime_dir'):
                current_pos['bars_since_flip'] = 0
            else:
                current_pos['bars_since_flip'] = min(9999, int(current_pos.get('bars_since_flip', 9999)) + 1)
            current_pos['regime_dir'] = reg_now
            current_pos['last_regime_bar'] = bar_time
        # Çıkış koşulları
        e10_prev, e30_prev, e90_prev = df['ema10'].iloc[-3], df['ema30'].iloc[-3], df['ema90'].iloc[-3]
        e10_last, e30_last, e90_last = df['ema10'].iloc[-2], df['ema30'].iloc[-2], df['ema90'].iloc[-2]
        exit_cross_long = (pd.notna(e10_prev) and pd.notna(e30_prev) and pd.notna(e10_last) and pd.notna(e30_last)
                           and (e10_prev >= e30_prev) and (e10_last < e30_last))
        exit_cross_short = (pd.notna(e10_prev) and pd.notna(e30_prev) and pd.notna(e10_last) and pd.notna(e30_last)
                            and (e10_prev <= e30_prev) and (e10_last > e30_last))
        regime_break_long = e30_last < e90_last
        regime_break_short = e30_last > e90_last
        # Reversal kapama
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: REVERSAL CLOSE 🔁\n"
                    f"Price: {fmt_sym(symbol, current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': current_pos['signal'], 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                current_pos = signal_cache[key]
        # BUY pozisyon aç
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'buy')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                logger.info(f"{symbol} {timeframe}: BUY atlandı (cooldown veya aynı bar) 🚫")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price - sl_atr_abs
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: BUY atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    trap_line = f"\nTrap Risk (Bull): {int(bull_score['score'])}/100 → {bull_score['label']}" if USE_TRAP_SCORING else ""
                    current_pos = {
                        'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': entry_price,
                        'lowest_price': None, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                        f"Sebep: EMA {reason}\n"
                        f"Entry: {fmt_sym(symbol, entry_price)}\n"
                        f"SL: {fmt_sym(symbol, sl_price)}\n"
                        f"TP1: {fmt_sym(symbol, tp1_price)}\n"
                        f"TP2: {fmt_sym(symbol, tp2_price)}{trap_line}"
                    )
                    save_state()
        # SELL pozisyon aç
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES) and
                (APPLY_COOLDOWN_BOTH_DIRECTIONS or current_pos['last_signal_type'] == 'sell')
            )
            if cooldown_active or current_pos.get('last_bar_time') == bar_time:
                logger.info(f"{symbol} {timeframe}: SELL atlandı (cooldown veya aynı bar) 🚫")
            else:
                entry_price = float(closed_candle['close']) if pd.notna(closed_candle['close']) else np.nan
                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_atr_abs = eff_sl_mult * atr_value
                sl_price = entry_price + sl_atr_abs
                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL fiyatı ({symbol} {timeframe}), skip.")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: SELL atlandı (anında SL riski) 🚫")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    trap_line = f"\nTrap Risk (Bear): {int(bear_score['score'])}/100 → {bear_score['label']}" if USE_TRAP_SCORING else ""
                    current_pos = {
                        'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price, 'highest_price': None,
                        'lowest_price': entry_price, 'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': bar_time,
                        'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                    }
                    signal_cache[key] = current_pos
                    await enqueue_message(
                        f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                        f"Sebep: EMA {reason}\n"
                        f"Entry: {fmt_sym(symbol, entry_price)}\n"
                        f"SL: {fmt_sym(symbol, sl_price)}\n"
                        f"TP1: {fmt_sym(symbol, tp1_price)}\n"
                        f"TP2: {fmt_sym(symbol, tp2_price)}{trap_line}"
                    )
                    save_state()
        # Pozisyon yönetimi (LONG)
        if current_pos['signal'] == 'buy':
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
                save_state()
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )
                save_state()
            if exit_cross_long or regime_break_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA EXIT (LONG) 🔁\n"
                    f"Price: {fmt_sym(symbol, current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP LONG ⛔\n"
                    f"Price: {fmt_sym(symbol, current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[key] = current_pos
        # Pozisyon yönetimi (SHORT)
        elif current_pos['signal'] == 'sell':
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']
                current_pos['tp1_hit'] = True 
                await enqueue_message(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\n"
                    f"Cur: {fmt_sym(symbol, current_price)} | TP1: {fmt_sym(symbol, current_pos['tp1_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %30 kapandı, Stop girişe çekildi."
                )
                save_state()
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await enqueue_message(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\n"
                    f"Cur: {fmt_sym(symbol, current_price)} | TP2: {fmt_sym(symbol, current_pos['tp2_price'])}\n"
                    f"P/L: {profit_percent:+.2f}% | %40 kapandı, kalan %30 açık."
                )
                save_state()
            if exit_cross_short or regime_break_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: EMA EXIT (SHORT) 🔁\n"
                    f"Price: {fmt_sym(symbol, current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100 if np.isfinite(current_price) and current_pos['entry_price'] else 0
                await enqueue_message(
                    f"{symbol} {timeframe}: STOP SHORT ⛔\n"
                    f"Price: {fmt_sym(symbol, current_price)}\n"
                    f"P/L: {profit_percent:+.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı."
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None, 'avg_atr_ratio': None,
                    'remaining_ratio': 1.0, 'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False, 'last_bar_time': None,
                    'regime_dir': current_pos.get('regime_dir'), 'bars_since_flip': current_pos.get('bars_since_flip', 9999), 'last_regime_bar': current_pos.get('last_regime_bar')
                }
                save_state()
                return
            signal_cache[key] = current_pos
        # Telemetri
        blocked_by_froth = 0 if froth_ok_long else 1
        blocked_by_trap = 0 if trap_ok_long else 1
        passed_because_strong = 1 if trend_strong_long and not froth_ok_long else 0
        logger.info(f"{symbol} {timeframe} blocked_by_froth:{blocked_by_froth} blocked_by_trap:{blocked_by_trap} passed_because_strong:{passed_because_strong}")
    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return
# ================== Main ==================
async def main():
    await load_markets()
    tz = pytz.timezone('Europe/Istanbul')
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
    while True:
        loop_start = time.time()
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
        logger.info(f"Tur bitti, {total_scanned} sembol tarandı, {elapsed:.1f}s sürdü, {sleep_sec:.1f}s bekle...")
        await asyncio.sleep(sleep_sec)
        save_state()
if __name__ == "__main__":
    asyncio.run(main())
