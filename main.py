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
from telegram.request import HTTPXRequest
from typing import Tuple, Optional

# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    file_handler = RotatingFileHandler('bot.log', maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# ================== Sabit Değerler ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TEST_MODE = False
if not BOT_TOKEN or not CHAT_ID:
    logger.warning("ENV yok → TEST_MODE=True (Telegram kapalı).")
    TEST_MODE = True
VERBOSE_LOG = False
SHARDS = int(os.getenv("SHARDS", "1"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 20
INSTANT_SL_BUFFER = 0.05
LOOKBACK_SMI = 20
ADX_PERIOD = 14
ADX_THRESHOLD = 15
APPLY_COOLDOWN_BOTH_DIRECTIONS = True
SMI_LIGHT_NORM_MAX = 0.75
SMI_LIGHT_ADAPTIVE = True
SMI_LIGHT_PCTL = 0.65
SMI_LIGHT_MAX_MIN = 0.60
SMI_LIGHT_MAX_MAX = 1.10
SMI_LIGHT_REQUIRE_SQUEEZE = False
USE_SMI_SLOPE_CONFIRM = True
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = 1.4
SIGNAL_MODE = "2of3"
REQUIRE_DIRECTION = False
MAX_CONCURRENT_FETCHES = 4
BATCH_SIZE = 8
INTER_BATCH_SLEEP = 5.0
SCAN_INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "60"))
LINEAR_ONLY = True
QUOTE_WHITELIST = ("USDT",)
MARKETS_REFRESH_INTERVAL = 6 * 3600
RETEST_CONFIRM_BARS = 5
LATE_WAIT_BARS = 15
CROSS_1030_CONFIRM_BARS = 5
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
TRAP_BASE_MAX = 39.0
VOLUME_GATE_MODE = "lite_tight"
VOL_LIQ_USE = True
VOL_LIQ_ROLL = 60
VOL_LIQ_QUANTILE = 0.50
VOL_LIQ_MIN_DVOL_USD = 20_000
VOL_LIQ_MIN_DVOL_LO = 10_000
VOL_LIQ_MIN_DVOL_HI = 150_000
VOL_LIQ_MED_FACTOR = 0.20
VOL_REF_WIN = 20
VOL_ATR_K = 1.8
VOL_ATR_CAP = 0.18
VOL_MIN_BASE = 1.00
LIQ_BYPASS_GOOD_SPIKE = True
GOOD_SPIKE_Z = 1.8
GOOD_BODY_MIN = 0.55
GOOD_UPWICK_MAX = 0.22
GOOD_DNWICK_MAX = 0.22
OBV_SLOPE_WIN = 5
VOL_OBV_TIGHT = 1.03
NTX_PERIOD = 14
NTX_K_EFF = 10
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
EMA_FAST = 10
EMA_MID = 30
EMA_SLOW = 90
RETEST_K_ATR = 0.30
EPOCH_EARLY_BARS = 15
APPLY_DEBOUNCE_EMA = True
DEB_WEAK = 2
DEB_MED = 1
DEB_STR = 0
ADX_SOFT = 28
ADX_HARD = 35
REQUIRE_RISING_FOR_SOFT = True
REQUIRE_DI_FOR_HARD = True
REQUIRE_NTX_FOR_HARD = True
EARLY_EXTRA_FILTER = True
EARLY_TRAP_MARGIN = 3.0

def _risk_label(score: float) -> str:
    if score < 20:
        return "Çok düşük risk 🟢"
    if score < 40:
        return "Düşük risk 🟢"
    if score < 60:
        return "Orta risk ⚠️"
    if score < 80:
        return "Yüksek risk 🟠"
    return "Aşırı risk 🔴"

logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== Borsa & Bot ==================
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
    'timeout': 60000
})
RATE_LIMIT_MS = max(200, getattr(exchange, 'rateLimit', 200))
MARKETS = {}

async def load_markets():
    global MARKETS
    logger.info("Loading markets...")
    MARKETS = await asyncio.to_thread(exchange.load_markets)
    logger.info(f"Markets loaded: {len(MARKETS)}")

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

telegram_bot = None
if not TEST_MODE:
    telegram_bot = telegram.Bot(
        token=BOT_TOKEN,
        request=HTTPXRequest(connection_pool_size=20, pool_timeout=30.0)
    )

# ================== Global State ==================
signal_cache = {}
message_queue = asyncio.Queue(maxsize=1000)
_fetch_sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
_rate_lock = asyncio.Lock()
_last_call_ts = 0.0
STATE_FILE = f'positions_{SHARD_INDEX}.json' if SHARDS > 1 else 'positions.json'
DT_KEYS = {"last_signal_time", "entry_time", "last_bar_time", "last_regime_bar"}
LAST_FLUSH = {}

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

async def save_state_async():
    async with _state_lock:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(signal_cache, f, default=_json_default)
        os.replace(tmp, STATE_FILE)

signal_cache = load_state()

# ================== Util ==================
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def rolling_z(series: pd.Series, win: int) -> float:
    s = series.tail(win).astype(float)
    if s.size < 5 or s.std(ddof=0) == 0 or not np.isfinite(s.iloc[-1]):
        return 0.0
    return float((s.iloc[-1] - s.mean()) / (s.std(ddof=0) + 1e-12))

def fmt_sym(symbol, x):
    try:
        p = MARKETS.get(symbol, {}).get('precision', {}).get('price', 5)
        fmt = f"{{:.{p}f}}"
        return fmt.format(float(x))
    except Exception:
        return str(x)

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
            if TEST_MODE or telegram_bot is None:
                await asyncio.sleep(0)
            else:
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                await asyncio.sleep(0.3)
        except (telegram.error.RetryAfter, telegram.error.TimedOut) as e:
            wait_time = getattr(e, 'retry_after', 5) + 2
            logger.warning(f"Telegram: RetryAfter, {wait_time-2}s bekle")
            await asyncio.sleep(wait_time)
            await enqueue_message(message)
        except Exception as e:
            logger.error(f"Telegram mesaj hatası: {str(e)}")
        finally:
            message_queue.task_done()

# ================== Rate-limit Dostu Fetch ==================
async def fetch_ohlcv_async(symbol, timeframe, limit):
    global _last_call_ts
    HARD_TIMEOUT = 45
    for attempt in range(4):
        try:
            async with _fetch_sem:
                async with _rate_lock:
                    now = asyncio.get_event_loop().time()
                    wait = max(0.0, (_last_call_ts + RATE_LIMIT_MS/1000.0) - now)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    _last_call_ts = asyncio.get_event_loop().time()
                ohlcv = await asyncio.wait_for(
                    asyncio.to_thread(exchange.fetch_ohlcv, symbol, timeframe, None, limit),
                    timeout=HARD_TIMEOUT
                )
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = (
                    pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                      .dt.tz_convert('Europe/Istanbul')
                      .dt.tz_localize(None)
                )
                return df
        except asyncio.TimeoutError:
            backoff = (2 ** attempt) * 1.5
            logger.warning(f"Timeout (hard) {symbol} {timeframe}, backoff {backoff:.1f}s")
            await asyncio.sleep(backoff)
        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
            backoff = (2 ** attempt) * 1.5
            logger.warning(f"Rate limit {symbol} {timeframe}, backoff {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            backoff = 1.0 + attempt
            logger.warning(f"Network/Timeout {symbol} {timeframe}, retry in {backoff:.1f}s ({e.__class__.__name__})")
            await asyncio.sleep(backoff)
    raise ccxt.NetworkError(f"fetch_ohlcv failed after retries: {symbol} {timeframe}")

# ================== Sembol Keşfi (Bybit) ==================
async def discover_bybit_symbols(linear_only=True, quote_whitelist=("USDT",)):
    markets = MARKETS
    syms = []
    for s, m in markets.items():
        if not m.get('active', True):
            continue
        if not m.get('swap', False):
            continue
        if linear_only and not m.get('linear', False):
            continue
        if m.get('quote') not in quote_whitelist:
            continue
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
        lambda x: np.median(np.abs(x - np.median(x))),
        raw=True
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
        return None, None, None, None, None, None
    if 'timestamp' in df.columns:
        if not np.issubdtype(df['timestamp'].dtype, np.datetime64):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
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
    df = ensure_atr(df, period=14)
    df = calculate_obv_and_volma(df, vol_ma_window=20, spike_window=60)
    df = calc_ntx(df, period=NTX_PERIOD, k_eff=NTX_K_EFF)
    df, adx_condition, di_condition_long, di_condition_short = calculate_adx(df, symbol)
    return df, df['squeeze_off'].iloc[-2], df['smi'].iloc[-2], adx_condition, di_condition_long, di_condition_short

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
        0.35 * er + 0.35 * slope_mag + 0.15 * pos_ratio + 0.15 * vol_sig
    ).clip(0, 1)
    df['ntx_raw'] = base
    df['ntx'] = df['ntx_raw'].ewm(alpha=1.0/period, adjust=False).mean() * 100.0
    return df

def ntx_threshold(atr_z: float) -> float:
    a = clamp((atr_z - NTX_ATRZ_LO) / (NTX_ATRZ_HI - NTX_ATRZ_LO + 1e-12), 0.0, 1.0)
    return NTX_THR_LO + a * (NTX_THR_HI - NTX_THR_LO)

def ntx_rising_strict(s: pd.Series, k: int = NTX_RISE_K_STRICT, min_net: float = NTX_RISE_MIN_NET, pos_ratio_th: float = NTX_RISE_POS_RATIO, eps: float = NTX_RISE_EPS) -> bool:
    if s is None or len(s) < k + 1:
        return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any():
        return False
    x = np.arange(len(w))
    slope, _ = np.polyfit(x, w.values, 1)
    diffs = np.diff(w.values)
    posr = (diffs > eps).mean() if diffs.size else 0.0
    net = w.iloc[-1] - w.iloc[0]
    return (slope > 0) and (net >= min_net) and (posr >= pos_ratio_th)

def ntx_rising_hybrid_guarded(df: pd.DataFrame, side: str, eps: float = NTX_RISE_EPS, min_ntx: float = NTX_MIN_FOR_HYBRID, k: int = NTX_RISE_K_HYBRID, froth_k: float = NTX_FROTH_K, trap_margin: float = NTX_HYBRID_TRAP_MARGIN, eff_trap_max: float = 39.0, trap_score_current: Optional[float] = None) -> bool:
    s = df['ntx'] if 'ntx' in df.columns else None
    if s is None or len(s) < k + 1:
        return False
    w = s.iloc[-(k+1):-1].astype(float)
    if w.isna().any():
        return False
    x = np.arange(len(w))
    slope, _ = np.polyfit(x, w.values, 1)
    last_diff = w.values[-1] - w.values[-2]
    if not (slope > 0 and last_diff > eps):
        return False
    if w.iloc[-1] < min_ntx:
        return False
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
            W_WICK * wick_sig + W_VOL * vol_sig + W_BBPROX * bb_prox + W_ATRZ * atr_sig +
            W_RSI * rsi_sig + W_MISC * misc_sig + W_FB * fb_sig
        )
        score = float(clamp(score, 0.0, 100.0))
        return {"score": score, "label": _risk_label(score)}
    except Exception as e:
        logger.warning(f"compute_trap_scores hata: {e}")
        return {"score": 0.0, "label": _risk_label(0.0)}

# === Hacim Filtresi ===
def _obv_slope_recent(df: pd.DataFrame, win=OBV_SLOPE_WIN) -> float:
    s = df['obv'].iloc[-(win+1):-1].astype(float)
    if s.size < 3 or s.isna().any():
        return 0.0
    x = np.arange(len(s))
    m, _ = np.polyfit(x, s.values, 1)
    return float(m)

def _dynamic_liq_floor(dv_series: pd.Series) -> float:
    s = dv_series.astype(float).dropna()
    if s.size < VOL_LIQ_ROLL:
        return VOL_LIQ_MIN_DVOL_USD
    med = float(s.tail(VOL_LIQ_ROLL).median())
    dyn = med * VOL_LIQ_MED_FACTOR
    return clamp(dyn, VOL_LIQ_MIN_DVOL_LO, VOL_LIQ_MIN_DVOL_HI)

def trend_relax_factor(adx_last, ntx_last=None, ntx_thr=None):
    s = 0.0
    if np.isfinite(adx_last):
        s += max(0.0, (adx_last - 15.0) / 20.0) * 0.6
    if ntx_last is not None and ntx_thr is not None and np.isfinite(ntx_last) and np.isfinite(ntx_thr):
        s += max(0.0, (ntx_last - ntx_thr) / 15.0) * 0.4
    return 1.0 - 0.35 * min(1.0, s)

def volume_gate(df: pd.DataFrame, side: str, atr_ratio: float, symbol: str = "", relax: float = 1.0) -> Tuple[bool, str]:
    if len(df) < max(VOL_LIQ_ROLL+2, VOL_REF_WIN+2):
        return False, "data_short"
    last = df.iloc[-2]
    close = float(last['close'])
    vol = float(last['volume'])
    dvol_usd = close * vol
    base = symbol.split('/')[0]
    is_major = base in {"BTC", "ETH"}
    if VOL_LIQ_USE and not TEST_MODE:
        dv = (df['close'] * df['volume']).astype(float)
        roll = dv.rolling(VOL_LIQ_ROLL, min_periods=VOL_LIQ_ROLL)
        q = roll.apply(lambda x: np.nanquantile(x, VOL_LIQ_QUANTILE), raw=True)
        qv = float(q.iloc[-2]) if pd.notna(q.iloc[-2]) else 0.0
        dyn_min = _dynamic_liq_floor(dv)
        if is_major:
            dyn_min = max(dyn_min, VOL_LIQ_MIN_DVOL_USD)
        hard_min = max(qv, dyn_min)
        liq_bypass = False
        if LIQ_BYPASS_GOOD_SPIKE:
            vol_z = float(last.get('vol_z', np.nan))
            body, up, dn = candle_body_wicks(last)
            if np.isfinite(vol_z) and vol_z >= GOOD_SPIKE_Z:
                if (side == "long" and body >= GOOD_BODY_MIN and up <= GOOD_UPWICK_MAX) or \
                   (side == "short" and body >= GOOD_BODY_MIN and dn <= GOOD_DNWICK_MAX):
                    liq_bypass = True
        if (dvol_usd < hard_min) and not liq_bypass:
            return False, f"HARD_FLOOR dvol={dvol_usd:.0f} < min={hard_min:.0f} (q{int(VOL_LIQ_QUANTILE*100)}={qv:.0f}, dyn={dyn_min:.0f})"
    dvol_ref = float((df['close']*df['volume']).rolling(VOL_REF_WIN).mean().iloc[-2])
    if not np.isfinite(dvol_ref) or dvol_ref <= 0:
        dvol_ref = float((df['close']*df['volume']).iloc[-(VOL_REF_WIN+1):-1].mean())
    if not np.isfinite(dvol_ref) or dvol_ref <= 0:
        dvol_ref = dvol_usd
    mult = 1.0 + clamp(atr_ratio * VOL_ATR_K, 0.0, VOL_ATR_CAP) if np.isfinite(atr_ratio) else 1.0
    mult = max(mult, VOL_MIN_BASE)
    if VOLUME_GATE_MODE in ("full", "lite_tight"):
        if VOL_OBV_TIGHT > 1.0:
            obv_m = _obv_slope_recent(df, win=OBV_SLOPE_WIN)
            if (side == "long" and obv_m <= 0) or (side == "short" and obv_m >= 0):
                mult *= VOL_OBV_TIGHT
    need = dvol_ref * (mult * relax)
    buffer = 0.90 if is_major else 0.95
    ok = dvol_usd > (need * buffer)
    if not ok:
        return False, f"SOFT_GATE dvol={dvol_usd:.0f} ≤ need={need:.0f} (ref={dvol_ref:.0f}, mult={mult:.2f}, relax={relax:.2f}, buf={buffer:.2f})"
    return True, f"OK dvol={dvol_usd:.0f} > need={need:.0f} (ref={dvol_ref:.0f}, mult={mult:.2f}, relax={relax:.2f})"

# === SMI shade classifier ===
def smi_shade(smi_val: float, atr_val: float, norm_cap: float, split: float = 0.50) -> str:
    if not (np.isfinite(smi_val) and np.isfinite(atr_val) and atr_val > 0 and np.isfinite(norm_cap) and norm_cap > 0):
        return "gray"
    n = smi_val / atr_val
    thr = split * norm_cap
    if n >= 0:
        return "light_green" if (0 <= n < thr) else "dark_green"
    else:
        a = abs(n)
        return "light_red" if (0 <= a < thr) else "dark_red"

# ================== Retest Mantığı (Erken Pencere) ==================
def _last_retest_bars(df: pd.DataFrame, side: str = "long", window: int = EPOCH_EARLY_BARS, k: float = RETEST_K_ATR) -> int:
    """
    Son retest'in kapalı muma (iloc[-2]) göre kaç bar önce olduğunu döndürür.
    Yoksa 10**6 döner. window sadece bakılan aralığı sınırlar.
    """
    sub = df.iloc[-(window+1):-1] # son 'window' bar, kapalı muma kadar
    if side == "long":
        touch = (sub['ema30'] - sub['low']).abs() <= (k * sub['atr'])
    else:
        touch = (sub['ema30'] - sub['high']).abs() <= (k * sub['atr'])
    idx = np.where(touch.values)[0]
    if idx.size == 0:
        return 10**6
    last_idx = int(idx[-1]) # sub içindeki son temas
    return (len(sub) - 1) - last_idx # kapalı muma göre bar mesafesi

def _bars_since_last(sig: pd.Series) -> int:
    if bool(sig.any()):
        arr = np.where(sig.values[:-1])[0]
        if arr.size:
            last_idx = int(arr[-1])
            return (len(sig) - 2) - last_idx
    return 10**6

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe='4h'):
    tz = pytz.timezone('Europe/Istanbul')
    try:
        if TEST_MODE:
            np.random.seed(42) # Reprodüksiyon için seed
            N = 200
            ts0 = int(time.time()*1000) - N*4*60*60*1000
            closes = np.abs(np.cumsum(np.random.randn(N))) * 0.05 + 0.3
            highs = closes + np.random.rand(N) * 0.02 * closes
            lows = closes - np.random.rand(N) * 0.02 * closes
            volumes = np.random.rand(N) * 10000
            ohlcv = [[ts0 + i*4*60*60*1000, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(N)]
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            df['timestamp'] = (
                pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                  .dt.tz_convert('Europe/Istanbul')
                  .dt.tz_localize(None)
            )
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            limit_need = max(150, LOOKBACK_ATR + 80, LOOKBACK_SMI + 40, ADX_PERIOD + 40, SCORING_WIN + 5)
            df = await fetch_ohlcv_async(symbol, timeframe, limit=limit_need)
        if df is None or df.empty or len(df) < 80:
            logger.warning(f"{symbol}: Yetersiz veri ({len(df) if df is not None else 0} mum), skip.")
            return
        calc = calculate_indicators(df, symbol, timeframe)
        if not calc or calc[0] is None:
            return
        df, smi_squeeze_off, smi_histogram, adx_condition, di_condition_long, di_condition_short = calc
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"Geçersiz ATR ({symbol} {timeframe}), skip.")
            return
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
        adx_ok = adx_condition
        rising_adx = adx_rising(df)
        adx_last = float(df['adx'].iloc[-2]) if pd.notna(df['adx'].iloc[-2]) else 0.0
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
        else:
            dir_long_ok, dir_short_ok = di_long, di_short
        trend_ok_long = bool(adx_condition or rising_long or ntx_ok)
        trend_ok_short = bool(adx_condition or rising_short or ntx_ok)
        logger.info(f"{symbol} {timeframe} NTX_last:{ntx_last:.2f} thr:{ntx_thr:.2f} ntx_ok:{ntx_ok} ntx_rising_str:{ntx_rising_str} rising_long:{rising_long} rising_short:{rising_short}")
        trend_strong_long = dir_long_ok and rising_long
        trend_strong_short = dir_short_ok and rising_short
        relax = trend_relax_factor(adx_last, ntx_last, ntx_thr)
        ok_l, reason_l = volume_gate(df, "long", avg_atr_ratio, symbol, relax=relax)
        ok_s, reason_s = volume_gate(df, "short", avg_atr_ratio, symbol, relax=relax)
        logger.info(f"{symbol} {timeframe} VOL_LONG {ok_l} | {reason_l}")
        logger.info(f"{symbol} {timeframe} VOL_SHORT {ok_s} | {reason_s}")
        bull_score = compute_trap_scores(df, side="long") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        bear_score = compute_trap_scores(df, side="short") if USE_TRAP_SCORING else {"score": 0.0, "label": _risk_label(0.0)}
        eff_trap_max = TRAP_BASE_MAX
        trap_ok_long = (bull_score["score"] <= eff_trap_max)
        trap_ok_short = (bear_score["score"] <= eff_trap_max)
        logger.info(f"{symbol} {timeframe} trap_thr:{eff_trap_max:.2f}")
        base_K = FROTH_GUARD_K_ATR
        K_long = min(base_K * 1.2, 1.3) if trend_strong_long else base_K
        K_short = min(base_K * 1.2, 1.3) if trend_strong_short else base_K
        ema_gap = abs(float(df['close'].iloc[-2]) - float(df['ema10'].iloc[-2]))
        if USE_FROTH_GUARD:
            froth_ok_long = (ema_gap <= K_long * atr_value) or trend_strong_long
            froth_ok_short = (ema_gap <= K_short * atr_value) or trend_strong_short
        else:
            froth_ok_long = True
            froth_ok_short = True
        closed_candle = df.iloc[-2]
        current_price = float(df['close'].iloc[-1]) if pd.notna(df['close'].iloc[-1]) else np.nan
        is_green = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] > closed_candle['open'])
        is_red = pd.notna(closed_candle['close']) and pd.notna(closed_candle['open']) and (closed_candle['close'] < closed_candle['open'])
        e10 = df['ema10']
        e30 = df['ema30']
        e90 = df['ema90']
        cross_up_3090 = (e30.shift(1) <= e90.shift(1)) & (e30 > e90)
        cross_dn_3090 = (e30.shift(1) >= e90.shift(1)) & (e30 < e90)
        in_early_L = (_bars_since_last(cross_up_3090) <= EPOCH_EARLY_BARS)
        in_early_S = (_bars_since_last(cross_dn_3090) <= EPOCH_EARLY_BARS)
        shade_curr = smi_shade(float(df['smi'].iloc[-2]), atr_value, SMI_LIGHT_NORM_MAX_EFF)
        smi_early_ok_L = (shade_curr == "light_green")
        smi_early_ok_S = (shade_curr == "light_red")
        cross_up_1030 = (e10.shift(1) <= e30.shift(1)) & (e10 > e30)
        cross_dn_1030 = (e10.shift(1) >= e30.shift(1)) & (e10 < e30)
        bars_since_1030_up = _bars_since_last(cross_up_1030)
        bars_since_1030_dn = _bars_since_last(cross_dn_1030)
        confirm_1030_L = (
            (bars_since_1030_up <= CROSS_1030_CONFIRM_BARS)
            and (e10.iloc[-2] > e30.iloc[-2] > e90.iloc[-2])
            and smi_early_ok_L
            and is_green
        )
        confirm_1030_S = (
            (bars_since_1030_dn <= CROSS_1030_CONFIRM_BARS)
            and (e10.iloc[-2] < e30.iloc[-2] < e90.iloc[-2])
            and smi_early_ok_S
            and is_red
        )
        bars_since_retest_L = _last_retest_bars(df, side="long", window=EPOCH_EARLY_BARS, k=RETEST_K_ATR)
        bars_since_retest_S = _last_retest_bars(df, side="short", window=EPOCH_EARLY_BARS, k=RETEST_K_ATR)
        retest_recent_L = (bars_since_retest_L <= RETEST_CONFIRM_BARS)
        retest_recent_S = (bars_since_retest_S <= RETEST_CONFIRM_BARS)
        retest_confirm_ok_L = (in_early_L and retest_recent_L and smi_early_ok_L and is_green)
        retest_confirm_ok_S = (in_early_S and retest_recent_S and smi_early_ok_S and is_red)
        buy_early = (
            retest_confirm_ok_L
            and trend_ok_long and dir_long_ok
            and ok_l and trap_ok_long and froth_ok_long
            and (closed_candle['close'] > closed_candle['ema30'] and closed_candle['close'] > closed_candle['ema90'])
        )
        sell_early = (
            retest_confirm_ok_S
            and trend_ok_short and dir_short_ok
            and ok_s and trap_ok_short and froth_ok_short
            and (closed_candle['close'] < closed_candle['ema30'] and closed_candle['close'] < closed_candle['ema90'])
        )
        buy_1030 = (
            confirm_1030_L
            and trend_ok_long and dir_long_ok
            and ok_l and trap_ok_long and froth_ok_long
            and (closed_candle['close'] > closed_candle['ema30'] and closed_candle['close'] > closed_candle['ema90'])
        )
        sell_1030 = (
            confirm_1030_S
            and trend_ok_short and dir_short_ok
            and ok_s and trap_ok_short and froth_ok_short
            and (closed_candle['close'] < closed_candle['ema30'] and closed_candle['close'] < closed_candle['ema90'])
        )
        regime_long = (e30.iloc[-2] > e90.iloc[-2])
        regime_short = (e30.iloc[-2] < e90.iloc[-2])
        regime_cross_up = regime_long and not (e30.iloc[-3] > e90.iloc[-3]) if len(e30) >= 3 else False
        regime_cross_dn = regime_short and not (e30.iloc[-3] < e90.iloc[-3]) if len(e30) >= 3 else False
        async with _state_lock:
            state = signal_cache.get(symbol, {})
            bars_since_flip = state.get('bars_since_regime_flip', 10**6)
            if regime_cross_up or regime_cross_dn:
                bars_since_flip = 0
            elif bars_since_flip < 10**6:
                bars_since_flip += 1
            state['bars_since_regime_flip'] = bars_since_flip
            wait_long = _debounce_required(
                side="long",
                adx_last=adx_last,
                rising=rising_long,
                di_align=di_long,
                ntx_rise=ntx_rising_hyb_long
            )
            wait_short = _debounce_required(
                side="short",
                adx_last=adx_last,
                rising=rising_short,
                di_align=di_short,
                ntx_rise=ntx_rising_hyb_short
            )
            debounce_ok_long = (bars_since_flip >= wait_long)
            debounce_ok_short = (bars_since_flip >= wait_short)
            retest_after_flip_L = (bars_since_retest_L <= bars_since_flip)
            retest_after_flip_S = (bars_since_retest_S <= bars_since_flip)
            late_long_ok = (regime_long and bars_since_flip >= LATE_WAIT_BARS and not retest_after_flip_L)
            late_short_ok = (regime_short and bars_since_flip >= LATE_WAIT_BARS and not retest_after_flip_S)
            buy_late = (
                (not in_early_L) and late_long_ok
                and smi_early_ok_L and is_green
                and trend_ok_long and dir_long_ok
                and ok_l and trap_ok_long and froth_ok_long
                and (closed_candle['close'] > closed_candle['ema30'] and closed_candle['close'] > closed_candle['ema90'])
            )
            sell_late = (
                (not in_early_S) and late_short_ok
                and smi_early_ok_S and is_red
                and trend_ok_short and dir_short_ok
                and ok_s and trap_ok_short and froth_ok_short
                and (closed_candle['close'] < closed_candle['ema30'] and closed_candle['close'] < closed_candle['ema90'])
            )
            buy_condition_ema = buy_early or buy_1030 or buy_late
            sell_condition_ema = sell_early or sell_1030 or sell_late
            buy_condition = (debounce_ok_long and buy_condition_ema) if APPLY_DEBOUNCE_EMA else buy_condition_ema
            sell_condition = (debounce_ok_short and sell_condition_ema) if APPLY_DEBOUNCE_EMA else sell_condition_ema
            eff_trap_max_early = eff_trap_max - EARLY_TRAP_MARGIN if (EARLY_EXTRA_FILTER and bars_since_flip <= max(DEB_WEAK, DEB_MED)) else eff_trap_max
            if EARLY_EXTRA_FILTER and bars_since_flip <= max(DEB_WEAK, DEB_MED):
                trap_ok_long_early = (bull_score["score"] <= eff_trap_max_early)
                trap_ok_short_early = (bear_score["score"] <= eff_trap_max_early)
                buy_condition = buy_condition and trap_ok_long_early
                sell_condition = sell_condition and trap_ok_short_early
            logger.info(
                f"{symbol} {timeframe} triggers: earlyL={buy_early} 1030L={buy_1030} lateL={buy_late} | "
                f"earlyS={sell_early} 1030S={sell_1030} lateS={sell_late}"
            )
            logger.info(f"{symbol} {timeframe} EMA: buy={buy_condition_ema} sell={sell_condition_ema}")
            logger.info(f"{symbol} {timeframe} debounce wait L:{wait_long} S:{wait_short} bars_since_flip:{bars_since_flip}")
            current_pos = state.get('position', None)
            last_signal_time = state.get('last_signal_time', None)
            last_bar_time = state.get('last_bar_time', None)
            state_changed = False
            now = datetime.now(tz)
            if last_bar_time is not None:
                last_bar_time = pd.to_datetime(last_bar_time)
                if last_bar_time >= df.index[-2]:
                    signal_cache[symbol] = state
                    return
            if last_signal_time is not None:
                last_signal_time = pd.to_datetime(last_signal_time)
                cooldown_ok = (now - last_signal_time) >= timedelta(minutes=COOLDOWN_MINUTES)
                if not cooldown_ok:
                    logger.info(f"{symbol} {timeframe} cooldown aktif, son sinyal: {(now - last_signal_time).total_seconds()/60:.1f} dk önce")
                    signal_cache[symbol] = state
                    return
            price_str = fmt_sym(symbol, current_price if np.isfinite(current_price) else closed_candle['close'])
            if buy_condition and current_pos != 'long':
                if sell_condition and APPLY_COOLDOWN_BOTH_DIRECTIONS:
                    logger.info(f"{symbol} {timeframe} Çift yön sinyal (L:{buy_condition}, S:{sell_condition}), skip.")
                    signal_cache[symbol] = state
                    return
                sl = float(df['close'].iloc[-2]) - SL_MULTIPLIER * atr_value
                if abs(float(df['close'].iloc[-1]) - sl) <= (INSTANT_SL_BUFFER * atr_value):
                    logger.info(f"{symbol} {timeframe} SL çok yakın, skip.")
                    signal_cache[symbol] = state
                    return
                tp1 = float(df['close'].iloc[-2]) + TP_MULTIPLIER1 * atr_value
                tp2 = float(df['close'].iloc[-2]) + TP_MULTIPLIER2 * atr_value
                sl = float(df['close'].iloc[-2]) - (SL_MULTIPLIER + SL_BUFFER) * atr_value
                msg = (
                    f"📈 {symbol} {timeframe} LONG sinyali\n"
                    f"💰 Fiyat: {price_str} USDT\n"
                    f"🎯 TP1: {fmt_sym(symbol, tp1)} ({TP_MULTIPLIER1}x ATR)\n"
                    f"🎯 TP2: {fmt_sym(symbol, tp2)} ({TP_MULTIPLIER2}x ATR)\n"
                    f"🛑 SL: {fmt_sym(symbol, sl)} ({SL_MULTIPLIER+SL_BUFFER:.1f}x ATR)\n"
                    f"⚖️ Risk: {bull_score['label']}\n"
                    f"🕒 {now.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                state['position'] = 'long'
                state['entry_price'] = float(df['close'].iloc[-2])
                state['sl'] = sl
                state['tp1'] = tp1
                state['tp2'] = tp2
                state['entry_time'] = now
                state['last_signal_time'] = now
                state['last_bar_time'] = df.index[-2]
                state_changed = True
                signal_cache[symbol] = state
                await enqueue_message(msg)
                logger.info(f"{symbol} {timeframe} LONG sinyali: {msg}")
            elif sell_condition and current_pos != 'short':
                if buy_condition and APPLY_COOLDOWN_BOTH_DIRECTIONS:
                    logger.info(f"{symbol} {timeframe} Çift yön sinyal (L:{buy_condition}, S:{sell_condition}), skip.")
                    signal_cache[symbol] = state
                    return
                sl = float(df['close'].iloc[-2]) + SL_MULTIPLIER * atr_value
                if abs(float(df['close'].iloc[-1]) - sl) <= (INSTANT_SL_BUFFER * atr_value):
                    logger.info(f"{symbol} {timeframe} SL çok yakın, skip.")
                    signal_cache[symbol] = state
                    return
                tp1 = float(df['close'].iloc[-2]) - TP_MULTIPLIER1 * atr_value
                tp2 = float(df['close'].iloc[-2]) - TP_MULTIPLIER2 * atr_value
                sl = float(df['close'].iloc[-2]) + (SL_MULTIPLIER + SL_BUFFER) * atr_value
                msg = (
                    f"📉 {symbol} {timeframe} SHORT sinyali\n"
                    f"💰 Fiyat: {price_str} USDT\n"
                    f"🎯 TP1: {fmt_sym(symbol, tp1)} ({TP_MULTIPLIER1}x ATR)\n"
                    f"🎯 TP2: {fmt_sym(symbol, tp2)} ({TP_MULTIPLIER2}x ATR)\n"
                    f"🛑 SL: {fmt_sym(symbol, sl)} ({SL_MULTIPLIER+SL_BUFFER:.1f}x ATR)\n"
                    f"⚖️ Risk: {bear_score['label']}\n"
                    f"🕒 {now.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                state['position'] = 'short'
                state['entry_price'] = float(df['close'].iloc[-2])
                state['sl'] = sl
                state['tp1'] = tp1
                state['tp2'] = tp2
                state['entry_time'] = now
                state['last_signal_time'] = now
                state['last_bar_time'] = df.index[-2]
                state_changed = True
                signal_cache[symbol] = state
                await enqueue_message(msg)
                logger.info(f"{symbol} {timeframe} SHORT sinyali: {msg}")
            else:
                state['last_bar_time'] = df.index[-2]
                state_changed = True
                signal_cache[symbol] = state
            if state_changed:
                now_ts = time.time()
                if symbol not in LAST_FLUSH or now_ts - LAST_FLUSH[symbol] > 120 or ('position' in state and state['position'] in ('long', 'short')):
                    await save_state_async()
                    LAST_FLUSH[symbol] = now_ts
    except Exception as e:
        logger.error(f"{symbol} {timeframe} hata: {str(e)}")

# ================== Ana Döngü ==================
async def main_loop():
    global MARKETS
    logger.info("Tarama başlıyor...")
    loop_start = time.time()
    total_scanned = 0
    now = time.time()
    if not MARKETS or (now - getattr(main_loop, 'last_markets_refresh', 0) > MARKETS_REFRESH_INTERVAL):
        await load_markets()
        main_loop.last_markets_refresh = now
    symbols = await discover_bybit_symbols(linear_only=LINEAR_ONLY, quote_whitelist=QUOTE_WHITELIST)
    if SHARDS > 1:
        before = len(symbols)
        symbols = [s for i, s in enumerate(symbols) if (i % SHARDS) == SHARD_INDEX]
        logger.info(f"Env sharding aktif: SHARDS={SHARDS} SHARD_INDEX={SHARD_INDEX} | {before} -> {len(symbols)} sembol")
    logger.info(f"Tarama başladı, sembol sayısı: {len(symbols)}")
    random.shuffle(symbols)
    total_scanned = len(symbols)
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    for bi, batch in enumerate(batches, start=1):
        tasks = [check_signals(symbol, timeframe='4h') for symbol in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, res in zip(batch, results):
            if isinstance(res, Exception):
                logger.error(f"Task hata: {sym} -> {repr(res)}")
        logger.info(f"Batch {bi}/{len(batches)} tamam: {len(batch)} sembol işlendi.")
        await asyncio.sleep(INTER_BATCH_SLEEP + random.random() * 0.5)
    elapsed = time.time() - loop_start
    sleep_sec = max(0.0, SCAN_INTERVAL_SEC - elapsed)
    logger.info(f"Tur bitti, {total_scanned} sembol tarandı, {elapsed:.1f}s sürdü, {sleep_sec:.1f}s bekle...")
    await asyncio.sleep(sleep_sec)

# ================== Başlangıç ==================
_state_lock = asyncio.Lock()

async def main():
    logger.info(f"Process started. PID={os.getpid()} TZ={time.tzname} SHARDS={SHARDS} SHARD_INDEX={SHARD_INDEX}")
    logger.info(f"TEST_MODE={TEST_MODE} RATE_LIMIT_MS={RATE_LIMIT_MS}")
    try:
        await load_markets()
        if not TEST_MODE and telegram_bot:
            await telegram_bot.send_message(chat_id=CHAT_ID, text="🟢 Bot başlatıldı.")
        asyncio.create_task(message_sender())
        while True:
            await main_loop() # main_loop tarayıp dinamik bekliyor
    except Exception as e:
        logger.error(f"Main loop hata: {str(e)}")
        if not TEST_MODE and telegram_bot:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=f"🔴 Bot hata: {str(e)}")
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
