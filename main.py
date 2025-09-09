import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import sys
import os

# ================== Sabit Değerler ==================
# Güvenlik notu: Token/Chat ID'yi ENV'den okuman önerilir.
BOT_TOKEN = os.getenv("BOT_TOKEN", "7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY")
CHAT_ID   = os.getenv("CHAT_ID", "-1002755412514")

TEST_MODE = False

# Diverjans & giriş tarafı (senin mantık)
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 0.5
LOOKBACK_DIVERGENCE = 30
LOOKBACK_CROSSOVER = 10
DIVERGENCE_MIN_DISTANCE = 5
MACD_MODE = "regime"  # "and" | "regime" | "off"

# Volatilite/ATR & TP/SL
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0
TP_MULTIPLIER2 = 3.5
SL_BUFFER = 0.3       # toplam SL ≈ 2.1×ATR

# === Normalize edilmiş % clamp ===
MIN_SL_PCT = 0.006   # %0.6
MAX_SL_PCT = 0.030   # %3.0
MIN_TP_PCT = 0.008   # %0.8
MAX_TP_PCT = 0.040   # %4.0

COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05

# === Crossover EXIT histerezisi (ATR tabanlı) ===
EXIT_HYST_ATR = 0.15

# === Likidite filtresi (opsiyonel) ===
USE_LIQ_FILTER = False
LIQ_ROLL_BARS = 60
LIQ_QUANTILE  = 0.70
LIQ_MIN_DVOL_USD = 0

# ================== Bull-trap filtresi (özellikle LONG'lar) ==================
USE_VOL_SPIKE_TRAP = True
VOL_MA_WINDOW = 20
SPIKE_WINDOW = 60
SPIKE_Z = 3.0
SPIKE_MULTI = 2.0
PRELOW_BARS = 5
PRELOW_MULT = 0.9
UPPER_WICK_MIN = 0.45
BODY_TO_RANGE_MAX = 0.50

# Breakout bypass & follow-through override
BODY_TO_RANGE_BREAKOUT_MIN = 0.60
FOLLOW_THROUGH_LOOKAHEAD   = 2
FOLLOW_THROUGH_WICK_MAX    = 0.35
FOLLOW_THROUGH_VOL_Z_MIN   = 1.5

# === Bull-trap için BB/KC kapısı (gate) ===
USE_TRAP_BBKC_GATE = True
BB_UPPER_PROX = 0.20         # BB üst banda yakınlık eşiği (0→banda yapışık)
KC_REQUIRE_ABOVE = False     # True: close >= kc_upper şartı eklenir (daha seçici)

# Telegram rate-limit
TG_CONCURRENCY = 6

# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(console_handler)
file_handler = logging.FileHandler('bot.log')
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

telegram_bot = Bot(token=BOT_TOKEN)
tg_sem = asyncio.Semaphore(TG_CONCURRENCY)

async def tg_send(text: str):
    try:
        async with tg_sem:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as e:
        logger.error(f"Telegram hata: {e}")

# ================== Util ==================
def clamp(x, lo, hi):
    return max(lo, min(hi, x))

# Pozisyon/Sinyal durumu
signal_cache = {}

# ================== Sembol Keşfi (Bybit USDT linear perp) ==================
def all_bybit_linear_usdt_symbols():
    mkts = exchange.load_markets()
    syms = []
    for s, m in mkts.items():
        if m.get('swap') and m.get('linear') and m.get('quote') == 'USDT' and not m.get('option') and m.get('active', True):
            syms.append(s)
    syms = sorted(set(syms))
    logger.info(f"Bybit USDT linear perp sembol sayısı: {len(syms)}")
    return syms

# ================== İndikatör Fonksiyonları ==================
def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros_like(closes, dtype=np.float64)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = (closes[i] * k) + (ema[i-1] * (1 - k))
    return ema

def calculate_sma(closes, period):
    sma = np.zeros_like(closes, dtype=np.float64)
    for i in range(len(closes)):
        if i < period - 1:
            sma[i] = 0.0
        else:
            sma[i] = np.mean(closes[i-period+1:i+1])
    return sma

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

def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi, dtype=np.float64)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    alpha = 2 / (ema_length + 1)
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * alpha) + (ema[i-1] * (1 - alpha))
    return ema

def calculate_macd(closes, timeframe):
    if timeframe == '1h':
        fast, slow, signal = 8, 17, 9
    else:
        fast, slow, signal = 12, 26, 9
    def ema_inner(x, n):
        k = 2 / (n + 1)
        e = np.zeros_like(x, dtype=np.float64)
        e[0] = x[0]
        for i in range(1, len(x)):
            e[i] = x[i] * k + e[i-1] * (1 - k)
        return e
    ema_fast = ema_inner(closes, fast)
    ema_slow = ema_inner(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema_inner(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

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

def ensure_atr(df, period=14):
    if 'atr' in df.columns:
        return df
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(window=period).mean()
    return df

def get_atr_values(df, lookback_atr=18):
    df = ensure_atr(df, period=14)
    if len(df) < lookback_atr + 2:
        return np.nan, np.nan
    atr_value = float(df['atr'].iloc[-2]) if pd.notna(df['atr'].iloc[-2]) else np.nan
    close_last = float(df['close'].iloc[-2]) if pd.notna(df['close'].iloc[-2]) else np.nan
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) and pd.notna(close_last) and close_last != 0 else np.nan
    return atr_value, avg_atr_ratio

# === BB/KC ===
def calculate_bb(df, period=20, mult=2.0):
    df['bb_mid'] = df['close'].rolling(period).mean()
    df['bb_std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['bb_mid'] + mult * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - mult * df['bb_std']
    return df

def calculate_kc(df, period=20, atr_period=20, mult=1.5):
    df['kc_mid'] = pd.Series(calculate_ema(df['close'].values, period), index=df.index)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr_kc'] = tr.rolling(atr_period).mean()
    df['kc_upper'] = df['kc_mid'] + mult * df['atr_kc']
    df['kc_lower'] = df['kc_mid'] - mult * df['atr_kc']
    return df

# === Wick/Gövde ölçümü ===
def candle_body_wicks(row):
    o, h, l, c = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    return body / rng, upper_wick / rng, lower_wick / rng

# === Robust hacim istatistikleri + vol_ma ===
def calculate_obv_and_volma(df, vol_ma_window=VOL_MA_WINDOW):
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
    df['vol_med'] = vol_s.rolling(SPIKE_WINDOW).median()
    df['vol_mad'] = vol_s.rolling(SPIKE_WINDOW).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True
    )
    denom = (1.4826 * df['vol_mad']).replace(0, np.nan)  # MAD -> sigma
    df['vol_z'] = (vol_s - df['vol_med']) / denom
    return df

def calculate_indicators(df, timeframe):
    if len(df) < 80:
        logger.warning("DF çok kısa, indikatör hesaplanamadı.")
        return None
    # zaman damgasını indexe al
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        df.set_index('timestamp', inplace=True)

    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(closes, timeframe)

    # Likidite metrikleri (opsiyonel)
    df['dvol'] = (df['close'] * df['volume']).astype(float)
    if USE_LIQ_FILTER:
        try:
            df['liq_thr'] = df['dvol'].rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS//2)\
                                      .quantile(LIQ_QUANTILE)
        except Exception:
            df['liq_thr'] = df['dvol'].rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS//2)\
                                      .apply(lambda x: np.quantile(x, LIQ_QUANTILE), raw=True)
        df['liq_ok'] = (df['dvol'] >= df['liq_thr']) & (df['dvol'] >= LIQ_MIN_DVOL_USD)
    else:
        df['liq_ok'] = True

    # BB/KC + ATR + hacim istatistikleri
    df = calculate_bb(df)
    df = calculate_kc(df)
    df = calculate_obv_and_volma(df, vol_ma_window=VOL_MA_WINDOW)
    df = ensure_atr(df, period=14)
    return df

# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe):
    try:
        # Veri
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            max_retries = 3
            df = None
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=max(150, LOOKBACK_ATR + 80))
                    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                    break
                except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
                    logger.warning(f"Timeout/Network ({symbol} {timeframe}), retry {attempt+1}/{max_retries}: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except (ccxt.BadSymbol, ccxt.BadRequest) as e:
                    logger.warning(f"Skip {symbol} {timeframe}: {e.__class__.__name__} - {e}")
                    return
            if df is None or df.empty:
                return

        # İndikatörler
        df = calculate_indicators(df, timeframe)
        if df is None:
            return

        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            logger.warning(f"ATR NaN/Inf ({symbol} {timeframe}), skip.")
            return

        # Likidite filtresi (son kapalı mum)
        liq_ok = bool(df['liq_ok'].iloc[-2])
        closed_candle = df.iloc[-2]

        key = f"{symbol}_{timeframe}"
        # trap state ekliyoruz
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
            'tp1_hit': False, 'tp2_hit': False,
            'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
        })

        # === Diverjans + EMA/SMA giriş (senin mantığın) ===
        lookback_div = LOOKBACK_DIVERGENCE
        price_slice = df['close'].values[-lookback_div-1:-1]
        ema_slice = df['rsi_ema'].values[-lookback_div-1:-1]
        ema13_slice = df['ema13'].values[-lookback_div-1:-1]
        sma34_slice = df['sma34'].values[-lookback_div-1:-1]

        price_highs, price_lows = find_local_extrema(price_slice)
        bullish = False
        bearish = False
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        min_distance = DIVERGENCE_MIN_DISTANCE

        if len(price_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            if (last_low - prev_low) >= min_distance:
                if price_slice[last_low] < price_slice[prev_low] and \
                   ema_slice[last_low] > (ema_slice[prev_low] + EMA_THRESHOLD) and \
                   ema_slice[last_low] < RSI_LOW:
                    bullish = True

        if len(price_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            if (last_high - prev_high) >= min_distance:
                if price_slice[last_high] > price_slice[prev_high] and \
                   ema_slice[last_high] < (ema_slice[prev_high] - EMA_THRESHOLD) and \
                   ema_slice[last_high] > RSI_HIGH:
                    bearish = True

        for i in range(1, LOOKBACK_CROSSOVER + 1):
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and \
               price_slice[-i] > sma34_slice[-i]:
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and \
               price_slice[-i] < sma34_slice[-i]:
                ema_sma_crossover_sell = True

        # MACD filtre
        macd_up = df['macd'].iloc[-2] > df['macd_signal'].iloc[-2]
        macd_down = df['macd'].iloc[-2] < df['macd_signal'].iloc[-2]
        hist_up = df['macd_hist'].iloc[-2] > 0
        hist_down = df['macd_hist'].iloc[-2] < 0

        if   MACD_MODE == "and":    macd_ok_long, macd_ok_short = (macd_up and hist_up), (macd_down and hist_down)
        elif MACD_MODE == "regime": macd_ok_long, macd_ok_short = macd_up, macd_down
        else:                        macd_ok_long, macd_ok_short = True, True

        # ================== BULL-TRAP FİLTRESİ (LONG iptali) ==================
        spike_trap_buy = False
        follow_through_ok = False

        body_r, upper_wick_r, lower_wick_r = candle_body_wicks(closed_candle)
        breakout_bypass = (body_r >= BODY_TO_RANGE_BREAKOUT_MIN and upper_wick_r <= (1.0 - BODY_TO_RANGE_BREAKOUT_MIN))

        # ---- BB/KC gate (opsiyonel) ----
        gate_ok = True
        if USE_TRAP_BBKC_GATE:
            bb_up  = float(closed_candle.get('bb_upper', np.nan))
            bb_mid = float(closed_candle.get('bb_mid',   np.nan))
            kc_up  = float(closed_candle.get('kc_upper', np.nan))
            close_ = float(closed_candle['close'])

            prox_ok = False
            if np.isfinite(bb_up) and np.isfinite(bb_mid) and (bb_up - bb_mid) > 0:
                rel = (bb_up - close_) / (bb_up - bb_mid)  # 0 → banda yapışık
                prox_ok = (rel <= BB_UPPER_PROX)

            kc_ok = True
            if KC_REQUIRE_ABOVE and np.isfinite(kc_up):
                kc_ok = (close_ >= kc_up)

            gate_ok = bool(prox_ok and kc_ok)

        if USE_VOL_SPIKE_TRAP and gate_ok and not breakout_bypass:
            closed = closed_candle
            pre_slice = df['volume'].iloc[-(PRELOW_BARS+2):-2]
            vol_ma = float(closed.get('vol_ma', np.nan)) if pd.notna(closed.get('vol_ma', np.nan)) else np.nan
            pre_ok = False
            if len(pre_slice) >= max(2, PRELOW_BARS - 1) and np.isfinite(vol_ma) and vol_ma > 0:
                pre_ok = (pre_slice.mean() <= PRELOW_MULT * vol_ma)

            vol_now = float(closed['volume'])
            vol_z_val = closed.get('vol_z', np.nan)
            vol_z_ok = (pd.notna(vol_z_val) and np.isfinite(vol_z_val) and float(vol_z_val) >= SPIKE_Z)
            multi_ok = (np.isfinite(vol_ma) and vol_ma > 0 and vol_now >= SPIKE_MULTI * vol_ma)
            spike_ok = bool(vol_z_ok or multi_ok)

            wick_ok = (upper_wick_r >= UPPER_WICK_MIN) and (body_r <= BODY_TO_RANGE_MAX)
            spike_trap_buy = bool(pre_ok and spike_ok and wick_ok)

            # Follow-through override (önceden trap aktifse)
            pos = signal_cache.get(key, {})
            if pos.get('trap_active', False):
                in_window = True
                if pos.get('trap_expire_idx') is not None:
                    in_window = (df.index[-2] <= pos['trap_expire_idx'])
                ft_vol_z = float(vol_z_val) if (pd.notna(vol_z_val) and np.isfinite(vol_z_val)) else np.nan
                if (in_window and
                    pd.notna(pos.get('trap_high')) and
                    closed['close'] > float(pos['trap_high']) and
                    body_r >= BODY_TO_RANGE_BREAKOUT_MIN and
                    upper_wick_r <= FOLLOW_THROUGH_WICK_MAX and
                    (pd.isna(ft_vol_z) or ft_vol_z >= FOLLOW_THROUGH_VOL_Z_MIN)):
                    follow_through_ok = True
                    spike_trap_buy = False
                    pos['trap_active'] = False
                    pos['trap_high'] = None
                    pos['trap_expire_idx'] = None
                    signal_cache[key] = pos

            vz = float(vol_z_val) if (pd.notna(vol_z_val) and np.isfinite(vol_z_val)) else float('nan')
            logger.info(f"{symbol} {timeframe} | TRAP={spike_trap_buy} BYPASS={breakout_bypass} FTHRU={follow_through_ok} GATE={gate_ok} "
                        f"(pre_ok={pre_ok}, spike_ok={spike_ok}, wick_ok={wick_ok}, vol_z={vz:.2f})")
        else:
            logger.info(f"{symbol} {timeframe} | TRAP SKIP (gate_ok={gate_ok}, bypass={breakout_bypass}) body={body_r:.2f} wickU={upper_wick_r:.2f}")

        # Trap state kaydet (follow-through için)
        if spike_trap_buy:
            bar_delta = (df.index[-2] - df.index[-3]) if len(df.index) >= 3 else pd.Timedelta(0)
            signal_cache[key] = {
                **current_pos,
                'trap_active': True,
                'trap_high': float(closed_candle['high']),
                'trap_expire_idx': df.index[-2] + bar_delta * FOLLOW_THROUGH_LOOKAHEAD
            }
        else:
            signal_cache[key] = {**current_pos, **signal_cache.get(key, {})}

        # === BUY/SELL giriş koşulları ===
        buy_condition  = (ema_sma_crossover_buy  and bullish and macd_ok_long  and liq_ok and (not spike_trap_buy))
        sell_condition = (ema_sma_crossover_sell and bearish and macd_ok_short and liq_ok)

        # Pozisyon & zaman
        current_pos = signal_cache.get(key, signal_cache[key])
        current_price = float(df['close'].iloc[-1])
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)

        # Reversal kapanış (zıt sinyal çıkarsa)
        if (buy_condition or sell_condition) and (current_pos['signal'] is not None):
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] != new_signal:
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                await tg_send(
                    f"{symbol} {timeframe}: REVERSAL CLOSE 🔁\n"
                    f"Price: {current_price:.6f}\nP/L: {profit_percent:.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} kapandı\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'remaining_ratio': 1.0,
                    'last_signal_time': None, 'last_signal_type': None, 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                current_pos = signal_cache[key]

        # ================== POZİSYON AÇILIŞI — BUY ==================
        if buy_condition and current_pos['signal'] != 'buy':
            cooldown_active = (
                current_pos['last_signal_time'] and
                current_pos['last_signal_type'] == 'buy' and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES)
            )
            if cooldown_active:
                logger.info(f"{symbol} {timeframe}: BUY atlandı (cooldown)")
            else:
                entry_price = float(closed_candle['close'])
                atr_ratio = (atr_value / entry_price) if (np.isfinite(atr_value) and np.isfinite(entry_price) and entry_price > 0) else np.nan

                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER  # 2.1
                sl_pct = clamp(eff_sl_mult * atr_ratio, MIN_SL_PCT, MAX_SL_PCT)
                sl_price = entry_price * (1 - sl_pct)

                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL ({symbol} {timeframe})")
                    return
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: BUY atlandı (anında SL riski)")
                else:
                    tp1_pct = clamp(TP_MULTIPLIER1 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp2_pct = clamp(TP_MULTIPLIER2 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp1_price = entry_price * (1 + tp1_pct)
                    tp2_price = entry_price * (1 + tp2_pct)

                    current_pos = {
                        'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now, 'last_signal_type': 'buy', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False,
                        'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                    }
                    signal_cache[key] = current_pos
                    await tg_send(
                        f"{symbol} {timeframe}: BUY (LONG) ✅\n"
                        f"Entry:{entry_price:.6f}\nSL:{sl_price:.6f}\nTP1:{tp1_price:.6f}\nTP2:{tp2_price:.6f}\n"
                        f"Time:{now.strftime('%H:%M:%S')}"
                    )

        # ================== POZİSYON AÇILIŞI — SELL ==================
        elif sell_condition and current_pos['signal'] != 'sell':
            cooldown_active = (
                current_pos['last_signal_time'] and
                current_pos['last_signal_type'] == 'sell' and
                (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES)
            )
            if cooldown_active:
                logger.info(f"{symbol} {timeframe}: SELL atlandı (cooldown)")
            else:
                entry_price = float(closed_candle['close'])
                atr_ratio = (atr_value / entry_price) if (np.isfinite(atr_value) and np.isfinite(entry_price) and entry_price > 0) else np.nan

                eff_sl_mult = SL_MULTIPLIER + SL_BUFFER
                sl_pct = clamp(eff_sl_mult * atr_ratio, MIN_SL_PCT, MAX_SL_PCT)
                sl_price = entry_price * (1 + sl_pct)

                if not np.isfinite(entry_price) or not np.isfinite(sl_price):
                    logger.warning(f"Geçersiz giriş/SL ({symbol} {timeframe})")
                    return
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: SELL atlandı (anında SL riski)")
                else:
                    tp1_pct = clamp(TP_MULTIPLIER1 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp2_pct = clamp(TP_MULTIPLIER2 * atr_ratio, MIN_TP_PCT, MAX_TP_PCT)
                    tp1_price = entry_price * (1 - tp1_pct)
                    tp2_price = entry_price * (1 - tp2_pct)

                    current_pos = {
                        'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price,
                        'tp1_price': tp1_price, 'tp2_price': tp2_price,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now, 'last_signal_type': 'sell', 'entry_time': now,
                        'tp1_hit': False, 'tp2_hit': False,
                        'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                    }
                    signal_cache[key] = current_pos
                    await tg_send(
                        f"{symbol} {timeframe}: SELL (SHORT) ✅\n"
                        f"Entry:{entry_price:.6f}\nSL:{sl_price:.6f}\nTP1:{tp1_price:.6f}\nTP2:{tp2_price:.6f}\n"
                        f"Time:{now.strftime('%H:%M:%S')}"
                    )

        # ================== POZİSYON YÖNETİMİ — LONG ==================
        if signal_cache.get(key, {}).get('signal') == 'buy':
            current_pos = signal_cache[key]
            current_price = float(df['close'].iloc[-1])

            # TP1
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # break-even
                current_pos['tp1_hit'] = True
                await tg_send(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\nCur:{current_price:.6f}\nTP1:{current_pos['tp1_price']:.6f}\n"
                    f"P/L:{profit_percent:.2f}%\n%30 kapandı, SL BE'ye alındı\n"
                    f"Kalan:%{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )

            # TP2
            elif not current_pos['tp2_hit'] and current_pos['tp1_hit'] and current_price >= current_pos['tp2_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await tg_send(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\nCur:{current_price:.6f}\nTP2:{current_pos['tp2_price']:.6f}\n"
                    f"P/L:{profit_percent:.2f}%\n%40 kapandı, kalan %30 açık\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )

            # Klasik SL/BE
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                await tg_send(
                    f"{symbol} {timeframe}: STOP LONG ⛔\nPrice:{current_price:.6f}\nP/L:{profit_percent:.2f}%\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos.get('last_signal_time'),
                    'last_signal_type': current_pos.get('last_signal_type'), 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            # === Crossover EXIT (bar kapanışında) ===
            ema13 = float(df['ema13'].iloc[-2])
            sma34 = float(df['sma34'].iloc[-2])
            close_ = float(df['close'].iloc[-2])
            ema_sma_gap = abs(ema13 - sma34)
            exit_ok = (close_ < ema13 < sma34) and (ema_sma_gap >= EXIT_HYST_ATR * float(df['atr'].iloc[-2]))
            if exit_ok:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                await tg_send(
                    f"{symbol} {timeframe}: CROSS EXIT LONG 🔚\nPrice:{current_price:.6f}\nP/L:{profit_percent:.2f}%\n"
                    f"EMA13:{ema13:.6f} < SMA34:{sma34:.6f} (gap {ema_sma_gap:.6f})\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos.get('last_signal_time'),
                    'last_signal_type': current_pos.get('last_signal_type'), 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            signal_cache[key] = current_pos

        # ================== POZİSYON YÖNETİMİ — SHORT ==================
        elif signal_cache.get(key, {}).get('signal') == 'sell':
            current_pos = signal_cache[key]
            current_price = float(df['close'].iloc[-1])

            # TP1
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.3
                current_pos['sl_price'] = current_pos['entry_price']  # BE
                current_pos['tp1_hit'] = True
                await tg_send(
                    f"{symbol} {timeframe}: TP1 Hit 🎯\nCur:{current_price:.6f}\nTP1:{current_pos['tp1_price']:.6f}\n"
                    f"P/L:{profit_percent:.2f}%\n%30 kapandı, SL BE'ye alındı\n"
                    f"Kalan:%{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )

            # TP2
            elif not current_pos['tp2_hit'] and current_pos['tp1_hit'] and current_price <= current_pos['tp2_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] -= 0.4
                current_pos['tp2_hit'] = True
                await tg_send(
                    f"{symbol} {timeframe}: TP2 Hit 🎯🎯\nCur:{current_price:.6f}\nTP2:{current_pos['tp2_price']:.6f}\n"
                    f"P/L:{profit_percent:.2f}%\n%40 kapandı, kalan %30 açık\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )

            # Klasik SL/BE
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                await tg_send(
                    f"{symbol} {timeframe}: STOP SHORT ⛔\nPrice:{current_price:.6f}\nP/L:{profit_percent:.2f}%\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos.get('last_signal_time'),
                    'last_signal_type': current_pos.get('last_signal_type'), 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            # === Crossover EXIT (bar kapanışında) ===
            ema13 = float(df['ema13'].iloc[-2])
            sma34 = float(df['sma34'].iloc[-2])
            close_ = float(df['close'].iloc[-2])
            ema_sma_gap = abs(ema13 - sma34)
            exit_ok = (close_ > ema13 > sma34) and (ema_sma_gap >= EXIT_HYST_ATR * float(df['atr'].iloc[-2]))
            if exit_ok:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                await tg_send(
                    f"{symbol} {timeframe}: CROSS EXIT SHORT 🔚\nPrice:{current_price:.6f}\nP/L:{profit_percent:.2f}%\n"
                    f"EMA13:{ema13:.6f} > SMA34:{sma34:.6f} (gap {ema_sma_gap:.6f})\n"
                    f"Time:{datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                )
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'remaining_ratio': 1.0, 'last_signal_time': current_pos.get('last_signal_time'),
                    'last_signal_type': current_pos.get('last_signal_type'), 'entry_time': None,
                    'tp1_hit': False, 'tp2_hit': False,
                    'trap_active': False, 'trap_high': None, 'trap_expire_idx': None
                }
                return

            signal_cache[key] = current_pos

    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await tg_send("Bot başladı 🟢 " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))

    timeframes = ['4h']
    symbols = all_bybit_linear_usdt_symbols()
    if not symbols:
        logger.error("Sembol listesi boş geldi, bölgesel kısıt/permission olabilir.")
        return

    batch_size = 20
    while True:
        tasks = []
        for timeframe in timeframes:
            for symbol in symbols:
                tasks.append(check_signals(symbol, timeframe))
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(2)
        logger.info("Taramalar tamam, 5 dk bekle...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
