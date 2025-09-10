import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import logging
import asyncio
from datetime import datetime, timedelta
import pytz
import sys

# ================== Sabit Değerler ==================
BOT_TOKEN = "7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY"
CHAT_ID = '-1002755412514'

TEST_MODE = False

RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 0.5

# === TSL YOK ===

LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0  # TP1 (%30)
TP_MULTIPLIER2 = 3.5  # TP2 (%40)
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05

# Sinyal toggles
MACD_MODE = "regime"
LOOKBACK_DIVERGENCE = 30
LOOKBACK_CROSSOVER = 10
DIVERGENCE_MIN_DISTANCE = 5

# === Likidite filtresi (opsiyonel) ===
USE_LIQ_FILTER = False
LIQ_ROLL_BARS = 60
LIQ_QUANTILE  = 0.70
LIQ_MIN_DVOL_USD = 0

# Telegram rate-limit (eşzamanlı mesaj sayısı)
TG_CONCURRENCY = 6

# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
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

# Pozisyon/Sinyal durumu
signal_cache = {}

# ================== Sembol Keşfi (TÜM Bybit USDT linear perp) ==================
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
    atr_value = float(df['atr'].iloc[-2])
    close_last = float(df['close'].iloc[-2])
    atr_series = df['atr'].iloc[-(lookback_atr+1):-1]
    avg_atr_ratio = float(atr_series.mean() / close_last) if len(atr_series) else np.nan
    return atr_value, avg_atr_ratio

def calculate_indicators(df, timeframe):
    if len(df) < 80:
        logger.warning("DF çok kısa, indikatör hesaplanamadı.")
        return None
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

        # Likidite filtresi (son kapalı mumda)
        liq_ok = bool(df['liq_ok'].iloc[-2])

        closed_candle = df.iloc[-2]
        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
            'highest_price': None, 'lowest_price': None,
            'avg_atr_ratio': None, 'remaining_ratio': 1.0,
            'last_signal_time': None, 'last_signal_type': None,
            'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
        })

        # Diverjans + giriş kesişimi
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

        if MACD_MODE == "and":
            macd_ok_long = macd_up and hist_up
            macd_ok_short = macd_down and hist_down
        elif MACD_MODE == "regime":
            macd_ok_long = macd_up
            macd_ok_short = macd_down
        else:
            macd_ok_long = True
            macd_ok_short = True

        logger.info(
            f"{symbol} {timeframe} | "
            f"DivBull={bullish}, DivBear={bearish} | "
            f"CrossBuy={ema_sma_crossover_buy}, CrossSell={ema_sma_crossover_sell} | "
            f"MACD_MODE={MACD_MODE} (up={macd_up}, hist_up={hist_up}) | "
            f"LIQ_OK={liq_ok}"
        )

        buy_condition  = ema_sma_crossover_buy  and bullish and macd_ok_long  and liq_ok
        sell_condition = ema_sma_crossover_sell and bearish and macd_ok_short and liq_ok

        # === EMA/SMA EXIT kesişimleri (son kapalı mum) ===
        ema_prev, sma_prev = df['ema13'].iloc[-3], df['sma34'].iloc[-3]
        ema_last, sma_last = df['ema13'].iloc[-2], df['sma34'].iloc[-2]
        exit_cross_long  = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev >= sma_prev) and (ema_last < sma_last))   # bearish cross -> long kapat
        exit_cross_short = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev <= sma_prev) and (ema_last > sma_last))   # bullish cross -> short kapat

        # Pozisyon yönetimi (önce reversal check)
        current_pos = signal_cache.get(key, current_pos)
        current_price = float(df.iloc[-1]['close'])
        tz = pytz.timezone('Europe/Istanbul')
        now = datetime.now(tz)

        if buy_condition or sell_condition:
            new_signal = 'buy' if buy_condition else 'sell'
            if current_pos['signal'] is not None and current_pos['signal'] != new_signal:
                # Reversal close
                if current_pos['signal'] == 'buy':
                    profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                    message_type = "LONG REVERSAL CLOSE 🚀" if profit_percent > 0 else "LONG REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                    message_type = "SHORT REVERSAL CLOSE 🚀" if profit_percent > 0 else "SHORT REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%" if profit_percent > 0 else f"Loss: {profit_percent:.2f}%"

                message = (
                    f"{symbol} {timeframe}: {message_type}\n"
                    f"Price: {current_price:.6f}\n"
                    f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                    f"{profit_text}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (reversal)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)

                # Reset state (cooldown bilgisini koru)
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                current_pos = signal_cache[key]

        # Sinyal açılışı
        if buy_condition and current_pos['signal'] != 'buy':
            if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'buy' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                logger.info(f"{symbol} {timeframe}: BUY atlandı (cooldown)")
            else:
                entry_price = float(closed_candle['close'])
                sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                if current_price <= sl_price + INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: BUY atlandı (anında SL riski)")
                else:
                    tp1_price = entry_price + (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price + (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'buy',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': entry_price,
                        'lowest_price': None,
                        'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'buy',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    message = (
                        f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Divergence: Bullish\n"
                        f"Entry: {entry_price:.6f}\nSL: {sl_price:.6f}\nTP1: {tp1_price:.6f}\nTP2: {tp2_price:.6f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await tg_send(message)

        elif sell_condition and current_pos['signal'] != 'sell':
            if current_pos['last_signal_time'] and current_pos['last_signal_type'] == 'sell' and (now - current_pos['last_signal_time']) < timedelta(minutes=COOLDOWN_MINUTES):
                logger.info(f"{symbol} {timeframe}: SELL atlandı (cooldown)")
            else:
                entry_price = float(closed_candle['close'])
                sl_price = entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
                if current_price >= sl_price - INSTANT_SL_BUFFER * atr_value:
                    logger.info(f"{symbol} {timeframe}: SELL atlandı (anında SL riski)")
                else:
                    tp1_price = entry_price - (TP_MULTIPLIER1 * atr_value)
                    tp2_price = entry_price - (TP_MULTIPLIER2 * atr_value)
                    current_pos = {
                        'signal': 'sell',
                        'entry_price': entry_price,
                        'sl_price': sl_price,
                        'tp1_price': tp1_price,
                        'tp2_price': tp2_price,
                        'highest_price': None,
                        'lowest_price': entry_price,
                        'avg_atr_ratio': avg_atr_ratio,
                        'remaining_ratio': 1.0,
                        'last_signal_time': now,
                        'last_signal_type': 'sell',
                        'entry_time': now,
                        'tp1_hit': False,
                        'tp2_hit': False
                    }
                    signal_cache[key] = current_pos
                    message = (
                        f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Divergence: Bearish\n"
                        f"Entry: {entry_price:.6f}\nSL: {sl_price:.6f}\nTP1: {tp1_price:.6f}\nTP2: {tp2_price:.6f}\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                    await tg_send(message)

        # === Pozisyon yönetimi ===
        if current_pos.get('signal') == 'buy':
            current_price = float(df.iloc[-1]['close'])
            # rapor amaçlı
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price

            # TP1
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.3)
                current_pos['sl_price'] = current_pos['entry_price']  # Break-even
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP1 Hit 🚀\n"
                    f"Cur: {current_price:.6f}\n"
                    f"TP1: {current_pos['tp1_price']:.6f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%30 satıldı, SL entry'ye çekildi: {current_pos['sl_price']:.6f}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)

            # TP2
            elif not current_pos['tp2_hit'] and current_price >= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.4)
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\n"
                    f"Cur: {current_price:.6f}\n"
                    f"TP2: {current_pos['tp2_price']:.6f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%40 satıldı, kalan %30 EMA/SMA çıkışına kadar\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)

            # ❗ EMA/SMA exit (bearish cross)
            if exit_cross_long:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol} {timeframe}: EMA/SMA EXIT (LONG) 🔁\n"
                    f"Price: {current_price:.6f}\n"
                    f"{'Profit:' if profit_percent >= 0 else 'Loss:'} {profit_percent:.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (kesişim çıkışı)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return

            # SL tetik
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.6f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.6f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await tg_send(message)
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return

            signal_cache[key] = current_pos

        elif current_pos.get('signal') == 'sell':
            current_price = float(df.iloc[-1]['close'])
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price

            # TP1
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.3)
                current_pos['sl_price'] = current_pos['entry_price']  # Break-even
                current_pos['tp1_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP1 Hit 🚀\n"
                    f"Cur: {current_price:.6f}\n"
                    f"TP1: {current_pos['tp1_price']:.6f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%30 satıldı, SL entry'ye çekildi: {current_pos['sl_price']:.6f}\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)

            # TP2
            elif not current_pos['tp2_hit'] and current_price <= current_pos['tp2_price'] and current_pos['tp1_hit']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.4)
                current_pos['tp2_hit'] = True
                message = (
                    f"{symbol} {timeframe}: TP2 Hit 🚀\n"
                    f"Cur: {current_price:.6f}\n"
                    f"TP2: {current_pos['tp2_price']:.6f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%40 satıldı, kalan %30 EMA/SMA çıkışına kadar\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)

            # ❗ EMA/SMA exit (bullish cross)
            if exit_cross_short:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol} {timeframe}: EMA/SMA EXIT (SHORT) 🔁\n"
                    f"Price: {current_price:.6f}\n"
                    f"{'Profit:' if profit_percent >= 0 else 'Loss:'} {profit_percent:.2f}%\n"
                    f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı (kesişim çıkışı)\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                await tg_send(message)
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return

            # SL tetik
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.6f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.6f}\n"
                        f"RSI_EMA: {closed_candle['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                await tg_send(message)
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp1_price': None, 'tp2_price': None,
                    'highest_price': None, 'lowest_price': None,
                    'avg_atr_ratio': None, 'remaining_ratio': 1.0,
                    'last_signal_time': current_pos['last_signal_time'],
                    'last_signal_type': current_pos['last_signal_type'],
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False
                }
                return

            signal_cache[key] = current_pos

    except Exception as e:
        logger.exception(f"Hata ({symbol} {timeframe}): {str(e)}")
        return

# ================== Main ==================
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await tg_send("Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))

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
