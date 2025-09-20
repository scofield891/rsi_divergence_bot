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
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
import json
import signal

# .env dosyasını yükle
load_dotenv()

# Environment değişkenlerini oku
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TEST_MODE = os.getenv("TEST_MODE", "False").lower() == "true"

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("BOT_TOKEN ve CHAT_ID environment variable'ları eksik!")

# ================== Sabit Değerler ==================
# === TSL YOK ===
LOOKBACK_ATR = 18
SL_MULTIPLIER = 1.8
TP_MULTIPLIER1 = 2.0 # TP1 (%30)
TP_MULTIPLIER2 = 3.5 # TP2 (%40)
SL_BUFFER = 0.3
COOLDOWN_MINUTES = 60
INSTANT_SL_BUFFER = 0.05
# Sinyal toggles
LOOKBACK_CROSSOVER = 10
# === Likidite filtresi ===
USE_LIQ_FILTER = True
LIQ_ROLL_BARS = 60
LIQ_QUANTILE = 0.70
LIQ_MIN_DVOL_USD = 50000
# Telegram rate-limit (eşzamanlı mesaj sayısı)
TG_CONCURRENCY = 6
# ================== Yalın Çekirdek Parametreleri ==================
ADX_THRESHOLD = 20
USE_FROTH_GUARD = True
FROTH_GUARD_K_ATR = 0.9
# Cache dosyası
CACHE_FILE = 'signal_cache.json'
# ================== Logging ==================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = RotatingFileHandler('bot.log', maxBytes=5_000_000, backupCount=3, encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)
# ================== Borsa & Bot ==================
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'linear'},
    'timeout': 60000,
    'rateLimit': 500  # ms cinsinden rate limit ayarı
})
telegram_bot = Bot(token=BOT_TOKEN)
tg_sem = asyncio.Semaphore(TG_CONCURRENCY)
async def tg_send(text: str):
    try:
        async with tg_sem:
            await telegram_bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as e:
        logger.error(f"Telegram hata: {e}")
        await asyncio.sleep(2)
# Pozisyon/Sinyal durumu (dosyadan yükle)
signal_cache = {}
cache_dirty = False  # Debounce için dirty flag
last_flush_time = datetime.now()
def _ser(o):
    if isinstance(o, (datetime, pd.Timestamp)):
        return o.isoformat()
    return o

def save_cache(force=False):
    global last_flush_time, cache_dirty
    now = datetime.now()
    if force or (cache_dirty and (now - last_flush_time) >= timedelta(seconds=5)):
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(signal_cache, f, default=_ser)
            logger.info("Signal cache dosyaya kaydedildi.")
        except Exception as e:
            logger.error(f"Cache kaydetme hata: {e}")
        finally:
            cache_dirty = False
            last_flush_time = now

def _parse_dt(x):
    try:
        return pd.to_datetime(x) if isinstance(x, str) else x
    except Exception:
        return x

def load_cache():
    global signal_cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            raw = json.load(f)
        # alanları geri çevir
        for k, v in raw.items():
            if isinstance(v, dict):
                for fld in ("last_signal_time","entry_time","last_candle_ts"):
                    if fld in v:
                        v[fld] = _parse_dt(v[fld])
        signal_cache = raw
        logger.info("Signal cache dosyadan yüklendi.")
    else:
        logger.info("Signal cache dosyası yok, yeni oluşturulacak.")

load_cache()
# Graceful shutdown
def _graceful(*_):
    save_cache(force=True)
    logger.info("Shutdown - cache kaydedildi.")
    sys.exit(0)
signal.signal(signal.SIGINT, _graceful)
signal.signal(signal.SIGTERM, _graceful)
# ================== Sembol Keşfi (TÜM Bybit USDT linear perp) ==================
def all_bybit_linear_usdt_symbols():
    try:
        mkts = exchange.load_markets()
    except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
        logger.warning(f"load_markets hata: {e}")
        return []
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
def calculate_adx(df, period=14):
    df['up'] = df['high'] - df['high'].shift()
    df['down'] = df['low'].shift() - df['low']
    df['plus_dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0)
    df['minus_dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
    df['tr'] = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
    atr = df['tr'].ewm(span=period, adjust=False).mean()
    df['plus_di'] = 100 * (df['plus_dm'].ewm(span=period, adjust=False).mean() / atr)
    df['minus_di'] = 100 * (df['minus_dm'].ewm(span=period, adjust=False).mean() / atr)
    sum_di = df['plus_di'] + df['minus_di']
    sum_di = np.where(sum_di == 0, np.nan, sum_di)  # 0 bölme önle
    df['dx'] = 100 * abs(df['plus_di'] - df['minus_di']) / sum_di
    df['adx'] = df['dx'].ewm(span=period, adjust=False).mean()
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
    df = calculate_adx(df)
    # Likidite metrikleri
    df['dvol'] = (df['close'] * df['volume']).astype(float)
    if USE_LIQ_FILTER:
        try:
            df['liq_thr'] = df['dvol'].rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS//2)\
                                      .quantile(LIQ_QUANTILE)
        except Exception:
            df['liq_thr'] = df['dvol'].rolling(LIQ_ROLL_BARS, min_periods=LIQ_ROLL_BARS//2)\
                                      .apply(lambda x: np.quantile(x, LIQ_QUANTILE), raw=True)
        # Ekstra NaN kontrolü
        if df['liq_thr'].isna().all():
            df['liq_ok'] = False
        else:
            df['liq_ok'] = (df['dvol'] >= df['liq_thr']) & (df['dvol'] >= LIQ_MIN_DVOL_USD)
            df['liq_ok'] = df['liq_ok'].fillna(False)
    else:
        df['liq_ok'] = True
    return df
# ================== Sinyal Döngüsü ==================
async def check_signals(symbol, timeframe):
    global cache_dirty, signal_cache
    try:
        # Veri
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.02 * closes
            lows = closes - np.random.rand(200) * 0.02 * closes
            volumes = np.random.rand(200) * 10000
            ms_per_bar = 4 * 3600 * 1000  # 4h ms
            ohlcv = [[i * ms_per_bar, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]  # timestamp ekle
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
                except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
                    logger.warning(f"Timeout/Network/Rate ({symbol} {timeframe}), retry {attempt+1}/{max_retries}: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except (ccxt.BadSymbol, ccxt.BadRequest) as e:
                    logger.warning(f"Skip {symbol} {timeframe}: {e.__class__.__name__} - {e}")
                    return
            if df is None or df.empty:
                return
        # Zaman damgasını indexe al
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df.set_index('timestamp', inplace=True)
        # İndikatörler
        if len(df) < max(80, LOOKBACK_CROSSOVER + 5):
            logger.warning(f"{symbol} {timeframe}: DF crossover için kısa, skip.")
            return
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
            'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
            'last_candle_ts': None  # Flood koruma için son mum timestamp
        })
        # Flood koruma: Aynı mumda tekrar giriş yok
        current_candle_ts = df.index[-2]
        if current_pos['last_candle_ts'] == current_candle_ts:
            logger.info(f"{symbol} {timeframe}: Aynı mumda sinyal atlandı (flood koruma)")
            return
        # Trend yönü ve crossover
        ema13_slice = df['ema13'].values[-(LOOKBACK_CROSSOVER+1):-1]
        sma34_slice = df['sma34'].values[-(LOOKBACK_CROSSOVER+1):-1]
        price_slice = df['close'].values[-(LOOKBACK_CROSSOVER+1):-1]
        ema13_last = df['ema13'].iloc[-2]
        sma34_last = df['sma34'].iloc[-2]
        trend_long = ema13_last > sma34_last
        trend_short = ema13_last < sma34_last
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        n = min(len(ema13_slice), len(sma34_slice), len(price_slice))
        for i in range(1, n):  # i: 1..n-1
            e_prev, e_cur = ema13_slice[-i-1], ema13_slice[-i]
            s_prev, s_cur = sma34_slice[-i-1], sma34_slice[-i]
            p_cur = price_slice[-i]
            if (e_prev <= s_prev) and (e_cur > s_cur) and (p_cur > s_cur):
                ema_sma_crossover_buy = True
            if (e_prev >= s_prev) and (e_cur < s_cur) and (p_cur < s_cur):
                ema_sma_crossover_sell = True
        # ADX filtre
        adx_last = df['adx'].iloc[-2]
        plus_di = df['plus_di'].iloc[-2]
        minus_di = df['minus_di'].iloc[-2]
        adx_ok_long = adx_last >= ADX_THRESHOLD and plus_di > minus_di
        adx_ok_short = adx_last >= ADX_THRESHOLD and minus_di > plus_di
        # Froth guard
        close_last = closed_candle['close']
        froth_ok_long = True
        froth_ok_short = True
        if USE_FROTH_GUARD:
            dist_long = (close_last - ema13_last) / atr_value if atr_value != 0 else 0
            dist_short = (ema13_last - close_last) / atr_value if atr_value != 0 else 0
            froth_ok_long = dist_long <= FROTH_GUARD_K_ATR
            froth_ok_short = dist_short <= FROTH_GUARD_K_ATR
        # Mum rengi filtre
        green_candle = closed_candle['close'] > closed_candle['open']
        red_candle = closed_candle['close'] < closed_candle['open']
        logger.info(
            f"{symbol} {timeframe} | "
            f"CrossBuy={ema_sma_crossover_buy}, CrossSell={ema_sma_crossover_sell} | "
            f"TrendLong={trend_long}, TrendShort={trend_short} | "
            f"ADXL={adx_ok_long}, ADXS={adx_ok_short} | "
            f"FrothL={froth_ok_long}, FrothS={froth_ok_short} | "
            f"LIQ_OK={liq_ok} | "
            f"Green={green_candle}, Red={red_candle}"
        )
        buy_condition = ema_sma_crossover_buy and trend_long and adx_ok_long and froth_ok_long and liq_ok and green_candle
        sell_condition = ema_sma_crossover_sell and trend_short and adx_ok_short and froth_ok_short and liq_ok and red_candle
        # === EMA/SMA EXIT kesişimleri (son kapalı mum) ===
        ema_prev = df['ema13'].iloc[-3] if len(df) > 2 else np.nan
        sma_prev = df['sma34'].iloc[-3] if len(df) > 2 else np.nan
        ema_last = df['ema13'].iloc[-2]
        sma_last = df['sma34'].iloc[-2]
        exit_cross_long = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev >= sma_prev) and (ema_last < sma_last)) # bearish cross -> long kapat
        exit_cross_short = (pd.notna(ema_prev) and pd.notna(sma_prev) and pd.notna(ema_last) and pd.notna(sma_last)
                            and (ema_prev <= sma_prev) and (ema_last > sma_last)) # bullish cross -> short kapat
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
                    profit_text = f"Profit: {profit_percent:.2f}%"
                    if profit_percent <= 0:
                        profit_text = f"Loss: {profit_percent:.2f}%"
                else:
                    profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                    message_type = "SHORT REVERSAL CLOSE 🚀" if profit_percent > 0 else "SHORT REVERSAL STOP 📉"
                    profit_text = f"Profit: {profit_percent:.2f}%"
                    if profit_percent <= 0:
                        profit_text = f"Loss: {profit_percent:.2f}%"
                message = (
                    f"{symbol} {timeframe}: {message_type}\n"
                    f"Price: {current_price:.6f}\n"
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
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'last_candle_ts': current_pos['last_candle_ts']
                }
                cache_dirty = True
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
                        'tp2_hit': False,
                        'last_candle_ts': current_candle_ts
                    }
                    signal_cache[key] = current_pos
                    cache_dirty = True
                    save_cache()
                    message = (
                        f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
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
                        'tp2_hit': False,
                        'last_candle_ts': current_candle_ts
                    }
                    signal_cache[key] = current_pos
                    cache_dirty = True
                    save_cache()
                    message = (
                        f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
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
                cache_dirty = True
            # TP1
            if not current_pos['tp1_hit'] and current_price >= current_pos['tp1_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.3)
                current_pos['sl_price'] = current_pos['entry_price'] # Break-even
                current_pos['tp1_hit'] = True
                signal_cache[key] = current_pos
                cache_dirty = True
                save_cache()
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
                signal_cache[key] = current_pos
                cache_dirty = True
                save_cache()
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
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'last_candle_ts': current_pos['last_candle_ts']
                }
                cache_dirty = True
                save_cache()
                return
            # SL tetik
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.6f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.6f}\n"
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
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'last_candle_ts': current_pos['last_candle_ts']
                }
                cache_dirty = True
                save_cache()
                return
            signal_cache[key] = current_pos
            cache_dirty = True
        elif current_pos.get('signal') == 'sell':
            current_price = float(df.iloc[-1]['close'])
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
                cache_dirty = True
            # TP1
            if not current_pos['tp1_hit'] and current_price <= current_pos['tp1_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = max(0.0, current_pos['remaining_ratio'] - 0.3)
                current_pos['sl_price'] = current_pos['entry_price'] # Break-even
                current_pos['tp1_hit'] = True
                signal_cache[key] = current_pos
                cache_dirty = True
                save_cache()
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
                signal_cache[key] = current_pos
                cache_dirty = True
                save_cache()
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
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'last_candle_ts': current_pos['last_candle_ts']
                }
                cache_dirty = True
                save_cache()
                return
            # SL tetik
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.6f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {now.strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.6f}\n"
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
                    'entry_time': None, 'tp1_hit': False, 'tp2_hit': False,
                    'last_candle_ts': current_pos['last_candle_ts']
                }
                cache_dirty = True
                save_cache()
                return
            signal_cache[key] = current_pos
            cache_dirty = True
        save_cache()
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
    shard_size = 10  # Shard (grup) boyutu
    shards = [symbols[i:i + shard_size] for i in range(0, len(symbols), shard_size)]
    while True:
        for timeframe in timeframes:
            for shard in shards:
                tasks = [check_signals(symbol, timeframe) for symbol in shard]
                await asyncio.gather(*tasks)
                await asyncio.sleep(2)  # Shard'lar arası bekle
        logger.info("Taramalar tamam, 2 dk bekle...")
        await asyncio.sleep(120)  # 2 dakika
        save_cache()  # Her döngü sonunda cache kaydet
if __name__ == "__main__":
    asyncio.run(main())
