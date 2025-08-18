import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import logging
import asyncio
from datetime import datetime
import pytz
import sys

# Sabit değerler
BOT_TOKEN = '7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY'
CHAT_ID = '-1002755412514'
TEST_MODE = False
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 0.5
TRAILING_ACTIVATION = 1.0
TRAILING_DISTANCE_BASE = 2.0
TRAILING_DISTANCE_HIGH_VOL = 3.0
VOLATILITY_THRESHOLD = 0.02
LOOKBACK_ATR = 18
SL_MULTIPLIER = 3.0
TP_MULTIPLIER = 5.0
SL_BUFFER = 0.3

# Sinyal toggles
MACD_MODE = "regime"  # "off" | "regime" | "and"
LOOKBACK_DIVERGENCE = 30
DIVERGENCE_MIN_DISTANCE = 5

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

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'timeout': 60000})
telegram_bot = Bot(token=BOT_TOKEN)
signal_cache = {}

# Indikatör fonksiyonları
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
    def ema(x, n):
        k = 2 / (n + 1)
        e = np.zeros_like(x, dtype=np.float64)
        e[0] = x[0]
        for i in range(1, len(x)):
            e[i] = x[i] * k + e[i-1] * (1 - k)
        return e
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def find_local_extrema(arr, order=4):
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
    closes = df['close'].values.astype(np.float64)
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    df['macd'], df['macd_signal'], df['macd_hist'] = calculate_macd(closes, timeframe)
    return df

# ----------------- sinyal döngüsü -----------------
async def check_signals(symbol, timeframe):
    try:
        # Veri
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.01
            lows = closes - np.random.rand(200) * 0.01
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=max(150, LOOKBACK_ATR + 80))
                    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
                    break
                except ccxt.RequestTimeout:
                    logger.warning(f"Timeout ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
        df = calculate_indicators(df, timeframe)
        atr_value, avg_atr_ratio = get_atr_values(df, LOOKBACK_ATR)
        if not np.isfinite(atr_value) or not np.isfinite(avg_atr_ratio):
            return
        last_row = df.iloc[-2]
        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {
            'signal': None, 'entry_price': None, 'sl_price': None, 'tp_price': None,
            'highest_price': None, 'lowest_price': None,
            'trailing_activated': False, 'avg_atr_ratio': None, 'trailing_distance': None,
            'remaining_ratio': 1.0
        })
        # Diverjans + kesişim (son 30 mum)
        lookback = LOOKBACK_DIVERGENCE
        price_slice = df['close'].values[-lookback-1:-1]
        ema_slice = df['rsi_ema'].values[-lookback-1:-1]
        ema13_slice = df['ema13'].values[-lookback-1:-1]
        sma34_slice = df['sma34'].values[-lookback-1:-1]
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
        for i in range(1, lookback):
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and \
               price_slice[-i] > sma34_slice[-i]:  # Crossover mumunda close teyidi
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and \
               price_slice[-i] < sma34_slice[-i]:  # Crossover mumunda close teyidi
                ema_sma_crossover_sell = True
        # MACD filtre (modlu)
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
        else:  # "off"
            macd_ok_long = True
            macd_ok_short = True
        # Log teşhis
        logger.info(
            f"{symbol} {timeframe} | "
            f"DivBull={bullish}, DivBear={bearish} | "
            f"CrossBuy={ema_sma_crossover_buy}, CrossSell={ema_sma_crossover_sell} | "
            f"MACD_MODE={MACD_MODE} (up={macd_up}, hist_up={hist_up}) | "
            f"BUY_OK={'YES' if (ema_sma_crossover_buy and bullish and macd_ok_long) else 'no'} | "
            f"SELL_OK={'YES' if (ema_sma_crossover_sell and bearish and macd_ok_short) else 'no'}"
        )
        buy_condition = ema_sma_crossover_buy and bullish and macd_ok_long
        sell_condition = ema_sma_crossover_sell and bearish and macd_ok_short
        # Sinyal açılışı
        tz = pytz.timezone('Europe/Istanbul')
        if buy_condition and current_pos['signal'] != 'buy':
            entry_price = float(last_row['close'])
            sl_price = entry_price - (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
            tp_price = entry_price + (TP_MULTIPLIER * atr_value)
            trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE)
            signal_cache[key] = {
                'signal': 'buy',
                'entry_price': entry_price,
                'sl_price': sl_price,
                'tp_price': tp_price,
                'highest_price': entry_price,
                'lowest_price': None,
                'trailing_activated': False,
                'avg_atr_ratio': avg_atr_ratio,
                'trailing_distance': trailing_distance,
                'remaining_ratio': 1.0
            }
            message = (
                f"{symbol} {timeframe}: BUY (LONG) 🚀\n"
                f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                f"Divergence: Bullish\n"
                f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP: {tp_price:.4f}\n"
                f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
            )
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
        elif sell_condition and current_pos['signal'] != 'sell':
            entry_price = float(last_row['close'])
            sl_price = entry_price + (SL_MULTIPLIER * atr_value + SL_BUFFER * atr_value)
            tp_price = entry_price - (TP_MULTIPLIER * atr_value)
            trailing_distance = (TRAILING_DISTANCE_HIGH_VOL if avg_atr_ratio > VOLATILITY_THRESHOLD else TRAILING_DISTANCE_BASE)
            signal_cache[key] = {
                'signal': 'sell',
                'entry_price': entry_price,
                'sl_price': sl_price,
                'tp_price': tp_price,
                'highest_price': None,
                'lowest_price': entry_price,
                'trailing_activated': False,
                'avg_atr_ratio': avg_atr_ratio,
                'trailing_distance': trailing_distance,
                'remaining_ratio': 1.0
            }
            message = (
                f"{symbol} {timeframe}: SELL (SHORT) 📉\n"
                f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                f"Divergence: Bearish\n"
                f"Entry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTP: {tp_price:.4f}\n"
                f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
            )
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
        # Pozisyon yönetimi (tam hali)
        current_pos = signal_cache.get(key, current_pos)
        if current_pos['signal'] == 'buy':
            current_price = float(df.iloc[-1]['close'])
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                return
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price
            td = current_pos['trailing_distance']
            if (current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr_value)
                and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"Vol Ratio: {current_pos['avg_atr_ratio']:.4f}\n"
                    f"TSL Distance: {td}\n"
                    f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['highest_price'] - (td * atr_value)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)
            if current_pos['remaining_ratio'] == 1.0 and current_price >= current_pos['tp_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = 0.5
                current_pos['sl_price'] = current_pos['entry_price']
                message = (
                    f"{symbol} {timeframe}: PARAYI VURDUK 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP Vuruldu: {current_pos['tp_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%50 satıldı, kalan %50 TSL ile takip ediliyor\n"
                    f"SL break-even'a çekildi: {current_pos['sl_price']:.4f}\n"
                    f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Partial TP: {message}")
            if current_price <= current_pos['sl_price']:
                profit_percent = ((current_price - current_pos['entry_price']) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: LONG 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP LONG 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                    )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp_price': None,
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0
                }
            signal_cache[key] = current_pos
        elif current_pos['signal'] == 'sell':
            current_price = float(df.iloc[-1]['close'])
            atr_value, _ = get_atr_values(df, LOOKBACK_ATR)
            if not np.isfinite(atr_value):
                return
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price
            td = current_pos['trailing_distance']
            if (current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr_value)
                and not current_pos['trailing_activated']):
                current_pos['trailing_activated'] = True
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                message = (
                    f"{symbol} {timeframe}: TRAILING ACTIVE 🚧\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"Entry Price: {current_pos['entry_price']:.4f}\n"
                    f"New SL: {current_pos['sl_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"Vol Ratio: {current_pos['avg_atr_ratio']:.4f}\n"
                    f"TSL Distance: {td}\n"
                    f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Trailing Activated: {message}")
            if current_pos['trailing_activated']:
                trailing_sl = current_pos['lowest_price'] + (td * atr_value)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)
            if current_pos['remaining_ratio'] == 1.0 and current_price <= current_pos['tp_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                current_pos['remaining_ratio'] = 0.5
                current_pos['sl_price'] = current_pos['entry_price']
                message = (
                    f"{symbol} {timeframe}: PARAYI VURDUK 🚀\n"
                    f"Current Price: {current_price:.4f}\n"
                    f"TP Vuruldu: {current_pos['tp_price']:.4f}\n"
                    f"Profit: {profit_percent:.2f}%\n"
                    f"%50 satıldı, kalan %50 TSL ile takip ediliyor\n"
                    f"SL break-even'a çekildi: {current_pos['sl_price']:.4f}\n"
                    f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Partial TP: {message}")
            if current_price >= current_pos['sl_price']:
                profit_percent = ((current_pos['entry_price'] - current_price) / current_pos['entry_price']) * 100
                if profit_percent > 0:
                    message = (
                        f"{symbol} {timeframe}: SHORT 🚀\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                        f"Profit: {profit_percent:.2f}%\nPARAYI VURDUK 🚀\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                    )
                else:
                    message = (
                        f"{symbol} {timeframe}: STOP SHORT 📉\n"
                        f"Price: {current_price:.4f}\n"
                        f"RSI_EMA: {last_row['rsi_ema']:.2f}\n"
                        f"Loss: {profit_percent:.2f}%\nSTOP 😞\n"
                        f"Kalan %{current_pos['remaining_ratio']*100:.0f} satıldı\n"
                        f"Time: {datetime.now(tz).strftime('%H:%M:%S')}"
                    )
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {
                    'signal': None, 'entry_price': None, 'sl_price': None, 'tp_price': None,
                    'highest_price': None, 'lowest_price': None, 'trailing_activated': False,
                    'avg_atr_ratio': None, 'trailing_distance': None, 'remaining_ratio': 1.0
                }
            signal_cache[key] = current_pos
        except Exception as e:
            logger.error(f"Hata ({symbol} {timeframe}): {str(e)}")

# Main
async def main():
    tz = pytz.timezone('Europe/Istanbul')
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    timeframes = ['1h', '2h', '4h']
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'ADAUSDT', 'SUIUSDT', 'WIFUSDT',
        'ENAUSDT', 'PENGUUSDT', '1000BONKUSDT', 'HYPEUSDT', 'AVAXUSDT', 'MOODENGUSDT', 'LINKUSDT', 'PUMPFUNUSDT', 'LTCUSDT', 'TRUMPUSDT',
        'AAVEUSDT', 'ARBUSDT', 'NEARUSDT', 'ONDOUSDT', 'POPCATUSDT', 'TONUSDT', 'OPUSDT', '1000FLOKIUSDT', 'SEIUSDT', 'HBARUSDT',
        'WLDUSDT', 'BNBUSDT', 'UNIUSDT', 'XLMUSDT', 'CRVUSDT', 'VIRTUALUSDT', 'AI16ZUSDT', 'TIAUSDT', 'TAOUSDT', 'APTUSDT',
        'DOTUSDT', 'SPXUSDT', 'ETCUSDT', 'LDOUSDT', 'BCHUSDT', 'INJUSDT', 'KASUSDT', 'ALGOUSDT', 'TRXUSDT', 'IPUSDT',
        'FILUSDT', 'STXUSDT', 'ATOMUSDT', 'RUNEUSDT', 'THETAUSDT', 'FETUSDT', 'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'CHZUSDT',
        'APEUSDT', 'GALAUSDT', 'IMXUSDT', 'DYDXUSDT', 'GMTUSDT', 'EGLDUSDT', 'ZKUSDT', 'NOTUSDT', 'ENSUSDT', 'JUPUSDT',
        'ATHUSDT', 'ICPUSDT', 'STRKUSDT', 'ORDIUSDT', 'PENDLEUSDT', 'PNUTUSDT', 'RENDERUSDT', 'OMUSDT', 'ZORAUSDT', 'SUSDT',
        'GRASSUSDT', 'TRBUSDT', 'MOVEUSDT', 'XAUTUSDT', 'POLUSDT', 'CVXUSDT', 'BRETTUSDT', 'SAROSUSDT', 'GOATUSDT', 'AEROUSDT',
        'JTOUSDT', 'HYPERUSDT', 'ETHFIUSDT', 'BERAUSDT'
    ]
    while True:
        tasks = []
        for timeframe in timeframes:
            for symbol in symbols:
                tasks.append(check_signals(symbol, timeframe))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(1)
        logger.info(f"Taramalar tamam, 5 dk bekle...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())