import ccxt
import numpy as np
import pandas as pd
from telegram import Bot
import os
import logging
import asyncio
from datetime import datetime
import pytz
import sys

# Sabit değerler
BOT_TOKEN = '7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY'
CHAT_ID = '-1002755412514'
TEST_MODE = False
VOLUME_FILTER = True
VOLUME_MULTIPLIER = 1.2
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 1.0

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

def calculate_ema(closes, span):
    k = 2 / (span + 1)
    ema = np.zeros_like(closes)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = (closes[i] * k) + (ema[i-1] * (1 - k))
    return ema

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return np.zeros(len(closes))
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    if down == 0:
        rs = float('inf') if up > 0 else 0
    else:
        rs = up / down
    rsi = np.zeros_like(closes)
    rsi[:period] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    for i in range(period, len(closes)):
        delta = deltas[i-1]
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        if down == 0:
            rs = float('inf') if up > 0 else 0
        else:
            rs = up / down
        rsi[i] = 100. - 100. / (1. + rs) if rs != float('inf') else 100.
    return rsi

def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * (2 / (ema_length + 1))) + (ema[i-1] * (1 - (2 / (ema_length + 1))))
    return ema

def find_local_extrema(arr, order=4):
    highs = []
    lows = []
    for i in range(order, len(arr) - order):
        left = arr[i-order:i]
        right = arr[i+1:i+order+1]
        if arr[i] > np.max(np.concatenate((left, right))):
            highs.append(i)
        if arr[i] < np.min(np.concatenate((left, right))):
            lows.append(i)
    return np.array(highs), np.array(lows)

def calculate_ema200(closes):
    return calculate_ema(closes, span=200)

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr.iloc[-1]

def calculate_macd(closes, fast=12, slow=26, signal=9):
    ema_fast = pd.Series(closes).ewm(span=fast, min_periods=fast-1).mean()
    ema_slow = pd.Series(closes).ewm(span=slow, min_periods=slow-1).mean()
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm(span=signal, min_periods=signal-1).mean()
    return macd.iloc[-1], macd_signal.iloc[-1]

def calculate_obv(df):
    obv = np.zeros(len(df))
    obv[0] = df['volume'][0]
    for i in range(1, len(df)):
        if df['close'][i] > df['close'][i-1]:
            obv[i] = obv[i-1] + df['volume'][i]
        elif df['close'][i] < df['close'][i-1]:
            obv[i] = obv[i-1] - df['volume'][i]
        else:
            obv[i] = obv[i-1]
    return obv

def volume_filter_check(volumes):
    if len(volumes) < 20:
        return True
    avg_volume = np.mean(volumes[-20:])
    current_volume = volumes[-1]
    return current_volume > avg_volume * VOLUME_MULTIPLIER

def calculate_indicators(df):
    closes = df['close'].values
    df['ema20'] = calculate_ema(closes, span=20)
    df['ema50'] = calculate_ema(closes, span=50)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    df['ema200'] = calculate_ema200(closes)
    df['atr'] = calculate_atr(df)
    df['obv'] = calculate_obv(df)
    macd, macd_signal = calculate_macd(closes)
    df['macd'] = macd
    df['macd_signal'] = macd_signal
    return df

async def check_signals(symbol, timeframe):
    try:
        if TEST_MODE:
            closes = np.abs(np.cumsum(np.random.randn(200))) * 0.05 + 0.3
            highs = closes + np.random.rand(200) * 0.01
            lows = closes - np.random.rand(200) * 0.01
            volumes = np.random.rand(200) * 10000
            ohlcv = [[0, closes[i], highs[i], lows[i], closes[i], volumes[i]] for i in range(200)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.info(f"Test modu: {symbol} {timeframe}")
        else:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=200)
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    break
                except ccxt.RequestTimeout as e:
                    logger.warning(f"Timeout ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except Exception as e:
                    raise

        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None})

        macd, macd_signal = last_row['macd'], last_row['macd_signal']
        
        lookback = 20
        price_slice = df['close'].values[-lookback:]
        ema_slice = df['rsi_ema'].values[-lookback:]
        volume_slice = df['volume'].values[-lookback:]
        price_highs, price_lows = find_local_extrema(price_slice)
        
        bullish = False
        bearish = False
        min_distance = 5

        if len(price_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            if (last_low - prev_low) >= min_distance:
                if price_slice[last_low] < price_slice[prev_low] and ema_slice[last_low] > (ema_slice[prev_low] + EMA_THRESHOLD) and ema_slice[last_low] < RSI_LOW:
                    bullish = True

        if len(price_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            if (last_high - prev_high) >= min_distance:
                if price_slice[last_high] > price_slice[prev_high] and ema_slice[last_high] < (ema_slice[prev_high] - EMA_THRESHOLD) and ema_slice[last_high] > RSI_HIGH:
                    bearish = True

        score = 0
        if bullish:
            score += 40
        if macd > macd_signal:
            score += 20
        if volume_filter_check(df['volume'].values):
            score += 10
        if last_row['obv'] > prev_row['obv']:
            score += 10

        buy_condition = (prev_row['ema20'] <= prev_row['ema50'] and last_row['ema20'] > last_row['ema50']) and \
                        last_row['close'] > last_row['ema200'] and \
                        bullish and \
                        score >= 50

        score = 0
        if bearish:
            score += 40
        if macd < macd_signal:
            score += 20
        if volume_filter_check(df['volume'].values):
            score += 10
        if last_row['obv'] < prev_row['obv']:
            score += 10

        sell_condition = (prev_row['ema20'] >= prev_row['ema50'] and last_row['ema20'] < last_row['ema50']) and \
                         last_row['close'] < last_row['ema200'] and \
                         bearish and \
                         score >= 50

        if VOLUME_FILTER and not volume_filter_check(df['volume'].values):
            logger.info(f"{symbol} {timeframe}: Hacim düşük")
            return

        if buy_condition and current_pos['signal'] != 'buy':
            entry_price = last_row['close']
            atr = last_row['atr']
            tp_price = entry_price + (3 * atr)
            sl_price = entry_price - (1.5 * atr)
            message = f"{symbol} {timeframe}: BUY (LONG) 🚀\nRSI_EMA: {last_row['rsi_ema']:.2f}\nDivergence: Bullish\nMACD: {macd:.4f}\nEntry: {entry_price:.4f}\nTP: {tp_price:.4f}\nSL: {sl_price:.4f}\nScore: {score}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
            signal_cache[key] = {'signal': 'buy', 'entry_price': entry_price, 'tp_price': tp_price, 'sl_price': sl_price}

        elif sell_condition and current_pos['signal'] != 'sell':
            entry_price = last_row['close']
            atr = last_row['atr']
            tp_price = entry_price - (3 * atr)
            sl_price = entry_price + (1.5 * atr)
            message = f"{symbol} {timeframe}: SELL (SHORT) 📉\nRSI_EMA: {last_row['rsi_ema']:.2f}\nDivergence: Bearish\nMACD: {macd:.4f}\nEntry: {entry_price:.4f}\nTP: {tp_price:.4f}\nSL: {sl_price:.4f}\nScore: {score}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
            signal_cache[key] = {'signal': 'sell', 'entry_price': entry_price, 'tp_price': tp_price, 'sl_price': sl_price}

        if current_pos['signal'] == 'buy':
            close_long_condition = (last_row['close'] <= current_pos['sl_price']) or (last_row['close'] >= current_pos['tp_price'])
            if close_long_condition:
                reason = "TP Hit" if last_row['close'] >= current_pos['tp_price'] else "SL Hit"
                message = f"{symbol} {timeframe}: CLOSE LONG 📉 ({reason})\nPrice: {last_row['close']:.4f}\nRSI_EMA: {last_row['rsi_ema']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None}

        elif current_pos['signal'] == 'sell':
            close_short_condition = (last_row['close'] >= current_pos['sl_price']) or (last_row['close'] <= current_pos['tp_price'])
            if close_short_condition:
                reason = "TP Hit" if last_row['close'] <= current_pos['tp_price'] else "SL Hit"
                message = f"{symbol} {timeframe}: CLOSE SHORT 🚀 ({reason})\nPrice: {last_row['close']:.4f}\nRSI_EMA: {last_row['rsi_ema']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None}

    except Exception as e:
        logger.error(f"Hata ({symbol} {timeframe}): {str(e)}")

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