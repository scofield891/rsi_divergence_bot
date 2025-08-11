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
RSI_LOW = 40
RSI_HIGH = 60
EMA_THRESHOLD = 1.0
TRAILING_ACTIVATION = 1.0  # 1x ATR kârda trailing SL devreye girer
TRAILING_DISTANCE = 2.0    # 2x ATR geri çekilmeye izin verir
SL_MULTIPLIER = 5.0        # Stop Loss için 5x ATR çarpanı

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

def calculate_sma(closes, period):
    sma = np.zeros_like(closes)
    for i in range(len(closes)):
        if i < period - 1:
            sma[i] = 0
        else:
            sma[i] = np.mean(closes[i-period+1:i+1])
    return sma

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

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr.iloc[-1]

def calculate_indicators(df):
    closes = df['close'].values
    df['ema13'] = calculate_ema(closes, span=13)
    df['sma34'] = calculate_sma(closes, period=34)
    df['rsi'] = calculate_rsi(closes)
    df['rsi_ema'] = calculate_rsi_ema(df['rsi'])
    atr = calculate_atr(df)
    return df, atr

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

        df, atr = calculate_indicators(df)
        last_row = df.iloc[-2]  # Kapanmış mum
        prev_row = df.iloc[-3]

        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {'signal': None, 'entry_price': None, 'sl_price': None, 'highest_price': None, 'lowest_price': None})

        lookback = 20
        price_slice = df['close'].values[-lookback-1:-1]
        ema_slice = df['rsi_ema'].values[-lookback-1:-1]
        ema13_slice = df['ema13'].values[-lookback-1:-1]
        sma34_slice = df['sma34'].values[-lookback-1:-1]
        price_highs, price_lows = find_local_extrema(price_slice)
        
        bullish = False
        bearish = False
        ema_sma_crossover_buy = False
        ema_sma_crossover_sell = False
        min_distance = 5

        # Divergence kontrolü (son 20 mum)
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

        # EMA13+SMA34 kesişim kontrolü (son 20 mum, mum kapanışında teyit)
        for i in range(1, lookback):
            if ema13_slice[-i-1] <= sma34_slice[-i-1] and ema13_slice[-i] > sma34_slice[-i] and df['close'].values[-i-1] > sma34_slice[-i]:
                ema_sma_crossover_buy = True
            if ema13_slice[-i-1] >= sma34_slice[-i-1] and ema13_slice[-i] < sma34_slice[-i] and df['close'].values[-i-1] < sma34_slice[-i]:
                ema_sma_crossover_sell = True

        logger.info(f"{symbol} {timeframe}: Divergence: {bullish or bearish}, EMA13+SMA34 Kesişim: {ema_sma_crossover_buy or ema_sma_crossover_sell}")

        buy_condition = ema_sma_crossover_buy and bullish
        sell_condition = ema_sma_crossover_sell and bearish

        if buy_condition and current_pos['signal'] != 'buy':
            entry_price = last_row['close']
            sl_price = entry_price - (SL_MULTIPLIER * atr)  # 5x ATR
            message = f"{symbol} {timeframe}: BUY (LONG) 🚀\nRSI_EMA: {last_row['rsi_ema']:.2f}\nDivergence: Bullish\nEntry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
            signal_cache[key] = {'signal': 'buy', 'entry_price': entry_price, 'sl_price': sl_price, 'highest_price': entry_price, 'lowest_price': None}

        elif sell_condition and current_pos['signal'] != 'sell':
            entry_price = last_row['close']
            sl_price = entry_price + (SL_MULTIPLIER * atr)  # 5x ATR
            message = f"{symbol} {timeframe}: SELL (SHORT) 📉\nRSI_EMA: {last_row['rsi_ema']:.2f}\nDivergence: Bearish\nEntry: {entry_price:.4f}\nSL: {sl_price:.4f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            logger.info(f"Sinyal: {message}")
            signal_cache[key] = {'signal': 'sell', 'entry_price': entry_price, 'sl_price': sl_price, 'highest_price': None, 'lowest_price': entry_price}

        # Trailing & Kapanış Mantığı (TP'siz, sadece SL/TSL)
        if current_pos['signal'] == 'buy':
            current_price = df.iloc[-1]['close']  # Güncel fiyat için son mum
            # En yüksek fiyat güncelle
            if current_pos['highest_price'] is None or current_price > current_pos['highest_price']:
                current_pos['highest_price'] = current_price

            # Trailing SL kârda 1x ATR ilerlerse devreye girer
            if current_price >= current_pos['entry_price'] + (TRAILING_ACTIVATION * atr):
                trailing_sl = current_pos['highest_price'] - (TRAILING_DISTANCE * atr)
                current_pos['sl_price'] = max(current_pos['sl_price'], trailing_sl)

            # Kapanış: sadece SL/TSL
            close_long_condition = (current_price <= current_pos['sl_price'])
            if close_long_condition:
                reason = "SL Hit"
                message = f"{symbol} {timeframe}: CLOSE LONG 📉 ({reason})\nPrice: {current_price:.4f}\nRSI_EMA: {last_row['rsi_ema']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'sl_price': None, 'highest_price': None, 'lowest_price': None}

        elif current_pos['signal'] == 'sell':
            current_price = df.iloc[-1]['close']  # Güncel fiyat için son mum
            # En düşük fiyat güncelle
            if current_pos['lowest_price'] is None or current_price < current_pos['lowest_price']:
                current_pos['lowest_price'] = current_price

            # Trailing SL kârda 1x ATR ilerlerse devreye girer
            if current_price <= current_pos['entry_price'] - (TRAILING_ACTIVATION * atr):
                trailing_sl = current_pos['lowest_price'] + (TRAILING_DISTANCE * atr)
                current_pos['sl_price'] = min(current_pos['sl_price'], trailing_sl)

            # Kapanış: sadece SL/TSL
            close_short_condition = (current_price >= current_pos['sl_price'])
            if close_short_condition:
                reason = "SL Hit"
                message = f"{symbol} {timeframe}: CLOSE SHORT 🚀 ({reason})\nPrice: {current_price:.4f}\nRSI_EMA: {last_row['rsi_ema']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Exit: {message}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'sl_price': None, 'highest_price': None, 'lowest_price': None}

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