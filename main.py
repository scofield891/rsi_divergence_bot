import ccxt
import time
import asyncio
from telegram import Bot
import numpy as np
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
import pytz

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
RSI_LOW = float(os.getenv('RSI_LOW', 40))
RSI_HIGH = float(os.getenv('RSI_HIGH', 60))
TEST_MODE = os.getenv('TEST_MODE', 'False').lower() == 'true'
VOLUME_FILTER = os.getenv('VOLUME_FILTER', 'False').lower() == 'true'
VOLUME_MULTIPLIER = float(os.getenv('VOLUME_MULTIPLIER', 1.2))
EMA_THRESHOLD = float(os.getenv('EMA_THRESHOLD', 1.0))

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Telegram logging susturma
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'verbose': False, 'timeout': 60000})
telegram_bot = Bot(token=BOT_TOKEN)
signal_cache = {}

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

def volume_filter_check(volumes):
    if len(volumes) < 20:
        return True
    avg_volume = np.mean(volumes[-20:])
    current_volume = volumes[-1]
    return current_volume > avg_volume * VOLUME_MULTIPLIER

async def check_divergence(symbol, timeframe):
    try:
        if TEST_MODE:
            closes = np.random.rand(100) * 100
            volumes = np.random.rand(100) * 10000
            logging.info(f"Test modu: {symbol} {timeframe} için dummy data kullanıldı")
        else:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
                    closes = np.array([x[4] for x in ohlcv])
                    volumes = np.array([x[5] for x in ohlcv])
                    break
                except ccxt.RequestTimeout as e:
                    logging.warning(f"Timeout ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except Exception as e:
                    raise

        rsi = calculate_rsi(closes, 14)
        rsi_ema = calculate_rsi_ema(rsi, 14)
        rsi_ema2 = np.roll(rsi_ema, 1)
        ema_color = 'lime' if rsi_ema[-1] > rsi_ema2[-1] else 'red'
        lookback = 50
        if len(closes) < lookback:
            return
        price_slice = closes[-lookback:]
        ema_slice = rsi_ema[-lookback:]
        volume_slice = volumes[-lookback:]
        price_highs, price_lows = find_local_extrema(price_slice)
        bullish = False
        bearish = False
        min_distance = 5
        if len(price_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            if (last_low - prev_low) >= min_distance:
                if price_slice[last_low] < price_slice[prev_low] and ema_slice[last_low] > (ema_slice[prev_low] + EMA_THRESHOLD):
                    bullish = True
        if len(price_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            if (last_high - prev_high) >= min_distance:
                if price_slice[last_high] > price_slice[prev_high] and ema_slice[last_high] < (ema_slice[prev_high] - EMA_THRESHOLD):
                    bearish = True
        logging.info(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI_EMA: {rsi_ema[-1]:.2f}, Color: {ema_color}")
        key = f"{symbol} {timeframe}"
        last_signal = signal_cache.get(key, (False, False))
        if (bullish or bearish) and (bullish, bearish) != last_signal:
            if (bullish and rsi_ema[-1] < RSI_LOW and ema_color == 'red') or (bearish and rsi_ema[-1] > RSI_HIGH and ema_color == 'lime'):
                if VOLUME_FILTER and not volume_filter_check(volume_slice):
                    logging.info(f"{symbol} {timeframe}: Hacim düşük, sinyal filtrelandı")
                    return
                rsi_str = f"{rsi_ema[-1]:.2f}"
                current_price = f"{closes[-1]:.2f}"
                tz = pytz.timezone('Europe/Istanbul')
                timestamp = datetime.now(tz).strftime('%H:%M:%S')
                if bullish:
                    message = f"{symbol} {timeframe}\nPozitif Uyumsuzluk: {bullish} 🚀 (Price LL, EMA HL)\nRSI_EMA: {rsi_str} ({ema_color.upper()})\nCurrent Price: {current_price} USDT\nSaat: {timestamp}"
                    try:
                        await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                        logging.info(f"Sinyal gönderildi: {message}")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.error(f"Telegram hata: {str(e)}")
                if bearish:
                    message = f"{symbol} {timeframe}\nNegatif Uyumsuzluk: {bearish} 📉 (Price HH, EMA LH)\nRSI_EMA: {rsi_str} ({ema_color.upper()})\nCurrent Price: {current_price} USDT\nSaat: {timestamp}"
                    try:
                        await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                        logging.info(f"Sinyal gönderildi: {message}")
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logging.error(f"Telegram hata: {str(e)}")
                signal_cache[key] = (bullish, bearish)
    except Exception as e:
        logging.error(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    tz = pytz.timezone('Europe/Istanbul')
    try:
        await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    except Exception as e:
        logging.error(f"Telegram başlatma hatası: {str(e)}")
    
    timeframes = ['30m', '1h', '2h', '4h']
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'ADAUSDT', 'SUIUSDT', 'WIFUSDT', 'ENAUSDT', 'PENGUUSDT', '1000BONKUSDT', 'HYPEUSDT', 'AVAXUSDT', 'MOODENGUSDT', 'LINKUSDT', 'PUMPFUNUSDT', 'LTCUSDT', 'TRUMPUSDT', 'AAVEUSDT', 'ARBUSDT', 'NEARUSDT', 'ONDOUSDT', 'POPCATUSDT', 'TONUSDT', 'OPUSDT', '1000FLOKIUSDT', 'SEIUSDT', 'HBARUSDT', 'WLDUSDT', 'BNBUSDT', 'UNIUSDT', 'XLMUSDT', 'CRVUSDT', 'VIRTUALUSDT', 'AI16ZUSDT', 'TIAUSDT', 'TAOUSDT', 'APTUSDT', 'DOTUSDT', 'SPXUSDT', 'ETCUSDT', 'LDOUSDT', 'BCHUSDT', 'INJUSDT', 'KASUSDT', 'ALGOUSDT', 'TRXUSDT', 'IPUSDT',
        'FILUSDT', 'STXUSDT', 'ATOMUSDT', 'RUNEUSDT', 'THETAUSDT', 'FETUSDT', 'AXSUSDT', 'SANDUSDT', 'MANAUSDT', 'CHZUSDT', 'APEUSDT', 'GALAUSDT', 'IMXUSDT', 'DYDXUSDT', 'GMTUSDT', 'EGLDUSDT', 'ZKUSDT', 'NOTUSDT',
        'ENSUSDT', 'JUPUSDT', 'ATHUSDT', 'ICPUSDT', 'STRKUSDT', 'ORDIUSDT', 'PENDLEUSDT', 'PNUTUSDT', 'RENDERUSDT', 'OMUSDT', 'ZORAUSDT', 'SUSDT', 'GRASSUSDT', 'TRBUSDT', 'MOVEUSDT', 'XAUTUSDT', 'POLUSDT', 'CVXUSDT', 'BRETTUSDT', 'SAROSUSDT', 'GOATUSDT', 'AEROUSDT', 'JTOUSDT', 'HYPERUSDT', 'ETHFIUSDT', 'BERAUSDT'
    ]
    while True:
        tasks = []
        for timeframe in timeframes:
            for symbol in symbols:
                tasks.append(check_divergence(symbol, timeframe))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(1)
        logging.info(f"Tüm taramalar tamamlandı, {len(tasks)} task işlendi, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())