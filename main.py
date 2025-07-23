import ccxt
import time
import asyncio
from telegram import Bot
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}})

telegram_bot = Bot(token=BOT_TOKEN)

signal_cache = {}  # Duplicate önleme

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return np.zeros(len(closes))
    deltas = np.diff(closes)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(closes)
    rsi[:period] = 100. - 100. / (1. + rs)

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
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)

    return rsi

def calculate_ema(closes, period):
    ema = np.zeros(len(closes))
    if len(closes) == 0:
        return ema
    ema[0] = closes[0]
    multiplier = 2 / (period + 1)
    for i in range(1, len(closes)):
        ema[i] = closes[i] * multiplier + ema[i-1] * (1 - multiplier)
    return ema

def calculate_volume_average(volumes, period=14):
    if len(volumes) < period:
        return 0
    return np.mean(volumes[-period:])

def calculate_bb_kc(closes, highs, lows, length_bb=20, mult_bb=2.0, length_kc=20, mult_kc=1.5, use_tr=True):
    if len(closes) < max(length_bb, length_kc):
        return False, False, False

    basis = np.roll(np.cumsum(closes) / length_bb, -1)
    dev = mult_bb * np.roll(np.std(closes), -1)
    upper_bb = basis + dev
    lower_bb = basis - dev

    ma = np.roll(np.cumsum(closes) / length_kc, -1)
    if use_tr:
        range_val = np.maximum(highs - lows, np.abs(highs - np.roll(closes, 1)), np.abs(lows - np.roll(closes, 1)))
    else:
        range_val = highs - lows
    rangema = np.roll(np.cumsum(range_val) / length_kc, -1)
    upper_kc = ma + rangema * mult_kc
    lower_kc = ma - rangema * mult_kc

    sqz_on = (lower_bb > lower_kc) & (upper_bb < upper_kc)
    sqz_off = (lower_bb < lower_kc) & (upper_bb > upper_kc)
    no_sqz = ~sqz_on & ~sqz_off

    return sqz_on[-1], sqz_off[-1], no_sqz[-1]

def calculate_squeeze_momentum(closes, sqz_on, sqz_off, no_sqz, length_kc=20):
    if len(closes) < length_kc:
        return 0, 'gray', 'gray'
    avg_hlc = np.roll(np.cumsum(closes) / length_kc, -1)
    avg_sma = np.roll(np.cumsum(closes) / length_kc, -1)
    val = np.roll(closes - (avg_hlc + avg_sma) / 2, -length_kc)
    bcolor = 'lime' if val[-1] > 0 and val[-1] > val[-2] else 'green' if val[-1] > 0 else 'red' if val[-1] < val[-2] else 'maroon'
    scolor = 'blue' if no_sqz else 'black' if sqz_on else 'gray'
    return val[-1], bcolor, scolor

async def check_signals(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=30)  # 30 mum geri döndü
        if not ohlcv or len(ohlcv) < 30:
            print(f"Uyarı ({symbol} {timeframe}): Yetersiz veri, ohlcv uzunluğu: {len(ohlcv)}")
            return
        closes = np.array([x[4] for x in ohlcv])
        highs = np.array([x[2] for x in ohlcv])
        lows = np.array([x[3] for x in ohlcv])
        volumes = np.array([x[5] for x in ohlcv])

        rsi = calculate_rsi(closes, 14)
        ema9 = calculate_ema(closes, 9)
        ema20 = calculate_ema(closes, 20)
        sqz_on, sqz_off, no_sqz = calculate_bb_kc(closes, highs, lows)
        val, bcolor, scolor = calculate_squeeze_momentum(closes, sqz_on, sqz_off, no_sqz)
        avg_volume = calculate_volume_average(volumes, 14)
        last_volume = volumes[-1] if len(volumes) > 0 else 0

        last_rsi = rsi[-1] if len(rsi) > 0 else 0
        prev_rsi = rsi[-2] if len(rsi) > 1 else 0
        ema9_last = ema9[-1] if len(ema9) > 0 else 0
        ema20_last = ema20[-1] if len(ema20) > 0 else 0
        volume_increase = last_volume > avg_volume * 1.3  # %30 hacim artışı
        squeeze_off = sqz_off

        buy = False  # Long
        sell = False  # Short
        if ema9_last > ema20_last and last_rsi < 30 and prev_rsi > 30 and volume_increase and squeeze_off:
            buy = True
        elif ema9_last < ema20_last and last_rsi > 70 and prev_rsi < 70 and volume_increase and squeeze_off:
            sell = True

        print(f"{symbol} {timeframe}: Buy: {buy}, Sell: {sell}, RSI: {last_rsi:.2f}, EMA9: {ema9_last:.2f}, EMA20: {ema20_last:.2f}, Volume Increase: {volume_increase}, Squeeze Off: {squeeze_off}")

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (buy, sell) != last_signal:
            if buy:
                message = f"{symbol} {timeframe}: BUY 🚀 (Pozitif Uyumsuzluk, RSI: {last_rsi:.2f}, Hacim Artışı, Squeeze Off)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            elif sell:
                message = f"{symbol} {timeframe}: SELL 📉 (Negatif Uyumsuzluk, RSI: {last_rsi:.2f}, Hacim Artışı, Squeeze Off)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            signal_cache[key] = (buy, sell)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Deneme Botu başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['1h', '2h', '4h']  # 2h var
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'ADAUSDT', 'SUIUSDT', 'WIFUSDT', 'ENAUSDT', 'PENGUUSDT', '1000BONKUSDT', 'HYPEUSDT', 'AVAXUSDT', 'MOODENGUSDT', 'LINKUSDT', 'PUMPFUNUSDT', 'LTCUSDT', 'TRUMPUSDT', 'AAVEUSDT', 'ARBUSDT', 'NEARUSDT', 'ONDOUSDT', 'POPCATUSDT', 'TONUSDT', 'OPUSDT', '1000FLOKIUSDT', 'SEIUSDT', 'HBARUSDT', 'WLDUSDT', 'BNBUSDT', 'UNIUSDT', 'XLMUSDT', 'CRVUSDT', 'VIRTUALUSDT', 'AI16ZUSDT', 'TIAUSDT', 'TAOUSDT', 'APTUSDT', 'DOTUSDT', 'SPXUSDT', 'ETCUSDT', 'LDOUSDT', 'BCHUSDT', 'INJUSDT', 'KASUSDT', 'ALGOUSDT', 'TRXUSDT', 'IPUSDT'
    ]

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_signals(symbol, timeframe)
                await asyncio.sleep(1)  # Rate limit
        print("Tüm taramalar tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())