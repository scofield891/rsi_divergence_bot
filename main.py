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

def calculate_vwap(highs, lows, closes, volumes):
    if len(closes) == 0:
        return 0
    typical_prices = (highs + lows + closes) / 3
    cum_tp_vol = np.cumsum(typical_prices * volumes)
    cum_vol = np.cumsum(volumes)
    vwap = cum_tp_vol / cum_vol
    return vwap[-1]  # Son VWAP değeri

# Opsiyonel MACD: Aktif etmek istersen uncomment et
# def calculate_macd(closes, fast=12, slow=26, signal=9):
#     ema_fast = calculate_ema(closes, fast)
#     ema_slow = calculate_ema(closes, slow)
#     macd = ema_fast - ema_slow
#     signal_line = calculate_ema(macd, signal)
#     return macd[-1], signal_line[-1]  # macd > signal for bullish

async def check_signals(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=50)
        if not ohlcv or len(ohlcv) < 50:
            print(f"Uyarı ({symbol} {timeframe}): Yetersiz veri, ohlcv uzunluğu: {len(ohlcv)}")
            return
        closes = np.array([x[4] for x in ohlcv])
        highs = np.array([x[2] for x in ohlcv])
        lows = np.array([x[3] for x in ohlcv])
        volumes = np.array([x[5] for x in ohlcv])

        rsi = calculate_rsi(closes, 14)
        ema9 = calculate_ema(closes, 9)
        ema20 = calculate_ema(closes, 20)
        avg_volume = calculate_volume_average(volumes, 14)
        last_volume = volumes[-1] if len(volumes) > 0 else 0
        vwap = calculate_vwap(highs, lows, closes, volumes)

        last_rsi = rsi[-1] if len(rsi) > 0 else 0
        prev_rsi = rsi[-2] if len(rsi) > 1 else 0
        ema9_last = ema9[-1] if len(ema9) > 0 else 0
        ema20_last = ema20[-1] if len(ema20) > 0 else 0
        volume_increase = last_volume > avg_volume * 1.2  # %20 hacim artışı

        # macd, signal_line = calculate_macd(closes)  # MACD opsiyonel, uncomment et
        # macd_bullish = macd > signal_line

        buy = False  # Long
        sell = False  # Short
        if ema9_last > ema20_last and closes[-1] > ema9_last and last_rsi < 30 and prev_rsi > 30 and volume_increase and closes[-1] > vwap:  # EMA crossover, ADX çıkarıldı
            buy = True
        elif ema9_last < ema20_last and closes[-1] < ema9_last and last_rsi > 70 and prev_rsi < 70 and volume_increase and closes[-1] < vwap:
            sell = True

        print(f"{symbol} {timeframe}: Buy: {buy}, Sell: {sell}, RSI: {last_rsi:.2f}, EMA9: {ema9_last:.2f}, EMA20: {ema20_last:.2f}, Volume Increase: {volume_increase}, VWAP: {vwap:.2f}")

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (buy, sell) != last_signal:
            if buy:
                message = f"{symbol} {timeframe}: BUY 🚀 (Pozitif Uyumsuzluk, RSI: {last_rsi:.2f}, Hacim Artışı, EMA Crossover, Price > VWAP)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            elif sell:
                message = f"{symbol} {timeframe}: SELL 📉 (Negatif Uyumsuzluk, RSI: {last_rsi:.2f}, Hacim Artışı, EMA Crossover, Price < VWAP)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            signal_cache[key] = (buy, sell)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Deneme Botu başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['30m', '1h', '2h', '4h']
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