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

# Exchange'i Kraken olarak, FX spot için
exchange = ccxt.kraken({'enableRateLimit': True})

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

def calculate_rsi_ema(rsi, ema_length=14):
    ema = np.zeros_like(rsi)
    if len(rsi) < ema_length:
        return ema
    ema[ema_length-1] = np.mean(rsi[:ema_length])
    for i in range(ema_length, len(rsi)):
        ema[i] = (rsi[i] * (2 / (ema_length + 1))) + (ema[i-1] * (1 - (2 / (ema_length + 1))))
    return ema

def find_local_extrema(arr, order=3):
    highs = []
    lows = []
    for i in range(order, len(arr) - order):
        if arr[i] == max(arr[i-order:i+order+1]):
            highs.append(i)
        if arr[i] == min(arr[i-order:i+order+1]):
            lows.append(i)
    return np.array(highs), np.array(lows)

async def check_divergence(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        closes = np.array([x[4] for x in ohlcv])
        rsi = calculate_rsi(closes, 14)
        rsi_ema = calculate_rsi_ema(rsi, 14)
        rsi_ema2 = np.roll(rsi_ema, 1)

        ema_color = 'lime' if rsi_ema[-1] > rsi_ema2[-1] else 'red'

        lookback = 30  # Sabit 30 bar
        if len(closes) < lookback:
            return

        price_slice = closes[-lookback:]
        ema_slice = rsi_ema[-lookback:]

        price_highs, price_lows = find_local_extrema(price_slice, order=3)

        bullish = False
        bearish = False

        # Bullish: Price LL, EMA HL (price low idx'lerde, 30 bar içinde)
        if len(price_lows) >= 2:
            for i_idx in range(len(price_lows) - 1, 0, -1):
                i = price_lows[i_idx]
                for j_idx in range(i_idx - 1, -1, -1):
                    j = price_lows[j_idx]
                    if (i - j) <= lookback:
                        if price_slice[i] < price_slice[j] and ema_slice[i] > ema_slice[j]:
                            bullish = True
                            break
                if bullish:
                    break

        # Bearish: Price HH, EMA LH (price high idx'lerde, 30 bar içinde)
        if len(price_highs) >= 2:
            for i_idx in range(len(price_highs) - 1, 0, -1):
                i = price_highs[i_idx]
                for j_idx in range(i_idx - 1, -1, -1):
                    j = price_highs[j_idx]
                    if (i - j) <= lookback:
                        if price_slice[i] > price_slice[j] and ema_slice[i] < ema_slice[j]:
                            bearish = True
                            break
                if bearish:
                    break

        print(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI_EMA: {rsi_ema[-1]:.2f}, Color: {ema_color}")

        key = f"{symbol} {timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        # Şartlar: Bullish <40, Bearish >60
        if (bullish or bearish) and (bullish, bearish) != last_signal:
            if (bullish and rsi_ema[-1] < 40) or (bearish and rsi_ema[-1] > 60):
                rsi_str = f"{rsi_ema[-1]:.2f}"
                if bullish:
                    message = f"{symbol} {timeframe}\nPozitif Uyumsuzluk: {bullish} 🚀 (Price LL, EMA HL)\nRSI_EMA: {rsi_str} ({ema_color.upper()})"
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                if bearish:
                    message = f"{symbol} {timeframe}\nNegatif Uyumsuzluk: {bearish} 📉 (Price HH, EMA LH)\nRSI_EMA: {rsi_str} ({ema_color.upper()})"
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                signal_cache[key] = (bullish, bearish)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['5m', '15m', '30m', '1h']  # Ekli
    # Pepperstone'da mevcut majör + minör FX pariteleri (egzotik dışarıda)
    symbols = [
        'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF', 'USD/CAD', 'AUD/USD', 'NZD/USD',  # Majörler
        'EUR/GBP', 'EUR/JPY', 'EUR/AUD', 'EUR/NZD', 'EUR/CAD', 'EUR/CHF',  # Minörler
        'GBP/JPY', 'GBP/AUD', 'GBP/NZD', 'GBP/CAD', 'GBP/CHF',
        'AUD/JPY', 'AUD/NZD', 'AUD/CAD', 'AUD/CHF',
        'NZD/JPY', 'NZD/CAD', 'NZD/CHF',
        'CAD/JPY', 'CAD/CHF',
        'CHF/JPY'
    ]  # Majör + minör, toplam ~28, exotics dışarıda

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_divergence(symbol, timeframe)
                await asyncio.sleep(1)
        print("Tüm taramalar tamamlandı, 2 dakika bekleniyor...")
        await asyncio.sleep(120)  # 2 dakika bekleme

if __name__ == "__main__":
    asyncio.run(main())