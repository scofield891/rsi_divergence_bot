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

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}})  # USDT futures/perpetual için 'linear'

telegram_bot = Bot(token=BOT_TOKEN)

signal_cache = {}  # Duplicate önleme

def calculate_rsi(closes, period=14):
    deltas = np.diff(closes)
    seed = deltas[:period+1]
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

async def get_top_volume_symbols(limit=250):
    try:
        markets = exchange.load_markets()
        futures_markets = [m for m in markets if m['future'] and m['active']]
        symbols = [m for m in futures_markets]
        tickers = exchange.fetch_tickers(symbols)
        sorted_symbols = sorted(tickers, key=lambda x: float(tickers[x].get('quoteVolume', 0)), reverse=True)[:limit]
        print(f"En hacimli {limit} futures symbol yüklendi: {sorted_symbols[:10]}")
        return sorted_symbols
    except Exception as e:
        print(f"Hacimli symbol çekme hatası: {str(e)}")
        return ['SOL/USDT', 'ETH/USDT']  # Fallback senin istediğin gibi SOL ve ETH

async def check_divergence(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        closes = np.array([x[4] for x in ohlcv])
        rsi = calculate_rsi(closes, 14)

        last_rsi = rsi[-1]
        prev_rsi = rsi[-2]

        bullish = False  # Pozitif uyumsuzluk
        bearish = False  # Negatif uyumsuzluk
        if last_rsi > 70 and prev_rsi < 70:
            bearish = True
        elif last_rsi < 30 and prev_rsi > 30:
            bullish = True

        print(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI: {last_rsi:.2f}")

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (bullish, bearish) != last_signal:
            message = f"{symbol} {timeframe}: Pozitif Uyumsuzluk: {bullish} 🚀, Negatif Uyumsuzluk: {bearish} 📉, RSI: {last_rsi:.2f}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            signal_cache[key] = (bullish, bearish)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['30m', '1h', '2h', '4h']
    symbols = await get_top_volume_symbols(250)

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_divergence(symbol, timeframe)
                await asyncio.sleep(1)  # Rate limit
        print("Tüm taramalar tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())