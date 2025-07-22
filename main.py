import ccxt
import talib
import time
import asyncio
from telegram import Bot
import numpy as np
from dotenv import load_dotenv
import os

# .env file'ı yükle
load_dotenv()

# Ayarlar (.env'den oku)
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

# Bybit bağlantısı (API key olmadan)
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})

# Telegram bot initialize
telegram_bot = Bot(token=BOT_TOKEN)

async def test_telegram():
    try:
        await telegram_bot.send_message(chat_id=CHAT_ID, text=f"Test mesajı, saat: {time.strftime('%H:%M:%S')} +03")
        print("Telegram testi başarılı!")
    except Exception as e:
        print(f"Telegram hatası: {str(e)} - Detay: {type(e).__name__}")

async def check_divergence(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        closes = [x[4] for x in ohlcv]
        rsi = talib.RSI(np.array(closes), timeperiod=14)

        last_rsi = rsi[-1]
        prev_rsi = rsi[-2]

        bullish = False
        bearish = False
        if last_rsi > 70 and prev_rsi < 70:  # Overbought
            bearish = True
        elif last_rsi < 30 and prev_rsi > 30:  # Oversold
            bullish = True

        print(f"{symbol} {timeframe}: Bullish: {bullish}, Bearish: {bearish}, RSI son değer: {last_rsi:.2f}")

        if bullish or bearish:
            message = f"{symbol} {timeframe}: Bullish: {bullish}, Bearish: {bearish}, RSI: {last_rsi:.2f}"
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)

    except Exception as e:
        print(f"Hata ({symbol}): {str(e)} - Detay: {type(e).__name__}")

async def main():
    await test_telegram()
    symbols = ['PERP/USDT', 'BTC/USDT']
    timeframe = '1h'

    while True:
        for symbol in symbols:
            await check_divergence(symbol, timeframe)
        print("Kontroller tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())