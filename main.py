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

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}})  # USDT futures/perpetual

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
    symbols = [
        'ETHUSDT', 'BTCUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'FARTCOINUSDT', '1000PEPEUSDT', 'SUIUSDT', 'ADAUSDT', 'WIFUSDT', 'ENAUSDT', 'PENGUUSDT', 'HYPEUSDT', '1000BONKUSDT', 'AVAXUSDT', 'LINKUSDT', 'MOODENGUSDT', 'PUMPFUNUSDT', 'TRUMPUSDT', 'LTCUSDT', 'SPKUSDT', 'AAVEUSDT', 'ARBUSDT', 'ONDOUSDT', 'NEARUSDT', 'POPCATUSDT', 'OPUSDT', 'SEIUSDT', '1000FLOKIUSDT', 'HBARUSDT', 'WLDUSDT', 'JUPUSDT', 'BNBUSDT', 'TONUSDT', 'UNIUSDT', 'XLMUSDT', 'VIRTUALUSDT', 'CRVUSDT', 'TIAUSDT', 'AI16ZUSDT', 'PNUTUSDT', 'GALAUSDT', 'TAOUSDT', 'APTUSDT', 'DOTUSDT', 'CFXUSDT', 'ETCUSDT', 'SPXUSDT', 'LDOUSDT', 'ENSUSDT', 'OMUSDT', 'SHIB1000USDT', 'BCHUSDT', 'ZORAUSDT', 'DRIFTUSDT', 'INJUSDT', 'JTOUSDT', 'RESOLVUSDT', 'UXLINKUSDT', 'KASUSDT', 'GOATUSDT', 'ETHFIUSDT', 'HYPERUSDT', 'ALGOUSDT', 'ORDIUSDT', 'RENDERUSDT', 'TRXUSDT', 'BRETTUSDT', 'XTZUSDT', 'LAUNCHCOINUSDT', 'STRKUSDT', '1000NEIROCTOUSDT', 'TRBUSDT', 'EIGENUSDT', 'BOMEUSDT', 'MOVEUSDT', 'PENDLEUSDT', 'MKRUSDT', 'FILUSDT', 'SOLAYERUSDT', 'MEWUSDT', 'HUSDT', 'SUSDT', 'ICPUSDT', 'ATOMUSDT', 'USELESSUSDT', 'BERAUSDT', 'POLUSDT', 'GRASSUSDT', 'AIXBTUSDT', 'SANDUSDT', 'STXUSDT', '1000000MOGUSDT', 'NOTUSDT', 'CHILLGUYUSDT', 'RUNEUSDT', 'COOKIEUSDT', 'PYTHUSDT', 'APEUSDT', 'IPUSDT', 'DYDXUSDT', 'JASMYUSDT', 'RAYDIUMUSDT', 'INITUSDT', 'SYRUPUSDT', 'BLURUSDT', 'GMTUSDT', 'AEROUSDT', 'KAITOUSDT', 'SUSHIUSDT', 'XAUTUSDT', 'ZROUSDT', 'MELANIAUSDT', 'GRTUSDT', 'CVXUSDT', 'USUALUSDT', 'ARKMUSDT', 'KAIAUSDT', '1INCHUSDT', 'ARUSDT', 'DEEPUSDT', 'FXSUSDT', 'MOCAUSDT', 'MANAUSDT', 'VETUSDT', 'PLUMEUSDT', 'MNTUSDT', 'NEIROETHUSDT', 'IMXUSDT', 'XMRUSDT', 'CAKEUSDT', 'AXSUSDT', 'THETAUSDT', 'COMPUSDT', 'MASKUSDT', 'LPTUSDT', 'FLRUSDT', 'CROUSDT', 'ZKUSDT', 'BANANAS31USDT', 'BEAMUSDT', 'DOGUSDT', 'NILUSDT', 'PAXGUSDT', 'SAROSUSDT', 'ATHUSDT', 'ALCHUSDT', 'KAVAUSDT', 'SUNDOGUSDT'
    ]  # Senin gönderdiğin coin listesi, 250'ye tamamlamak için hacim çekmeyi kaldırdım

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_divergence(symbol, timeframe)
                await asyncio.sleep(1)  # Rate limit delay
        print("Tüm taramalar tamamlandı, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())