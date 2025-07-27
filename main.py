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

# Exchange'i BingX olarak değiştiriyoruz, FX perpetual için 'swap' type
exchange = ccxt.bingx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})

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
        rsi_ema2 = np.roll(rsi_ema, 2)

        ema_color = 'lime' if rsi_ema[-1] > rsi_ema2[-1] else 'red'

        min_lookback = 10
        max_lookback = 40
        lookback = min(max_lookback, len(closes))
        if lookback < min_lookback:
            return

        price_slice = closes[-lookback:]
        ema_slice = rsi_ema[-lookback:]

        price_highs, price_lows = find_local_extrema(price_slice, order=3)
        ema_highs, ema_lows = find_local_extrema(ema_slice, order=3)

        bullish = False  # Pozitif: Fiyat LL yaparken EMA HL yaparsa
        bearish = False  # Negatif: Fiyat HH yaparken EMA LH yaparsa

        # Bullish - en az 2 dip, >=3 ise trend kontrol (daha fazla sinyal)
        if len(price_lows) >= 2 and len(ema_lows) >= 2:
            last_low = price_lows[-1]
            prev_low = price_lows[-2]
            last_ema_low = ema_lows[-1]
            prev_ema_low = ema_lows[-2]
            core_bullish = price_slice[last_low] < price_slice[prev_low] and ema_slice[last_ema_low] > ema_slice[prev_ema_low]
            if len(price_lows) >= 3 and len(ema_lows) >= 3:
                prev_prev_low = price_lows[-3]
                if price_slice[prev_low] < price_slice[prev_prev_low]:
                    bullish = core_bullish
                else:
                    bullish = core_bullish  # Zorunlu değil
            else:
                bullish = core_bullish

        # Bearish - en az 2 tepe, >=3 ise trend kontrol (daha fazla sinyal)
        if len(price_highs) >= 2 and len(ema_highs) >= 2:
            last_high = price_highs[-1]
            prev_high = price_highs[-2]
            last_ema_high = ema_highs[-1]
            prev_ema_high = ema_highs[-2]
            core_bearish = price_slice[last_high] > price_slice[prev_high] and ema_slice[last_ema_high] < ema_slice[prev_ema_high]
            if len(price_highs) >= 3 and len(ema_highs) >= 3:
                prev_prev_high = price_highs[-3]
                if price_slice[prev_high] > price_slice[prev_prev_high]:
                    bearish = core_bearish
                else:
                    bearish = core_bearish  # Zorunlu değil
            else:
                bearish = core_bearish

        print(f"{symbol} {timeframe}: Pozitif: {bullish}, Negatif: {bearish}, RSI_EMA: {rsi_ema[-1]:.2f}, Color: {ema_color}")

        key = f"{symbol} {timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (bullish, bearish) != last_signal and (rsi_ema[-1] < 35 or rsi_ema[-1] > 65):
            rsi_str = f"{rsi_ema[-1]:.2f}".replace('.', '\\.')
            if bullish:
                message = rf"\*{symbol} {timeframe}\*: \nPozitif Uyumsuzluk: {bullish} 🚀 \(Price LL, EMA HL\)\nRSI_EMA: {rsi_str} \({ema_color.upper()}\)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='MarkdownV2')
            if bearish:
                message = rf"\*{symbol} {timeframe}\*: \nNegatif Uyumsuzluk: {bearish} 📉 \(Price HH, EMA LH\)\nRSI_EMA: {rsi_str} \({ema_color.upper()}\)"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='MarkdownV2')
            signal_cache[key] = (bullish, bearish)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['5m', '15m']
    # BingX'te mevcut FX perpetual çiftleri (USDT settled)
    symbols = [
        'EURUSD-USDT', 'GBPUSD-USDT', 'AUDUSD-USDT', 'NZDUSD-USDT', 'USDCAD-USDT', 'USDCHF-USDT', 'USDJPY-USDT',
        'EURGBP-USDT', 'EURAUD-USDT', 'EURNZD-USDT', 'EURCAD-USDT', 'EURCHF-USDT', 'EURJPY-USDT',
        'GBPAUD-USDT', 'GBPNZD-USDT', 'GBPCAD-USDT', 'GBPCHF-USDT', 'GBPJPY-USDT',
        'AUDNZD-USDT', 'AUDCAD-USDT', 'AUDCHF-USDT', 'AUDJPY-USDT',
        'NZDCAD-USDT', 'NZDCHF-USDT', 'NZDJPY-USDT',
        'CADCHF-USDT', 'CADJPY-USDT',
        'CHFJPY-USDT'
    ]  # BingX'te mevcut FX perpetuals, formatı test et (bazı çiftler AUDCAD-USDT gibi olabilir)

    while True:
        for timeframe in timeframes:
            for symbol in symbols:
                await check_divergence(symbol, timeframe)
                await asyncio.sleep(1)
        print("Tüm taramalar tamamlandı, 2 dakika bekleniyor...")
        await asyncio.sleep(120)  # 2 dakika bekleme

if __name__ == "__main__":
    asyncio.run(main())