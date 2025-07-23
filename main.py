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

def calculate_macd(closes, fast=12, slow=26, signal=9):
    ema_fast = calculate_ema(closes, fast)
    ema_slow = calculate_ema(closes, slow)
    macd = ema_fast - ema_slow
    signal_line = calculate_ema(macd, signal)
    return macd, signal_line

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

def calculate_rolling_vwap(highs, lows, closes, volumes, window=48):
    vwap = np.zeros(len(closes))
    for i in range(window - 1, len(closes)):
        slice_highs = highs[i - window + 1:i + 1]
        slice_lows = lows[i - window + 1:i + 1]
        slice_closes = closes[i - window + 1:i + 1]
        slice_volumes = volumes[i - window + 1:i + 1]
        typical_prices = (slice_highs + slice_lows + slice_closes) / 3
        cum_tp_vol = np.sum(typical_prices * slice_volumes)
        cum_vol = np.sum(slice_volumes)
        vwap[i] = cum_tp_vol / cum_vol if cum_vol != 0 else 0
    return vwap

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return 0
    tr = np.maximum(highs[1:] - lows[1:], np.maximum(np.abs(highs[1:] - closes[:-1]), np.abs(lows[1:] - closes[:-1])))
    atr = np.zeros(len(closes))
    atr[period] = np.mean(tr[:period])
    for i in range(period + 1, len(closes)):
        atr[i] = (atr[i-1] * (period - 1) + tr[i-1]) / period
    return atr[-1]

async def check_signals(symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=50)
        if not ohlcv or len(ohlcv) < 50:
            message = f"Uyarı ({symbol} {timeframe}): Yetersiz veri, ohlcv uzunluğu: {len(ohlcv)}"
            print(message)
            await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            return
        closes = np.array([x[4] for x in ohlcv])
        highs = np.array([x[2] for x in ohlcv])
        lows = np.array([x[3] for x in ohlcv])
        volumes = np.array([x[5] for x in ohlcv])

        macd, signal_line = calculate_macd(closes)
        ema9 = calculate_ema(closes, 9)
        ema20 = calculate_ema(closes, 20)
        vwap = calculate_rolling_vwap(highs, lows, closes, volumes)
        avg_volume = calculate_volume_average(volumes, 14)
        last_volume = volumes[-1] if len(volumes) > 0 else 0
        atr = calculate_atr(highs, lows, closes)

        macd_last = macd[-1] if len(macd) > 0 else 0
        signal_last = signal_line[-1] if len(signal_line) > 0 else 0
        ema9_last = ema9[-1] if len(ema9) > 0 else 0
        ema20_last = ema20[-1] if len(ema20) > 0 else 0
        vwap_last = vwap[-1] if len(vwap) > 0 else 0
        volume_increase = last_volume > avg_volume  # Ortalama üstü
        current_price = closes[-1]

        buy = False
        sell = False
        stop_loss = 0
        take_profit = 0
        if ema9_last > ema20_last and current_price > ema9_last and macd_last > signal_last and volume_increase and current_price > vwap_last:
            buy = True
            stop_loss = current_price - 1.5 * atr
            take_profit = current_price + (current_price - stop_loss) * 2
        elif ema9_last < ema20_last and current_price < ema9_last and macd_last < signal_last and volume_increase and current_price < vwap_last:
            sell = True
            stop_loss = current_price + 1.5 * atr
            take_profit = current_price - (stop_loss - current_price) * 2

        print(f"{symbol} {timeframe}: Buy: {buy}, Sell: {sell}, MACD: {macd_last:.2f}, Signal: {signal_last:.2f}, EMA9: {ema9_last:.2f}, EMA20: {ema20_last:.2f}, Volume Increase: {volume_increase}, VWAP: {vwap_last:.2f}, ATR: {atr:.2f}, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f}")

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (buy, sell) != last_signal:
            if buy:
                message = f"{symbol} {timeframe}: BUY 🚀 (MACD Crossover, Hacim Artışı, EMA Crossover, Price > VWAP, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f})"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            elif sell:
                message = f"{symbol} {timeframe}: SELL 📉 (MACD Crossover, Hacim Artışı, EMA Crossover, Price < VWAP, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f})"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            signal_cache[key] = (buy, sell)

    except Exception as e:
        error_msg = f"Hata ({symbol} {timeframe}): {str(e)}"
        print(error_msg)
        await telegram_bot.send_message(chat_id=CHAT_ID, text=error_msg)

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