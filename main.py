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

def calculate_bb_kc(closes, highs, lows, length_bb=20, mult_bb=2.0, length_kc=20, mult_kc=1.5, use_tr=True):
    if len(closes) < max(length_bb, length_kc):
        return False, False, False

    basis = np.mean(closes[-length_bb:])
    dev = mult_bb * np.std(closes[-length_bb:])
    upper_bb = basis + dev
    lower_bb = basis - dev

    ma = np.mean(closes[-length_kc:])

    if use_tr:
        prev_closes = np.roll(closes, 1)
        range_val = np.maximum(highs - lows, np.maximum(np.abs(highs - prev_closes), np.abs(lows - prev_closes)))
    else:
        range_val = highs - lows
    rangema = np.mean(range_val[-length_kc:])
    upper_kc = ma + rangema * mult_kc
    lower_kc = ma - rangema * mult_kc

    sqz_on = (lower_bb > lower_kc) and (upper_bb < upper_kc)
    sqz_off = (lower_bb < lower_kc) and (upper_bb > upper_kc)
    no_sqz = not sqz_on and not sqz_off

    return sqz_on, sqz_off, no_sqz

def calculate_squeeze_momentum(closes, highs, lows, sqz_on, sqz_off, no_sqz, length_kc=20):
    if len(closes) < length_kc * 2 - 1:
        return 0, 'gray', 'gray'

    value = np.zeros(len(closes))
    for i in range(length_kc - 1, len(closes)):
        slice_closes = closes[i - length_kc + 1: i + 1]
        slice_highs = highs[i - length_kc + 1: i + 1]
        slice_lows = lows[i - length_kc + 1: i + 1]
        m_avg_i = np.mean(slice_closes)
        highest_i = np.max(slice_highs)
        lowest_i = np.min(slice_lows)
        m1_i = (highest_i + lowest_i) / 2
        value[i] = slice_closes[-1] - (m1_i + m_avg_i) / 2

    min_idx = len(closes) - length_kc
    if min_idx < length_kc - 1:
        return 0, 'gray', 'gray'
    value_window = value[min_idx:]
    x = np.arange(len(value_window))
    fit = np.polyfit(x, value_window, 1)
    val = fit[0] * (len(value_window) - 1) + fit[1]

    prev_min_idx = len(closes) - length_kc - 1
    if prev_min_idx >= length_kc - 1:
        prev_value_window = value[prev_min_idx: prev_min_idx + length_kc]
        prev_x = np.arange(len(prev_value_window))
        prev_fit = np.polyfit(prev_x, prev_value_window, 1)
        prev_val = prev_fit[0] * (len(prev_value_window) - 1) + prev_fit[1]
    else:
        prev_val = val

    if val > 0 and val > prev_val:
        bcolor = 'lime'
    elif val > 0:
        bcolor = 'green'
    elif val < 0 and val < prev_val:
        bcolor = 'red'
    else:
        bcolor = 'maroon'

    scolor = 'blue' if no_sqz else 'black' if sqz_on else 'gray'

    return val, bcolor, scolor

def calculate_trp_resistance(closes, highs):
    count = 0
    max_count = 0
    resistance = 0
    for i in range(4, len(closes)):
        if closes[i] > closes[i-4]:
            count += 1
        else:
            count = 0
        if count > max_count:
            max_count = count
            if max_count >= 9:
                resistance = np.max(highs[i-max_count+1:i+1])
    return resistance

def calculate_trp_support(closes, lows):
    count = 0
    max_count = 0
    support = float('inf')
    for i in range(4, len(closes)):
        if closes[i] < closes[i-4]:
            count += 1
        else:
            count = 0
        if count > max_count:
            max_count = count
            if max_count >= 9:
                support = np.min(lows[i-max_count+1:i+1])
    return support

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

        rsi = calculate_rsi(closes, 14)
        sqz_on, sqz_off, no_sqz = calculate_bb_kc(closes, highs, lows)
        val, bcolor, scolor = calculate_squeeze_momentum(closes, highs, lows, sqz_on, sqz_off, no_sqz)
        prev_val, prev_bcolor, prev_scolor = calculate_squeeze_momentum(closes[:-1], highs[:-1], lows[:-1], sqz_on, sqz_off, no_sqz) if len(closes) > 1 else (0, 'gray', 'gray')
        td_resistance = calculate_trp_resistance(closes, highs)
        td_support = calculate_trp_support(closes, lows)
        atr = calculate_atr(highs, lows, closes)

        last_rsi = rsi[-1] if len(rsi) > 0 else 0
        prev_rsi = rsi[-2] if len(rsi) > 1 else 0
        current_price = closes[-1]
        current_high = highs[-1]
        current_low = lows[-1]

        buy = False
        sell = False
        stop_loss = 0
        take_profit = 0
        if last_rsi > 35 and prev_rsi < 35 and bcolor == 'maroon' and prev_bcolor == 'red' and current_low <= td_support:
            buy = True
            stop_loss = current_price - 1.5 * atr
            take_profit = current_price + (current_price - stop_loss) * 2
        elif last_rsi < 65 and prev_rsi > 65 and bcolor == 'green' and prev_bcolor == 'lime' and current_high >= td_resistance:
            sell = True
            stop_loss = current_price + 1.5 * atr
            take_profit = current_price - (stop_loss - current_price) * 2

        print(f"{symbol} {timeframe}: Buy: {buy}, Sell: {sell}, RSI: {last_rsi:.2f}, Prev RSI: {prev_rsi:.2f}, BColor: {bcolor}, Prev BColor: {prev_bcolor}, TD Resistance: {td_resistance:.2f}, TD Support: {td_support:.2f}, ATR: {atr:.2f}, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f}")

        key = f"{symbol}_{timeframe}"
        last_signal = signal_cache.get(key, (False, False))

        if (buy, sell) != last_signal:
            if buy:
                message = f"{symbol} {timeframe}: BUY 🚀 (Pozitif RSI Uyumsuzluk, Squeeze Kırmızıdan Koyu Kırmızıya, Candle <= TD Support, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f})"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            elif sell:
                message = f"{symbol} {timeframe}: SELL 📉 (Negatif RSI Uyumsuzluk, Squeeze Yeşilden Koyu Yeşile, Candle >= TD Resistance, Stop-Loss: {stop_loss:.2f}, Take-Profit: {take_profit:.2f})"
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
            signal_cache[key] = (buy, sell)

    except Exception as e:
        print(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    await telegram_bot.send_message(chat_id=CHAT_ID, text="Deneme Botu başladı, saat: " + time.strftime('%H:%M:%S'))
    timeframes = ['1h', '2h', '4h']  # 2h eklendi
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