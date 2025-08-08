import ccxt
import numpy as np
import pandas as pd
import ta.momentum as momentum
import ta.volatility as volatility
from telegram import Bot
from dotenv import load_dotenv
import os
import logging
import asyncio
from datetime import datetime
import pytz

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
TEST_MODE = os.getenv('TEST_MODE', 'False').lower() == 'true'

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
file_handler = logging.FileHandler('bot.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Telegram logging susturma
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'timeout': 60000})
telegram_bot = Bot(token=BOT_TOKEN)
signal_cache = {}

def calculate_indicators(df):
    bb = volatility.BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    df['bb_middle'] = bb.bollinger_mavg()
    df['ema3'] = df['close'].ewm(span=3, adjust=False).mean()
    rsi = momentum.RSIIndicator(close=df['close'], window=14)
    df['rsi'] = rsi.rsi()
    return df

async def check_signals(symbol, timeframe):
    try:
        if TEST_MODE:
            closes = np.cumsum(np.random.randn(100)) + 60000
            volumes = np.random.rand(100) * 10000
            ohlcv = [[0, 0, 0, 0, closes[i], volumes[i]] for i in range(100)]
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            logger.info(f"Test modu: {symbol} {timeframe} için dummy data kullanıldı")
        else:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    break
                except ccxt.RequestTimeout as e:
                    logger.warning(f"Timeout ({symbol} {timeframe}), retry {attempt+1}/{max_retries}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(5)
                except Exception as e:
                    raise
        
        df = calculate_indicators(df)
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        key = f"{symbol}_{timeframe}"
        current_pos = signal_cache.get(key, {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None})
        
        buy_condition = (prev_row['close'] <= prev_row['bb_lower'] and last_row['close'] > last_row['bb_lower']) and \
                        (prev_row['close'] <= prev_row['ema3'] and last_row['close'] > last_row['ema3']) and \
                        last_row['rsi'] < 30
        sell_condition = (prev_row['close'] <= prev_row['bb_upper'] and last_row['close'] > last_row['bb_upper']) and \
                         (prev_row['close'] >= prev_row['ema3'] and last_row['close'] < last_row['ema3']) and \
                         last_row['rsi'] > 70
        
        if buy_condition and current_pos['signal'] != 'buy':
            entry_price = last_row['close']
            tp_price = last_row['bb_upper']
            sl_price = last_row['bb_lower'] * 0.99
            message = f"{symbol} {timeframe}: BUY (LONG) 🚀\nRSI: {last_row['rsi']:.2f}\nEntry Price: {entry_price:.2f}\nTP (BB Upper): {tp_price:.2f}\nSL (BB Lower -1%): {sl_price:.2f}\nSistem exit: BB Middle altına düşüş veya RSI >60\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            try:
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Sinyal gönderildi: {message}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Telegram hata: {str(e)}")
            signal_cache[key] = {'signal': 'buy', 'entry_price': entry_price, 'tp_price': tp_price, 'sl_price': sl_price}
        
        elif sell_condition and current_pos['signal'] != 'sell':
            entry_price = last_row['close']
            tp_price = last_row['bb_lower']
            sl_price = last_row['bb_upper'] * 1.01
            message = f"{symbol} {timeframe}: SELL (SHORT) 📉\nRSI: {last_row['rsi']:.2f}\nEntry Price: {entry_price:.2f}\nTP (BB Lower): {tp_price:.2f}\nSL (BB Upper +1%): {sl_price:.2f}\nSistem exit: BB Middle üstüne çıkış veya RSI <40\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
            try:
                await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                logger.info(f"Sinyal gönderildi: {message}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Telegram hata: {str(e)}")
            signal_cache[key] = {'signal': 'sell', 'entry_price': entry_price, 'tp_price': tp_price, 'sl_price': sl_price}
        
        if current_pos['signal'] == 'buy':
            close_long_condition = (last_row['close'] < last_row['bb_middle']) or (last_row['rsi'] > 60) or \
                                   (last_row['close'] <= current_pos['sl_price']) or (last_row['close'] >= current_pos['tp_price'])
            if close_long_condition:
                reason = "TP Hit" if last_row['close'] >= current_pos['tp_price'] else "SL Hit" if last_row['close'] <= current_pos['sl_price'] else "Exit Condition"
                message = f"{symbol} {timeframe}: CLOSE LONG 📉 ({reason})\nCurrent Price: {last_row['close']:.2f}\nRSI: {last_row['rsi']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                try:
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Exit sinyali gönderildi: {message}")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Telegram hata: {str(e)}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None}
        
        elif current_pos['signal'] == 'sell':
            close_short_condition = (last_row['close'] > last_row['bb_middle']) or (last_row['rsi'] < 40) or \
                                    (last_row['close'] >= current_pos['sl_price']) or (last_row['close'] <= current_pos['tp_price'])
            if close_short_condition:
                reason = "TP Hit" if last_row['close'] <= current_pos['tp_price'] else "SL Hit" if last_row['close'] >= current_pos['sl_price'] else "Exit Condition"
                message = f"{symbol} {timeframe}: CLOSE SHORT 🚀 ({reason})\nCurrent Price: {last_row['close']:.2f}\nRSI: {last_row['rsi']:.2f}\nTime: {datetime.now(pytz.timezone('Europe/Istanbul')).strftime('%H:%M:%S')}"
                try:
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=message)
                    logger.info(f"Exit sinyali gönderildi: {message}")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Telegram hata: {str(e)}")
                signal_cache[key] = {'signal': None, 'entry_price': None, 'tp_price': None, 'sl_price': None}
    
    except Exception as e:
        logger.error(f"Hata ({symbol} {timeframe}): {str(e)}")

async def main():
    tz = pytz.timezone('Europe/Istanbul')
    try:
        await telegram_bot.send_message(chat_id=CHAT_ID, text="Bot başladı, saat: " + datetime.now(tz).strftime('%H:%M:%S'))
    except Exception as e:
        logger.error(f"Telegram başlatma hatası: {str(e)}")
    
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
                tasks.append(check_signals(symbol, timeframe))
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            await asyncio.gather(*tasks[i:i+batch_size])
            await asyncio.sleep(1)
        logger.info(f"Tüm taramalar tamamlandı, {len(tasks)} task işlendi, 5 dakika bekleniyor...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())