import os
import ccxt
import numpy as np
import pandas as pd
import asyncio
import logging
import sys
from datetime import datetime, timedelta
import pytz
from telegram import Bot
import signal as os_signal
import time
import inspect

"""
Bybit High-WR Scanner (4H regime + 2H entry) – v1.4.0
- Telegram kuyruk + rate limit (pool timeout fix)
- Wilder DI normalize (/period) – ATR ile tutarlı
- High-WR entry gate (daha seçici): trigger AND (pullback OR hidden div) AND confirm AND body_ok AND liquidity_ok
- 4H rejim + 2H giriş, SL/TP (partial) + TSL, cooldown, auto-symbol list
NOT: BOT_TOKEN/CHAT_ID kullanıcının isteğiyle hardcoded.
"""

# ================== KULLANICI AYARLARI ==================
BOT_TOKEN = "7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY"
CHAT_ID   = "-1002755412514"
TZ = 'Europe/Istanbul'

# Çalışma modu
LIVE_SCAN_MODE  = True
BACKTEST_MODE   = False

# Taramalar
REGIME_TF = '4h'
ENTRY_TF  = '2h'
SCAN_SLEEP_SEC = 300
BATCH_SIZE = 16
COOLDOWN_MINUTES = 90

# --- YÜKSEK WIN-RATE için preset ---
SCORE_MIN = 8            # 6 -> 8 (daha seçici)
ADX_MIN_4H = 20
ADX_MAX_4H = 35
ATRPCT_MIN_4H = 0.012    # %1.2
ATRPCT_MAX_4H = 0.050    # %5.0
PULLBACK_TOL_ATR = 0.08
DI_ADX_MIN_2H = 18

# Divergence penceresi
DIV_LOOKBACK = 30
DIV_MIN_DISTANCE = 6

# SL/TP/Trailing
SL_ATR_MULT        = 1.6
TP1_ATR_MULT       = 1.8
TP2_ATR_MIN_MULT   = 3.0
TP2_ATR_MAX_MULT   = 3.5
TSL_ACTIVATION_ATR = 1.0
TSL_K_LOWVOL       = 2.0
TSL_K_HIGHVOL      = 2.5
VOL_SPLIT_ATRPCT   = 0.030  # %3

# Veri limitleri
LIMIT_4H = 260
LIMIT_2H = 520

# ================== LOG ==================
logger = logging.getLogger("scanner")
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler('scanner.log'); fh.setFormatter(fmt); logger.addHandler(fh)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== EXCHANGE & TELEGRAM ==================
exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'timeout': 60000})
telegram_bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None

# --- Telegram Kuyruk + Rate Limit ---
SEND_Q = asyncio.Queue()
RATE_LIMIT_MSGS_PER_SEC = 3  # 3 mesaj/sn

async def _tg_worker():
    while True:
        text = await SEND_Q.get()
        try:
            if telegram_bot:
                if inspect.iscoroutinefunction(telegram_bot.send_message):
                    # async bot (ptb v20+)
                    await telegram_bot.send_message(chat_id=CHAT_ID, text=text)
                else:
                    # sync bot (ptb v13)
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: telegram_bot.send_message(chat_id=CHAT_ID, text=text))
            else:
                logger.info("TELEGRAM(MOCK): " + text)
        except Exception as e:
            logger.error(f"Telegram hata (worker): {e}")
            await asyncio.sleep(1.0)
            try:
                if telegram_bot:
                    if inspect.iscoroutinefunction(telegram_bot.send_message):
                        await telegram_bot.send_message(chat_id=CHAT_ID, text=text)
                    else:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, lambda: telegram_bot.send_message(chat_id=CHAT_ID, text=text))
            except Exception as e2:
                logger.error(f"Telegram tekrar hata: {e2}")
        finally:
            SEND_Q.task_done()
            await asyncio.sleep(1.0 / RATE_LIMIT_MSGS_PER_SEC)

async def tg_send(text: str):
    await SEND_Q.put(text)

# ================== İNDİKATÖR FONKSİYONLARI ==================
def ema(arr, n):
    arr = np.asarray(arr, dtype=np.float64)
    out = np.empty_like(arr)
    k = 2.0 / (n + 1.0)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1 - k)
    return out

def macd_lines(closes, fast=12, slow=26, signal=9):
    efast = ema(closes, fast)
    eslow = ema(closes, slow)
    macd = efast - eslow
    sig = ema(macd, signal)
    hist = macd - sig
    return macd, sig, hist

def rsi(arr, period=14):
    arr = np.asarray(arr, dtype=np.float64)
    deltas = np.diff(arr)
    rsi_vals = np.zeros_like(arr)
    if len(arr) < period + 1:
        return rsi_vals
    up = deltas.clip(min=0)
    down = -deltas.clip(max=0)
    roll_up = np.empty_like(arr); roll_down = np.empty_like(arr)
    roll_up[:period] = np.nan; roll_down[:period] = np.nan
    roll_up[period] = up[:period].mean()
    roll_down[period] = down[:period].mean()
    for i in range(period+1, len(arr)):
        roll_up[i] = (roll_up[i-1]*(period-1) + up[i-1]) / period
        roll_down[i] = (roll_down[i-1]*(period-1) + down[i-1]) / period
    rs = roll_up / roll_down
    rsi_vals[:period] = 50.0
    rsi_vals[period:] = 100.0 - (100.0 / (1.0 + rs[period:]))
    rsi_vals = np.nan_to_num(rsi_vals, nan=50.0, posinf=100.0, neginf=0.0)
    return rsi_vals

def compute_atr_dm_di_adx(df, period=14):
    high = df['high'].values.astype(np.float64)
    low  = df['low'].values.astype(np.float64)
    close= df['close'].values.astype(np.float64)

    tr = np.zeros_like(close)
    tr[0] = high[0] - low[0]
    for i in range(1, len(close)):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))

    up_move = np.maximum(high[1:]-high[:-1], 0.0)
    dn_move = np.maximum(low[:-1]-low[1:], 0.0)
    plus_dm  = np.zeros_like(close); plus_dm[1:]  = np.where((up_move>dn_move)&(up_move>0), up_move, 0.0)
    minus_dm = np.zeros_like(close); minus_dm[1:] = np.where((dn_move>up_move)&(dn_move>0), dn_move, 0.0)

    # Wilder smoothing
    atr = np.zeros_like(close); atr[period-1] = np.sum(tr[:period])
    for i in range(period, len(close)):
        atr[i] = atr[i-1] - (atr[i-1]/period) + tr[i]
    atr = atr / period

    p_dm_s = np.zeros_like(close); p_dm_s[period-1] = np.sum(plus_dm[:period])
    m_dm_s = np.zeros_like(close); m_dm_s[period-1] = np.sum(minus_dm[:period])
    for i in range(period, len(close)):
        p_dm_s[i] = p_dm_s[i-1] - (p_dm_s[i-1]/period) + plus_dm[i]
        m_dm_s[i] = m_dm_s[i-1] - (m_dm_s[i-1]/period) + minus_dm[i]

    # DI'larda /period ekledik (ATR zaten /period normalize)
    plus_di  = 100.0 * (p_dm_s / np.maximum(atr,1e-9)) / period
    minus_di = 100.0 * (m_dm_s / np.maximum(atr,1e-9)) / period

    dx = 100.0 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-9)
    adx = np.zeros_like(close); adx[:2*period-1] = np.nan
    if len(close) >= 2*period:
        adx[2*period-1] = np.nanmean(dx[period:2*period])
        for i in range(2*period, len(close)):
            adx[i] = ((adx[i-1] * (period - 1)) + dx[i]) / period

    return np.nan_to_num(atr), np.nan_to_num(plus_di), np.nan_to_num(minus_di), np.nan_to_num(adx)

def find_local_extrema(arr, order=3):
    highs, lows = [], []
    for i in range(order, len(arr)-order):
        left = arr[i-order:i]; right = arr[i+1:i+order+1]
        if arr[i] > np.max(np.concatenate((left, right))): highs.append(i)
        if arr[i] < np.min(np.concatenate((left, right))): lows.append(i)
    return np.array(highs), np.array(lows)

def hidden_divergence(price, osc, lookback=30, min_distance=5, order=3):
    sl_p = price[-lookback-1:-1]; sl_o = osc[-lookback-1:-1]
    highs, lows = find_local_extrema(sl_p, order=order)
    bull = bear = False
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if (b-a) >= min_distance and sl_p[b] > sl_p[a] and sl_o[b] < sl_o[a]:
            bull = True
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if (b-a) >= min_distance and sl_p[b] < sl_p[a] and sl_o[b] > sl_o[a]:
            bear = True
    return bull, bear

def in_pullback_zone(close_val, ema20, ema50, atr_val, tol_atr=0.10, direction='long'):
    lo = min(ema20, ema50) - tol_atr * atr_val
    hi = max(ema20, ema50) + tol_atr * atr_val
    if direction == 'long':
        return (close_val >= ema20) and (lo <= close_val <= hi)
    else:
        return (close_val < ema20) and (lo <= close_val <= hi)  # short daha katı

# ================== SEMBOL LİSTESİ (OTOMATİK) ==================
def all_bybit_linear_usdt_symbols(exchange):
    markets = exchange.load_markets()
    syms = []
    for s, m in markets.items():
        if m.get('swap') and m.get('linear') and m.get('quote') == 'USDT' and not m.get('option') and m.get('active', True):
            syms.append(s)
    return sorted(set(syms))

# ================== GÜVENLİ OHLCV ÇEKME ==================
def safe_fetch_ohlcv(symbol, timeframe, limit, retries=3, backoff=3):
    for attempt in range(retries):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            if attempt == retries-1:
                raise
            sleep_s = backoff * (attempt+1)
            logger.warning(f"fetch_ohlcv retry {attempt+1}/{retries} {symbol} {timeframe} ({e}), {sleep_s}s bekle")
            time.sleep(sleep_s)

# ================== DURUM (STATE) ==================
last_signal_time = {}   # { "SYMBOL": datetime }  # direction-independent cooldown
positions = {}          # { symbol: {...} }

def vol_k_from_atr_pct(atrpct):
    return TSL_K_LOWVOL if atrpct <= VOL_SPLIT_ATRPCT else TSL_K_HIGHVOL

# ================== SİNYAL DEĞERLENDİRME ==================
def evaluate_signal(df4, df2):
    # 4H rejim
    closes4 = df4['close'].values.astype(np.float64)
    macd4, sig4, hist4 = macd_lines(closes4, 12, 26, 9)
    ema50_4 = ema(closes4, 50); ema200_4 = ema(closes4, 200)
    atr4, pdi4, mdi4, adx4 = compute_atr_dm_di_adx(df4, 14)

    c4 = closes4[-2]
    regime_long  = (c4 > ema200_4[-2]) and (ema50_4[-2] > ema200_4[-2])
    regime_short = (c4 < ema200_4[-2]) and (ema50_4[-2] < ema200_4[-2])
    macd_ok_long_4h  = (hist4[-2] >= 0)
    macd_ok_short_4h = (hist4[-2] <= 0)
    atrpct4 = atr4[-2] / max(c4, 1e-9)
    atr_ok_4h = (ATRPCT_MIN_4H <= atrpct4 <= ATRPCT_MAX_4H)
    adx_ok_4h = (ADX_MIN_4H <= adx4[-2] <= ADX_MAX_4H)

    # 2H giriş
    closes2 = df2['close'].values.astype(np.float64)
    rsi2    = rsi(closes2, 14)
    rsi_ema9= ema(rsi2, 9)
    ema20_2 = ema(closes2, 20)
    ema50_2 = ema(closes2, 50)
    atr2, pdi2, mdi2, adx2 = compute_atr_dm_di_adx(df2, 14)

    c2 = closes2[-2]
    atr2_last = atr2[-2]
    bull_hidden, bear_hidden = hidden_divergence(closes2, rsi2, lookback=DIV_LOOKBACK, min_distance=DIV_MIN_DISTANCE, order=3)
    pullback_long  = in_pullback_zone(c2, ema20_2[-2], ema50_2[-2], atr2_last, PULLBACK_TOL_ATR, 'long')
    pullback_short = in_pullback_zone(c2, ema20_2[-2], ema50_2[-2], atr2_last, PULLBACK_TOL_ATR, 'short')
    rsi50_long     = (rsi2[-2] >= 50.0)
    rsi50_short    = (rsi2[-2] <= 50.0)
    trigger_long   = (rsi2[-3] <= rsi_ema9[-3]) and (rsi2[-2] > rsi_ema9[-2])
    trigger_short  = (rsi2[-3] >= rsi_ema9[-3]) and (rsi2[-2] < rsi_ema9[-2])
    di_ok_long     = (pdi2[-2] > mdi2[-2]) and (adx2[-2] >= DI_ADX_MIN_2H)
    di_ok_short    = (mdi2[-2] > pdi2[-2]) and (adx2[-2] >= DI_ADX_MIN_2H)

    # 2H mum teyidi
    close_2h = df2['close'].iloc[-2]; open_2h = df2['open'].iloc[-2]
    confirm_long  = (close_2h > ema20_2[-2]) and (close_2h > open_2h)
    confirm_short = (close_2h < ema20_2[-2]) and (close_2h < open_2h)

    # Gövde gücü: zayıf/doji mumları ele
    body = abs(close_2h - open_2h)
    body_ok_long  = (close_2h > open_2h) and (body >= 0.5 * atr2_last)
    body_ok_short = (close_2h < open_2h) and (body >= 0.5 * atr2_last)

    # Likidite: son 60 barın %60 persentili
    dollar_vol2 = (df2['close'] * df2['volume']).astype(float)
    recent = dollar_vol2.iloc[-61:-1]
    pct60 = np.nanpercentile(recent, 60) if recent.size else dollar_vol2.iloc[-2]
    liquidity_ok = bool(dollar_vol2.iloc[-2] >= pct60)

    long_crit  = [
        regime_long, macd_ok_long_4h, atr_ok_4h, adx_ok_4h,
        pullback_long, bull_hidden, rsi50_long, di_ok_long, trigger_long,
        confirm_long, liquidity_ok
    ]
    short_crit = [
        regime_short, macd_ok_short_4h, atr_ok_4h, adx_ok_4h,
        pullback_short, bear_hidden, rsi50_short, di_ok_short, trigger_short,
        confirm_short, liquidity_ok
    ]

    score_long  = sum(bool(x) for x in long_crit)
    score_short = sum(bool(x) for x in short_crit)

    # Entry gate (daha seçici)
    entry_gate_long  = trigger_long  and (pullback_long  or bull_hidden)  and confirm_long  and body_ok_long  and liquidity_ok
    entry_gate_short = trigger_short and (pullback_short or bear_hidden)  and confirm_short and body_ok_short and liquidity_ok

    direction = None
    reasons = None
    if entry_gate_long and score_long >= SCORE_MIN and score_long >= score_short and regime_long:
        direction = 'LONG'; reasons = long_crit
    elif entry_gate_short and score_short >= SCORE_MIN and score_short > score_long and regime_short:
        direction = 'SHORT'; reasons = short_crit

    meta = {
        "c2": c2, "atr2": atr2_last, "atrpct4": atrpct4,
        "adx4": adx4[-2], "rsi2": rsi2[-2], "adx2": adx2[-2], "+di2": pdi2[-2], "-di2": mdi2[-2],
        "score_long": score_long, "score_short": score_short,
        "reasons": reasons
    }
    return direction, meta

# ================== YARDIMCILAR ==================
def last_swing(df2, direction):
    # basit swing: son 10 barın high/low'u
    if direction == 'LONG':
        sw_low = float(df2['low'].iloc[-11:-1].min())
        return sw_low
    else:
        sw_high = float(df2['high'].iloc[-11:-1].max())
        return sw_high

def tp2_mult_from_atrpct(atrpct4):
    return TP2_ATR_MAX_MULT if atrpct4 <= VOL_SPLIT_ATRPCT else TP2_ATR_MIN_MULT

# ================== YÖNETİM ==================
async def manage_positions(symbol, df2_recent, atrpct4):
    if symbol not in positions or positions[symbol]['side'] is None:
        return
    pos = positions[symbol]

    last_closed = df2_recent.iloc[-2]  # kapalı bar
    live_bar    = df2_recent.iloc[-1]  # açık bar
    live_high = float(live_bar['high']); live_low = float(live_bar['low']); live_close = float(live_bar['close'])

    k = vol_k_from_atr_pct(atrpct4)
    atr2 = pos['atr2']

    def j(*parts): return "\n".join(parts)

    if pos['side'] == 'LONG':
        pos['highest'] = max(pos['highest'], live_high, float(last_closed['high']))
        if (not pos['tsl_on']) and (live_close >= pos['entry'] + TSL_ACTIVATION_ATR*atr2):
            pos['tsl_on'] = True
            await tg_send(j(
                f"{symbol} 2H: LONG TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}"
            ))
        if pos['tsl_on']:
            tsl = pos['highest'] - k*atr2
            if tsl > pos['sl']:
                pos['sl'] = tsl
        if (not pos['tp1_hit']) and (live_high >= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] -= 0.35
            pos['sl'] = pos['entry']
            await tg_send(j(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}"
            ))
        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_high >= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] -= 0.35
            await tg_send(j(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing"
            ))
        if live_low <= pos['sl']:
            pnl = (pos['sl'] - pos['entry'])/pos['entry']*100 * pos['remaining']
            await tg_send(j(
                f"{symbol} 2H: LONG EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%"
            ))
            del positions[symbol]

    else:  # SHORT
        pos['lowest'] = min(pos['lowest'], live_low, float(last_closed['low']))
        if (not pos['tsl_on']) and (live_close <= pos['entry'] - TSL_ACTIVATION_ATR*atr2):
            pos['tsl_on'] = True
            await tg_send(j(
                f"{symbol} 2H: SHORT TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}"
            ))
        if pos['tsl_on']:
            tsl = pos['lowest'] + k*atr2
            if tsl < pos['sl']:
                pos['sl'] = tsl
        if (not pos['tp1_hit']) and (live_low <= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] -= 0.35
            pos['sl'] = pos['entry']
            await tg_send(j(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}"
            ))
        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_low <= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] -= 0.35
            await tg_send(j(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing"
            ))
        if live_high >= pos['sl']:
            pnl = (pos['entry'] - pos['sl'])/pos['entry']*100 * pos['remaining']
            await tg_send(j(
                f"{symbol} 2H: SHORT EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%"
            ))
            del positions[symbol]

# ================== SİMBOL TARAYICI ==================
async def scan_symbol(symbol):
    try:
        df4 = pd.DataFrame(safe_fetch_ohlcv(symbol, REGIME_TF, LIMIT_4H),
                           columns=['timestamp','open','high','low','close','volume'])
        df2 = pd.DataFrame(safe_fetch_ohlcv(symbol, ENTRY_TF, LIMIT_2H),
                           columns=['timestamp','open','high','low','close','volume'])
        if len(df4) < 120 or len(df2) < 120:
            logger.warning(f"{symbol}: Veri yetersiz (4H: {len(df4)}, 2H: {len(df2)})")
            return

        direction, meta = evaluate_signal(df4, df2)

        # Pozisyon yönetimi (mevcut varsa)
        await manage_positions(symbol, df2.iloc[-2:].copy(), meta['atrpct4'])

        if not direction:
            logger.info(f"{symbol}: sinyal yok | L={meta['score_long']} S={meta['score_short']}")
            return

        # Cooldown (sembol bazlı)
        key = f"{symbol}"
        tz = pytz.timezone(TZ); now = datetime.now(tz)
        if key in last_signal_time and (now - last_signal_time[key]) < timedelta(minutes=COOLDOWN_MINUTES):
            logger.info(f"{symbol} {direction}: cooldown aktif")
            return
        last_signal_time[key] = now

        # SL/TP hesap
        c2 = meta['c2']; atr2 = meta['atr2']; atrpct4 = meta['atrpct4']
        tp2_mult = tp2_mult_from_atrpct(atrpct4)

        if direction == 'LONG':
            swing = last_swing(df2, 'LONG')
            sl = max(c2 - SL_ATR_MULT*atr2, swing)
            tp1 = c2 + TP1_ATR_MULT*atr2
            tp2 = c2 + tp2_mult*atr2
            positions[symbol] = {"side":"LONG","entry":c2,"sl":sl,
                                 "tp1_price":tp1,"tp2_price":tp2,"atr2":atr2,
                                 "tsl_on":False,"highest":c2,"lowest":None,
                                 "tp1_hit":False,"tp2_hit":False,"remaining":1.0}
        else:
            swing = last_swing(df2, 'SHORT')
            sl = min(c2 + SL_ATR_MULT*atr2, swing)
            tp1 = c2 - TP1_ATR_MULT*atr2
            tp2 = c2 - tp2_mult*atr2
            positions[symbol] = {"side":"SHORT","entry":c2,"sl":sl,
                                 "tp1_price":tp1,"tp2_price":tp2,"atr2":atr2,
                                 "tsl_on":False,"highest":None,"lowest":c2,
                                 "tp1_hit":False,"tp2_hit":False,"remaining":1.0}

        # Mesaj (güvenli join)
        names = [
            "4H Regime","4H MACD hist","4H ATR% band","4H ADX band",
            "2H Pullback","2H Hidden Div","2H RSI50","2H DI/ADX","2H RSI~EMA(9) trig",
            "2H Candle Confirm","Liquidity >= perc60"
        ]
        marks = ["✅" if b else "—" for b in meta['reasons']]
        score = meta['score_long'] if direction=='LONG' else meta['score_short']
        lines = [f"- {n}: {m}" for n, m in zip(names, marks)]
        detail_lines = "\n".join(lines)

        msg = "\n".join([
            f"{symbol} {ENTRY_TF}: {direction} SİNYAL ✅ (Skor {score}/{len(marks)})",
            f"Entry={c2:.6f}  SL={positions[symbol]['sl']:.6f}",
            f"TP1={positions[symbol]['tp1_price']:.6f}  TP2={positions[symbol]['tp2_price']:.6f}",
            f"ADX4H={meta['adx4']:.1f}  ATR%4H={meta['atrpct4']*100:.2f}%  RSI2H={meta['rsi2']:.1f}",
            detail_lines
        ])

        await tg_send(msg)
        logger.info(msg)

    except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
        logger.warning(f"{symbol}: network/timeout {e}")
    except ccxt.BaseError as e:
        logger.warning(f"{symbol}: exchange error {e}")
    except Exception as e:
        logger.exception(f"{symbol}: hata {e}")

# ================== BACKTEST (opsiyonel hızlı) ==================
def backtest_symbol(symbol, bars_2h=800):
    try:
        df4 = pd.DataFrame(safe_fetch_ohlcv(symbol, REGIME_TF, max(LIMIT_4H, 240)),
                           columns=['timestamp','open','high','low','close','volume'])
        df2 = pd.DataFrame(safe_fetch_ohlcv(symbol, ENTRY_TF,  max(LIMIT_2H, bars_2h)),
                           columns=['timestamp','open','high','low','close','volume'])
        if len(df4) < 120 or len(df2) < 200:
            return None
        wins = losses = 0
        i_start = 200
        for i in range(i_start, len(df2)-2):
            df2w = df2.iloc[:i+1]
            df4w = df4.iloc[: max(120, int((i/2)))]
            direction, meta = evaluate_signal(df4w, df2w)
            if not direction:
                continue
            entry = meta['c2']; atr2 = meta['atr2']
            tp1 = entry + TP1_ATR_MULT*atr2 if direction=='LONG' else entry - TP1_ATR_MULT*atr2
            tp2 = entry + TP2_ATR_MIN_MULT*atr2 if direction=='LONG' else entry - TP2_ATR_MIN_MULT*atr2
            end_j = min(i+40, len(df2)-1)
            ok = False
            for j in range(i+1, end_j+1):
                h = float(df2['high'].iloc[j]); l = float(df2['low'].iloc[j])
                if direction=='LONG' and (h>=tp1 or h>=tp2): ok=True; break
                if direction=='SHORT' and (l<=tp1 or l<=tp2): ok=True; break
            if ok: wins+=1
            else: losses+=1
        total = wins+losses
        if total==0: return None
        return {"symbol": symbol, "trades": total, "winrate": round(100*wins/total,2)}
    except Exception as e:
        logger.warning(f"backtest {symbol}: {e}")
        return None

# ================== ANA DÖNGÜ ==================
async def main():
    tz = pytz.timezone(TZ)
    await tg_send("Scanner başladı: " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("Scanner başladı: " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))

    # Telegram worker başlat
    asyncio.create_task(_tg_worker())

    symbols = all_bybit_linear_usdt_symbols(exchange)
    logger.info(f"Taranacak semboller: {len(symbols)}")
    if not symbols:
        logger.error("Sembol bulunamadı (permissions/region?).")
        return

    if BACKTEST_MODE:
        logger.info("BACKTEST başlıyor...")
        rows = []
        for s in symbols[:30]:
            r = backtest_symbol(s)
            if r: rows.append(r)
        if rows:
            avg_wr = np.nanmean([x['winrate'] for x in rows if x and x.get('winrate') is not None])
            await tg_send(f"Backtest bitti. Ortalama WR: {avg_wr:.2f}% ({len(rows)} sembol)")
        return

    # Canlı tarama
    stop_flag = False
    def _stop(*args):
        nonlocal stop_flag; stop_flag = True
        logger.info("Kapatma sinyali alındı. Döngü tamamlanınca çıkılacak.")
    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        try: os_signal.signal(sig, _stop)
        except Exception: pass

    while not stop_flag:
        try:
            tasks = [scan_symbol(s) for s in symbols]
            for i in range(0, len(tasks), BATCH_SIZE):
                await asyncio.gather(*tasks[i:i+BATCH_SIZE])
                await asyncio.sleep(2)
            logger.info(f"Tur bitti. {SCAN_SLEEP_SEC//60} dk bekleniyor...")
            await asyncio.sleep(SCAN_SLEEP_SEC)
        except Exception as e:
            logger.exception(f"Ana döngü hata: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
