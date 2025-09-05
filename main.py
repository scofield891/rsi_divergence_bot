# -*- coding: utf-8 -*-
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
from telegram.request import HTTPXRequest
import signal as os_signal
import time

"""
Bybit High-WR Scanner (4H regime + 2H entry) – v1.7.0

Güncel değişiklik:
- 2H EMA13/SMA34 kesişimi son 10 kapalı mum içinde gerçekleşmiş olmalı (hard gate).
- Kesişimin yönü: long için EMA13↑SMA34, short için EMA13↓SMA34.
- Fiyat konumu: long'ta kapanış SMA34 üstü, short'ta SMA34 altı.

Diğerleri:
- SCORE_MIN=8
- RSI50 (long ≥50, short ≤50)
- SL=1.8×ATR%, TP1=2.0×ATR%, TP2=3.5×ATR%
- TP1 %30 + BE, TP2 %40, kalan %30 TSL
- Wilder ADX/DI, likidite & body filtreleri
- Telegram queue + rate limit
"""

# ================== KULLANICI AYARLARI ==================
BOT_TOKEN = "7608720362:AAHp10_7CVfEYoBtPWlQPxH37rrn40NbIuY"
CHAT_ID = "-1002755412514"
TZ = 'Europe/Istanbul'

LIVE_SCAN_MODE = True
BACKTEST_MODE = False

REGIME_TF = '4h'
ENTRY_TF  = '2h'
SCAN_SLEEP_SEC = 300
BATCH_SIZE = 16
COOLDOWN_MINUTES = 90

# --- High-WR preset ---
SCORE_MIN = 8
ADX_MIN_4H = 20
ADX_MAX_4H = 35
ATRPCT_MIN_4H = 0.012   # %1.2
ATRPCT_MAX_4H = 0.050   # %5.0
PULLBACK_TOL_ATR = 0.08 # 0.08 x ATR (2H)
DI_ADX_MIN_2H = 18

DIV_LOOKBACK = 30
DIV_MIN_DISTANCE = 6

# SL/TP/Trail (ATR% bazlı)
SL_ATR_MULT        = 1.8
TP1_ATR_MULT       = 2.0
TP2_ATR_MULT       = 3.5
TSL_ACTIVATION_ATR = 1.0
TSL_K_LOWVOL  = 2.0
TSL_K_HIGHVOL = 2.5
VOL_SPLIT_ATRPCT = 0.030  # %3

BODY_ATR_MULT = 0.5
LIQ_ROLL_BARS = 60
LIQ_QUANTILE  = 0.60

TP1_CLOSE_RATIO   = 0.30
TP2_CLOSE_RATIO   = 0.40

LIMIT_4H = 260
LIMIT_2H = 520

TELEGRAM_MAX_MSG_PER_SEC = 3.0

# EMA13/SMA34 kesişim lookback
CROSS_LOOKBACK = 10  # son 10 kapalı mum içinde

# ================== LOG ==================
logger = logging.getLogger("scanner")
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler('scanner.log'); fh.setFormatter(fmt); logger.addHandler(fh)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# ================== EXCHANGE ==================
exchange = ccxt.bybit({'enableRateLimit': True, 'options': {'defaultType': 'linear'}, 'timeout': 60000})

# ================== TELEGRAM (Kuyruk + Worker) ==================
_request = HTTPXRequest(
    connection_pool_size=20,
    read_timeout=20.0,
    write_timeout=20.0,
    connect_timeout=10.0,
    pool_timeout=5.0,
)
telegram_bot = Bot(token=BOT_TOKEN, request=_request) if BOT_TOKEN else None
_msg_queue: asyncio.Queue[str] = asyncio.Queue()

async def tg_send(text: str):
    await _msg_queue.put(text)

async def telegram_worker():
    if not telegram_bot:
        while True:
            msg = await _msg_queue.get()
            logger.info(f"TELEGRAM(MOCK): {msg}")
            _msg_queue.task_done()
    else:
        min_interval = 1.0 / TELEGRAM_MAX_MSG_PER_SEC
        last_sent = 0.0
        while True:
            text = await _msg_queue.get()
            now = time.monotonic()
            delta = now - last_sent
            if delta < min_interval:
                await asyncio.sleep(min_interval - delta)
            try:
                await telegram_bot.send_message(chat_id=CHAT_ID, text=text)
            except Exception as e:
                logger.error(f"Telegram hata: {e}")
            last_sent = time.monotonic()
            _msg_queue.task_done()

# ================== İNDİKATÖR FONKSİYONLARI ==================
def ema(arr, n):
    arr = np.asarray(arr, dtype=np.float64)
    out = np.empty_like(arr)
    k = 2.0 / (n + 1.0)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1 - k)
    return out

def sma(arr, n):
    s = pd.Series(np.asarray(arr, dtype=np.float64))
    return s.rolling(n).mean().to_numpy()

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
        roll_up[i]   = (roll_up[i-1]*(period-1)   + up[i-1])   / period
        roll_down[i] = (roll_down[i-1]*(period-1) + down[i-1]) / period
    rs = roll_up / roll_down
    rsi_vals[:period] = 50.0
    rsi_vals[period:] = 100.0 - (100.0 / (1.0 + rs[period:]))
    return np.nan_to_num(rsi_vals, nan=50.0, posinf=100.0, neginf=0.0)

def compute_atr_dm_di_adx(df, period=14):
    """Wilder ATR/DI/ADX."""
    high = df['high'].values.astype(np.float64)
    low  = df['low'].values.astype(np.float64)
    close= df['close'].values.astype(np.float64)

    tr = np.zeros_like(close)
    tr[0] = high[0] - low[0]
    for i in range(1, len(close)):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))

    up_move = np.maximum(high[1:] - high[:-1], 0.0)
    dn_move = np.maximum(low[:-1]  - low[1:],  0.0)
    plus_dm  = np.zeros_like(close); plus_dm[1:]  = np.where((up_move>dn_move) & (up_move>0), up_move, 0.0)
    minus_dm = np.zeros_like(close); minus_dm[1:] = np.where((dn_move>up_move) & (dn_move>0), dn_move, 0.0)

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

    plus_di  = 100.0 * (p_dm_s / np.maximum(atr, 1e-9))
    minus_di = 100.0 * (m_dm_s / np.maximum(atr, 1e-9))

    dx  = 100.0 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-9)
    adx = np.zeros_like(close); adx[:2*period-1] = np.nan
    if len(close) >= 2*period:
        adx[2*period-1] = np.nanmean(dx[period:2*period])
        for i in range(2*period, len(close)):
            adx[i] = ((adx[i-1]*(period-1)) + dx[i]) / period

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
        return (close_val <  ema20) and (lo <= close_val <= hi)

# ================== SEMBOLLER ==================
def all_bybit_linear_usdt_symbols(exchange):
    markets = exchange.load_markets()
    syms = []
    for s, m in markets.items():
        if m.get('swap') and m.get('linear') and m.get('quote') == 'USDT' and not m.get('option') and m.get('active', True):
            syms.append(s)
    return sorted(set(syms))

# ================== GÜVENLİ OHLCV ==================
def safe_fetch_ohlcv(symbol, timeframe, limit, retries=3, backoff=3):
    for attempt in range(retries):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            if attempt == retries - 1:
                raise
            sleep_s = backoff * (attempt + 1)
            logger.warning(f"fetch_ohlcv retry {attempt+1}/{retries} {symbol} {timeframe} ({e}), {sleep_s}s bekle")
            time.sleep(sleep_s)

# ================== DURUM ==================
last_signal_time = {}  # { symbol: datetime }
positions        = {}  # { symbol: {...} }

def vol_k_from_atr_pct(atrpct):
    return TSL_K_LOWVOL if atrpct <= VOL_SPLIT_ATRPCT else TSL_K_HIGHVOL

# ======= EMA13/SMA34 KESİŞİM “SON N BAR” =======
def crossed_up_recent(ema_arr, sma_arr, lb):
    # son lb kapalı barda (t=-2 ... -lb-1) herhangi birinde up-cross
    for k in range(2, lb + 2):
        if ema_arr[-k-1] <= sma_arr[-k-1] and ema_arr[-k] > sma_arr[-k]:
            return True
    return False

def crossed_down_recent(ema_arr, sma_arr, lb):
    for k in range(2, lb + 2):
        if ema_arr[-k-1] >= sma_arr[-k-1] and ema_arr[-k] < sma_arr[-k]:
            return True
    return False

# ================== SİNYAL DEĞERLENDİRME ==================
def evaluate_signal(df4, df2):
    # --- 4H rejim ---
    closes4 = df4['close'].values.astype(np.float64)
    macd4, sig4, hist4 = macd_lines(closes4, 12, 26, 9)
    ema50_4  = ema(closes4, 50)
    ema200_4 = ema(closes4, 200)
    atr4, pdi4, mdi4, adx4 = compute_atr_dm_di_adx(df4, 14)

    c4 = closes4[-2]
    regime_long  = (c4 > ema200_4[-2]) and (ema50_4[-2] > ema200_4[-2])
    regime_short = (c4 < ema200_4[-2]) and (ema50_4[-2] < ema200_4[-2])

    macd_ok_long_4h  = (hist4[-2] >= 0)
    macd_ok_short_4h = (hist4[-2] <= 0)

    atrpct4 = atr4[-2] / max(c4, 1e-9)
    atr_ok_4h = (ATRPCT_MIN_4H <= atrpct4 <= ATRPCT_MAX_4H)
    adx_ok_4h = (ADX_MIN_4H <= adx4[-2] <= ADX_MAX_4H)

    # --- 2H giriş ---
    closes2 = df2['close'].values.astype(np.float64)
    opens2  = df2['open'].values.astype(np.float64)
    rsi2    = rsi(closes2, 14)
    rsi_ema9= ema(rsi2, 9)
    ema13_2 = ema(closes2, 13)
    sma34_2 = sma(closes2, 34)
    ema20_2 = ema(closes2, 20)
    ema50_2 = ema(closes2, 50)
    atr2, pdi2, mdi2, adx2 = compute_atr_dm_di_adx(df2, 14)

    c2        = closes2[-2]
    o2        = opens2[-2]
    atr2_last = atr2[-2]
    atrpct2   = atr2_last / max(c2, 1e-9)

    # ZORUNLU koşul: son 10 kapalı mum içinde kesişim + fiyat konumu
    cross_long_recent  = crossed_up_recent(ema13_2, sma34_2, CROSS_LOOKBACK)
    cross_short_recent = crossed_down_recent(ema13_2, sma34_2, CROSS_LOOKBACK)

    hard_long  = cross_long_recent  and (c2 > sma34_2[-2])
    hard_short = cross_short_recent and (c2 < sma34_2[-2])

    bull_hidden, bear_hidden = hidden_divergence(closes2, rsi2, lookback=DIV_LOOKBACK, min_distance=DIV_MIN_DISTANCE, order=3)
    pullback_long  = in_pullback_zone(c2, ema20_2[-2], ema50_2[-2], atr2_last, PULLBACK_TOL_ATR, 'long')
    pullback_short = in_pullback_zone(c2, ema20_2[-2], ema50_2[-2], atr2_last, PULLBACK_TOL_ATR, 'short')

    rsi50_long  = (rsi2[-2] >= 50.0)
    rsi50_short = (rsi2[-2] <= 50.0)

    trigger_long  = (rsi2[-3] <= rsi_ema9[-3]) and (rsi2[-2] >  rsi_ema9[-2])
    trigger_short = (rsi2[-3] >= rsi_ema9[-3]) and (rsi2[-2] <  rsi_ema9[-2])

    di_ok_long  = (pdi2[-2] >  mdi2[-2]) and (adx2[-2] >= DI_ADX_MIN_2H)
    di_ok_short = (mdi2[-2] >  pdi2[-2]) and (adx2[-2] >= DI_ADX_MIN_2H)

    confirm_long  = (c2 > ema20_2[-2]) and (c2 > o2)
    confirm_short = (c2 < ema20_2[-2]) and (c2 < o2)
    body_size     = abs(c2 - o2)
    body_ok       = (body_size >= BODY_ATR_MULT * (atrpct2 * c2))

    dollar_vol2 = (df2['close'] * df2['volume']).astype(float)
    q = dollar_vol2.rolling(LIQ_ROLL_BARS, min_periods=max(10, LIQ_ROLL_BARS//2)).quantile(LIQ_QUANTILE)
    liquidity_ok = bool(dollar_vol2.iloc[-2] >= q.iloc[-2])

    long_crit = [
        regime_long, macd_ok_long_4h, atr_ok_4h, adx_ok_4h,
        hard_long,            # <— EMA13/SMA34 cross ≤10 bar + price above SMA34
        pullback_long, bull_hidden, rsi50_long, di_ok_long, trigger_long,
        confirm_long, body_ok, liquidity_ok
    ]
    short_crit = [
        regime_short, macd_ok_short_4h, atr_ok_4h, adx_ok_4h,
        hard_short,           # <— EMA13/SMA34 cross ≤10 bar + price below SMA34
        pullback_short, bear_hidden, rsi50_short, di_ok_short, trigger_short,
        confirm_short, body_ok, liquidity_ok
    ]

    score_long  = sum(bool(x) for x in long_crit)
    score_short = sum(bool(x) for x in short_crit)

    direction = None
    reasons   = None
    # ZORUNLU: hard_long/hard_short sağlanmalı
    if score_long >= SCORE_MIN and score_long >= score_short and regime_long and hard_long:
        direction, reasons = 'LONG',  long_crit
    elif score_short >= SCORE_MIN and score_short >  score_long and regime_short and hard_short:
        direction, reasons = 'SHORT', short_crit

    meta = {
        "c2": c2, "o2": o2, "atr2": atr2_last, "atrpct2": atrpct2,
        "atrpct4": atrpct4, "adx4": adx4[-2], "rsi2": rsi2[-2], "adx2": adx2[-2],
        "+di2": pdi2[-2], "-di2": mdi2[-2],
        "score_long": score_long, "score_short": score_short,
        "reasons": reasons
    }
    return direction, meta

# ================== YARDIMCILAR ==================
def last_swing(df2, direction):
    if direction == 'LONG':
        return float(df2['low'].iloc[-11:-1].min())
    else:
        return float(df2['high'].iloc[-11:-1].max())

def vol_k_from_atr_pct(atrpct):
    return TSL_K_LOWVOL if atrpct <= VOL_SPLIT_ATRPCT else TSL_K_HIGHVOL

# ================== YÖNETİM ==================
async def manage_positions(symbol, df2_recent, atrpct4):
    if symbol not in positions or positions[symbol]['side'] is None:
        return
    pos = positions[symbol]

    last_closed = df2_recent.iloc[-2]
    live_bar    = df2_recent.iloc[-1]
    live_high = float(live_bar['high']); live_low = float(live_bar['low']); live_close = float(live_bar['close'])

    k = vol_k_from_atr_pct(atrpct4)
    atr_px = pos.get('atr_px', pos['atr2'])  # ATR px adımı

    def J(*parts): return "\n".join(parts)

    if pos['side'] == 'LONG':
        pos['highest'] = max(pos['highest'], live_high, float(last_closed['high']))

        if (not pos['tsl_on']) and (live_close >= pos['entry'] + TSL_ACTIVATION_ATR * atr_px):
            pos['tsl_on'] = True
            await tg_send(J(
                f"{symbol} 2H: LONG TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}"
            ))

        if pos['tsl_on']:
            tsl = pos['highest'] - k * atr_px
            if tsl > pos['sl']:
                pos['sl'] = tsl

        if (not pos['tp1_hit']) and (live_high >= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] = max(0.0, pos['remaining'] - TP1_CLOSE_RATIO)
            pos['sl'] = max(pos['sl'], pos['entry'])  # BE
            await tg_send(J(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}"
            ))

        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_high >= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] = max(0.0, pos['remaining'] - TP2_CLOSE_RATIO)
            await tg_send(J(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing"
            ))

        if live_low <= pos['sl']:
            pnl = (pos['sl'] - pos['entry'])/pos['entry']*100 * pos['remaining']
            await tg_send(J(
                f"{symbol} 2H: LONG EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%"
            ))
            del positions[symbol]

    else:  # SHORT
        pos['lowest'] = min(pos['lowest'], live_low, float(last_closed['low']))

        if (not pos['tsl_on']) and (live_close <= pos['entry'] - TSL_ACTIVATION_ATR * atr_px):
            pos['tsl_on'] = True
            await tg_send(J(
                f"{symbol} 2H: SHORT TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}"
            ))

        if pos['tsl_on']:
            tsl = pos['lowest'] + k * atr_px
            if tsl < pos['sl']:
                pos['sl'] = tsl

        if (not pos['tp1_hit']) and (live_low <= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] = max(0.0, pos['remaining'] - TP1_CLOSE_RATIO)
            pos['sl'] = min(pos['sl'], pos['entry'])  # BE
            await tg_send(J(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}"
            ))

        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_low <= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] = max(0.0, pos['remaining'] - TP2_CLOSE_RATIO)
            await tg_send(J(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing"
            ))

        if live_high >= pos['sl']:
            pnl = (pos['entry'] - pos['sl'])/pos['entry']*100 * pos['remaining']
            await tg_send(J(
                f"{symbol} 2H: SHORT EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%"
            ))
            del positions[symbol]

# ================== SİNYAL TARAYICI ==================
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

        # Mevcut pozisyon varsa yönet
        await manage_positions(symbol, df2.iloc[-2:].copy(), meta['atrpct4'])

        if not direction:
            logger.info(f"{symbol}: sinyal yok | L={meta['score_long']} S={meta['score_short']}")
            return

        # Cooldown
        key = symbol
        tz = pytz.timezone(TZ); now = datetime.now(tz)
        if key in last_signal_time and (now - last_signal_time[key]) < timedelta(minutes=COOLDOWN_MINUTES):
            logger.info(f"{symbol} {direction}: cooldown aktif")
            return
        last_signal_time[key] = now

        # ATR% -> fiyat adımı
        c2        = meta['c2']
        atr2      = meta['atr2']
        atrpct2   = meta['atrpct2']
        atrpct4   = meta['atrpct4']
        atr_px    = atrpct2 * c2

        # SL/TP
        if direction == 'LONG':
            swing = last_swing(df2, 'LONG')
            sl  = max(c2 - SL_ATR_MULT * atr_px, swing)
            tp1 = c2 + TP1_ATR_MULT * atr_px
            tp2 = c2 + TP2_ATR_MULT * atr_px
            positions[symbol] = {
                "side":"LONG","entry":c2,"sl":sl,
                "tp1_price":tp1,"tp2_price":tp2,
                "atr2":atr2,"atr_pct":atrpct2,"atr_px":atr_px,
                "tsl_on":False,"highest":c2,"lowest":None,
                "tp1_hit":False,"tp2_hit":False,"remaining":1.0
            }
        else:
            swing = last_swing(df2, 'SHORT')
            sl  = min(c2 + SL_ATR_MULT * atr_px, swing)
            tp1 = c2 - TP1_ATR_MULT * atr_px
            tp2 = c2 - TP2_ATR_MULT * atr_px
            positions[symbol] = {
                "side":"SHORT","entry":c2,"sl":sl,
                "tp1_price":tp1,"tp2_price":tp2,
                "atr2":atr2,"atr_pct":atrpct2,"atr_px":atr_px,
                "tsl_on":False,"highest":None,"lowest":c2,
                "tp1_hit":False,"tp2_hit":False,"remaining":1.0
            }

        # Telegram mesajı
        names = [
            "4H Regime","4H MACD hist","4H ATR% band","4H ADX band",
            f"2H EMA13/SMA34 Cross≤{CROSS_LOOKBACK}","2H Pullback","2H Hidden Div","2H RSI50",
            "2H DI/ADX","2H RSI~EMA(9) trig","2H Candle Confirm","2H Body>=0.5*ATR",
            f"Liquidity >= Q{int(LIQ_QUANTILE*100)}"
        ]
        marks = ["✅" if b else "—" for b in meta['reasons']]
        score = meta['score_long'] if direction=='LONG' else meta['score_short']
        detail_lines = "\n".join(f"- {n}: {m}" for n, m in zip(names, marks))

        msg = "\n".join([
            f"{symbol} {ENTRY_TF.lower()}: {direction} SİNYAL ✅ (Skor {score}/{len(marks)})",
            f"Entry={c2:.6f}  SL={positions[symbol]['sl']:.6f}",
            f"TP1={positions[symbol]['tp1_price']:.6f}  TP2={positions[symbol]['tp2_price']:.6f}",
            f"ADX4H={meta['adx4']:.1f}  ATR%4H={atrpct4*100:.2f}%  RSI2H={meta['rsi2']:.1f}",
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

# ================== BASİT BACKTEST (opsiyonel) ==================
def backtest_symbol(symbol, bars_2h=800):
    try:
        df4 = pd.DataFrame(safe_fetch_ohlcv(symbol, REGIME_TF, max(LIMIT_4H, 240)),
                           columns=['timestamp','open','high','low','close','volume'])
        df2 = pd.DataFrame(safe_fetch_ohlcv(symbol, ENTRY_TF, max(LIMIT_2H, bars_2h)),
                           columns=['timestamp','open','high','low','close','volume'])
        if len(df4) < 120 or len(df2) < 200:
            return None
        wins = losses = 0
        i_start = 200
        for i in range(i_start, len(df2)-2):
            df2w = df2.iloc[:i+1]
            df4w = df4.iloc[: max(120, int(i/2))]
            direction, meta = evaluate_signal(df4w, df2w)
            if not direction:
                continue
            entry = meta['c2']; atr2 = meta['atr2']
            tp1 = entry + TP1_ATR_MULT*atr2 if direction=='LONG' else entry - TP1_ATR_MULT*atr2
            tp2 = entry + TP2_ATR_MULT*atr2 if direction=='LONG' else entry - TP2_ATR_MULT*atr2
            end_j = min(i+40, len(df2)-1)
            ok = False
            for j in range(i+1, end_j+1):
                h = float(df2['high'].iloc[j]); l = float(df2['low'].iloc[j])
                if direction=='LONG'  and (h>=tp1 or h>=tp2): ok=True; break
                if direction=='SHORT' and (l<=tp1 or l<=tp2): ok=True; break
            if ok: wins+=1
            else: losses+=1
        total = wins+losses
        if total==0: return None
        return {"symbol": symbol, "trades": total, "winrate": round(100*wins/total,2)}
    except Exception as e:
        logger.warning(f"backtest {symbol}: {e}")
        return None

# ================== ANA ==================
async def main():
    tz = pytz.timezone(TZ)

    # Telegram worker'ı önce başlat
    worker_task = asyncio.create_task(telegram_worker())

    start_msg = "Scanner başladı: " + datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    await tg_send(start_msg)
    logger.info(start_msg)

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

    # Graceful stop
    stop_flag = False
    def _stop(*args):
        nonlocal stop_flag
        stop_flag = True
        logger.info("Kapatma sinyali alındı. Döngü tamamlanınca çıkılacak.")
    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        try: os_signal.signal(sig, _stop)
        except Exception: pass

    try:
        while not stop_flag:
            tasks = [scan_symbol(s) for s in symbols]
            for i in range(0, len(tasks), BATCH_SIZE):
                await asyncio.gather(*tasks[i:i+BATCH_SIZE])
                await asyncio.sleep(2)
            logger.info(f"Tur bitti. {SCAN_SLEEP_SEC//60} dk bekleniyor...")
            await asyncio.sleep(SCAN_SLEEP_SEC)
    finally:
        try:
            await _msg_queue.join()
        finally:
            worker_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
