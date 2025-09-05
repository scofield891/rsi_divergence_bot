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

# Bybit High-WR Scanner (4H regime + 2H entry) – v1.3.2
# - High-WR preset (seçici parametreler)
# - 2H mum teyidi + likidite filtresi
# - Bybit USDT linear perp sembollerini otomatik tarar
# - SL/TP (partial) + Trailing
# - Telegram bildirimleri, cooldown, basit hata toleransı
# Not: BOT_TOKEN ve CHAT_ID'i çevre değişkenlerinden okuyun.

# ================== KULLANICI AYARLARI ==================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # Güvenlik için default boş bırakıldı
CHAT_ID   = os.getenv("CHAT_ID",  "")
TZ        = 'Europe/Istanbul'

# Çalışma modu
LIVE_SCAN_MODE  = True   # canlı tarama ve yönetim
BACKTEST_MODE   = False  # basit backtest (önemsizse False kalsın)

# Taramalar
REGIME_TF = '4h'
ENTRY_TF  = '2h'
SCAN_SLEEP_SEC = 300
BATCH_SIZE = 16
COOLDOWN_MINUTES = 90

# --- YÜKSEK WIN-RATE için preset ---
SCORE_MIN = 6          # 5 -> 6 (daha seçici)
ADX_MIN_4H = 20        # 18 -> 20
ADX_MAX_4H = 35        # 40 -> 35
ATRPCT_MIN_4H = 0.012  # %1.0 -> %1.2
ATRPCT_MAX_4H = 0.050  # %6.0 -> %5.0
PULLBACK_TOL_ATR = 0.08 # 0.10 -> 0.08

# Divergence / DI-ADX pencereleri
DIV_LOOKBACK = 30       # hidden divergence için bakılan pencere (bar)
DIV_MIN_DISTANCE = 6    # iki ekstrem arası min mesafe
DI_ADX_MIN_2H = 18      # 2H DI/ADX için minimum ADX eşiği

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

# Güvenli satır birleştirme
NL = "
"

async def tg_send(text: str):
    if not telegram_bot:
        logger.info(f"TELEGRAM(MOCK): {text}")
        return
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, telegram_bot.send_message, CHAT_ID, text)

async def tg_send_lines(*parts):
    await tg_send(NL.join(parts))

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
    for i in range(period, len(close)): atr[i] = atr[i-1] - (atr[i-1]/period) + tr[i]
    atr = atr / period

    p_dm_s = np.zeros_like(close); p_dm_s[period-1] = np.sum(plus_dm[:period])
    m_dm_s = np.zeros_like(close); m_dm_s[period-1] = np.sum(minus_dm[:period])
    for i in range(period, len(close)):
        p_dm_s[i] = p_dm_s[i-1] - (p_dm_s[i-1]/period) + plus_dm[i]
        m_dm_s[i] = m_dm_s[i-1] - (m_dm_s[i-1]/period) + minus_dm[i]

    # DI: 100 * (smoothed_DM/period) / ATR  (ATR zaten period'e bölünmüş)
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
        return (close_val <= ema20) and (lo <= close_val <= hi)

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
last_signal_time = {}   # { "SYMBOL_DIR": datetime }
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

    # 2H mum teyidi + likidite filtresi
    confirm_long  = (df2['close'].iloc[-2] > ema20_2[-2]) and (df2['close'].iloc[-2] > df2['open'].iloc[-2])
    confirm_short = (df2['close'].iloc[-2] < ema20_2[-2]) and (df2['close'].iloc[-2] < df2['open'].iloc[-2])

    dollar_vol2 = (df2['close'] * df2['volume']).astype(float)
    med = dollar_vol2.rolling(30, min_periods=1).median()
    liquidity_ok = bool(dollar_vol2.iloc[-2] >= med.iloc[-2])

    long_crit = [
        regime_long, macd_ok_long_4h, atr_ok_4h, adx_ok_4h,
        pullback_long, bull_hidden, rsi50_long, di_ok_long, trigger_long,
        confirm_long, liquidity_ok
    ]
    short_crit= [
        regime_short, macd_ok_short_4h, atr_ok_4h, adx_ok_4h,
        pullback_short, bear_hidden, rsi50_short, di_ok_short, trigger_short,
        confirm_short, liquidity_ok
    ]

    score_long  = sum(bool(x) for x in long_crit)
    score_short = sum(bool(x) for x in short_crit)

    direction = None
    reasons = None
    if score_long >= SCORE_MIN and score_long >= score_short and regime_long:
        direction = 'LONG'; reasons = long_crit
    elif score_short >= SCORE_MIN and score_short > score_long and regime_short:
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

    if pos['side'] == 'LONG':
        pos['highest'] = max(pos['highest'], live_high, float(last_closed['high']))
        if (not pos['tsl_on']) and (live_close >= pos['entry'] + TSL_ACTIVATION_ATR*atr2):
            pos['tsl_on'] = True
            await tg_send_lines(
                f"{symbol} 2H: LONG TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}",
            )
        if pos['tsl_on']:
            tsl = pos['highest'] - k*atr2
            if tsl > pos['sl']:
                pos['sl'] = tsl
        if (not pos['tp1_hit']) and (live_high >= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] -= 0.35
            pos['sl'] = pos['entry']
            await tg_send_lines(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}",
            )
        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_high >= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] -= 0.35
            await tg_send_lines(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing",
            )
        if live_low <= pos['sl']:
            pnl = (pos['sl'] - pos['entry'])/pos['entry']*100
            await tg_send_lines(
                f"{symbol} 2H: LONG EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%",
            )
            positions[symbol] = {"side": None}

    else:  # SHORT
        pos['lowest'] = min(pos['lowest'], live_low, float(last_closed['low']))
        if (not pos['tsl_on']) and (live_close <= pos['entry'] - TSL_ACTIVATION_ATR*atr2):
            pos['tsl_on'] = True
            await tg_send_lines(
                f"{symbol} 2H: SHORT TSL aktif 🔧",
                f"Entry: {pos['entry']:.6f}  New SL: {pos['sl']:.6f}",
            )
        if pos['tsl_on']:
            tsl = pos['lowest'] + k*atr2
            if tsl < pos['sl']:
                pos['sl'] = tsl
        if (not pos['tp1_hit']) and (live_low <= pos['tp1_price']):
            pos['tp1_hit'] = True
            pos['remaining'] -= 0.35
            pos['sl'] = pos['entry']
            await tg_send_lines(
                f"{symbol} 2H: TP1 🎯",
                f"TP1={pos['tp1_price']:.6f}  SL->BE {pos['sl']:.6f}  Kalan %{pos['remaining']*100:.0f}",
            )
        if pos['tp1_hit'] and (not pos['tp2_hit']) and (live_low <= pos['tp2_price']):
            pos['tp2_hit'] = True
            pos['remaining'] -= 0.35
            await tg_send_lines(
                f"{symbol} 2H: TP2 🎯",
                f"TP2={pos['tp2_price']:.6f}  Kalan %{pos['remaining']*100:.0f} trailing",
            )
        if live_high >= pos['sl']:
            pnl = (pos['entry'] - pos['sl'])/pos['entry']*100
            await tg_send_lines(
                f"{symbol} 2H: SHORT EXIT ✅",
                f"Exit={pos['sl']:.6f}  PnL={pnl:.2f}%",
            )
            positions[symbol] = {"side": None}

# ================== SİMBOL TARAYICI ==================
async def scan_symbol(symbol):
    try:
        df4 = pd.DataFrame(safe_fetch_ohlcv(symbol, REGIME_TF, LIMIT_4H),
                           columns=['timestamp','open','high','low','close','volume'])
        df2 = pd.DataFrame(safe_fetch_ohlcv(symbol, ENTRY_TF,  LIMIT_2H),
                           columns=['timestamp','open','high','low','close','volume'])
        if len(df4) < 120 or len(df2) < 120:
            return

        direction, meta = evaluate_signal(df4, df2)

        # Pozisyon yönetimi (mevcut varsa)
        await manage_positions(symbol, df2.iloc[-2:].copy(), meta['atrpct4'])

        if not direction:
            logger.info(f"{symbol}: sinyal yok | L={meta['score_long']} S={meta['score_short']}")
            return

        # Cooldown kontrol
        key = f"{symbol}_{direction}"
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

        # Mesaj (join ile güvenli)
        names = [
            "4H Regime","4H MACD hist","4H ATR% band","4H ADX band",
            "2H Pullback","2H Hidden Div","2H RSI50","2H DI/ADX","2H RSI~EMA(9) trig",
            "2H Candle Confirm","Liquidity >= 30-bar median"
        ]
        marks = ["✅" if b else "—" for b in meta['reasons']]
        score = meta['score_long'] if direction == 'LONG' else meta['score_short']
        lines = [f"- {n}: {m}" for n, m in zip(names, marks)]
        detail_lines = NL.join(lines)
        msg_lines = [
            f"{symbol} {ENTRY_TF}: {direction} SİNYAL ✅ (Skor {score}/{len(marks)})",
            f"Entry={c2:.6f}  SL={positions[symbol]['sl']:.6f}",
            f"TP1={positions[symbol]['tp1_price']:.6f}  TP2={positions[symbol]['tp2_price']:.6f}",
            f"ADX4H={meta['adx4']:.1f}  ATR%4H={meta['atrpct4']*100:.2f}%  RSI2H={meta['rsi2']:.1f}",
            detail_lines,
        ]
        msg = NL.join(msg_lines)

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
    # İsteğe bağlı basit backtest – WR odaklı değilseniz kullanmayın
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
    await tg_send_lines(
        "Scanner başladı:",
        datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    )

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
            await tg_send_lines(
                f"Backtest bitti. Ortalama WR: {avg_wr:.2f}% ({len(rows)} sembol)"
            )
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
