import asyncio
import json
import gzip
import os
import logging
import sqlite3
import gc
import time
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# === НАСТРОЙКИ v8.33 (1h Telemetry Logging & USDC Filter) ===
DB_PATH = 'bot.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', -1003407154454))
BINGX_API_KEY = os.getenv('BINGX_API_KEY')
BINGX_SECRET = os.getenv('BINGX_SECRET')

RISK_PER_TRADE = 0.02      
MAX_POSITIONS = 3           
LEVERAGE = 10               
MAX_SPREAD_PERCENT = 0.002  
MIN_VOLUME_USDT = 1000000  
MIN_NOTIONAL_USDT = 10.0    
WHALE_VOLUME_MULTIPLIER = 8.0 

SMC_TIMEFRAME = '15m'

CRITICAL_CVD_USDT = 150000  
GLOBAL_CVD = {}             
COOLDOWN_CACHE = {}         
LEVERAGE_CACHE = set()

MAX_CONCURRENT_TASKS = 10  
SCAN_LIMIT = 300           

EXCLUDED_KEYWORDS = ['NCS', 'NCFX', 'NCCO', 'NCSI', 'NIKKEI', 'NASDAQ', 'SP500', 'GOLD', 'SILVER', 'AUT', 'XAU', 'PAXG', 'EUR', '1000', 'LUNC', 'USTC', 'BTC/', 'ETH/', 'BNB/', 'SOL/', 'XRP/', 'ADA/', 'TRX/', 'USDC']

SYMBOL_MAP = {'TONCOIN': 'TON', 'SATS': '1000SATS', 'RATS': '1000RATS', 'PEPE': '1000PEPE', 'BONK': '1000BONK', 'FLOKI': '1000FLOKI', 'SHIB': '1000SHIB', 'LUNA': 'LUNA2'}

GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 
NOTIFIED_SYMBOLS = set() 
GLOBAL_BALANCE = 0.0 

HOT_LIST = {}  
ACTIVE_WSS_CONNECTIONS = set()
daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': 0.0}
active_positions = []

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

def get_mem_usage():
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if 'VmRSS' in line: return f"{int(line.split()[1]) / 1024:.1f} MB"
    except: pass
    return "N/A"

exchange = ccxt_async.bingx({
    'apiKey': BINGX_API_KEY, 'secret': BINGX_SECRET,
    'options': {'defaultType': 'swap', 'marginMode': 'isolated'}, 'enableRateLimit': True
})

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db_conn(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER)''')
    try: c.execute("ALTER TABLE daily_stats ADD COLUMN wins INTEGER DEFAULT 0")
    except: pass
    try: c.execute("ALTER TABLE daily_stats ADD COLUMN prev_winrate REAL DEFAULT 0.0")
    except: pass
    conn.commit(); conn.close()

def save_positions():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),))
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins, prev_winrate) VALUES (1, ?, ?, ?, ?)", 
              (daily_stats['pnl'], daily_stats['trades'], daily_stats.get('wins', 0), daily_stats.get('prev_winrate', 0.0)))
    conn.commit(); conn.close()

def load_positions():
    global active_positions, daily_stats
    try:
        conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
        if row: active_positions = json.loads(row[0])
        c.execute("SELECT pnl, trades, wins, prev_winrate FROM daily_stats WHERE id = 1")
        stat_row = c.fetchone()
        if stat_row: daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'], daily_stats['prev_winrate'] = stat_row
        conn.close()
    except Exception: pass

async def send_tg_msg(text):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp: pass
    except: pass

async def update_balance_task():
    global GLOBAL_BALANCE
    while True:
        try:
            bal = await exchange.fetch_balance()
            GLOBAL_BALANCE = float(bal['USDT']['free'])
        except: pass
        await asyncio.sleep(10)

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_rsi(data, window=14):
    if len(data) <= window: return 50
    diffs = np.diff(data)
    gains = np.maximum(diffs, 0); losses = np.abs(np.minimum(diffs, 0))
    avg_gain = np.mean(gains[-window:]); avg_loss = np.mean(losses[-window:])
    if avg_loss == 0: return 100
    return 100 - (100 / (1 + (avg_gain / avg_loss)))

def find_fvg_details(h, l, direction):
    for i in range(len(h)-1, len(h)-12, -1):
        if direction == 'Long' and l[i] > h[i-2]: return {'found': True, 'top': l[i], 'bottom': h[i-2]}
        elif direction == 'Short' and h[i] < l[i-2]: return {'found': True, 'top': l[i-2], 'bottom': h[i]}
    return {'found': False}

async def detect_setups(sym, btc_trend, altseason, btc_volatility_pct, vol_24h):
    try:
        ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=205)
        if not ohlcv or len(ohlcv) < 200: return None
        
        o, h, l, c, v = (np.array([x[i] for x in ohlcv], dtype=float) for i in range(1, 6))
        current_price = c[-1]
        ema_200 = calculate_ema(c, 200)
        rsi_15m = calculate_rsi(c, 14)
        atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-24:])
        
        avg_volume = np.mean(v[-22:-2])
        has_volume = (v[-1] > avg_volume * 1.3) or (v[-2] > avg_volume * 1.3)
        
        if (h[-1] - l[-1]) < (atr * 0.5): return None

        leg_high, leg_low = np.max(h[-50:-1]), np.min(l[-50:-1])
        is_high_momentum = btc_volatility_pct > 2.0
        discount_ratio, premium_ratio = (0.85, 0.15) if is_high_momentum else (0.6, 0.4)
        
        eq_level_long = leg_low + (leg_high - leg_low) * discount_ratio  
        eq_level_short = leg_low + (leg_high - leg_low) * premium_ratio 

# 1. СНАЧАЛА РАССЧИТЫВАЕМ SMA-200 (до проверок!)
        sma_200 = np.mean(c[-200:]) if len(c) >= 200 else np.mean(c)

        # Проверяем сигналы с учетом Макро-Тренда
        if is_long_signal:
            if current_price < sma_200:
                logging.info(f"🗑 [ОТМЕНА] {sym}: Игнорируем LONG, монета в глобальном даунтренде (Цена < SMA200)")
                return None # <--- ИСПРАВЛЕНО (Вместо continue)     
            
            if ((btc_trend == 'Long') or altseason) and current_price > ema_200:
                if (c[-1] > np.max(h[-11:-1])) and has_volume and current_price <= eq_level_long:
                    fvg = find_fvg_details(h, l, 'Long')
                    if fvg['found']:
                        if is_high_momentum and fvg['bottom'] <= current_price <= fvg['top']:
                            sl = fvg['bottom'] - (atr * 1.5)
                            return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': fvg['bottom'], 'ob_high': fvg['top'], 'pattern': '15m Momentum FVG', 'mode': 'SMC', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}
                    for i in range(len(c)-2, len(c)-11, -1):
                        if c[i] < o[i]:  
                            ob_low, ob_high = l[i], h[i]
                            if i >= 15 and (is_high_momentum or ob_low <= np.min(l[i-15:i])):
                                if current_price > ob_high and (current_price - ob_high) < (atr * 1.0):
                                    sl = ob_low - (atr * 1.5)  
                                    return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '15m OB Breaker', 'mode': 'SMC', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}

        if is_short_signal:
            if current_price > sma_200:
                logging.info(f"🗑 [ОТМЕНА] {sym}: Игнорируем SHORT, монета в глобальном аптренде (Цена > SMA200)")
                return None # <--- ИСПРАВЛЕНО (Вместо continue)
            
            if (btc_trend == 'Short') and current_price < ema_200:
                if (c[-1] < np.min(l[-11:-1])) and has_volume and current_price >= eq_level_short:
                    fvg = find_fvg_details(h, l, 'Short')
                    if fvg['found']:
                        if is_high_momentum and fvg['bottom'] <= current_price <= fvg['top']:
                            sl = fvg['top'] + (atr * 1.5)
                            return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': fvg['bottom'], 'ob_high': fvg['top'], 'pattern': '15m Momentum FVG', 'mode': 'SMC', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}
                    for i in range(len(c)-2, len(c)-11, -1):
                        if c[i] > o[i]:  
                            ob_high, ob_low = h[i], l[i]
                            if i >= 15 and (is_high_momentum or ob_high >= np.max(h[i-15:i])):
                                if current_price < ob_low and (ob_low - current_price) < (atr * 1.0):
                                    sl = ob_high + (atr * 1.5)
                                    return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': sl, 'atr': atr, 'ob_low': ob_low, 'ob_high': ob_high, 'pattern': '15m OB Breaker', 'mode': 'SMC', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}

        bb_window = 20
        sma_20 = calculate_ema(c, bb_window) 
        std_dev = np.std(c[-bb_window:])
        upper_bb, lower_bb = sma_20 + (2 * std_dev), sma_20 - (2 * std_dev)
        bb_width = (upper_bb - lower_bb) / sma_20 * 100
        grid_has_volume = (v[-1] > avg_volume * 1.30) or (v[-2] > avg_volume * 1.30)

        # Рассчитываем Макро-Тренд (SMA-200) для защиты от входов против паровоза
        sma_200 = np.mean(c[-200:]) if len(c) >= 200 else np.mean(c)

        # Рассчитываем средний объем для вывода в Telegram
        avg_vol = np.mean(v[-20:]) if len(v) >= 20 else np.mean(v)
        current_volume = v[-1]
        # Высчитываем на сколько процентов текущий объем больше среднего
        vol_percent = int((current_volume / avg_vol) * 100) if avg_vol > 0 else 0
        
        now_utc = datetime.now(timezone.utc)
        skip_grid = btc_volatility_pct > 2.5 or (now_utc.hour == 13 and now_utc.minute >= 30) or (now_utc.hour == 14 and now_utc.minute <= 30)
        grid_width_limit = max(1.5, min(3.0, btc_volatility_pct * 1.5))
        candle_range = max(h[-1] - l[-1], 0.0001)
        
        lower_wick_ratio = (np.minimum(o[-1], c[-1]) - l[-1]) / candle_range
        upper_wick_ratio = (h[-1] - np.maximum(o[-1], c[-1])) / candle_range

        if not skip_grid and bb_width < grid_width_limit and grid_has_volume:
            if (btc_volatility_pct < 2.0 or current_price > ema_200) and current_price <= lower_bb * 1.002 and rsi_15m < 45 and lower_wick_ratio >= 0.2: 
                return {'symbol': sym, 'direction': 'Long', 'entry_price': current_price, 'sl': lower_bb - (atr * 1.5), 'atr': atr, 'ob_low': lower_bb, 'ob_high': upper_bb, 'pattern': 'BB Scalp (Pinbar)', 'mode': 'GRID', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}
            elif (btc_volatility_pct < 2.0 or current_price < ema_200) and current_price >= upper_bb * 0.998 and rsi_15m > 55 and upper_wick_ratio >= 0.2: 
                return {'symbol': sym, 'direction': 'Short', 'entry_price': current_price, 'sl': upper_bb + (atr * 1.5), 'atr': atr, 'ob_low': lower_bb, 'ob_high': upper_bb, 'pattern': 'BB Scalp (Pinbar)', 'mode': 'GRID', 'btc_vol': btc_volatility_pct, 'altseason': altseason, 'vol_24h': vol_24h}

        return None
    except Exception: return None

async def process_single_coin(sym, btc_trend, altseason, btc_volatility_pct, vol_24h, sem):
    async with sem: 
        try:
            signal = await detect_setups(sym, btc_trend, altseason, btc_volatility_pct, vol_24h)
            await asyncio.sleep(0.05) 
            return sym, signal
        except Exception: return sym, None

async def radar_task():
    global HOT_LIST, NOTIFIED_SYMBOLS
    await exchange.load_markets() 
    await asyncio.sleep(5)
    while True:
        try:
            if GLOBAL_STOP_UNTIL and datetime.now(timezone.utc) < GLOBAL_STOP_UNTIL:
                await asyncio.sleep(60); continue
            if len(active_positions) >= MAX_POSITIONS:
                await asyncio.sleep(60); continue

            btc_trend, altseason, btc_volatility_pct = 'Short', False, 2.0
            try:
                btc_ohlcv, eth_ohlcv = await asyncio.gather(
                    exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205),
                    exchange.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                )
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_trend = 'Long' if btc_c[-1] > calculate_ema(btc_c, 200) else 'Short'
                altseason = (np.array([x[4] for x in eth_ohlcv], dtype=float)[-1] / btc_c[-1]) > calculate_ema((np.array([x[4] for x in eth_ohlcv], dtype=float) / btc_c), 200)
                btc_sma = np.mean(btc_c[-20:])
                btc_volatility_pct = (4 * np.std(btc_c[-20:])) / btc_sma * 100
            except: pass

            tickers = await exchange.fetch_tickers()
            
            # === ДОБАВЛЯЕМ СТАТИСТИКУ РАДАРА ===
            stats = {'total': 0, 'low_vol': 0, 'waiting': 0, 'passed': 0}
            
            temp_symbols = []
            for sym, m in exchange.markets.items():
                if m.get('type') != 'swap' or not m.get('active'): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: continue
                
                stats['total'] += 1 # Считаем все монеты
                
                tick = tickers.get(sym)
                if not tick: 
                    stats['low_vol'] += 1
                    continue
                
                q_vol = float(tick.get('quoteVolume') or 0)
                if q_vol < MIN_VOLUME_USDT: 
                    stats['low_vol'] += 1
                    continue
                    
                ask, bid = float(tick.get('ask') or 0), float(tick.get('bid') or 0)
                if ask > 0 and bid > 0 and (ask - bid) / ask > MAX_SPREAD_PERCENT: 
                    continue
                    
                if not any(pos['symbol'].split(':')[0] == sym.split(':')[0] for pos in active_positions):
                    temp_symbols.append((sym, q_vol))
                    
            valid_symbols_data = [(x[0], x[1]) for x in sorted(temp_symbols, key=lambda x: x[1], reverse=True)[:SCAN_LIMIT]]
            sem = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
            results = []
            
            for i in range(0, len(valid_symbols_data), 10):
                chunk = valid_symbols_data[i:i+10]
                # Запускаем задачи анализа FVG и структуры
                tasks = [process_single_coin(s, btc_trend, altseason, btc_volatility_pct, v, sem) for s, v in chunk if s.split(':')[0] not in last_signals or (datetime.now(timezone.utc) - last_signals[s.split(':')[0]] > timedelta(hours=4))]
                if tasks:
                    results.extend(await asyncio.gather(*tasks, return_exceptions=True))
                    
            # === ПОДСЧЕТ И ВЫВОД РАДАРА ===
            # ИСПРАВЛЕНО: Async бот возвращает кортежи (sym, signal), где signal может быть None
            valid_results = [r for r in results if isinstance(r, tuple) and len(r) == 2 and r[1] is not None]
            stats['passed'] = len(valid_results)
            stats['waiting'] = stats['total'] - stats['low_vol'] - stats['passed']
            
            logging.info(f"🔎 [РАДАР] Всего: {stats['total']} -> Неликвид: {stats['low_vol']} -> Ждем Сетап: {stats['waiting']} -> ВХОДЫ: {stats['passed']}")

            new_hot_list = {}
            for res in results:
                # Защита от ошибок, если gather вернул Exception вместо кортежа
                if not isinstance(res, tuple) or len(res) != 2: continue
                sym, signal = res
                
                if signal:
                    new_hot_list[sym] = signal
                    if sym not in HOT_LIST and sym not in NOTIFIED_SYMBOLS:
                        NOTIFIED_SYMBOLS.add(sym)
                        await send_tg_msg(f"🎯 **РАДАР [{signal['mode']}]:** {sym.split(':')[0]} взят на мушку! (24h Vol: {signal['vol_24h']/1000000:.1f}M)")

            HOT_LIST = new_hot_list
            NOTIFIED_SYMBOLS = {s for s in NOTIFIED_SYMBOLS if s in HOT_LIST}
            
            # В Async-боте есть функция get_mem_usage, поэтому оставляем этот лог
            try:
                mem_log = get_mem_usage()
            except:
                mem_log = "N/A"
                
            logging.info(f"🔎 [РАДАР] Скан завершен. На мушке: {len(HOT_LIST)} | ОЗУ: {mem_log} | BTC Vol: {btc_volatility_pct:.2f}%")
            gc.collect() 
            await asyncio.sleep(300) 
        except Exception as e: 
            logging.error(f"Radar Loop Error: {e}")
            await asyncio.sleep(60)

async def execute_trade(signal, current_ws_price):
    global COOLDOWN_CACHE, LEVERAGE_CACHE
    sym = signal['symbol']
    clean_name = sym.split(':')[0]
    
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: return
    if GLOBAL_BALANCE <= 0: return 

    try:
        contract_size = float(exchange.market(sym).get('contractSize', 1.0))
        current_market_price = current_ws_price 
        free_usdt = GLOBAL_BALANCE 
        
        actual_sl_dist = max(abs(signal['entry_price'] - signal['sl']), current_market_price * 0.004)
        is_long = signal['direction'] == 'Long'
        
        sl_raw = current_market_price - actual_sl_dist if is_long else current_market_price + actual_sl_dist
        tp1_raw = current_market_price + (actual_sl_dist * 1.5) if is_long else current_market_price - (actual_sl_dist * 1.5)
        tp2_raw = current_market_price + (actual_sl_dist * 3.0) if is_long else current_market_price - (actual_sl_dist * 3.0)
            
        sl_price = float(exchange.price_to_precision(sym, sl_raw))
        tp1_price = float(exchange.price_to_precision(sym, tp1_raw))
        tp2_price = float(exchange.price_to_precision(sym, tp2_raw))

        if (is_long and sl_price >= current_market_price) or (not is_long and sl_price <= current_market_price): return

        risk_amount = free_usdt * RISK_PER_TRADE
        final_qty_coins = min(risk_amount / actual_sl_dist if actual_sl_dist > 0 else 0, ((free_usdt * 0.95) / MAX_POSITIONS) * LEVERAGE / current_market_price)
        qty = float(exchange.amount_to_precision(sym, final_qty_coins / contract_size if contract_size > 0 else final_qty_coins))
        if qty <= 0 or qty * contract_size * current_market_price < MIN_NOTIONAL_USDT: return 

        pos_side = 'LONG' if is_long else 'SHORT'
        bot_client_id = f"OMNI_{clean_name}_{int(time.time()*100)}"
        
        if f"{sym}_{pos_side}" not in LEVERAGE_CACHE:
            await exchange.set_leverage(LEVERAGE, sym, params={'side': pos_side})
            LEVERAGE_CACHE.add(f"{sym}_{pos_side}")
            
        await exchange.create_market_order(sym, 'buy' if is_long else 'sell', qty, params={'positionSide': pos_side, 'clientOrderId': bot_client_id})
        sl_ord = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', qty, params={'triggerPrice': sl_price, 'positionSide': pos_side, 'stopLossPrice': sl_price})
        
        timeout_p = 1.5 if signal['mode'] == 'GRID' else 3.0
        timeout_a = 3.0 if signal['mode'] == 'GRID' else 6.0
        
        active_positions.append({
            'symbol': sym, 'direction': signal['direction'], 'entry_price': current_market_price, 'initial_qty': qty, 
            'sl_price': sl_price, 'tp1': tp1_price, 'tp2': tp2_price, 'atr': signal.get('atr', current_market_price*0.015),
            'tp1_hit': False, 'pre_tp1_hit': False, 'sl_order_id': sl_ord['id'], 'position_side': pos_side, 
            'open_time': datetime.now(timezone.utc).isoformat(), 'mode': signal['mode'],
            'timeout_profit': timeout_p, 'timeout_any': timeout_a
        })
        last_signals[clean_name] = datetime.now(timezone.utc)
        await asyncio.to_thread(save_positions)
        
        await send_tg_msg(f"💥 **ВЫСТРЕЛ [{signal['mode']}]: {clean_name}**\nНаправление: **#{signal['direction'].upper()}**\n{signal.get('score_info', 'ТОП Сигнал')}\nПаттерн: {signal.get('pattern', 'N/A')}\n\nЦена: {current_market_price}\nОбъем: {qty} контр.\nSL: {sl_price}\nTP1: {tp1_price}\nTP2: {tp2_price}")
    except Exception as e: 
        if "101204" in str(e) or "Insufficient margin" in str(e): COOLDOWN_CACHE[sym] = time.time() + 1800 

async def wss_sniper_worker(sym, setup_data):
    global GLOBAL_CVD
    raw_base_coin = sym.split(':')[0].split('/')[0]
    clean_name = raw_base_coin  
    ws_base_coin = SYMBOL_MAP.get(raw_base_coin, raw_base_coin)
    sym_bingx_s = f"{raw_base_coin}-USDT"; sym_bingx_f = f"{raw_base_coin}-USDT"
    sym_mexc = f"{ws_base_coin}USDT"; sym_bybit = f"{ws_base_coin}USDT"; sym_binance = f"{ws_base_coin}usdt".lower()

    state = {'trades': [], 'start_time': time.time(), 'active': True, 'current_price': setup_data['entry_price']}
    peak_metrics = {ex: {'vol': 0, 'dom': 0} for ex in ['bx_s', 'bx_f', 'mc_s', 'mc_f', 'bb_s', 'bb_f', 'bn_f']}

    vol_24h = setup_data.get('vol_24h', 1000000)
    avg_15s_vol = vol_24h / 5760 
    btc_vol = setup_data.get('btc_vol', 2.0)
    
    if btc_vol < 1.0: dyn_mult = 1.2; min_thresh = 1000
    elif btc_vol < 1.8: dyn_mult = 1.7; min_thresh = 2000
    else: dyn_mult = 2.0; min_thresh = 2500
        
    base_thresh_15s = max(avg_15s_vol * dyn_mult, min_thresh)
    base_thresh_5s = base_thresh_15s * 0.4
    
    thresh_5s = {'bx_s': base_thresh_5s*0.3, 'bx_f': base_thresh_5s*0.3, 'mc_s': base_thresh_5s*0.3, 'mc_f': base_thresh_5s*0.3, 'bb_s': base_thresh_5s*0.5, 'bb_f': base_thresh_5s*1.0, 'bn_f': base_thresh_5s*1.5}
    thresh_15s = {'bx_s': base_thresh_15s*0.3, 'bx_f': base_thresh_15s*0.3, 'mc_s': base_thresh_15s*0.3, 'mc_f': base_thresh_15s*0.3, 'bb_s': base_thresh_15s*0.5, 'bb_f': base_thresh_15s*1.0, 'bn_f': base_thresh_15s*1.5}
    dynamic_whale_limit = avg_15s_vol * WHALE_VOLUME_MULTIPLIER

    async def l_ws(url, payload, ex_code, is_binance=False):
        while state['active']:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        if payload: await ws.send_str(json.dumps(payload))
                        async for msg in ws:
                            if not state['active']: break
                            try:
                                if is_binance and msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if 'p' in data and 'q' in data:
                                        state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if data.get('m') else 'b', 'v': float(data['p']) * float(data['q'])})
                                        state['current_price'] = float(data['p'])
                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    d = json.loads(gzip.decompress(msg.data).decode('utf-8'))
                                    if "ping" in d: await ws.send_str(json.dumps({"pong": d["ping"]}))
                                    elif "data" in d and d["data"]:
                                        for t in (d["data"] if isinstance(d["data"], list) else [d["data"]]):
                                            if 'p' in t and (t.get('q') or t.get('v')):
                                                state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if t.get('m') else 'b', 'v': float(t['p']) * float(t.get('q', t.get('v', 0)))})
                                                if ex_code == 'bx_f': state['current_price'] = float(t['p'])
                                elif msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if "data" in data and isinstance(data["data"], list):
                                        for t in data["data"]:
                                            if 'p' in t and 'v' in t: state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if t.get('S') == "Sell" else 'b', 'v': float(t['p']) * float(t['v'])})
                                    elif "d" in data and "deals" in data["d"]:
                                        for t in (data["d"]["deals"] if isinstance(data["d"]["deals"], list) else [data["d"]["deals"]]):
                                            if 'p' in t and 'v' in t: state['trades'].append({'ts': time.time(), 'ex': ex_code, 'side': 's' if str(t.get('S')) == '2' or str(t.get('T')) == '2' else 'b', 'v': float(t['p']) * float(t['v'])})
                            except: pass
            except: await asyncio.sleep(1)

    tasks = [
        asyncio.create_task(l_ws("wss://open-api-ws.bingx.com/market", {"id": "1", "reqType": "sub", "dataType": f"{sym_bingx_s}@trade"}, 'bx_s')),
        asyncio.create_task(l_ws("wss://open-api-swap.bingx.com/swap-market", {"id": "1", "reqType": "sub", "dataType": f"{sym_bingx_f}@trade"}, 'bx_f')),
        asyncio.create_task(l_ws("wss://wbs-api.mexc.com/ws", {"method": "SUBSCRIPTION", "params": [f"spot@public.deals.v3.api@{sym_mexc}"]}, 'mc_s')),
        asyncio.create_task(l_ws("wss://contract.mexc.com/edge", {"method": "sub.deal", "param": {"symbol": f"{ws_base_coin}_USDT"}}, 'mc_f')),
        asyncio.create_task(l_ws("wss://stream.bybit.com/v5/public/spot", {"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}, 'bb_s')),
        asyncio.create_task(l_ws("wss://stream.bybit.com/v5/public/linear", {"op": "subscribe", "args": [f"publicTrade.{sym_bybit}"]}, 'bb_f')),
        asyncio.create_task(l_ws(f"wss://fstream.binance.com/ws/{sym_binance}@aggTrade", None, 'bn_f', True))
    ]

    try:
        is_long = (setup_data['direction'] == 'Long')
        
        while (sym in HOT_LIST or any(p['symbol'] == sym for p in active_positions)) and state['active']:
            await asyncio.sleep(1) 
            now = time.time()
            state['trades'] = [t for t in state['trades'] if now - t['ts'] <= 60]
            elapsed_time = now - state['start_time']
            
            if sym not in HOT_LIST or len(active_positions) >= MAX_POSITIONS or elapsed_time < 2: continue
            
            vols_5s = {ex: {'b': 0, 's': 0} for ex in peak_metrics}; vols_15s = {ex: {'b': 0, 's': 0} for ex in peak_metrics}
            for t in state['trades']:
                dt = now - t['ts']
                if dt <= 15:
                    vols_15s[t['ex']][t['side']] += t['v']
                    if dt <= 5: vols_5s[t['ex']][t['side']] += t['v']

            tots_5s = {ex: vols_5s[ex]['b'] + vols_5s[ex]['s'] for ex in peak_metrics}
            tots_15s = {ex: vols_15s[ex]['b'] + vols_15s[ex]['s'] for ex in peak_metrics}
            labels = {'bx_s': 'BingX (S)', 'bx_f': 'BingX (F)', 'mc_s': 'MEXC (S)', 'mc_f': 'MEXC (F)', 'bb_s': 'Bybit (S)', 'bb_f': 'Bybit (F)', 'bn_f': 'Binance (F)'}
            
            for ex in peak_metrics:
                t15 = tots_15s[ex]
                if t15 > peak_metrics[ex]['vol']: peak_metrics[ex]['vol'] = t15
                pct15 = (vols_15s[ex]['b'] / t15 * 100) if is_long and t15 > 0 else (vols_15s[ex]['s'] / t15 * 100) if not is_long and t15 > 0 else 0
                if pct15 > peak_metrics[ex]['dom']: peak_metrics[ex]['dom'] = pct15

            oracle_triggered = False; valid_names = []; valid_exchanges_str = []
            for ex in peak_metrics:
                t5, t15 = tots_5s[ex], tots_15s[ex]
                pct5 = (vols_5s[ex]['b'] / t5 * 100) if is_long and t5 > 0 else (vols_5s[ex]['s'] / t5 * 100) if not is_long and t5 > 0 else 0
                pct15 = (vols_15s[ex]['b'] / t15 * 100) if is_long and t15 > 0 else (vols_15s[ex]['s'] / t15 * 100) if not is_long and t15 > 0 else 0
                
                if (t5 >= thresh_5s[ex] and pct5 >= 60) or (t15 >= thresh_15s[ex] and pct15 >= 55):
                    if ex in ['bn_f', 'bb_f']: oracle_triggered = True
                    valid_names.append(labels[ex]); valid_exchanges_str.append(f"{labels[ex]} ({max(pct5, pct15):.0f}%)")

            all_vols = {ex: {'b': sum(t['v'] for t in state['trades'] if t['ex'] == ex and t['side'] == 'b'), 's': sum(t['v'] for t in state['trades'] if t['ex'] == ex and t['side'] == 's')} for ex in peak_metrics}
            global_delta = sum(all_vols[ex]['b'] for ex in all_vols) - sum(all_vols[ex]['s'] for ex in all_vols)
            max_vol = max([tots_15s[ex] for ex in tots_15s]) if tots_15s else 0
            
            is_whale = False
            if max_vol >= dynamic_whale_limit and ((is_long and global_delta > dynamic_whale_limit * 0.4) or (not is_long and global_delta < -dynamic_whale_limit * 0.4)):
                is_whale = True
            
            if oracle_triggered or len(valid_names) >= 2 or is_whale:
                ob_range = abs(setup_data['ob_high'] - setup_data['ob_low'])
                allowed_upper = setup_data['ob_high'] + ob_range * 1.5
                allowed_lower = setup_data['ob_low'] - ob_range * 1.5
                
                if allowed_lower <= state['current_price'] <= allowed_upper:
                    price_shift_pct = ((state['current_price'] - setup_data['entry_price']) / setup_data['entry_price']) * 100
                    
                    if (is_long and global_delta < -15000) or (not is_long and global_delta > 15000):
                        logging.info(f"🗑 [ОТМЕНА] {clean_name}: CVD против нас ({global_delta:.0f})")
                        if sym in HOT_LIST: del HOT_LIST[sym]
                        continue
                    
                    if (is_long and price_shift_pct < -0.5) or (not is_long and price_shift_pct > 0.5):
                        logging.info(f"🗑 [ОТМЕНА] {clean_name}: Проскальзывание > 0.5% ({price_shift_pct:.2f}%)")
                        if sym in HOT_LIST: del HOT_LIST[sym]
                        continue
                    
                    if sym in HOT_LIST: del HOT_LIST[sym]
                    
                    # === ИЗМЕНЕНИЕ ЗДЕСЬ: Расчет и вывод % объема в Telegram ===
                    vol_percent = int((max_vol / avg_15s_vol) * 100) if avg_15s_vol > 0 else 0
                    
                    if is_whale: 
                        setup_data['score_info'] = f"🐋 ВХОД С КИТОМ! (Объем: +{vol_percent}% от среднего)"
                    elif oracle_triggered: 
                        setup_data['score_info'] = f"🔮 СИГНАЛ ОРАКУЛА! ({', '.join(valid_exchanges_str)}) за {int(elapsed_time)}с, Объем: +{vol_percent}%"
                    else: 
                        setup_data['score_info'] = f"🔥 СТАНДАРТ ПОДТВЕРЖДЕНИЕ ({len(valid_names)} бирж) за {int(elapsed_time)}с" 
                    
                    await execute_trade(setup_data, state['current_price']) 
                else:
                    logging.info(f"🗑 [ОТМЕНА] {clean_name}: Цена вылетела за пределы расширенного OB")
                    if sym in HOT_LIST: del HOT_LIST[sym]
            
            elif elapsed_time >= 60:
                log_bn = f"{int(peak_metrics['bn_f']['vol'])}$/{int(thresh_15s['bn_f'])}$ ({peak_metrics['bn_f']['dom']:.0f}%)"
                logging.info(f"🗑 [ОТМЕНА] {clean_name}: Недобор объемов. Binance: {log_bn}")
                if sym in HOT_LIST: del HOT_LIST[sym]
                continue
    finally:
        state['active'] = False
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        ACTIVE_WSS_CONNECTIONS.discard(sym)

async def sniper_manager():
    while True:
        for sym, setup_data in list(HOT_LIST.items()):
            if sym not in ACTIVE_WSS_CONNECTIONS:
                ACTIVE_WSS_CONNECTIONS.add(sym)
                asyncio.create_task(wss_sniper_worker(sym, setup_data))
        await asyncio.sleep(5)

async def monitor_positions_job():
    global active_positions, daily_stats, CONSECUTIVE_LOSSES
    while True:
        await asyncio.sleep(5) 
        try:
            positions_raw = await exchange.fetch_positions()
            if not active_positions: continue
            
            symbols_to_fetch = [p['symbol'] for p in active_positions]
            tickers = await exchange.fetch_tickers(symbols_to_fetch)
            ohlcv_tasks = [exchange.fetch_ohlcv(sym, timeframe='1m', limit=2) for sym in symbols_to_fetch]
            ohlcv_results = await asyncio.gather(*ohlcv_tasks, return_exceptions=True)

            updated = []
            for idx, pos in enumerate(active_positions):
                sym = pos['symbol']
                clean_name = sym.split(':')[0]
                is_long = pos['direction'] == 'Long'
                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                ticker = tickers.get(sym, {}).get('last', pos['entry_price'])
                
                if not curr:
                    pnl = (ticker - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - ticker) * pos['initial_qty']
                    daily_stats['trades'] += 1; daily_stats['pnl'] += pnl
                    if pnl > 0: daily_stats['wins'] += 1; CONSECUTIVE_LOSSES = 0; await send_tg_msg(f"✅ **{clean_name} закрыта в плюс!**\nPNL: +{pnl:.2f} USDT")
                    else: CONSECUTIVE_LOSSES += 1; await send_tg_msg(f"🛑 **{clean_name} выбита по SL.**\nPNL: {pnl:.2f} USDT")
                    continue

                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (ticker - pos['entry_price']) * float(curr['contracts']) if is_long else (pos['entry_price'] - ticker) * float(curr['contracts'])

                if hours_passed >= pos.get('timeout_any', 4.0) or (hours_passed >= pos.get('timeout_profit', 2.0) and pnl > 0):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']), params={'positionSide': pos['position_side']})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        
                        # ИСПРАВЛЕНИЕ: Добавляем сделку по таймауту в статистику
                        daily_stats['trades'] += 1
                        daily_stats['pnl'] += pnl
                        if pnl > 0: 
                            daily_stats['wins'] += 1
                            CONSECUTIVE_LOSSES = 0
                        else: 
                            CONSECUTIVE_LOSSES += 1
                            
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} **{clean_name} закрыта по ТАЙМАУТУ!**\nPNL: {pnl:.2f} USDT")
                        continue 
                    except: pass

                ohlcv = ohlcv_results[idx]
                if isinstance(ohlcv, Exception) or not ohlcv: updated.append(pos); continue
                high_p, low_p = max([float(c[2]) for c in ohlcv]), min([float(c[3]) for c in ohlcv])

                if not pos.get('tp1_hit') and not pos.get('pre_tp1_hit'):
                    trigger = pos['entry_price'] + abs(pos['tp1'] - pos['entry_price']) * 0.8 * (1 if is_long else -1)
                    if (is_long and high_p >= trigger) or (not is_long and low_p <= trigger):
                        try:
                            if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                            be_p = float(exchange.price_to_precision(sym, pos['entry_price'] * (1.001 if is_long else 0.999)))
                            new_sl = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': be_p, 'positionSide': pos['position_side'], 'stopLossPrice': be_p})
                            pos.update({'sl_order_id': new_sl['id'], 'current_sl': be_p, 'pre_tp1_hit': True})
                        except: pass

                if not pos.get('tp1_hit') and ((is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1'])):
                    try:
                        close_qty = float(exchange.amount_to_precision(sym, float(curr['contracts']) * 0.50))
                        if close_qty > 0: await exchange.create_market_order(sym, 'sell' if is_long else 'buy', close_qty, params={'positionSide': pos['position_side']}); pos['initial_qty'] = float(curr['contracts']) - close_qty 
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        be_p = float(exchange.price_to_precision(sym, pos['entry_price'] * (1.002 if is_long else 0.998)))
                        new_sl = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': be_p, 'positionSide': pos['position_side'], 'stopLossPrice': be_p})
                        atr_step = pos.get('atr', abs(pos['entry_price'] - pos['sl_price']) * 0.5)
                        pos.update({'tp1_hit': True, 'sl_order_id': new_sl['id'], 'micro_step': atr_step*0.3, 'trail_distance': atr_step*0.8, 'trail_trigger': pos['tp1'] + atr_step*0.3 * (1 if is_long else -1)})
                        await send_tg_msg(f"💰 **{clean_name} TP1 взят!** (50% закрыто).\n🛡 Запущен Трейлинг.")
                    except: pass

                if pos.get('tp1_hit'):
                    tt, ms, td = pos.get('trail_trigger'), pos.get('micro_step'), pos.get('trail_distance')
                    if (is_long and high_p >= tt) or (not is_long and low_p <= tt):
                        try:
                            if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                            nsl = float(exchange.price_to_precision(sym, tt - td if is_long else tt + td))
                            new_sl_order = await exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'triggerPrice': nsl, 'positionSide': pos['position_side'], 'stopLossPrice': nsl})
                            pos.update({'sl_order_id': new_sl_order['id'], 'trail_trigger': tt + ms if is_long else tt - ms})
                        except: pass
                updated.append(pos)
            active_positions = updated
            await asyncio.to_thread(save_positions)
        except: pass

# === НОВАЯ ФУНКЦИЯ: ТЕЛЕМЕТРИЯ ===
async def log_bot_status_task():
    while True:
        await asyncio.sleep(3600) # Изменено на 3600 (Печать в логи каждые 60 минут)
        winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats['trades'] > 0 else 0
        active = ", ".join([f"{p['symbol'].split(':')[0]} ({p['direction']})" for p in active_positions]) or "Нет"
        logging.info(f"📊 [СТАТИСТИКА] Сделок: {daily_stats['trades']} | Винрейт: {winrate:.1f}% | PNL: {daily_stats['pnl']:.2f}$ | Открыто: {active}")

async def daily_report_task():
    global daily_stats
    reported_today = False
    while True:
        await asyncio.sleep(30)
        now = datetime.now(timezone.utc)
        if now.hour == 20 and now.minute == 0 and not reported_today:
            trades = daily_stats['trades']; wins = daily_stats['wins']; pnl = daily_stats['pnl']
            winrate = (wins / trades * 100) if trades > 0 else 0
            prev_winrate = daily_stats.get('prev_winrate', 0.0); diff = winrate - prev_winrate
            diff_str = f"+{diff:.1f}%" if diff > 0 else f"{diff:.1f}%"
            report = (f"🗓 **ИТОГИ ДНЯ (Async Bot):** {now.strftime('%d.%m.%Y')}\n\n📉 Открыто сделок за день: {trades}\n🎯 Винрейт: {winrate:.1f}% ({diff_str})\n💵 Прибыль за день: {pnl:.2f} USDT")
            await send_tg_msg(report)
            daily_stats['prev_winrate'] = winrate; daily_stats['pnl'] = 0.0; daily_stats['trades'] = 0; daily_stats['wins'] = 0
            await asyncio.to_thread(save_positions)
            reported_today = True
        elif now.hour != 20: reported_today = False

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Bot v8.33 Active (1h Telemetry Logging)")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
    server.serve_forever()

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск ядра v8.33: 1h Telemetry Logging & USDC Filter...")
    await asyncio.gather(
        radar_task(), 
        sniper_manager(), 
        monitor_positions_job(), 
        daily_report_task(), 
        update_balance_task(),
        log_bot_status_task() 
    )

if __name__ == '__main__':
    Thread(target=run_server, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logging.info("\nОстановка.")
