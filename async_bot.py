import asyncio
import json
import os
import logging
import sqlite3
import gc
import time
import ccxt.async_support as ccxt_async
import numpy as np
import aiohttp
from datetime import datetime, timezone
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

DB_PATH = '/data/bot.db' if os.path.exists('/data') else 'bot.db'

TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = -1003955653290
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

# НАСТРОЙКИ РИСКА ДЛЯ ПРОП-КОМПАНИИ
RISK_PER_TRADE = 0.005      # 0.5% риска на сделку (безопасно для Prop)
MAX_POSITIONS = 3           
LEVERAGE = 5                # Безопасное плечо для Prop-счетов
MIN_VOLUME_USD = 1000000    
MIN_SL_PCT = 1.5            
MAX_SL_PCT = 6.0            

SMC_TIMEFRAME = '15m'
COOLDOWN_CACHE = {}         
SCAN_LIMIT = 300           

EXCLUDED_KEYWORDS = [
    'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 'FLOKI', 'BOME',
    'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI', 'PEOPLE'
]

daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': 0.0, 'start_balance': 0.0, 'gross_profit': 0.0, 'gross_loss': 0.0}
active_positions = []
NOTIFIED_SYMBOLS = set() 
REPORTED_TODAY = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

# Подключение Kraken Futures
exchange = ccxt_async.krakenfutures({
    'apiKey': KRAKEN_API_KEY, 
    'secret': KRAKEN_SECRET,
    'enableRateLimit': True
})

# Оракулы глобального объема
ORACLES = [
    ccxt_async.binance({'enableRateLimit': True}),
    ccxt_async.bybit({'enableRateLimit': True}),
    ccxt_async.mexc({'enableRateLimit': True})
]

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_db_conn(); c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (id INTEGER PRIMARY KEY, pnl REAL, trades INTEGER, wins INTEGER)''')
    for col in ['wins', 'prev_winrate', 'start_balance', 'gross_profit', 'gross_loss']:
        try: c.execute(f"ALTER TABLE daily_stats ADD COLUMN {col} REAL DEFAULT 0.0")
        except: pass
    conn.commit(); conn.close()

def save_positions():
    conn = get_db_conn(); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),))
    c.execute("INSERT OR REPLACE INTO daily_stats (id, pnl, trades, wins, prev_winrate, start_balance, gross_profit, gross_loss) VALUES (1, ?, ?, ?, ?, ?, ?, ?)", 
              (daily_stats.get('pnl', 0.0), daily_stats.get('trades', 0), daily_stats.get('wins', 0), 
               daily_stats.get('prev_winrate', 0.0), daily_stats.get('start_balance', 0.0),
               daily_stats.get('gross_profit', 0.0), daily_stats.get('gross_loss', 0.0)))
    conn.commit(); conn.close()

def load_positions():
    global active_positions, daily_stats
    try:
        conn = get_db_conn(); c = conn.cursor()
        c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
        if row: active_positions = json.loads(row[0])
        c.execute("SELECT pnl, trades, wins, prev_winrate, start_balance, gross_profit, gross_loss FROM daily_stats WHERE id = 1")
        stat_row = c.fetchone()
        if stat_row: 
            daily_stats['pnl'], daily_stats['trades'], daily_stats['wins'], daily_stats['prev_winrate'], daily_stats['start_balance'], daily_stats['gross_profit'], daily_stats['gross_loss'] = stat_row
        conn.close()
    except Exception: pass

async def send_tg_msg(text):
    if not TOKEN: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"} 
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp: pass
    except: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1); ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

def calculate_rsi(prices, window=14):
    if len(prices) < window: return 50
    diffs = np.diff(prices); gains = np.maximum(diffs, 0); losses = np.abs(np.minimum(diffs, 0))
    avg_gain = np.mean(gains[-window:]); avg_loss = np.mean(losses[-window:])
    if avg_loss == 0: return 100
    if avg_gain == 0: return 0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))

def calculate_vwap(h, l, c, v):
    if len(c) < 50: return c[-1]
    typ_price = (h[-50:] + l[-50:] + c[-50:]) / 3
    vol_sum = np.sum(v[-50:])
    if vol_sum == 0: return c[-1]
    return np.sum(typ_price * v[-50:]) / vol_sum

def analyze_fvg(o, h, l, c):
    fvg_list = []
    for i in range(1, len(c)-1):
        if l[i-1] > h[i+1]: fvg_list.append({'type': 'Bearish', 'top': l[i-1], 'bottom': h[i+1], 'index': i})
        elif h[i-1] < l[i+1]: fvg_list.append({'type': 'Bullish', 'top': l[i+1], 'bottom': h[i-1], 'index': i})
    return fvg_list

def analyze_structure(h, l, c):
    pivots = []
    for i in range(2, len(c)-2):
        if h[i] == max(h[i-2:i+3]): pivots.append({'type': 'HH' if not pivots or h[i] > pivots[-1]['price'] else 'LH', 'price': h[i], 'index': i})
        elif l[i] == min(l[i-2:i+3]): pivots.append({'type': 'HL' if not pivots or l[i] > pivots[-1]['price'] else 'LL', 'price': l[i], 'index': i})
    
    trend = 'Neutral'; bos_choch = None
    if len(pivots) >= 4:
        recent = pivots[-4:]
        if recent[-1]['type'] in ['HH', 'HL'] and recent[-3]['type'] in ['HH', 'HL']: trend = 'Bullish'
        elif recent[-1]['type'] in ['LL', 'LH'] and recent[-3]['type'] in ['LL', 'LH']: trend = 'Bearish'
        if trend == 'Bearish' and c[-1] > recent[-1]['price']: bos_choch = 'CHoCH_Bullish'
        elif trend == 'Bullish' and c[-1] < recent[-1]['price']: bos_choch = 'CHoCH_Bearish'
    return trend, bos_choch

async def verify_global_volume(base_coin):
    check_sym = f"{base_coin}/USDT"
    async def fetch_ex(ex):
        try:
            ohlcv = await ex.fetch_ohlcv(check_sym, timeframe=SMC_TIMEFRAME, limit=25)
            if not ohlcv or len(ohlcv) < 20: return False
            v = np.array([x[5] for x in ohlcv])
            avg_vol = np.mean(v[-22:-2])
            return v[-2] > (avg_vol * 1.20) 
        except: return False
        
    tasks = [fetch_ex(ex) for ex in ORACLES]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    confirmations = [r for r in results if r is True]
    return len(confirmations) >= 1

async def safe_create_order(sym, order_type, side, qty, params):
    try:
        return await exchange.create_order(sym, order_type, side, qty, params=params)
    except Exception as e:
        error_msg = str(e)
        if "reduceOnly" in error_msg.lower():
            clean_params = {k: v for k, v in params.items() if k.lower() != 'reduceonly'}
            return await exchange.create_order(sym, order_type, side, qty, params=clean_params)
        raise e

async def execute_trade(sym, signal_data, strategy_name="SMC Async"):
    global active_positions, COOLDOWN_CACHE
    clean_sym = sym.split(':')[0].split('/')[0]
    
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    if clean_sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[clean_sym]: return
        
    direction, current_price, sl_price, tp_price = signal_data['mode'], signal_data['price'], signal_data['sl_price'], signal_data['tp_price']
    
    try:
        bal = await exchange.fetch_balance()
        free_usd = float(bal.get('USDT', {}).get('free', 0)) or float(bal.get('USD', {}).get('free', 0))
        if free_usd <= 0: return

        actual_sl_dist = abs(current_price - sl_price)
        if actual_sl_dist <= 0: return
        sl_pct = (actual_sl_dist / current_price) * 100
        if sl_pct > MAX_SL_PCT:
            COOLDOWN_CACHE[clean_sym] = time.time() + 3600
            return
            
        risk_amount = free_usd * RISK_PER_TRADE
        
        # Специфика Kraken: учет Contract Size
        market = exchange.markets.get(sym, {})
        contract_size = float(market.get('contractSize', 1.0))
        
        qty_coins = risk_amount / actual_sl_dist
        qty = float(exchange.amount_to_precision(sym, qty_coins / contract_size if contract_size > 0 else qty_coins))
        if qty <= 0: return
        
        try: await exchange.set_leverage(LEVERAGE, sym)
        except: pass
        
        # Удален positionSide, только базовое направление
        side = 'buy' if direction == 'Long' else 'sell'
        sl_side = 'sell' if direction == 'Long' else 'buy'
        
        order = await exchange.create_market_order(sym, side, qty)
    except Exception as e: 
        logging.error(f"Trade Open Error {sym}: {e}")
        COOLDOWN_CACHE[clean_sym] = time.time() + 3600 
        return

    sl_id = None
    try:
        # Kraken Futures использует stopPrice
        sl_ord = await safe_create_order(sym, 'stop_market', sl_side, qty, {'stopPrice': sl_price, 'reduceOnly': True})
        sl_id = sl_ord['id']
    except Exception as e:
        COOLDOWN_CACHE[clean_sym] = time.time() + 14400 
        await send_tg_msg(f"🚨 <b>АВАРИЯ ПО {clean_sym}</b>\nОшибка установки SL: <code>{e}</code>\nЭкстренное закрытие...")
        try: await safe_create_order(sym, 'market', sl_side, qty, {'reduceOnly': True})
        except: pass
        return 

    active_positions.append({
        'symbol': sym, 'direction': direction, 'entry_price': current_price, 
        'initial_qty': qty, 'current_qty': qty, 'sl_price': sl_price, 'current_sl': sl_price, 
        'tp1': tp_price, 'sl_order_id': sl_id, 'open_time': datetime.now(timezone.utc).isoformat(),
        'strategy': strategy_name, 'be_moved': False, 'tp80_hit': False, 'tp100_hit': False,
        'atr': actual_sl_dist * 0.5 
    })
    await asyncio.to_thread(save_positions)
    
    oracle_text = "\n🌐 <i>Подтверждено Оракулом</i>" if strategy_name == "GRID Oracle" else ""
    
    analytics_text = ""
    if strategy_name == "SMC Async":
        btc_dist = signal_data.get('btc_ema_dist', 0)
        btc_eval = "Сильный" if abs(btc_dist) > 1.5 else "Умеренный" if abs(btc_dist) > 0.5 else "Флэт/Слабый"
        btc_trend_str = f"{signal_data.get('btc_trend', 'N/A')} ({btc_eval}, откл. {btc_dist:+.2f}%)"

        if signal_data.get('altseason'):
            diff = signal_data.get('eth_btc_diff', 0)
            alt_eval = "🔥 Горячая фаза" if diff > 1.5 else "📈 Набирает силу" if diff > 0.5 else "Начальная стадия"
            alt_str = f"🟢 Активен ({alt_eval}, перевес ETH {diff:+.2f}%)"
        else:
            alt_str = "🔴 Выключен"

        vol_ratio = signal_data.get('vol_ratio', 0)
        setup_eval = "💎 Топовый (Высокий объем)" if vol_ratio > 2.0 else "👍 Хороший (Средний объем)"

        ema_dist = signal_data.get('ema_dist', 0)
        mode = signal_data['mode']
        if mode == 'Long':
            ema_eval = "💎 Идеально" if 0 < ema_dist <= 2.0 else "🟡 Норма" if 2.0 < ema_dist <= 3.5 else "⚠️ Опасно"
        else:
            ema_eval = "💎 Идеально" if -2.0 <= ema_dist < 0 else "🟡 Норма" if -3.5 <= ema_dist < -2.0 else "⚠️ Опасно"

        rsi = signal_data.get('rsi', 0)
        if mode == 'Long':
            rsi_eval = "🔥 Идеально (Остыл)" if rsi < 45 else "👍 Норма" if rsi <= 60 else "⚠️ Перегрет"
        else:
            rsi_eval = "🔥 Идеально (Перегрет)" if rsi > 55 else "👍 Норма" if rsi >= 40 else "⚠️ Остыл"

        vwap = signal_data.get('vwap', 0)
        vwap_dist = (current_price - vwap) / vwap * 100
        if mode == 'Long':
            vwap_eval = f"Выше (Поддержка {vwap_dist:+.2f}%) 🟢" if vwap_dist > 0 else f"Ниже (Давление {vwap_dist:+.2f}%) 🔴"
        else:
            vwap_eval = f"Ниже (Сопротивление {vwap_dist:+.2f}%) 🟢" if vwap_dist < 0 else f"Выше (Опасно {vwap_dist:+.2f}%) 🔴"

        analytics_text = (f"📊 <b>Аналитика сетапа (SMC):</b>\n"
                          f"• Тренд BTC: {btc_trend_str}\n"
                          f"• Альтсезон: {alt_str}\n"
                          f"• Подтверждение: {signal_data.get('confirm_type', 'N/A')} ✅\n"
                          f"• Слом структуры: {signal_data.get('choch_type', 'N/A')} [{setup_eval}]\n"
                          f"• Отклонение от EMA200: {ema_dist:+.2f}% [{ema_eval}]\n"
                          f"• Осциллятор (RSI): {rsi:.1f} [{rsi_eval}]\n"
                          f"• Цена к VWAP: {vwap_eval}")
    else: 
        analytics_text = (f"📊 <b>Параметры входа (GRID):</b>\n"
                          f"• Всплеск объема: {signal_data.get('vol_ratio', 0):.1f}x от среднего\n"
                          f"• Размер пробойной свечи: {signal_data.get('candle_size', 0):.2f}%")

    msg = (f"💥 <b>ВЫСТРЕЛ [KRAKEN {strategy_name} v8.45]: {clean_sym}</b>{oracle_text}\n"
           f"Направление: <b>#{direction}</b>\nЦена: {current_price}\n"
           f"Объем (контр.): {qty}\nSL: {sl_price} ({sl_pct:.2f}%)\n"
           f"Smart TP Цель: {tp_price}\n\n{analytics_text}")
    
    await send_tg_msg(msg)

async def monitor_positions_task():
    global active_positions, daily_stats, COOLDOWN_CACHE
    while True:
        try:
            if not active_positions: await asyncio.sleep(15); continue
            positions_raw = await exchange.fetch_positions()
            tickers = await exchange.fetch_tickers([p['symbol'] for p in active_positions])
            
            updated = []
            for pos in active_positions:
                sym = pos['symbol']
                clean_name = sym.split(':')[0].split('/')[0]
                is_long = pos['direction'] == 'Long'
                
                market = exchange.markets.get(sym, {})
                contract_size = float(market.get('contractSize', 1.0))
                
                if 'current_qty' not in pos: pos['current_qty'] = pos['initial_qty']
                if 'be_moved' not in pos: pos['be_moved'] = False
                if 'tp80_hit' not in pos: pos['tp80_hit'] = False
                if 'tp100_hit' not in pos: pos['tp100_hit'] = False
                if 'atr' not in pos: pos['atr'] = abs(pos['entry_price'] - pos['sl_price']) * 0.5
                if 'current_sl' not in pos: pos['current_sl'] = pos['sl_price']

                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                ticker = tickers.get(sym, {}).get('last', pos['entry_price'])
                sl_side = 'sell' if is_long else 'buy'
                
                if not curr:
                    exit_price = pos['current_sl']
                    chunk_pnl = (exit_price - pos['entry_price']) * pos['current_qty'] * contract_size if is_long else (pos['entry_price'] - exit_price) * pos['current_qty'] * contract_size
                    daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                    daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_pnl
                    COOLDOWN_CACHE[clean_name] = time.time() + 14400
                    
                    if chunk_pnl < 0 and not pos['be_moved']:
                        daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(chunk_pnl)
                        await send_tg_msg(f"🛑 <b>{clean_name} закрыта по SL.</b>\nPNL: {chunk_pnl:.2f} USD")
                    else:
                        await send_tg_msg(f"🛡 <b>{clean_name} закрыта по Трейлингу/Б/У!</b>\nPNL: {chunk_pnl:+.2f} USD")
                    continue

                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (ticker - pos['entry_price']) * pos['current_qty'] * contract_size if is_long else (pos['entry_price'] - ticker) * pos['current_qty'] * contract_size

                if hours_passed >= 3.0 or (hours_passed >= 1.5 and pnl > 0):
                    try:
                        await safe_create_order(sym, 'market', sl_side, pos['current_qty'], {'reduceOnly': True})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + pnl
                        COOLDOWN_CACHE[clean_name] = time.time() + 14400
                        if pnl > 0: daily_stats['wins'] = daily_stats.get('wins', 0) + 1; daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + pnl
                        else: daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(pnl)
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} <b>{clean_name} закрыта по ТАЙМАУТУ!</b>\nPNL: {pnl:+.2f} USD")
                        continue
                    except: pass

                try: 
                    ohlcv = await exchange.fetch_ohlcv(sym, timeframe='1m', limit=2)
                    high_p = max([float(c[2]) for c in ohlcv]); low_p = min([float(c[3]) for c in ohlcv])
                except: high_p = low_p = ticker

                entry = pos['entry_price']
                dist = pos['tp1'] - entry
                tp60 = entry + dist * 0.60
                tp80 = entry + dist * 0.80
                tp100 = pos['tp1']

                if not pos['be_moved']:
                    if (is_long and high_p >= tp60) or (not is_long and low_p <= tp60):
                        be_price = float(exchange.price_to_precision(sym, entry * (1.002 if is_long else 0.998)))
                        if pos.get('sl_order_id'): 
                            try: await exchange.cancel_order(pos['sl_order_id'], sym)
                            except: pass
                        new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'stopPrice': be_price, 'reduceOnly': True})
                        pos.update({'be_moved': True, 'sl_order_id': new_sl['id'], 'current_sl': be_price})
                        await send_tg_msg(f"🛡 <b>{clean_name}</b>: Цена прошла 60%. SL перенесен в Б/У.")

                if pos['be_moved'] and not pos['tp80_hit']:
                    if (is_long and high_p >= tp80) or (not is_long and low_p <= tp80):
                        close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.25))
                        if close_qty > 0:
                            await safe_create_order(sym, 'market', sl_side, close_qty, {'reduceOnly': True})
                            if pos.get('sl_order_id'): 
                                try: await exchange.cancel_order(pos['sl_order_id'], sym)
                                except: pass
                            pos['current_qty'] = float(exchange.amount_to_precision(sym, pos['current_qty'] - close_qty))
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'stopPrice': pos['current_sl'], 'reduceOnly': True})
                            
                            chunk_pnl = (tp80 - entry) * close_qty * contract_size if is_long else (entry - tp80) * close_qty * contract_size
                            daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_pnl
                            pos.update({'tp80_hit': True, 'sl_order_id': new_sl['id']})
                            await send_tg_msg(f"💸 <b>{clean_name}</b>: Цена прошла 80%. Закрыто 25% объема. (PNL: +{chunk_pnl:.2f})")

                if pos['tp80_hit'] and not pos['tp100_hit']:
                    if (is_long and high_p >= tp100) or (not is_long and low_p <= tp100):
                        close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.50))
                        if close_qty > 0:
                            await safe_create_order(sym, 'market', sl_side, close_qty, {'reduceOnly': True})
                            if pos.get('sl_order_id'): 
                                try: await exchange.cancel_order(pos['sl_order_id'], sym)
                                except: pass
                            pos['current_qty'] = float(exchange.amount_to_precision(sym, pos['current_qty'] - close_qty))
                            
                            trail_sl = float(exchange.price_to_precision(sym, tp100 - pos['atr'] if is_long else tp100 + pos['atr']))
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'stopPrice': trail_sl, 'reduceOnly': True})
                            
                            chunk_pnl = (tp100 - entry) * close_qty * contract_size if is_long else (entry - tp100) * close_qty * contract_size
                            daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + chunk_pnl
                            daily_stats['wins'] = daily_stats.get('wins', 0) + 1 
                            pos.update({'tp100_hit': True, 'sl_order_id': new_sl['id'], 'current_sl': trail_sl})
                            await send_tg_msg(f"💰 <b>{clean_name}</b>: Тейк 100% взят! Закрыто еще 50%. Включен Трейлинг. (PNL: +{chunk_pnl:.2f})")

                if pos['tp100_hit']:
                    trail_sl = float(exchange.price_to_precision(sym, high_p - pos['atr'] if is_long else low_p + pos['atr']))
                    if (is_long and trail_sl > pos['current_sl']) or (not is_long and trail_sl < pos['current_sl']):
                        if pos.get('sl_order_id'): 
                            try: await exchange.cancel_order(pos['sl_order_id'], sym)
                            except: pass
                        try:
                            new_sl = await safe_create_order(sym, 'stop_market', sl_side, pos['current_qty'], {'stopPrice': trail_sl, 'reduceOnly': True})
                            pos.update({'sl_order_id': new_sl['id'], 'current_sl': trail_sl})
                        except: pass

                updated.append(pos)
            active_positions = updated; await asyncio.to_thread(save_positions)
        except Exception as e: pass
        await asyncio.sleep(15)

async def process_smc_coin(sym, ctx, sem):
    async with sem:
        try:
            await asyncio.sleep(0.3)
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=200)
            if not ohlcv or len(ohlcv) < 50: return sym, 'no_data'
            
            o, h, l, c, v = np.array([x[1] for x in ohlcv], dtype=float), np.array([x[2] for x in ohlcv], dtype=float), np.array([x[3] for x in ohlcv], dtype=float), np.array([x[4] for x in ohlcv], dtype=float), np.array([x[5] for x in ohlcv], dtype=float)
            trend, bos_choch = analyze_structure(h, l, c)
            fvgs = analyze_fvg(o, h, l, c)
            current_price = c[-1]; ema200 = calculate_ema(c, 200)
            ema_dist = ((current_price - ema200) / ema200) * 100
            
            is_green_candle = c[-1] > o[-1]
            is_red_candle = c[-1] < o[-1]
            
            vwap = calculate_vwap(h, l, c, v)
            rsi_curr = calculate_rsi(c[:-1], 14)
            rsi_prev = calculate_rsi(c[:-2], 14)
            atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-14:])
            
            active_fvg = next((fvg for fvg in reversed(fvgs) if (
                (fvg['type'] == 'Bullish' and current_price > fvg['top'] and (len(c) - fvg['index']) <= 15) or 
                (fvg['type'] == 'Bearish' and current_price < fvg['bottom'] and (len(c) - fvg['index']) <= 15)
            )), None)
            
            if not bos_choch: return sym, 'no_choch'
            if not active_fvg: return sym, 'no_fvg'
            
            avg_vol = np.mean(v[-22:-2])
            vol_ratio = v[-2] / avg_vol if avg_vol > 0 else 0
            if vol_ratio < 1.30 or vol_ratio > 5.0: return sym, 'no_volume'
            
            candle_body = abs(c[-2] - o[-2]) / o[-2] * 100
            if candle_body < 0.5: return sym, 'weak_candle'

            mode = 'Long' if bos_choch == 'CHoCH_Bullish' and active_fvg['type'] == 'Bullish' else 'Short' if bos_choch == 'CHoCH_Bearish' and active_fvg['type'] == 'Bearish' else None
            if not mode: return sym, 'no_setup'
            
            btc_trend = ctx['btc_trend']
            altseason = ctx['altseason']
            
            if mode == 'Long' and btc_trend == 'Short' and not altseason: return sym, 'wrong_trend'
            if mode == 'Short' and btc_trend == 'Long': return sym, 'wrong_trend'

            if mode == 'Long' and current_price < vwap: return sym, 'vwap_reject'
            if mode == 'Short' and current_price > vwap: return sym, 'vwap_reject'
            
            # --- ИСПРАВЛЕННЫЕ СНАЙПЕРСКИЕ ФИЛЬТРЫ V8.45 ---
            confirm_type = ""
            if mode == 'Long':
                if not is_green_candle: return sym, 'no_confirm'
                if rsi_curr < rsi_prev: return sym, 'rsi_falling'
                if ema_dist > 3.5: return sym, 'overextended'
                if rsi_curr > 60: return sym, 'rsi_exhausted'
                confirm_type = "Зеленая свеча + Загиб RSI"
            else:
                if not is_red_candle: return sym, 'no_confirm'
                if rsi_curr > rsi_prev: return sym, 'rsi_falling'
                if ema_dist < -3.5: return sym, 'overextended'
                if rsi_curr < 40: return sym, 'rsi_exhausted'
                confirm_type = "Красная свеча + Загиб RSI"

            if mode == 'Long':
                sl_price = min(l[active_fvg['index']:]) - (atr * 2.5) 
                if (current_price - sl_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 - MIN_SL_PCT/100)
                tp_price = current_price + (current_price - sl_price) * 1.5
            else:
                sl_price = max(h[active_fvg['index']:]) + (atr * 2.5)
                if (sl_price - current_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 + MIN_SL_PCT/100)
                tp_price = current_price - (sl_price - current_price) * 1.5

            return sym, {
                'mode': mode, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price,
                'btc_trend': btc_trend, 'btc_ema_dist': ctx['btc_ema_dist'],
                'altseason': altseason, 'eth_btc_diff': ctx['eth_btc_diff'],
                'fvg_type': active_fvg['type'], 'choch_type': bos_choch,
                'ema_dist': ema_dist, 'rsi': rsi_curr, 'vwap': vwap, 'vol_ratio': vol_ratio,
                'confirm_type': confirm_type
            }
        except: return sym, 'error'

async def smc_radar_task():
    global NOTIFIED_SYMBOLS
    await exchange.load_markets() 
    while True:
        try:
            if len(active_positions) >= MAX_POSITIONS: await asyncio.sleep(60); continue
            
            btc_trend = 'Long'
            altseason = False
            btc_ema_dist = 0.0
            eth_btc_diff = 0.0
            
            try:
                try: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USD:USD', timeframe=SMC_TIMEFRAME, limit=205)
                except: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_ema = calculate_ema(btc_c, 200)
                btc_trend = 'Long' if btc_c[-1] > btc_ema else 'Short'
                btc_ema_dist = (btc_c[-1] - btc_ema) / btc_ema * 100

                try: eth_ohlcv = await exchange.fetch_ohlcv('ETH/USD:USD', timeframe=SMC_TIMEFRAME, limit=205)
                except: eth_ohlcv = await exchange.fetch_ohlcv('ETH/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                eth_c = np.array([x[4] for x in eth_ohlcv], dtype=float)
                
                eth_return = (eth_c[-1] - eth_c[-50]) / eth_c[-50] * 100
                btc_return = (btc_c[-1] - btc_c[-50]) / btc_c[-50] * 100
                eth_btc_diff = eth_return - btc_return
                
                if eth_btc_diff > 0.5 and eth_c[-1] > calculate_ema(eth_c, 200):
                    altseason = True
            except: pass
            
            global_ctx = {
                'btc_trend': btc_trend,
                'btc_ema_dist': btc_ema_dist,
                'altseason': altseason,
                'eth_btc_diff': eth_btc_diff
            }

            tickers = await exchange.fetch_tickers()
            temp_symbols = []
            for sym, tick in tickers.items():
                clean_sym = sym.split(':')[0].split('/')[0]
                if exchange.markets.get(sym, {}).get('active') is False: continue
                if not (sym.endswith('USD') or sym.endswith('USDT')): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                if clean_sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[clean_sym]: continue
                if any(pos['symbol'].split(':')[0].split('/')[0] == clean_sym for pos in active_positions): continue
                
                # ФИКС КРАКЕНА: Умный подсчет объема
                q_vol = tick.get('quoteVolume')
                if not q_vol:
                    base_vol = tick.get('baseVolume', 0)
                    last_price = tick.get('last', 0)
                    q_vol = float(base_vol) * float(last_price) if base_vol and last_price else 0
                
                temp_symbols.append((sym, float(q_vol)))
            
            stats = {'total': len(tickers), 'no_choch': 0, 'no_fvg': 0, 'no_volume': 0, 'wrong_trend': 0, 'vwap_reject': 0, 'overextended': 0, 'no_confirm': 0, 'rsi_falling': 0, 'rsi_exhausted': 0, 'passed': 0}
            
            # ФИКС: Сортируем монеты по ликвидности и берем самые объемные (безопаснее жесткого лимита)
            valid_symbols_data = [sym for sym, vol in sorted(temp_symbols, key=lambda x: x[1], reverse=True)][:SCAN_LIMIT]
            
            logging.info(f"⏳ [SMC РАДАР] Опрос {len(valid_symbols_data)} монет (Альтсезон: {'ON' if altseason else 'OFF'})...")
            
            sem = asyncio.Semaphore(10); tasks = [process_smc_coin(s, global_ctx, sem) for s in valid_symbols_data]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_results = []
            for r in results:
                if isinstance(r, tuple) and len(r) == 2:
                    sym, signal = r
                    if signal == 'no_choch': stats['no_choch'] += 1
                    elif signal == 'no_fvg': stats['no_fvg'] += 1
                    elif signal == 'no_volume' or signal == 'weak_candle': stats['no_volume'] += 1
                    elif signal == 'wrong_trend': stats['wrong_trend'] += 1
                    elif signal == 'vwap_reject': stats['vwap_reject'] += 1
                    elif signal == 'overextended': stats['overextended'] += 1
                    elif signal == 'no_confirm': stats['no_confirm'] += 1
                    elif signal == 'rsi_falling': stats['rsi_falling'] += 1
                    elif signal == 'rsi_exhausted': stats['rsi_exhausted'] += 1
                    elif isinstance(signal, dict): 
                        stats['passed'] += 1
                        valid_results.append((sym, signal))

            logging.info(f"🔎 [SMC] Откл: Структура({stats['no_choch']}) FVG({stats['no_fvg']}) Объем({stats['no_volume']}) Тренд({stats['wrong_trend']}) VWAP({stats['vwap_reject']}) Пузырь({stats['overextended']}) Свеча/Хук({stats['no_confirm'] + stats['rsi_falling']}) RSI_лимит({stats['rsi_exhausted']}) -> ВХОДЫ: {stats['passed']}")

            for sym, signal in valid_results:
                if sym not in NOTIFIED_SYMBOLS: 
                    NOTIFIED_SYMBOLS.add(sym)
                    await execute_trade(sym, signal, "SMC Async")
            gc.collect(); await asyncio.sleep(60) 
        except Exception as e: logging.error(f"SMC Radar Error: {e}"); await asyncio.sleep(60)

async def process_grid_coin(sym, sem):
    async with sem:
        try:
            await asyncio.sleep(0.3)
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=25)
            if not ohlcv or len(ohlcv) < 20: return sym, 'no_data'
            
            o, h, l, c, v = np.array([x[1] for x in ohlcv]), np.array([x[2] for x in ohlcv]), np.array([x[3] for x in ohlcv]), np.array([x[4] for x in ohlcv]), np.array([x[5] for x in ohlcv])
            avg_vol = np.mean(v[-22:-2])
            
            vol_ratio = v[-2] / avg_vol if avg_vol > 0 else 0
            if vol_ratio < 1.30 or vol_ratio > 6.0: return sym, 'no_vol_spike'
            
            candle_size_pct = abs(c[-2] - o[-2]) / o[-2] * 100
            if candle_size_pct < 1.0: return sym, 'no_price_spike'
            
            direction = 'Long' if c[-2] > o[-2] else 'Short'
            current_price = c[-1]
            atr = np.mean(np.maximum(h[1:]-l[1:], np.maximum(np.abs(h[1:]-c[:-1]), np.abs(l[1:]-c[:-1])))[-14:])
            
            base_coin = sym.split('/')[0]
            if not await verify_global_volume(base_coin):
                return sym, 'oracle_rejected'

            if direction == 'Long':
                sl_price = l[-2] - (atr * 2.0)
                if (current_price - sl_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 - MIN_SL_PCT/100)
                tp_price = current_price + (current_price - sl_price) * 1.5
            else:
                sl_price = h[-2] + (atr * 2.0)
                if (sl_price - current_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 + MIN_SL_PCT/100)
                tp_price = current_price - (sl_price - current_price) * 1.5

            return sym, {'mode': direction, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price, 'vol_ratio': vol_ratio, 'candle_size': candle_size_pct}
        except: return sym, 'error'

async def grid_radar_task():
    global NOTIFIED_SYMBOLS
    await exchange.load_markets() 
    await asyncio.sleep(30) 
    while True:
        try:
            if len(active_positions) >= MAX_POSITIONS: await asyncio.sleep(60); continue
            tickers = await exchange.fetch_tickers()
            temp_symbols = []
            for sym, tick in tickers.items():
                clean_sym = sym.split(':')[0].split('/')[0]
                if exchange.markets.get(sym, {}).get('active') is False: continue
                if not (sym.endswith('USD') or sym.endswith('USDT')): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                if clean_sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[clean_sym]: continue
                if any(pos['symbol'].split(':')[0].split('/')[0] == clean_sym for pos in active_positions): continue
                
                # ФИКС КРАКЕНА: Умный подсчет объема
                q_vol = tick.get('quoteVolume')
                if not q_vol:
                    base_vol = tick.get('baseVolume', 0)
                    last_price = tick.get('last', 0)
                    q_vol = float(base_vol) * float(last_price) if base_vol and last_price else 0
                
                temp_symbols.append((sym, float(q_vol)))
            
            stats = {'vol_fail': 0, 'price_fail': 0, 'oracle_reject': 0, 'passed': 0}
            
            # ФИКС: Сортируем монеты по ликвидности
            valid_symbols_data = [sym for sym, vol in sorted(temp_symbols, key=lambda x: x[1], reverse=True)][:SCAN_LIMIT]
            
            logging.info(f"⏳ [GRID РАДАР] Опрос {len(valid_symbols_data)} монет...")
            
            sem = asyncio.Semaphore(10); tasks = [process_grid_coin(s, sem) for s in valid_symbols_data]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_results = []
            for r in results:
                if isinstance(r, tuple) and len(r) == 2:
                    sym, signal = r
                    if signal == 'no_vol_spike': stats['vol_fail'] += 1
                    elif signal == 'no_price_spike': stats['price_fail'] += 1
                    elif signal == 'oracle_rejected': stats['oracle_reject'] += 1
                    elif isinstance(signal, dict): 
                        stats['passed'] += 1
                        valid_results.append((sym, signal))

            logging.info(f"🌐 [GRID РАДАР] Откл. объемом: {stats['vol_fail']} -> Нет пробоя: {stats['price_fail']} -> Оракул: {stats['oracle_reject']} -> ВХОДЫ: {stats['passed']}")

            for sym, signal in valid_results:
                if sym not in NOTIFIED_SYMBOLS: 
                    NOTIFIED_SYMBOLS.add(sym)
                    await execute_trade(sym, signal, "GRID Oracle")
            await asyncio.sleep(60) 
        except Exception as e: logging.error(f"GRID Radar Error: {e}"); await asyncio.sleep(60)

async def print_stats_hourly():
    global daily_stats, REPORTED_TODAY
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.hour == 20 and not REPORTED_TODAY:
                bal = await exchange.fetch_balance()
                current_balance = float(bal.get('USDT', {}).get('total', 0)) or float(bal.get('USD', {}).get('total', 0))
                start_bal = daily_stats.get('start_balance', 0.0)
                pct_change = ((current_balance - start_bal) / start_bal * 100) if start_bal > 0 else 0.0
                winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats['trades'] > 0 else 0
                await send_tg_msg(f"🗓 <b>ИТОГИ ДНЯ (KRAKEN Async v8.45):</b> {now.strftime('%d.%m.%Y')}\n\n📉 Закрыто сделок: {daily_stats['trades']}\n🎯 Винрейт: {winrate:.1f}%\n💵 Net PNL: {daily_stats['pnl']:+.2f} USD\n\n🏦 <b>Баланс:</b> {current_balance:.2f} USD\n📊 <b>Изменение:</b> {pct_change:+.2f}%\n<i>*В работе: {len(active_positions)}</i>")
                daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': winrate, 'start_balance': current_balance, 'gross_profit': 0.0, 'gross_loss': 0.0}
                await asyncio.to_thread(save_positions); REPORTED_TODAY = True
            elif now.hour != 20: REPORTED_TODAY = False
            
            if now.minute == 0:
                active = ", ".join([p['symbol'].split(':')[0].split('/')[0] for p in active_positions]) or "Нет"
                winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats['trades'] > 0 else 0
                logging.info(f"📊 [ТЕЛЕМЕТРИЯ v8.45 KRAKEN] В работе: {active} | Винрейт: {winrate:.1f}%")
        except: pass
        await asyncio.sleep(60)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Kraken Prop Async Bot Active v8.45")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler); server.serve_forever()

async def main():
    init_db(); load_positions()
    if daily_stats.get('start_balance', 0.0) == 0.0:
        try: 
            bal = await exchange.fetch_balance()
            daily_stats['start_balance'] = float(bal.get('USDT', {}).get('total', 0)) or float(bal.get('USD', {}).get('total', 0))
            await asyncio.to_thread(save_positions)
        except: pass
        
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА v8.45 (Sniper Unchained for Prop)...")
    await send_tg_msg("🟢 <b>KRAKEN ASYNC БОТ v8.45</b> запущен (Полный порт логики BingX на архитектуру Kraken)!")
    Thread(target=run_server, daemon=True).start()
    
    asyncio.create_task(monitor_positions_task())
    asyncio.create_task(print_stats_hourly())
    asyncio.create_task(smc_radar_task())
    await grid_radar_task()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
