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
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer

# === НАСТРОЙКИ PROP FIRM (Breakout / Kraken) ===
DB_PATH = 'bot_prop.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = -1003955653290  # <--- ПРОСТО ВПИШИ ЦИФРЫ СЮДА
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

RISK_PER_TRADE = 0.005      # РИСК 0.5%
MAX_POSITIONS = 3           
LEVERAGE = 5                
MAX_SPREAD_PERCENT = 1.0    # ДЛЯ ДЕМО
MIN_VOLUME_USDT = 0         # ДЛЯ ДЕМО
SMC_TIMEFRAME = '15m'
COOLDOWN_CACHE = {}         
SCAN_LIMIT = 300           

# СОКРАЩЕННЫЙ СПИСОК (Для Демо сервера)
EXCLUDED_KEYWORDS = [
    'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 'FLOKI', 'BOME',
    'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI', 'PEOPLE'
]

daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': 0.0}
active_positions = []
HOT_LIST = {}  
NOTIFIED_SYMBOLS = set() 

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

def get_mem_usage():
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if 'VmRSS' in line: return f"{int(line.split()[1]) / 1024:.1f} MB"
    except: pass
    return "N/A"

# === ИНИЦИАЛИЗАЦИЯ БИРЖИ KRAKEN ===
exchange = ccxt_async.krakenfutures({
    'apiKey': KRAKEN_API_KEY, 
    'secret': KRAKEN_SECRET,
    'enableRateLimit': True
})
exchange.set_sandbox_mode(True) # ДЕМО РЕЖИМ

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
              (daily_stats.get('pnl', 0.0), daily_stats.get('trades', 0), daily_stats.get('wins', 0), daily_stats.get('prev_winrate', 0.0)))
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
    if not TOKEN or GROUP_CHAT_ID == 0: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"} # ИСПОЛЬЗУЕМ HTML
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp: pass
    except: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1)
    ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

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

# === ТОРГОВЫЙ МОДУЛЬ (ДОБАВЛЕН) ===
async def execute_trade(sym, direction, current_price, sl_price, tp_price):
    global active_positions
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: return
        
    try:
        bal = await exchange.fetch_balance()
        # Поддержка USD для демо и USDT для реала
        free_usd = float(bal.get('USDT', {}).get('free', 0)) or float(bal.get('USD', {}).get('free', 0))
        if free_usd <= 0: return

        actual_sl_dist = abs(current_price - sl_price)
        if actual_sl_dist <= 0: return
        risk_amount = free_usd * RISK_PER_TRADE
        
        market = exchange.markets[sym]
        contract_size = float(market.get('contractSize', 1.0))
        
        qty_coins = risk_amount / actual_sl_dist
        qty = qty_coins / contract_size if contract_size > 0 else qty_coins
        qty = float(exchange.amount_to_precision(sym, qty))
        if qty <= 0: return
        
        try: await exchange.set_leverage(LEVERAGE, sym)
        except: pass
        
        side = 'buy' if direction == 'Long' else 'sell'
        sl_side = 'sell' if direction == 'Long' else 'buy'
        
        order = await exchange.create_market_order(sym, side, qty)
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={'stopLossPrice': sl_price})
        
        active_positions.append({
            'symbol': sym, 'direction': direction, 'entry_price': current_price, 
            'initial_qty': qty, 'sl_price': sl_price, 'tp1': tp_price, 
            'sl_order_id': sl_ord['id'], 'open_time': datetime.now(timezone.utc).isoformat()
        })
        await asyncio.to_thread(save_positions)
        
        sl_pct = (actual_sl_dist / current_price) * 100
        await send_tg_msg(f"💥 <b>ВЫСТРЕЛ [SMC Async]: {sym.split(':')[0]}</b>\nНаправление: <b>#{direction}</b>\n\nЦена: {current_price}\nОбъем: {qty}\nSL: {sl_price} ({sl_pct:.2f}%)\nTP: {tp_price}")
    except Exception as e: 
        logging.error(f"Trade execution error {sym}: {e}")

async def monitor_positions_task():
    global active_positions, daily_stats, COOLDOWN_CACHE
    while True:
        try:
            if not active_positions:
                await asyncio.sleep(15); continue
                
            positions_raw = await exchange.fetch_positions()
            symbols_to_fetch = [p['symbol'] for p in active_positions]
            tickers = await exchange.fetch_tickers(symbols_to_fetch)
            
            updated = []
            for pos in active_positions:
                sym = pos['symbol']
                clean_name = sym.split(':')[0]
                is_long = pos['direction'] == 'Long'
                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                ticker = tickers.get(sym, {}).get('last', pos['entry_price'])
                
                if not curr:
                    pnl = (ticker - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - ticker) * pos['initial_qty']
                    daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                    
                    if pnl > 0: 
                        daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        await send_tg_msg(f"✅ <b>{clean_name} закрыта в плюс!</b>\nPNL: +{pnl:.2f} USD")
                    else: 
                        COOLDOWN_CACHE[sym] = time.time() + 14400
                        await send_tg_msg(f"🛑 <b>{clean_name} выбита по SL.</b>\nPNL: {pnl:.2f} USD\n❄️ Монета заморожена.")
                    continue

                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (ticker - pos['entry_price']) * float(curr['contracts']) if is_long else (pos['entry_price'] - ticker) * float(curr['contracts'])

                if hours_passed >= 3.0 or (hours_passed >= 1.5 and pnl > 0):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']))
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        if pnl > 0: daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        else: COOLDOWN_CACHE[sym] = time.time() + 14400
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} <b>{clean_name} закрыта по ТАЙМАУТУ!</b>\nPNL: {pnl:.2f} USD")
                        continue
                    except: pass

                # Проверка тейк-профита
                if (is_long and ticker >= pos['tp1']) or (not is_long and ticker <= pos['tp1']):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']))
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        await send_tg_msg(f"💰 <b>{clean_name} TP взят!</b>\nPNL: +{pnl:.2f} USD")
                        continue
                    except: pass

                updated.append(pos)
                
            active_positions = updated
            await asyncio.to_thread(save_positions)
        except Exception as e: pass
        await asyncio.sleep(15)

async def process_single_coin(sym, btc_trend, q_vol, sem):
    async with sem:
        try:
            await asyncio.sleep(0.3)
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=100)
            if not ohlcv or len(ohlcv) < 50: return sym, None
            
            o = np.array([x[1] for x in ohlcv], dtype=float)
            h = np.array([x[2] for x in ohlcv], dtype=float)
            l = np.array([x[3] for x in ohlcv], dtype=float)
            c = np.array([x[4] for x in ohlcv], dtype=float)
            
            trend, bos_choch = analyze_structure(h, l, c)
            fvgs = analyze_fvg(o, h, l, c)
            current_price = c[-1]
            
            active_fvg = None
            for fvg in reversed(fvgs):
                if (fvg['type'] == 'Bullish' and current_price > fvg['top']) or (fvg['type'] == 'Bearish' and current_price < fvg['bottom']):
                    active_fvg = fvg; break
            
            if not active_fvg or not bos_choch: return sym, None
            
            mode = 'Long' if bos_choch == 'CHoCH_Bullish' and active_fvg['type'] == 'Bullish' else 'Short' if bos_choch == 'CHoCH_Bearish' and active_fvg['type'] == 'Bearish' else None
            
            if not mode: return sym, None  # <--- ВОТ ЭТА ЗАЩИТА ОТ NONE
            
            if mode == 'Long' and btc_trend != 'Long': return sym, None
            if mode == 'Short' and btc_trend != 'Short': return sym, None

            # РАСЧЕТ SL и TP по Smart Money
            if mode == 'Long':
                sl_price = min(l[active_fvg['index']:]) * 0.998
                if sl_price >= current_price: sl_price = current_price * 0.99
                tp_price = current_price + (current_price - sl_price) * 1.5
            else:
                sl_price = max(h[active_fvg['index']:]) * 1.002
                if sl_price <= current_price: sl_price = current_price * 1.01
                tp_price = current_price - (sl_price - current_price) * 1.5

            signal = {'mode': mode, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price, 'vol_24h': q_vol, 'fvg': active_fvg}
            return sym, signal
        except: return sym, None

async def radar_task():
    global HOT_LIST, NOTIFIED_SYMBOLS
    await exchange.load_markets() 
    await asyncio.sleep(5)
    while True:
        try:
            if len(active_positions) >= MAX_POSITIONS:
                await asyncio.sleep(60); continue

            btc_trend, btc_volatility_pct = 'Short', 2.0
            try:
                btc_ohlcv = await exchange.fetch_ohlcv('BTC/USD:USD', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_trend = 'Long' if btc_c[-1] > calculate_ema(btc_c, 200) else 'Short'
                btc_sma = np.mean(btc_c[-20:])
                btc_volatility_pct = (4 * np.std(btc_c[-20:])) / btc_sma * 100
            except: pass

            tickers = await exchange.fetch_tickers()
            stats = {'total': 0, 'low_vol': 0, 'waiting': 0, 'passed': 0}
            temp_symbols = []
            
            for sym, m in exchange.markets.items():
                if m.get('active') is False: continue
                if not (sym.endswith(':USD') or sym.endswith(':USDT')): continue
                if any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS): continue
                if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: continue
                
                stats['total'] += 1 
                tick = tickers.get(sym)
                if not tick: continue
                q_vol = float(tick.get('quoteVolume') or 0)
                
                if not any(pos['symbol'].split(':')[0] == sym.split(':')[0] for pos in active_positions):
                    temp_symbols.append((sym, q_vol))
                    
            valid_symbols_data = [(x[0], x[1]) for x in sorted(temp_symbols, key=lambda x: x[1], reverse=True)[:SCAN_LIMIT]]
            sem = asyncio.Semaphore(10)
            results = []
            
            for i in range(0, len(valid_symbols_data), 10):
                chunk = valid_symbols_data[i:i+10]
                tasks = [process_single_coin(s, btc_trend, v, sem) for s, v in chunk]
                if tasks: results.extend(await asyncio.gather(*tasks, return_exceptions=True))
                    
            valid_results = [r for r in results if isinstance(r, tuple) and len(r) == 2 and r[1] is not None]
            stats['passed'] = len(valid_results)
            stats['waiting'] = stats['total'] - stats['passed']
            
            logging.info(f"🔎 [РАДАР] Всего: {stats['total']} -> Неликвид: 0 -> Ждем Сетап: {stats['waiting']} -> ВХОДЫ: {stats['passed']}")

            for res in valid_results:
                sym, signal = res
                if sym not in NOTIFIED_SYMBOLS:
                    NOTIFIED_SYMBOLS.add(sym)
                    await send_tg_msg(f"🎯 <b>РАДАР [{signal['mode']}]:</b> {sym.split(':')[0]} найден сетап!")
                    # Открываем сделку на бирже!
                    await execute_trade(sym, signal['mode'], signal['price'], signal['sl_price'], signal['tp_price'])

            logging.info(f"🔎 [РАДАР] Скан завершен. На мушке: {len(valid_results)} | ОЗУ: {get_mem_usage()} | BTC Vol: {btc_volatility_pct:.2f}%")
            gc.collect(); await asyncio.sleep(60) 
        except Exception as e: 
            logging.error(f"Radar Loop Error: {e}"); await asyncio.sleep(60)

async def print_stats_hourly():
    global daily_stats
    while True:
        try:
            winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats.get('trades', 0) > 0 else 0
            active = ", ".join([f"{p['symbol'].split(':')[0]}" for p in active_positions]) or "Нет"
            logging.info(f"📊 [СТАТИСТИКА] Сделок: {daily_stats.get('trades', 0)} | Винрейт: {winrate:.1f}% | Открыто: {active}")
        except: pass
        await asyncio.sleep(3600)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Kraken Prop Async Bot Active")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
    server.serve_forever()

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА (Trading Enabled, Prop Firm Risk, HTML Telegram)...")
    
    # === ДОБАВЛЯЕМ СТАРТОВОЕ СООБЩЕНИЕ ===
    await send_tg_msg("🟢 <b>KRAKEN ASYNC БОТ</b> успешно запущен и готов к работе!")
    
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(monitor_positions_task()) 
    asyncio.create_task(print_stats_hourly())
    await radar_task()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
