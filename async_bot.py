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

# === НАСТРОЙКИ PROP FIRM (Breakout / Kraken) ===
DB_PATH = 'bot_prop.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
# Добавили значение по умолчанию "0", чтобы избежать ошибки NoneType
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID', 0)) 
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

RISK_PER_TRADE = 0.005      
MAX_POSITIONS = 3           
LEVERAGE = 5                
MAX_SPREAD_PERCENT = 1.0    # РАЗРЕШАЕМ широкие спреды демо-сервера
MIN_VOLUME_USDT = 0         # РАЗРЕШАЕМ пустые объемы демо-сервера
MIN_NOTIONAL_USDT = 10.0    
WHALE_VOLUME_MULTIPLIER = 8.0 

SMC_TIMEFRAME = '15m'
CRITICAL_CVD_USDT = 150000  
GLOBAL_CVD = {}             
COOLDOWN_CACHE = {}         

MAX_CONCURRENT_TASKS = 10  
SCAN_LIMIT = 300           

# СОКРАЩЕННЫЙ СПИСОК
EXCLUDED_KEYWORDS = [
    'FART', 'PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 'FLOKI', 'BOME',
    'MEME', 'TURBO', 'SATS', 'RATS', 'ORDI', 'PEOPLE'
]
GLOBAL_STOP_UNTIL = None
CONSECUTIVE_LOSSES = 0
last_signals = {} 
NOTIFIED_SYMBOLS = set() 
GLOBAL_BALANCE = 0.0 

HOT_LIST = {}  
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

# === ИНИЦИАЛИЗАЦИЯ БИРЖИ KRAKEN (АСИНХРОННАЯ) ===
exchange = ccxt_async.krakenfutures({
    'apiKey': KRAKEN_API_KEY, 
    'secret': KRAKEN_SECRET,
    'enableRateLimit': True
})
# ВАЖНО: Включаем демо-сервер (Sandbox) для тестирования!
exchange.set_sandbox_mode(True)

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
    # Меняем парсер на стабильный HTML
    payload = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"}
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

async def process_single_coin(sym, btc_trend, altseason, btc_volatility_pct, q_vol, sem):
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
            
            if mode == 'Long' and btc_trend != 'Long': return sym, None
            if mode == 'Short' and btc_trend != 'Short': return sym, None

            signal = {'mode': mode, 'price': current_price, 'vol_24h': q_vol, 'fvg': active_fvg, 'time': datetime.now(timezone.utc).isoformat()}
            last_signals[sym.split(':')[0]] = datetime.now(timezone.utc)
            return sym, signal
        except: return sym, None

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
                # Меняем USDT на USD для фьючерсов Kraken
                btc_ohlcv = await exchange.fetch_ohlcv('BTC/USD:USD', timeframe=SMC_TIMEFRAME, limit=205)
                eth_ohlcv = await exchange.fetch_ohlcv('ETH/USD:USD', timeframe=SMC_TIMEFRAME, limit=205)
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
                # ИСПРАВЛЕННЫЙ БЛОК проверки NoneType
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
                tasks = [process_single_coin(s, btc_trend, altseason, btc_volatility_pct, v, sem) for s, v in chunk if s.split(':')[0] not in last_signals or (datetime.now(timezone.utc) - last_signals[s.split(':')[0]] > timedelta(hours=4))]
                if tasks:
                    results.extend(await asyncio.gather(*tasks, return_exceptions=True))
                    
            valid_results = [r for r in results if isinstance(r, tuple) and len(r) == 2 and r[1] is not None]
            stats['passed'] = len(valid_results)
            stats['waiting'] = stats['total'] - stats['low_vol'] - stats['passed']
            
            logging.info(f"🔎 [РАДАР] Всего: {stats['total']} -> Неликвид: {stats['low_vol']} -> Ждем Сетап: {stats['waiting']} -> ВХОДЫ: {stats['passed']}")

            new_hot_list = {}
            for res in results:
                if not isinstance(res, tuple) or len(res) != 2: continue
                sym, signal = res
                
                if signal:
                    new_hot_list[sym] = signal
                    if sym not in HOT_LIST and sym not in NOTIFIED_SYMBOLS:
                        NOTIFIED_SYMBOLS.add(sym)
                        # Используем HTML-теги для жирного текста
                        await send_tg_msg(f"🎯 <b>РАДАР [{signal['mode']}]:</b> {sym.split(':')[0]} взят на мушку! (24h Vol: {signal['vol_24h']/1000000:.1f}M)")

            HOT_LIST = new_hot_list
            NOTIFIED_SYMBOLS = {s for s in NOTIFIED_SYMBOLS if s in HOT_LIST}
                
            logging.info(f"🔎 [РАДАР] Скан завершен. На мушке: {len(HOT_LIST)} | ОЗУ: {get_mem_usage()} | BTC Vol: {btc_volatility_pct:.2f}%")
            gc.collect() 
            await asyncio.sleep(300) 
        except Exception as e: 
            logging.error(f"Radar Loop Error: {e}")
            await asyncio.sleep(60)

async def print_stats_hourly():
    global daily_stats
    while True:
        try:
            winrate = (daily_stats['wins'] / daily_stats['trades'] * 100) if daily_stats.get('trades', 0) > 0 else 0
            active = ", ".join([f"{p['symbol'].split(':')[0]}" for p in active_positions]) or "Нет"
            logging.info(f"📊 [СТАТИСТИКА] Сделок: {daily_stats.get('trades', 0)} | Винрейт: {winrate:.1f}% | PNL: {daily_stats.get('pnl', 0.0):.2f}$ | Открыто: {active}")
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
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА (Prop Firm: 0.5% Risk, Meme-Filter)...")
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(print_stats_hourly())
    await radar_task()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
