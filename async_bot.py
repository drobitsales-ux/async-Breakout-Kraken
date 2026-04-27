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

DB_PATH = 'bot_prop.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = -1003955653290
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

RISK_PER_TRADE = 0.005      # ПРОП РИСК 0.5%
MAX_POSITIONS = 3           # ЛИМИТ 3 СДЕЛКИ
LEVERAGE = 5                
MIN_SL_PCT = 1.0            
MAX_SL_PCT = 5.0            

SMC_TIMEFRAME = '15m'
COOLDOWN_CACHE = {}         
SCAN_LIMIT = 300           
EXCLUDED_KEYWORDS = ['PEPE', 'SHIB', 'DOGE', 'WIF', 'BONK', 'FLOKI', 'MEME']

active_positions = []
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

exchange = ccxt_async.krakenfutures({'apiKey': KRAKEN_API_KEY, 'secret': KRAKEN_SECRET, 'enableRateLimit': True})

# === ИНИЦИАЛИЗАЦИЯ ОРАКУЛОВ (Binance, Bybit, MEXC) ===
ORACLES = [
    ccxt_async.binance({'enableRateLimit': True}),
    ccxt_async.bybit({'enableRateLimit': True}),
    ccxt_async.mexc({'enableRateLimit': True})
]

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)
def init_db(): conn = get_db_conn(); conn.cursor().execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)'''); conn.commit(); conn.close()
def save_positions(): conn = get_db_conn(); conn.cursor().execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),)); conn.commit(); conn.close()
def load_positions(): 
    global active_positions; conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
    if row: active_positions = json.loads(row[0])
    conn.close()

async def send_tg_msg(text):
    if not TOKEN: return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "HTML"})
    except: pass

def calculate_ema(data, window):
    if len(data) < window: return data[-1]
    alpha = 2 / (window + 1); ema = data[0]
    for price in data[1:]: ema = (price * alpha) + (ema * (1 - alpha))
    return ema

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

def analyze_fvg(o, h, l, c):
    fvg_list = []
    for i in range(1, len(c)-1):
        if l[i-1] > h[i+1]: fvg_list.append({'type': 'Bearish', 'top': l[i-1], 'bottom': h[i+1], 'index': i})
        elif h[i-1] < l[i+1]: fvg_list.append({'type': 'Bullish', 'top': l[i+1], 'bottom': h[i-1], 'index': i})
    return fvg_list

# === ЛОГИКА ОРАКУЛА: ПРОВЕРКА ГЛОБАЛЬНЫХ ОБЪЕМОВ ===
async def verify_global_volume(base_coin):
    check_sym = f"{base_coin}/USDT"
    async def fetch_ex(ex):
        try:
            ohlcv = await ex.fetch_ohlcv(check_sym, timeframe=SMC_TIMEFRAME, limit=25)
            if not ohlcv or len(ohlcv) < 20: return False
            v = np.array([x[5] for x in ohlcv])
            avg_vol = np.mean(v[-22:-2])
            return v[-2] > (avg_vol * 1.20) # Фильтр +20%
        except: return False
        
    tasks = [fetch_ex(ex) for ex in ORACLES]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    confirmations = [r for r in results if r is True]
    return len(confirmations) >= 1

async def execute_trade(sym, signal_data):
    global active_positions, COOLDOWN_CACHE
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
        
    direction, current_price, sl_price, tp_price = signal_data['mode'], signal_data['price'], signal_data['sl_price'], signal_data['tp_price']
    
    try:
        bal = await exchange.fetch_balance()
        free_usd = float(bal.get('USDT', {}).get('free', 0)) or float(bal.get('USD', {}).get('free', 0))
        if free_usd <= 0: return

        actual_sl_dist = abs(current_price - sl_price)
        qty_coins = (free_usd * RISK_PER_TRADE) / actual_sl_dist
        
        target_notional = qty_coins * current_price
        allowed_notional = free_usd * LEVERAGE * 0.90 
        if target_notional > allowed_notional: qty_coins = allowed_notional / current_price
        
        qty = float(exchange.amount_to_precision(sym, qty_coins))
        if qty <= 0: return
        
        try: await exchange.set_leverage(LEVERAGE, sym)
        except: pass
        
        side = 'buy' if direction == 'Long' else 'sell'; sl_side = 'sell' if direction == 'Long' else 'buy'
        
        await exchange.create_market_order(sym, side, qty)
        sl_ord = await exchange.create_order(sym, 'stop_market', sl_side, qty, params={'stopLossPrice': sl_price, 'reduceOnly': True})
        
        active_positions.append({'symbol': sym, 'direction': direction, 'entry_price': current_price, 'initial_qty': qty, 'sl_price': sl_price, 'tp1': tp_price, 'sl_order_id': sl_ord['id'], 'open_time': datetime.now(timezone.utc).isoformat()})
        await asyncio.to_thread(save_positions)
        await send_tg_msg(f"💥 <b>ВЫСТРЕЛ [SMC Async KRAKEN]: {sym.split(':')[0]}</b>\n🌐 <i>Подтверждено Оракулом (Binance/Bybit/MEXC)</i>\nНаправление: <b>#{direction}</b>\nЦена: {current_price}\nОбъем: {qty}\nSL: {sl_price}\nTP: {tp_price}")
    except Exception as e: logging.error(f"Trade execution error {sym}: {e}")

async def monitor_positions_task():
    global active_positions, COOLDOWN_CACHE
    while True:
        try:
            if not active_positions: await asyncio.sleep(15); continue
            positions_raw = await exchange.fetch_positions(); tickers = await exchange.fetch_tickers([p['symbol'] for p in active_positions])
            updated = []
            
            for pos in active_positions:
                sym, clean_name, is_long = pos['symbol'], pos['symbol'].split(':')[0], pos['direction'] == 'Long'
                curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
                ticker = tickers.get(sym, {}).get('last', pos['entry_price'])
                
                if not curr:
                    pnl = (pos['sl_price'] - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - pos['sl_price']) * pos['initial_qty']
                    COOLDOWN_CACHE[sym] = time.time() + 14400
                    await send_tg_msg(f"🛑 <b>{clean_name} выбита по SL.</b>\nPNL: {pnl:.2f} USD"); continue

                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (ticker - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - ticker) * pos['initial_qty']

                if hours_passed >= 3.0 or (hours_passed >= 1.5 and pnl > 0):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', pos['initial_qty'], params={'reduceOnly': True})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        if pnl < 0: COOLDOWN_CACHE[sym] = time.time() + 14400
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} <b>{clean_name} закрыта по ТАЙМАУТУ!</b>\nPNL: {pnl:+.2f} USD"); continue
                    except: pass

                if (is_long and ticker >= pos['tp1']) or (not is_long and ticker <= pos['tp1']):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', pos['initial_qty'], params={'reduceOnly': True})
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        await send_tg_msg(f"💰 <b>{clean_name} TP взят!</b>\nPNL: {pnl:+.2f} USD"); continue
                    except: pass

                updated.append(pos)
            active_positions = updated; await asyncio.to_thread(save_positions)
        except Exception as e: pass
        await asyncio.sleep(15)

async def process_single_coin(sym, btc_trend, sem):
    async with sem:
        try:
            await asyncio.sleep(0.3)
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=200)
            if not ohlcv or len(ohlcv) < 50: return sym, None
            
            o, h, l, c = np.array([x[1] for x in ohlcv], dtype=float), np.array([x[2] for x in ohlcv], dtype=float), np.array([x[3] for x in ohlcv], dtype=float), np.array([x[4] for x in ohlcv], dtype=float)
            trend, bos_choch = analyze_structure(h, l, c)
            fvgs = analyze_fvg(o, h, l, c)
            current_price = c[-1]; ema200 = calculate_ema(c, 200)
            
            active_fvg = next((fvg for fvg in reversed(fvgs) if (fvg['type'] == 'Bullish' and current_price > fvg['top']) or (fvg['type'] == 'Bearish' and current_price < fvg['bottom'])), None)
            if not active_fvg or not bos_choch: return sym, None
            mode = 'Long' if bos_choch == 'CHoCH_Bullish' and active_fvg['type'] == 'Bullish' else 'Short' if bos_choch == 'CHoCH_Bearish' and active_fvg['type'] == 'Bearish' else None
            if not mode or (mode == 'Long' and btc_trend != 'Long') or (mode == 'Short' and btc_trend != 'Short'): return sym, None

            # ПРОВЕРКА ОРАКУЛОМ (Глобальный объем)
            base_coin = sym.split('/')[0]
            if not await verify_global_volume(base_coin):
                return sym, None

            if mode == 'Long':
                sl_price = min(l[active_fvg['index']:]) * 0.998
                if (current_price - sl_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 - MIN_SL_PCT/100)
                tp_price = current_price + (current_price - sl_price) * 1.5
            else:
                sl_price = max(h[active_fvg['index']:]) * 1.002
                if (sl_price - current_price) / current_price * 100 < MIN_SL_PCT: sl_price = current_price * (1 + MIN_SL_PCT/100)
                tp_price = current_price - (sl_price - current_price) * 1.5

            return sym, {'mode': mode, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price, 'btc_trend': btc_trend}
        except: return sym, None

async def radar_task():
    await exchange.load_markets() 
    while True:
        try:
            if len(active_positions) >= MAX_POSITIONS: await asyncio.sleep(60); continue
            btc_trend = 'Long'
            try:
                try: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT:USDT', timeframe=SMC_TIMEFRAME, limit=205)
                except: btc_ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe=SMC_TIMEFRAME, limit=205)
                btc_c = np.array([x[4] for x in btc_ohlcv], dtype=float)
                btc_trend = 'Long' if btc_c[-1] > calculate_ema(btc_c, 200) else 'Short'
            except: pass

            tickers = await exchange.fetch_tickers()
            temp_symbols = [(sym, float(tick.get('quoteVolume') or 0)) for sym, tick in tickers.items() if (sym.endswith(':USDT') or sym.endswith(':USD')) and exchange.markets[sym].get('active') is not False and not any(kw in sym.upper() for kw in EXCLUDED_KEYWORDS) and sym not in COOLDOWN_CACHE and not any(pos['symbol'].split(':')[0] == sym.split(':')[0] for pos in active_positions)]
            valid_symbols_data = [x[0] for x in sorted(temp_symbols, key=lambda x: x[1], reverse=True)[:SCAN_LIMIT]]
            
            sem = asyncio.Semaphore(10); results = []
            for i in range(0, len(valid_symbols_data), 10):
                tasks = [process_single_coin(s, btc_trend, sem) for s in valid_symbols_data[i:i+10]]
                if tasks: results.extend(await asyncio.gather(*tasks, return_exceptions=True))
                    
            valid_results = [r for r in results if isinstance(r, tuple) and len(r) == 2 and r[1] is not None]
            logging.info(f"🔎 [РАДАР] Всего: {len(tickers)} -> ВХОДЫ (Одобренные Оракулом): {len(valid_results)}")

            for sym, signal in valid_results:
                await execute_trade(sym, signal)
            gc.collect(); await asyncio.sleep(60) 
        except Exception as e: logging.error(f"Radar Error: {e}"); await asyncio.sleep(60)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Kraken Prop Async Bot Active")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler); server.serve_forever()

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА (Risk: 0.5%, Max Pos: 3, Oracle GRID)...")
    await send_tg_msg("🟢 <b>KRAKEN ASYNC БОТ</b> запущен (Risk: 0.5%, Max Pos: 3, Оракул: Вкл)!")
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(monitor_positions_task()) 
    await radar_task()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
