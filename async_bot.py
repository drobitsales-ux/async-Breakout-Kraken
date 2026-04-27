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
        await send_tg_msg(f"💥 <b>ВЫСТРЕЛ [SMC Async KRAKEN]: {sym.split(':')[0]}</b>\nНаправление: <b>#{direction}</b>\nЦена: {current_price}\nОбъем: {qty}\nSL: {sl_price}\nTP: {tp_price}")
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

from http.server import BaseHTTPRequestHandler, HTTPServer
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Kraken Async Bot Active")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler); server.serve_forever()

async def main():
    init_db(); load_positions()
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА (Risk: 0.5%, Max Pos: 3)...")
    await send_tg_msg("🟢 <b>KRAKEN ASYNC БОТ</b> успешно запущен (Risk: 0.5%, Max Pos: 3)!")
    from threading import Thread
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(monitor_positions_task()) 
    while True: await asyncio.sleep(3600)

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
