import telebot
import numpy as np
import time
import os
import logging
import sqlite3
import json
import ccxt
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

DB_PATH = 'bot_prop.db'  
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = -1003955653290  
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

RISK_PER_TRADE = 0.005 # ПРОП 0.5%
MAX_POSITIONS = 3           
LEVERAGE = 5                
COOLDOWN_CACHE = {}

active_positions = []
bot = telebot.TeleBot(TOKEN)
exchange = ccxt.krakenfutures({'apiKey': KRAKEN_API_KEY, 'secret': KRAKEN_SECRET, 'enableRateLimit': True})

def get_db_conn(): return sqlite3.connect(DB_PATH, check_same_thread=False)
def init_db(): conn = get_db_conn(); conn.cursor().execute('''CREATE TABLE IF NOT EXISTS active_positions (id INTEGER PRIMARY KEY, data TEXT)'''); conn.commit(); conn.close()
def save_positions(): conn = get_db_conn(); conn.cursor().execute("INSERT OR REPLACE INTO active_positions (id, data) VALUES (1, ?)", (json.dumps(active_positions),)); conn.commit(); conn.close()
def load_positions(): 
    global active_positions; conn = get_db_conn(); c = conn.cursor()
    c.execute("SELECT data FROM active_positions WHERE id = 1"); row = c.fetchone()
    if row: active_positions = json.loads(row[0])
    conn.close()

def execute_trade(sym, signal_data):
    global active_positions, COOLDOWN_CACHE
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    direction, current_price, sl_price, tp1_price = signal_data['mode'], signal_data['price'], signal_data['sl_price'], signal_data['tp1_price']
    
    try:
        bal = exchange.fetch_balance(); free_usd = float(bal.get('USDT', {}).get('free', 0)) or float(bal.get('USD', {}).get('free', 0))
        if free_usd <= 0: return

        qty_coins = (free_usd * RISK_PER_TRADE) / abs(current_price - sl_price)
        allowed_notional = free_usd * LEVERAGE * 0.90 
        if (qty_coins * current_price) > allowed_notional: qty_coins = allowed_notional / current_price
        
        qty = float(exchange.amount_to_precision(sym, qty_coins))
        if qty <= 0: return
        
        try: exchange.set_leverage(LEVERAGE, sym)
        except: pass 
        
        exchange.create_market_order(sym, 'buy' if direction == 'Long' else 'sell', qty)
        sl_ord = exchange.create_order(sym, 'stop_market', 'sell' if direction == 'Long' else 'buy', qty, params={'stopLossPrice': sl_price})
        
        active_positions.append({'symbol': sym, 'direction': direction, 'entry_price': current_price, 'initial_qty': qty, 'sl_price': sl_price, 'current_sl': sl_price, 'tp1': tp1_price, 'tp1_hit': False, 'sl_order_id': sl_ord['id'], 'open_time': datetime.now(timezone.utc).isoformat(), 'atr': signal_data['atr']})
        save_positions()
        bot.send_message(GROUP_CHAT_ID, f"💥 <b>ВЫСТРЕЛ [KRAKEN RSI]: {sym.split(':')[0]}</b>\nНаправление: <b>#{direction.upper()}</b>\nЦена: {current_price}\nОбъем: {qty}\nSL: {sl_price}\nTP1: {tp1_price}", parse_mode="HTML")
    except Exception as e: logging.error(f"Trade error {sym}: {e}")

def monitor_positions_job():
    global active_positions, COOLDOWN_CACHE
    try:
        if not active_positions: return
        positions_raw = exchange.fetch_positions(); tickers = exchange.fetch_tickers([p['symbol'] for p in active_positions])
        updated = []
        
        for pos in active_positions:
            sym, clean_name, is_long = pos['symbol'], pos['symbol'].split(':')[0], pos['direction'] == 'Long'
            curr = next((r for r in positions_raw if r['symbol'] == sym and float(r.get('contracts', 0)) > 0), None)
            ticker = tickers.get(sym, {}).get('last', pos['entry_price'])
            
            if not curr:
                exit_price = pos.get('current_sl', pos['sl_price'])
                pnl = (exit_price - pos['entry_price']) * pos['initial_qty'] if is_long else (pos['entry_price'] - exit_price) * pos['initial_qty']
                if pnl > 0 and pos.get('tp1_hit'): bot.send_message(GROUP_CHAT_ID, f"🛡 <b>{clean_name} выбита по Трейлингу!</b>\nPNL: {pnl:+.2f} USD", parse_mode="HTML")
                else: COOLDOWN_CACHE[sym] = time.time() + 14400; bot.send_message(GROUP_CHAT_ID, f"🛑 <b>{clean_name} выбита по SL.</b>\nPNL: {pnl:.2f} USD", parse_mode="HTML")
                continue

            if not pos.get('tp1_hit'):
                try: ohlcv = exchange.fetch_ohlcv(sym, timeframe='1m', limit=2); high_p = max([float(c[2]) for c in ohlcv]); low_p = min([float(c[3]) for c in ohlcv])
                except: high_p = low_p = ticker
                
                if (is_long and high_p >= pos['tp1']) or (not is_long and low_p <= pos['tp1']):
                    try:
                        close_qty = float(exchange.amount_to_precision(sym, pos['initial_qty'] * 0.50))
                        if close_qty > 0: 
                            exchange.create_market_order(sym, 'sell' if is_long else 'buy', close_qty)
                            pos['initial_qty'] = float(exchange.amount_to_precision(sym, pos['initial_qty'] - close_qty))
                        if pos.get('sl_order_id'): exchange.cancel_order(pos['sl_order_id'], sym)
                        
                        be_p = float(exchange.price_to_precision(sym, pos['entry_price'] * (1.002 if is_long else 0.998)))
                        new_sl = exchange.create_order(sym, 'stop_market', 'sell' if is_long else 'buy', pos['initial_qty'], params={'stopLossPrice': be_p})
                        pos.update({'tp1_hit': True, 'sl_order_id': new_sl['id'], 'current_sl': be_p, 'trail_trigger': pos['tp1'] + (pos['atr'] * 0.3 * (1 if is_long else -1))})
                        bot.send_message(GROUP_CHAT_ID, f"💰 <b>{clean_name} TP1 взят!</b> (50% закрыто). Трейлинг запущен.", parse_mode="HTML")
                    except: pass
            
            updated.append(pos)
        active_positions = updated; save_positions()
    except Exception: pass

if __name__ == '__main__':
    init_db(); load_positions()
    logging.info("🚀 Запуск KRAKEN RSI БОТА (Fixed Qty & Logic)...")
    try: bot.send_message(GROUP_CHAT_ID, "🟢 <b>KRAKEN RSI БОТ</b> успешно запущен!", parse_mode="HTML")
    except: pass
    scheduler = BackgroundScheduler(); scheduler.add_job(monitor_positions_job, 'interval', seconds=15); scheduler.start()
    while True: time.sleep(1)
