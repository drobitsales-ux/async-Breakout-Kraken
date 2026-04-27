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

# === НАСТРОЙКИ KRAKEN (REAL TRADE - TEST 80$) ===
DB_PATH = 'bot_prop.db' 
TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_CHAT_ID = -1003955653290
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_SECRET = os.getenv('KRAKEN_SECRET')

# ВНИМАНИЕ: 5% ТОЛЬКО ДЛЯ ТЕСТОВОГО СЧЕТА 80$! ДЛЯ ПРОПА ВЕРНИ НА 0.005
RISK_PER_TRADE = 0.05       
MAX_POSITIONS = 3           
LEVERAGE = 5                
MIN_SL_PCT = 1.0            
MAX_SL_PCT = 5.0            

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

# === ПОДКЛЮЧЕНИЕ К РЕАЛЬНОМУ KRAKEN (Sandbox отключен) ===
exchange = ccxt_async.krakenfutures({
    'apiKey': KRAKEN_API_KEY, 
    'secret': KRAKEN_SECRET,
    'enableRateLimit': True
})
# exchange.set_sandbox_mode(True) <-- УДАЛЕНО ДЛЯ РЕАЛЬНОГО СЧЕТА

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

async def execute_trade(sym, signal_data):
    global active_positions, COOLDOWN_CACHE
    if len(active_positions) >= MAX_POSITIONS or any(p['symbol'] == sym for p in active_positions): return
    if sym in COOLDOWN_CACHE and time.time() < COOLDOWN_CACHE[sym]: return
        
    direction, current_price, sl_price, tp_price = signal_data['mode'], signal_data['price'], signal_data['sl_price'], signal_data['tp_price']
    
    try:
        bal = await exchange.fetch_balance()
        free_usd = float(bal.get('USDT', {}).get('free', 0)) or float(bal.get('USD', {}).get('free', 0))
        if free_usd <= 0: return

        actual_sl_dist = abs(current_price - sl_price)
        if actual_sl_dist <= 0: return
        
        sl_pct = (actual_sl_dist / current_price) * 100
        if sl_pct > MAX_SL_PCT:
            COOLDOWN_CACHE[sym] = time.time() + 3600
            return
            
        risk_amount = free_usd * RISK_PER_TRADE
        market = exchange.markets[sym]
        contract_size = float(market.get('contractSize', 1.0))
        
        qty_coins = risk_amount / actual_sl_dist
        qty = float(exchange.amount_to_precision(sym, qty_coins / contract_size if contract_size > 0 else qty_coins))
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
        
        msg = (
            f"💥 <b>ВЫСТРЕЛ [SMC Async REAL]: {sym.split(':')[0]}</b>\n"
            f"Направление: <b>#{direction}</b>\n\n"
            f"Цена: {current_price}\n"
            f"Объем (контр.): {qty}\n"
            f"SL: {sl_price} ({sl_pct:.2f}%)\n"
            f"TP: {tp_price}\n\n"
            f"📊 <b>Аналитика сетапа:</b>\n"
            f"🔸 Размер FVG: {signal_data['fvg_size']:.2f}%\n"
            f"🔸 Отклон. от EMA200: {signal_data['ema_dist']:.2f}%\n"
            f"🔸 BTC Тренд: {signal_data['btc_trend']}\n"
            f"🔸 Объем 24ч: {signal_data['vol_24h']/1000000:.1f}M$"
        )
        await send_tg_msg(msg)
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
                
                market = exchange.markets.get(sym, {})
                contract_size = float(market.get('contractSize', 1.0))
                
                if not curr:
                    pnl = (ticker - pos['entry_price']) * pos['initial_qty'] * contract_size if is_long else (pos['entry_price'] - ticker) * pos['initial_qty'] * contract_size
                    daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                    daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + pnl
                    
                    if pnl > 0: 
                        daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + pnl
                        await send_tg_msg(f"✅ <b>{clean_name} закрыта в плюс!</b>\nPNL: {pnl:+.2f} USD")
                    else: 
                        daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(pnl)
                        COOLDOWN_CACHE[sym] = time.time() + 14400
                        await send_tg_msg(f"🛑 <b>{clean_name} выбита по SL.</b>\nPNL: {pnl:.2f} USD\n❄️ Монета заморожена.")
                    continue

                if 'open_time' not in pos: pos['open_time'] = datetime.now(timezone.utc).isoformat()
                hours_passed = (datetime.now(timezone.utc) - datetime.fromisoformat(pos['open_time'])).total_seconds() / 3600
                pnl = (ticker - pos['entry_price']) * float(curr['contracts']) * contract_size if is_long else (pos['entry_price'] - ticker) * float(curr['contracts']) * contract_size

                if hours_passed >= 3.0 or (hours_passed >= 1.5 and pnl > 0):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']))
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + pnl
                        if pnl > 0: 
                            daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                            daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + pnl
                        else: 
                            daily_stats['gross_loss'] = daily_stats.get('gross_loss', 0.0) + abs(pnl)
                            COOLDOWN_CACHE[sym] = time.time() + 14400
                        await send_tg_msg(f"{'✅' if pnl > 0 else '🛑'} <b>{clean_name} закрыта по ТАЙМАУТУ!</b>\nPNL: {pnl:+.2f} USD")
                        continue
                    except: pass

                if (is_long and ticker >= pos['tp1']) or (not is_long and ticker <= pos['tp1']):
                    try:
                        await exchange.create_market_order(sym, 'sell' if is_long else 'buy', float(curr['contracts']))
                        if pos.get('sl_order_id'): await exchange.cancel_order(pos['sl_order_id'], sym)
                        daily_stats['trades'] = daily_stats.get('trades', 0) + 1
                        daily_stats['wins'] = daily_stats.get('wins', 0) + 1
                        daily_stats['pnl'] = daily_stats.get('pnl', 0.0) + pnl
                        daily_stats['gross_profit'] = daily_stats.get('gross_profit', 0.0) + pnl
                        await send_tg_msg(f"💰 <b>{clean_name} TP взят!</b>\nPNL: {pnl:+.2f} USD")
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
            ohlcv = await exchange.fetch_ohlcv(sym, timeframe=SMC_TIMEFRAME, limit=200)
            if not ohlcv or len(ohlcv) < 50: return sym, None
            
            o = np.array([x[1] for x in ohlcv], dtype=float)
            h = np.array([x[2] for x in ohlcv], dtype=float)
            l = np.array([x[3] for x in ohlcv], dtype=float)
            c = np.array([x[4] for x in ohlcv], dtype=float)
            
            trend, bos_choch = analyze_structure(h, l, c)
            fvgs = analyze_fvg(o, h, l, c)
            current_price = c[-1]
            ema200 = calculate_ema(c, 200)
            
            active_fvg = None
            for fvg in reversed(fvgs):
                if (fvg['type'] == 'Bullish' and current_price > fvg['top']) or (fvg['type'] == 'Bearish' and current_price < fvg['bottom']):
                    active_fvg = fvg; break
            
            if not active_fvg or not bos_choch: return sym, None
            
            mode = 'Long' if bos_choch == 'CHoCH_Bullish' and active_fvg['type'] == 'Bullish' else 'Short' if bos_choch == 'CHoCH_Bearish' and active_fvg['type'] == 'Bearish' else None
            if not mode: return sym, None
            
            if mode == 'Long' and btc_trend != 'Long': return sym, None
            if mode == 'Short' and btc_trend != 'Short': return sym, None

            if mode == 'Long':
                sl_price = min(l[active_fvg['index']:]) * 0.998
                if (current_price - sl_price) / current_price * 100 < MIN_SL_PCT:
                    sl_price = current_price * (1 - MIN_SL_PCT/100)
                tp_price = current_price + (current_price - sl_price) * 1.5
            else:
                sl_price = max(h[active_fvg['index']:]) * 1.002
                if (sl_price - current_price) / current_price * 100 < MIN_SL_PCT:
                    sl_price = current_price * (1 + MIN_SL_PCT/100)
                tp_price = current_price - (sl_price - current_price) * 1.5

            fvg_size = abs(active_fvg['top'] - active_fvg['bottom']) / current_price * 100
            ema_dist = abs(current_price - ema200) / current_price * 100

            signal = {
                'mode': mode, 'price': current_price, 'sl_price': sl_price, 'tp_price': tp_price, 
                'vol_24h': q_vol, 'fvg_size': fvg_size, 'ema_dist': ema_dist, 'btc_trend': btc_trend
            }
            return sym, signal
        except: return sym, None

async def radar_task():
    global NOTIFIED_SYMBOLS
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
            
            logging.info(f"🔎 [РАДАР] Всего: {stats['total']} -> Ждем Сетап: {stats['waiting']} -> ВХОДЫ: {stats['passed']}")

            for res in valid_results:
                sym, signal = res
                if sym not in NOTIFIED_SYMBOLS:
                    NOTIFIED_SYMBOLS.add(sym)
                    await execute_trade(sym, signal)

            gc.collect(); await asyncio.sleep(60) 
        except Exception as e: 
            logging.error(f"Radar Loop Error: {e}"); await asyncio.sleep(60)

async def print_stats_hourly():
    global daily_stats, REPORTED_TODAY
    while True:
        try:
            now = datetime.now(timezone.utc)
            trades = daily_stats.get('trades', 0)
            winrate = (daily_stats['wins'] / trades * 100) if trades > 0 else 0
            pnl = daily_stats.get('pnl', 0.0)
            
            active = ", ".join([f"{p['symbol'].split(':')[0]}" for p in active_positions]) or "Нет"
            logging.info(f"📊 [СТАТИСТИКА] Сделок: {trades} | Винрейт: {winrate:.1f}% | PNL: {pnl:+.2f}$ | Открыто: {active}")
            
            if now.hour == 20 and not REPORTED_TODAY:
                bal = await exchange.fetch_balance()
                current_balance = float(bal.get('USDT', {}).get('total', 0)) or float(bal.get('USD', {}).get('total', 0))
                
                start_bal = daily_stats.get('start_balance', 0.0)
                pct_change = ((current_balance - start_bal) / start_bal * 100) if start_bal > 0 else 0.0
                
                # Расширенная аналитика
                g_profit = daily_stats.get('gross_profit', 0.0)
                g_loss = daily_stats.get('gross_loss', 0.0)
                avg_win = (g_profit / daily_stats['wins']) if daily_stats['wins'] > 0 else 0.0
                losses_count = trades - daily_stats['wins']
                avg_loss = (g_loss / losses_count) if losses_count > 0 else 0.0
                profit_factor = (g_profit / g_loss) if g_loss > 0 else float('inf')
                
                report = (
                    f"🗓 <b>ИТОГИ ДНЯ (KRAKEN Async Bot REAL):</b> {now.strftime('%d.%m.%Y')}\n\n"
                    f"📉 Закрыто сделок: {trades}\n"
                    f"🎯 Винрейт: {winrate:.1f}%\n"
                    f"💵 Net PNL: {pnl:+.2f} USD\n\n"
                    f"📊 <b>Детальная Аналитика:</b>\n"
                    f"🔸 Средняя прибыль (Сделка): +{avg_win:.2f} $\n"
                    f"🔸 Средний убыток (Сделка): -{avg_loss:.2f} $\n"
                    f"🔸 Profit Factor: {profit_factor:.2f}\n\n"
                    f"🏦 <b>Баланс аккаунта:</b> {current_balance:.2f} USD\n"
                    f"🚀 <b>Изменение за день:</b> {pct_change:+.2f}%\n\n"
                    f"<i>*Сделок в работе: {len(active_positions)}</i>"
                )
                await send_tg_msg(report)
                
                daily_stats = {'pnl': 0.0, 'trades': 0, 'wins': 0, 'prev_winrate': winrate, 'start_balance': current_balance, 'gross_profit': 0.0, 'gross_loss': 0.0}
                await asyncio.to_thread(save_positions)
                REPORTED_TODAY = True
            elif now.hour != 20:
                REPORTED_TODAY = False
                
        except: pass
        await asyncio.sleep(3600)

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Kraken Real Async Bot Active")
    def log_message(self, format, *args): return 

def run_server():
    server = HTTPServer(('0.0.0.0', int(os.environ.get('PORT', 10000))), HealthCheckHandler)
    server.serve_forever()

async def main():
    init_db(); load_positions()
    
    if daily_stats.get('start_balance', 0.0) == 0.0:
        try:
            bal = await exchange.fetch_balance()
            curr = float(bal.get('USDT', {}).get('total', 0)) or float(bal.get('USD', {}).get('total', 0))
            daily_stats['start_balance'] = curr
            await asyncio.to_thread(save_positions)
        except: pass
        
    logging.info("🚀 Запуск KRAKEN ASYNC БОТА (РЕАЛЬНЫЙ СЧЕТ, Риск 5% (Для 80$), Profit Analytics)...")
    await send_tg_msg("🟢 <b>KRAKEN ASYNC БОТ</b> успешно запущен на РЕАЛЬНОМ счете!")
    
    Thread(target=run_server, daemon=True).start()
    asyncio.create_task(monitor_positions_task()) 
    asyncio.create_task(print_stats_hourly())
    await radar_task()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
