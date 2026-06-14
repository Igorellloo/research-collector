#!/usr/bin/env python3
"""
BYBIT RESEARCH COLLECTOR
Цель: накопление исторической базы для анализа закономерностей
      перед крупными движениями рынка (10%+).

Что делает:
  - Каждую минуту сохраняет market_snapshot для всех монет
  - Обнаруживает движения ≥ SQUEEZE_THRESHOLD_PCT за 90 минут
  - Для каждого события сохраняет признаки за 5/15/30/60/120 минут до
  - Через 61 минуту после события сохраняет результаты

Что НЕ делает:
  - Не генерирует сигналы
  - Не считает score
  - Не работает с ликвидациями
  - Не отправляет алерты
  - Не определяет направление для торговли

Архитектура БД:
  market_snapshots → squeeze_events → squeeze_features → squeeze_outcomes
"""

import asyncio
import json
import time
import aiohttp
import websockets
from datetime import datetime
from collections import defaultdict, deque
import aiosqlite

# ── Настройки ────────────────────────────────────────────────────
DB_PATH                 = "research.db"
SYMBOLS_PER_WS          = 10
SNAPSHOT_INTERVAL_SEC   = 60       # снимок рынка раз в минуту
SQUEEZE_CHECK_INTERVAL  = 60       # проверка сквизов раз в минуту
SQUEEZE_THRESHOLD_PCT   = 10.0     # порог движения для фиксации события
SQUEEZE_WINDOW_SEC      = 5400     # окно поиска экстремума — 90 минут
SQUEEZE_COOLDOWN_SEC    = 1800     # cooldown на монету — 30 минут
FUNDING_UPDATE_SEC      = 300      # обновление funding раз в 5 минут
BTC_CONTEXT_UPDATE_SEC  = 300      # обновление BTC контекста раз в 5 минут
HISTORY_DEPTH_SEC       = 7200     # глубина хранения истории в памяти — 2 часа
# ─────────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"
BYBIT_WS_SPOT   = "wss://stream.bybit.com/v5/public/spot"
BYBIT_REST      = "https://api.bybit.com"

# ── In-memory хранилища ──────────────────────────────────────────
price_cache            = {}                                         # sym → last price
price_history          = defaultdict(lambda: deque(maxlen=20000))  # (ts, price)
oi_cache               = {}                                         # sym → last OI (contracts)
oi_history             = defaultdict(lambda: deque(maxlen=20000))  # (ts, oi_contracts)
cvd_futures_history    = defaultdict(lambda: deque(maxlen=20000))  # (ts, delta_usd)
cvd_spot_history       = defaultdict(lambda: deque(maxlen=20000))  # (ts, delta_usd)
volume_history         = defaultdict(lambda: deque(maxlen=20000))  # (ts, notional_usd)

# Кеши для REST данных
_funding_cache         = {}   # sym → funding_rate (float)
_funding_last_update   = {}   # sym → timestamp
_btc_context_cache     = (None, None)   # (btc_change_1h, btc_change_4h)
_btc_context_last_upd  = 0.0

session = None
db      = None


# ════════════════════════════════════════════════════════════════
# БД: инициализация
# ════════════════════════════════════════════════════════════════
async def init_db():
    await db.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT    NOT NULL,
            symbol           TEXT    NOT NULL,

            -- Цена и объём
            price            REAL,
            volume_1m        REAL,
            volume_5m        REAL,
            volume_15m       REAL,

            -- Open Interest
            oi_usd           REAL,
            oi_delta_1m      REAL,
            oi_delta_5m      REAL,
            oi_delta_15m     REAL,
            oi_delta_60m     REAL,
            oi_acceleration  REAL,   -- (oi_delta_1m - oi_delta_5m/5) / (oi_delta_5m/5)

            -- Futures CVD
            futures_cvd_1m   REAL,
            futures_cvd_5m   REAL,
            futures_cvd_15m  REAL,
            futures_cvd_60m  REAL,

            -- Spot CVD
            spot_cvd_1m      REAL,
            spot_cvd_5m      REAL,
            spot_cvd_15m     REAL,
            spot_cvd_60m     REAL,

            -- Дополнительные признаки
            cvd_efficiency   REAL,   -- abs(futures_cvd_5m) / volume_5m
            range_pct_30m    REAL,   -- (max - min) за 30м / price

            -- Контекст рынка
            funding_rate     REAL,
            btc_change_1h    REAL,
            btc_change_4h    REAL
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_snap_sym_ts
        ON market_snapshots (symbol, timestamp)
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS squeeze_events (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol           TEXT    NOT NULL,
            direction        TEXT    NOT NULL,   -- LONG / SHORT

            -- Три ценовые точки
            start_price      REAL,
            peak_price       REAL,
            peak_time        TEXT,
            end_price        REAL,

            -- Движения
            move_pct         REAL,       -- start → peak
            pullback_pct     REAL,       -- peak → end (откат)

            event_time       TEXT NOT NULL,  -- = peak_time
            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_squeeze_sym_time
        ON squeeze_events (symbol, event_time)
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS squeeze_features (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id         INTEGER REFERENCES squeeze_events(id),
            symbol           TEXT    NOT NULL,
            event_time       TEXT    NOT NULL,
            minutes_before   INTEGER NOT NULL,   -- 5 / 15 / 30 / 60 / 120
            snapshot_time    TEXT,

            -- Базовые метрики
            price            REAL,
            oi_usd           REAL,
            oi_delta_5m      REAL,
            oi_delta_15m     REAL,
            oi_acceleration  REAL,

            -- CVD раздельно
            futures_cvd_5m   REAL,
            futures_cvd_15m  REAL,
            spot_cvd_5m      REAL,
            spot_cvd_15m     REAL,

            -- Объём
            volume_5m        REAL,
            volume_15m       REAL,

            -- Дополнительные признаки
            cvd_efficiency   REAL,
            range_pct_30m    REAL,

            -- Контекст
            funding_rate     REAL,
            btc_change_1h    REAL,

            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_features_event_min
        ON squeeze_features (event_id, minutes_before)
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS squeeze_outcomes (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id             INTEGER REFERENCES squeeze_events(id),
            symbol               TEXT    NOT NULL,
            event_time           TEXT    NOT NULL,
            end_price            REAL,

            -- Цены после
            price_after_15m      REAL,
            price_after_30m      REAL,
            price_after_60m      REAL,

            -- Движения от end_price
            move_after_15m_pct   REAL,
            move_after_30m_pct   REAL,
            move_after_60m_pct   REAL,

            fetched_at           TEXT DEFAULT (datetime('now'))
        )
    """)

    await db.commit()
    print(f"{GREEN}БД {DB_PATH}: все таблицы готовы{RESET}")
    print(f"{GREEN}  market_snapshots / squeeze_events / squeeze_features / squeeze_outcomes{RESET}")


# ════════════════════════════════════════════════════════════════
# Хелперы: агрегация истории
# ════════════════════════════════════════════════════════════════
def _sum_window(dq, now: float, seconds: int) -> float:
    """Сумма значений в deque за последние seconds секунд."""
    return sum(v for t, v in dq if now - t <= seconds)


def _delta_pct(dq, now: float, seconds: int):
    """% изменение между значением seconds секунд назад и текущим."""
    if not dq:
        return None
    cutoff  = now - seconds
    old_val = None
    for t, v in dq:
        if t <= cutoff:
            old_val = v
        else:
            break
    current = dq[-1][1] if dq else None
    if old_val is None or current is None or old_val == 0:
        return None
    return (current - old_val) / abs(old_val) * 100


def _range_pct(sym: str, now: float, seconds: int, price: float):
    """(max - min) за последние seconds секунд / price * 100."""
    if price <= 0:
        return None
    vals = [p for t, p in price_history.get(sym, []) if now - t <= seconds and p > 0]
    if len(vals) < 2:
        return None
    return (max(vals) - min(vals)) / price * 100


def _oi_acceleration(sym: str, now: float):
    """
    Ускорение OI: насколько темп изменения OI за 1м отличается
    от среднего темпа за 5м.
    Формула: (oi_delta_1m - oi_delta_5m/5) / max(abs(oi_delta_5m/5), 0.0001)
    """
    dq = oi_history.get(sym)
    if not dq:
        return None
    d1m = _delta_pct(dq, now, 60)
    d5m = _delta_pct(dq, now, 300)
    if d1m is None or d5m is None:
        return None
    avg_rate_5m = d5m / 5.0
    denom       = abs(avg_rate_5m) if abs(avg_rate_5m) > 1e-6 else 1e-6
    return (d1m - avg_rate_5m) / denom


def _cvd_efficiency(sym: str, now: float, seconds: int):
    """
    abs(futures_cvd) / volume за окно seconds.
    Показывает насколько агрессивно CVD двигается относительно объёма.
    0 = хаотично, 1 = полностью направленно.
    """
    fh  = cvd_futures_history.get(sym, deque())
    vh  = volume_history.get(sym, deque())
    cvd = abs(_sum_window(fh, now, seconds))
    vol = _sum_window(vh, now, seconds)
    if vol <= 0:
        return None
    return min(cvd / vol, 1.0)   # не больше 1.0


# ════════════════════════════════════════════════════════════════
# REST: funding и BTC контекст
# ════════════════════════════════════════════════════════════════
async def _get_funding(sym: str, now: float):
    """Возвращает funding rate из кеша, обновляя раз в FUNDING_UPDATE_SEC."""
    if now - _funding_last_update.get(sym, 0) > FUNDING_UPDATE_SEC:
        url = f"{BYBIT_REST}/v5/market/tickers?category=linear&symbol={sym}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                if isinstance(data, dict) and data.get('retCode') == 0:
                    items = data.get('result', {}).get('list', [])
                    if items:
                        val = items[0].get('fundingRate')
                        _funding_cache[sym]       = float(val) if val is not None else None
        except Exception:
            pass
        _funding_last_update[sym] = now
    return _funding_cache.get(sym)


async def _get_btc_context(now: float):
    """Возвращает (btc_change_1h, btc_change_4h) из кеша."""
    global _btc_context_cache, _btc_context_last_upd
    if now - _btc_context_last_upd > BTC_CONTEXT_UPDATE_SEC:
        results = {}
        for interval, label in [('60', '1h'), ('240', '4h')]:
            url = (f"{BYBIT_REST}/v5/market/kline"
                   f"?category=linear&symbol=BTCUSDT&interval={interval}&limit=2")
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    if isinstance(data, dict) and data.get('retCode') == 0:
                        candles = data.get('result', {}).get('list', [])
                        if len(candles) >= 2:
                            c_now  = float(candles[0][4])
                            c_prev = float(candles[1][4])
                            results[label] = (c_now - c_prev) / c_prev * 100 if c_prev else None
                        else:
                            results[label] = None
                    else:
                        results[label] = None
            except Exception:
                results[label] = None
        _btc_context_cache    = (results.get('1h'), results.get('4h'))
        _btc_context_last_upd = now
    return _btc_context_cache


# ════════════════════════════════════════════════════════════════
# REST: kline для squeeze_outcomes
# ════════════════════════════════════════════════════════════════
async def _fetch_kline_after(sym: str, entry_ts: float, end_price: float):
    """Запрашивает 1m-свечи за 61 минуту после события."""
    now_ts = int(time.time())
    url = (f"{BYBIT_REST}/v5/market/kline"
           f"?category=linear&symbol={sym}"
           f"&interval=1&start={int(entry_ts)}000&end={now_ts}000&limit=65")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if not isinstance(data, dict) or data.get('retCode') != 0:
                return None
            candles = data.get('result', {}).get('list', [])
            if not candles:
                return None
            candles = sorted(candles, key=lambda c: int(c[0]))
            closes  = [float(c[4]) for c in candles]

            def _pa(minutes):
                idx = min(minutes, len(closes)) - 1
                return closes[idx] if idx >= 0 else None

            def _mp(minutes):
                p = _pa(minutes)
                return (p - end_price) / end_price * 100 if p and end_price else None

            return {
                'price_after_15m':    _pa(15),
                'price_after_30m':    _pa(30),
                'price_after_60m':    _pa(60),
                'move_after_15m_pct': _mp(15),
                'move_after_30m_pct': _mp(30),
                'move_after_60m_pct': _mp(60),
            }
    except Exception as e:
        print(f"{RED}[outcomes] fetch_kline {sym}: {e}{RESET}")
        return None


# ════════════════════════════════════════════════════════════════
# REST: список символов
# ════════════════════════════════════════════════════════════════
async def fetch_active_symbols():
    url = f"{BYBIT_REST}/v5/market/instruments-info?category=linear"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
            if not isinstance(data, dict) or data.get('retCode') != 0:
                return []
            return [
                i['symbol'] for i in data.get('result', {}).get('list', [])
                if i.get('status') == 'Trading' and i.get('symbol', '').endswith('USDT')
            ]
    except Exception as e:
        print(f"{RED}fetch_active_symbols: {e}{RESET}")
        return []


async def fetch_active_spot_symbols():
    url = f"{BYBIT_REST}/v5/market/instruments-info?category=spot"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
            if not isinstance(data, dict) or data.get('retCode') != 0:
                return set()
            return {
                i['symbol'] for i in data.get('result', {}).get('list', [])
                if i.get('status') == 'Trading' and i.get('symbol', '').endswith('USDT')
            }
    except Exception as e:
        print(f"{RED}fetch_active_spot_symbols: {e}{RESET}")
        return set()


# ════════════════════════════════════════════════════════════════
# WebSocket: фьючерсы (Futures CVD + OI)
# ════════════════════════════════════════════════════════════════
async def ws_linear_stream(symbols: list):
    while True:
        try:
            async with websockets.connect(
                BYBIT_WS_LINEAR,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
            ) as ws:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Linear WS подключён ({len(symbols)} символов)")

                for i in range(0, len(symbols), 5):
                    batch = symbols[i:i + 5]
                    args  = []
                    for s in batch:
                        args.append(f"publicTrade.{s}")
                        args.append(f"tickers.{s}")
                    if args:
                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.05)

                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if 'topic' not in data or 'data' not in data:
                        continue

                    topic   = data['topic']
                    payload = data['data']
                    now_ts  = time.time()

                    # ── publicTrade → Futures CVD + объём + цена ──
                    if topic.startswith('publicTrade.'):
                        sym   = topic.split('.', 1)[1]
                        items = payload if isinstance(payload, list) else [payload]
                        for item in items:
                            side = item.get('S') or item.get('side')
                            try:
                                price = float(item.get('p') or item.get('price'))
                                size  = float(item.get('v') or item.get('size'))
                            except (TypeError, ValueError):
                                continue
                            if price <= 0 or size <= 0:
                                continue
                            notional = price * size
                            delta    = notional if side == 'Buy' else -notional
                            cvd_futures_history[sym].append((now_ts, delta))
                            volume_history[sym].append((now_ts, notional))
                            price_cache[sym] = price
                            price_history[sym].append((now_ts, price))

                    # ── tickers → OI ──────────────────────────────
                    elif topic.startswith('tickers.'):
                        sym   = topic.split('.', 1)[1]
                        items = payload if isinstance(payload, list) else [payload]
                        for item in items:
                            try:
                                oi    = float(item.get('openInterest', 0) or 0)
                                price = float(item.get('lastPrice',    0) or 0)
                            except (TypeError, ValueError):
                                continue
                            if oi > 0:
                                oi_cache[sym] = oi
                                oi_history[sym].append((now_ts, oi))
                            if price > 0:
                                price_cache[sym] = price
                                price_history[sym].append((now_ts, price))

        except Exception as e:
            print(f"{RED}Linear WS разорван ({len(symbols)} символов): {e}, переподключение...{RESET}")
            await asyncio.sleep(3)


# ════════════════════════════════════════════════════════════════
# WebSocket: спот (Spot CVD)
# ════════════════════════════════════════════════════════════════
async def ws_spot_stream(symbols: list, valid_spot: set):
    while True:
        try:
            subscribed = [s for s in symbols if s in valid_spot]
            if not subscribed:
                await asyncio.sleep(60)
                continue

            async with websockets.connect(
                BYBIT_WS_SPOT,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
            ) as ws:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Spot WS подключён ({len(subscribed)}/{len(symbols)} символов)")

                for i in range(0, len(subscribed), 5):
                    batch = subscribed[i:i + 5]
                    args  = [f"publicTrade.{s}" for s in batch]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.05)

                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if 'topic' not in data or 'data' not in data:
                        continue
                    topic = data['topic']
                    if not topic.startswith('publicTrade.'):
                        continue
                    sym     = topic.split('.', 1)[1]
                    payload = data['data']
                    items   = payload if isinstance(payload, list) else [payload]
                    for item in items:
                        side = item.get('S') or item.get('side')
                        try:
                            price = float(item.get('p') or item.get('price'))
                            size  = float(item.get('v') or item.get('size'))
                        except (TypeError, ValueError):
                            continue
                        if price <= 0 or size <= 0:
                            continue
                        notional = price * size
                        delta    = notional if side == 'Buy' else -notional
                        cvd_spot_history[sym].append((time.time(), delta))

        except Exception as e:
            print(f"{RED}Spot WS разорван ({len(symbols)} символов): {e}, переподключение...{RESET}")
            await asyncio.sleep(3)


# ════════════════════════════════════════════════════════════════
# Задача 1: snapshot_collector
# Каждую минуту делает снимок рынка по всем монетам
# ════════════════════════════════════════════════════════════════
async def snapshot_collector():
    print(f"{CYAN}[snapshot] Запущен (интервал {SNAPSHOT_INTERVAL_SEC}с){RESET}")

    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL_SEC)
        now = time.time()
        ts  = datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')

        btc_1h, btc_4h = await _get_btc_context(now)

        symbols = [s for s, p in price_cache.items() if p > 0]
        if not symbols:
            continue

        rows = []
        for sym in symbols:
            price = price_cache.get(sym, 0)
            if price <= 0:
                continue

            # OI
            oi_raw = oi_cache.get(sym, 0)
            oi_usd = oi_raw * price if oi_raw > 0 else None
            oi_dq  = oi_history.get(sym, deque())
            oi_d1  = _delta_pct(oi_dq, now, 60)
            oi_d5  = _delta_pct(oi_dq, now, 300)
            oi_d15 = _delta_pct(oi_dq, now, 900)
            oi_d60 = _delta_pct(oi_dq, now, 3600)
            oi_acc = _oi_acceleration(sym, now)

            # Futures CVD
            fh  = cvd_futures_history.get(sym, deque())
            f1  = _sum_window(fh, now, 60)
            f5  = _sum_window(fh, now, 300)
            f15 = _sum_window(fh, now, 900)
            f60 = _sum_window(fh, now, 3600)

            # Spot CVD
            sh  = cvd_spot_history.get(sym, deque())
            s1  = _sum_window(sh, now, 60)
            s5  = _sum_window(sh, now, 300)
            s15 = _sum_window(sh, now, 900)
            s60 = _sum_window(sh, now, 3600)

            # Объём
            vh  = volume_history.get(sym, deque())
            v1  = _sum_window(vh, now, 60)
            v5  = _sum_window(vh, now, 300)
            v15 = _sum_window(vh, now, 900)

            # Дополнительные признаки
            cvd_eff  = _cvd_efficiency(sym, now, 300)   # за 5м
            rng_30m  = _range_pct(sym, now, 1800, price)

            # Funding (из кеша)
            funding = await _get_funding(sym, now)

            rows.append((
                ts, sym, price,
                v1, v5, v15,
                oi_usd, oi_d1, oi_d5, oi_d15, oi_d60, oi_acc,
                f1, f5, f15, f60,
                s1, s5, s15, s60,
                cvd_eff, rng_30m,
                funding, btc_1h, btc_4h
            ))

        if rows:
            try:
                await db.executemany("""
                    INSERT INTO market_snapshots (
                        timestamp, symbol, price,
                        volume_1m, volume_5m, volume_15m,
                        oi_usd, oi_delta_1m, oi_delta_5m, oi_delta_15m, oi_delta_60m,
                        oi_acceleration,
                        futures_cvd_1m, futures_cvd_5m, futures_cvd_15m, futures_cvd_60m,
                        spot_cvd_1m, spot_cvd_5m, spot_cvd_15m, spot_cvd_60m,
                        cvd_efficiency, range_pct_30m,
                        funding_rate, btc_change_1h, btc_change_4h
                    ) VALUES (
                        ?,?,?,
                        ?,?,?,
                        ?,?,?,?,?,?,
                        ?,?,?,?,
                        ?,?,?,?,
                        ?,?,
                        ?,?,?
                    )
                """, rows)
                await db.commit()
                print(f"{CYAN}[snapshot] {len(rows)} монет @ {ts}{RESET}")
            except Exception as e:
                print(f"{RED}[snapshot] Ошибка записи: {e}{RESET}")

        # Чистим историю старше HISTORY_DEPTH_SEC
        cutoff = now - HISTORY_DEPTH_SEC
        for hist_dict in (price_history, oi_history,
                          cvd_futures_history, cvd_spot_history,
                          volume_history):
            for sym in list(hist_dict.keys()):
                dq = hist_dict.get(sym)
                if dq is None:
                    continue
                while dq and dq[0][0] < cutoff:
                    dq.popleft()


# ════════════════════════════════════════════════════════════════
# Задача 2: squeeze_detector
# Ищет движения ≥ SQUEEZE_THRESHOLD_PCT в скользящем окне 90 минут
# ════════════════════════════════════════════════════════════════
async def squeeze_detector():
    print(f"{CYAN}[squeeze] Запущен "
          f"(порог {SQUEEZE_THRESHOLD_PCT}%, окно {SQUEEZE_WINDOW_SEC//60}м){RESET}")

    _registered: dict = {}  # sym → timestamp последней регистрации

    while True:
        await asyncio.sleep(SQUEEZE_CHECK_INTERVAL)
        now    = time.time()
        cutoff = now - SQUEEZE_WINDOW_SEC

        for sym in list(price_cache.keys()):
            cur_price = price_cache.get(sym, 0)
            if cur_price <= 0:
                continue

            ph = price_history.get(sym)
            if not ph or len(ph) < 2:
                continue

            # Cooldown
            if now - _registered.get(sym, 0) < SQUEEZE_COOLDOWN_SEC:
                continue

            # Точка открытия окна и тики внутри
            window_ticks = []
            price_at_open = None
            for t, p in ph:
                if t <= cutoff:
                    price_at_open = p
                else:
                    window_ticks.append((t, p))

            if price_at_open is None or not window_ticks:
                continue

            max_price = max(p for _, p in window_ticks)
            min_price = min(p for _, p in window_ticks)
            max_t     = next(t for t, p in window_ticks if p == max_price)
            min_t     = next(t for t, p in window_ticks if p == min_price)

            pump_pct = (max_price - price_at_open) / price_at_open * 100
            dump_pct = (min_price - price_at_open) / price_at_open * 100

            # Выбираем доминирующее движение
            if pump_pct >= abs(dump_pct):
                move_pct   = pump_pct
                direction  = "LONG"
                peak_price = max_price
                peak_t     = max_t
            else:
                move_pct   = dump_pct   # отрицательное
                direction  = "SHORT"
                peak_price = min_price
                peak_t     = min_t

            if abs(move_pct) < SQUEEZE_THRESHOLD_PCT:
                continue

            _registered[sym] = now

            pullback_pct = (cur_price - peak_price) / peak_price * 100
            peak_ts      = datetime.fromtimestamp(peak_t).strftime('%Y-%m-%d %H:%M:%S')

            pb_str = f"  откат={pullback_pct:+.1f}%" if abs(pullback_pct) >= 0.5 else ""
            color  = GREEN if direction == "LONG" else RED
            print(f"{BOLD}{color}[squeeze] 🔥 {sym}  "
                  f"{move_pct:+.1f}% ({direction})  "
                  f"peak@{peak_ts[11:16]}{pb_str}{RESET}")

            try:
                cursor = await db.execute("""
                    INSERT INTO squeeze_events (
                        symbol, direction,
                        start_price, peak_price, peak_time, end_price,
                        move_pct, pullback_pct, event_time
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                """, (
                    sym, direction,
                    price_at_open, peak_price, peak_ts, cur_price,
                    move_pct, pullback_pct, peak_ts
                ))
                await db.commit()
                event_id = cursor.lastrowid
            except Exception as e:
                print(f"{RED}[squeeze] Ошибка записи события: {e}{RESET}")
                continue

            # Фоновые задачи: признаки ДО и результаты ПОСЛЕ
            asyncio.create_task(
                _extract_features(event_id, sym, peak_ts)
            )
            asyncio.create_task(
                _fetch_outcomes(event_id, sym, cur_price, peak_ts)
            )


# ════════════════════════════════════════════════════════════════
# Задача 2a: извлечение признаков ДО события
# ════════════════════════════════════════════════════════════════
async def _extract_features(event_id: int, sym: str, event_time: str):
    """
    Ищет снимки в market_snapshots за 5/15/30/60/120 минут до peak_time
    и записывает в squeeze_features.
    """
    offsets = [5, 15, 30, 60, 120]
    try:
        event_ts = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S').timestamp()
        rows = []
        for minutes in offsets:
            target_ts = event_ts - minutes * 60
            cursor = await db.execute("""
                SELECT
                    timestamp, price, oi_usd,
                    oi_delta_5m, oi_delta_15m, oi_acceleration,
                    futures_cvd_5m, futures_cvd_15m,
                    spot_cvd_5m, spot_cvd_15m,
                    volume_5m, volume_15m,
                    cvd_efficiency, range_pct_30m,
                    funding_rate, btc_change_1h
                FROM market_snapshots
                WHERE symbol = ?
                  AND CAST(strftime('%s', timestamp) AS REAL) BETWEEN ? AND ?
                ORDER BY ABS(CAST(strftime('%s', timestamp) AS REAL) - ?) ASC
                LIMIT 1
            """, (sym, target_ts - 90, target_ts + 90, target_ts))
            row = await cursor.fetchone()
            if not row:
                continue
            rows.append((
                event_id, sym, event_time, minutes,
                row[0],   # snapshot_time
                row[1],   # price
                row[2],   # oi_usd
                row[3],   # oi_delta_5m
                row[4],   # oi_delta_15m
                row[5],   # oi_acceleration
                row[6],   # futures_cvd_5m
                row[7],   # futures_cvd_15m
                row[8],   # spot_cvd_5m
                row[9],   # spot_cvd_15m
                row[10],  # volume_5m
                row[11],  # volume_15m
                row[12],  # cvd_efficiency
                row[13],  # range_pct_30m
                row[14],  # funding_rate
                row[15],  # btc_change_1h
            ))

        if rows:
            await db.executemany("""
                INSERT INTO squeeze_features (
                    event_id, symbol, event_time, minutes_before,
                    snapshot_time, price, oi_usd,
                    oi_delta_5m, oi_delta_15m, oi_acceleration,
                    futures_cvd_5m, futures_cvd_15m,
                    spot_cvd_5m, spot_cvd_15m,
                    volume_5m, volume_15m,
                    cvd_efficiency, range_pct_30m,
                    funding_rate, btc_change_1h
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rows)
            await db.commit()
            print(f"{CYAN}[features] event_id={event_id} {sym}: "
                  f"{len(rows)}/5 срезов записано{RESET}")
        else:
            print(f"{YELLOW}[features] event_id={event_id} {sym}: "
                  f"снимков нет (нужно 2+ часа работы для дальних срезов){RESET}")
    except Exception as e:
        print(f"{RED}[features] Ошибка event_id={event_id}: {e}{RESET}")


# ════════════════════════════════════════════════════════════════
# Задача 2b: результаты ПОСЛЕ события
# ════════════════════════════════════════════════════════════════
async def _fetch_outcomes(event_id: int, sym: str,
                          end_price: float, event_time: str):
    """Ждёт 61 минуту, затем запрашивает kline и записывает результаты."""
    await asyncio.sleep(3660)
    entry_ts = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S').timestamp()
    result   = await _fetch_kline_after(sym, entry_ts, end_price)
    if not result:
        return
    try:
        await db.execute("""
            INSERT INTO squeeze_outcomes (
                event_id, symbol, event_time, end_price,
                price_after_15m, price_after_30m, price_after_60m,
                move_after_15m_pct, move_after_30m_pct, move_after_60m_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            event_id, sym, event_time, end_price,
            result['price_after_15m'],
            result['price_after_30m'],
            result['price_after_60m'],
            result['move_after_15m_pct'],
            result['move_after_30m_pct'],
            result['move_after_60m_pct'],
        ))
        await db.commit()
        m15 = result['move_after_15m_pct']
        m30 = result['move_after_30m_pct']
        m60 = result['move_after_60m_pct']
        print(f"{CYAN}[outcomes] event_id={event_id} {sym}  "
              f"15m={m15:+.1f}%  30m={m30:+.1f}%  60m={m60:+.1f}%{RESET}"
              if m15 is not None else
              f"{CYAN}[outcomes] event_id={event_id} {sym}: записано{RESET}")
    except Exception as e:
        print(f"{RED}[outcomes] Ошибка event_id={event_id}: {e}{RESET}")


# ════════════════════════════════════════════════════════════════
# Задача 3: daily_report
# Раз в сутки в 00:00 UTC печатает сводку по накопленным данным
# ════════════════════════════════════════════════════════════════
async def daily_report():
    print(f"{CYAN}[report] Ежесуточный отчёт в 00:00 UTC{RESET}")
    while True:
        now_dt   = datetime.utcnow()
        from datetime import timedelta
        next_run = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_run <= now_dt:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now_dt).total_seconds())

        report_dt = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
        try:
            # Общая статистика
            r1 = await (await db.execute(
                "SELECT COUNT(*), COUNT(DISTINCT symbol) FROM market_snapshots"
            )).fetchone()
            r2 = await (await db.execute(
                "SELECT COUNT(*), SUM(direction='LONG'), SUM(direction='SHORT') FROM squeeze_events"
            )).fetchone()
            r3 = await (await db.execute(
                "SELECT COUNT(*) FROM squeeze_outcomes"
            )).fetchone()
            r4 = await (await db.execute(
                "SELECT AVG(ABS(move_pct)), MAX(ABS(move_pct)) FROM squeeze_events"
            )).fetchone()

            # Средние признаки за 15м до события
            r5 = await (await db.execute("""
                SELECT AVG(sf.oi_delta_5m), AVG(sf.futures_cvd_5m),
                       AVG(sf.spot_cvd_5m), AVG(sf.funding_rate),
                       AVG(sf.cvd_efficiency), AVG(sf.oi_acceleration)
                FROM squeeze_features sf
                WHERE sf.minutes_before = 15
            """)).fetchone()

            # Топ символов по количеству событий
            top_rows = await (await db.execute("""
                SELECT symbol, COUNT(*) n, AVG(ABS(move_pct)) avg_move
                FROM squeeze_events
                GROUP BY symbol ORDER BY n DESC LIMIT 10
            """)).fetchall()

            sep = '═' * 80
            print(f"\n{BOLD}{CYAN}{sep}")
            print(f"  📊 ЕЖЕДНЕВНЫЙ ОТЧЁТ @ {report_dt}")
            print(f"{sep}{RESET}")
            print(f"  Снимков в БД:      {r1[0]:,}  ({r1[1]} символов)")
            print(f"  Событий (сквизов): {r2[0]}  "
                  f"(LONG: {r2[1]}  SHORT: {r2[2]})")
            print(f"  Результатов:       {r3[0]}")
            if r4[0]:
                print(f"  Avg |move|:        {r4[0]:.1f}%  Max: {r4[1]:.1f}%")
            if r5 and r5[0] is not None:
                print(f"\n  Признаки за 15м ДО события:")
                print(f"    OI delta 5m:     {r5[0]:+.2f}%")
                print(f"    Futures CVD 5m:  {r5[1]:+,.0f}")
                print(f"    Spot CVD 5m:     {r5[2]:+,.0f}")
                fr_str = f"{r5[3]*100:+.4f}%" if r5[3] else "н/д"
                print(f"    Funding rate:    {fr_str}")
                if r5[4]: print(f"    CVD efficiency:  {r5[4]:.3f}")
                if r5[5]: print(f"    OI acceleration: {r5[5]:+.2f}")
            if top_rows:
                print(f"\n  Топ символов по событиям:")
                for row in top_rows:
                    print(f"    {row[0]:<16} n={row[1]}  avg={row[2]:.1f}%")
            print(f"{BOLD}{CYAN}{sep}{RESET}\n")
        except Exception as e:
            print(f"{RED}[report] Ошибка: {e}{RESET}")


# ════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════
async def main():
    global session, db
    print(f"\n{BOLD}{'═'*60}")
    print(f"  BYBIT RESEARCH COLLECTOR")
    print(f"  Снимок: {SNAPSHOT_INTERVAL_SEC}с  "
          f"Порог: {SQUEEZE_THRESHOLD_PCT}%  "
          f"Окно: {SQUEEZE_WINDOW_SEC//60}м")
    print(f"{'═'*60}{RESET}\n")

    async with aiohttp.ClientSession() as client_sess:
        session = client_sess

        async with aiosqlite.connect(DB_PATH) as conn:
            db = conn
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await init_db()

            all_symbols = await fetch_active_symbols()
            if not all_symbols:
                print(f"{RED}Не удалось получить список инструментов.{RESET}")
                return

            spot_symbols = await fetch_active_spot_symbols()
            print(f"{GREEN}Фьючерсов: {len(all_symbols)}  "
                  f"Спот: {len(spot_symbols)}{RESET}")

            # WebSocket задачи
            ws_tasks = []
            for i in range(0, len(all_symbols), SYMBOLS_PER_WS):
                ws_tasks.append(ws_linear_stream(all_symbols[i:i + SYMBOLS_PER_WS]))
            for i in range(0, len(all_symbols), SYMBOLS_PER_WS):
                ws_tasks.append(ws_spot_stream(all_symbols[i:i + SYMBOLS_PER_WS], spot_symbols))

            await asyncio.gather(
                *ws_tasks,
                snapshot_collector(),
                squeeze_detector(),
                daily_report(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{RED}Остановлено{RESET}")
