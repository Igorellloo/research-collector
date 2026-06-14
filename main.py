"""
Обёртка для Render.com.
Запускает research_collector + минимальный HTTP сервер на /health,
чтобы Render не засыпал. /db — скачать базу данных.
"""

import asyncio
import os
from aiohttp import web
import research_collector

DB_PATH = "/tmp/research.db"


async def health_handler(request):
    return web.Response(text="OK")


async def db_download_handler(request):
    """Скачать текущую БД через браузер."""
    if not os.path.exists(DB_PATH):
        return web.Response(text="БД не найдена", status=404)
    return web.FileResponse(
        path=DB_PATH,
        headers={"Content-Disposition": "attachment; filename=research.db"}
    )


async def status_handler(request):
    """Простая статистика по БД."""
    if not os.path.exists(DB_PATH):
        return web.Response(text="БД не найдена", status=404)
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        r1 = await (await db.execute("SELECT COUNT(*), COUNT(DISTINCT symbol) FROM market_snapshots")).fetchone()
        r2 = await (await db.execute("SELECT COUNT(*) FROM squeeze_events")).fetchone()
        r3 = await (await db.execute("SELECT COUNT(*) FROM squeeze_outcomes")).fetchone()
        r4 = await (await db.execute("SELECT MAX(timestamp) FROM market_snapshots")).fetchone()
    text = f"""BYBIT RESEARCH COLLECTOR — статус
================================
Снимков в БД:      {r1[0]:,}  ({r1[1]} символов)
Событий (сквизов): {r2[0]}
Результатов:       {r3[0]}
Последний снимок:  {r4[0] or "нет данных"}

Скачать БД: /db
"""
    return web.Response(text=text)


async def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", status_handler)
    app.router.add_get("/status", status_handler)
    app.router.add_get("/db", db_download_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[health] HTTP сервер запущен на порту {port}")
    while True:
        await asyncio.sleep(3600)


async def main():
    await asyncio.gather(
        run_health_server(),
        research_collector.main(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановлено")
