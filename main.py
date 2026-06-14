"""
Обёртка для Render.com.
Запускает research_collector + минимальный HTTP сервер на /health,
чтобы Render не засыпал.
"""

import asyncio
import os
from aiohttp import web
import research_collector


async def health_handler(request):
    return web.Response(text="OK")


async def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", health_handler)
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
