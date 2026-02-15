import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware

from powerreader.aggregation import setup_scheduler
from powerreader.api import router as api_router
from powerreader.config import Settings
from powerreader.db import init_db
from powerreader.mqtt import MqttSubscriber

_PKG_DIR = Path(__file__).parent
_templates = Jinja2Templates(directory=_PKG_DIR / "templates")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    _app.state.db_path = settings.db_path
    await init_db(settings.db_path)
    scheduler = setup_scheduler(settings.db_path, settings.raw_retention_days)
    scheduler.start()
    subscriber = MqttSubscriber(settings)
    subscriber.start(asyncio.get_event_loop())
    yield
    scheduler.shutdown()
    subscriber.stop()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-inline';"
            " style-src 'self' 'unsafe-inline'"
        )
        return response


app = FastAPI(title="powerreader", lifespan=lifespan)
app.add_middleware(SecurityHeadersMiddleware)
app.include_router(api_router)
app.mount("/static", StaticFiles(directory=_PKG_DIR / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    return _templates.TemplateResponse(request, "dashboard.html")


@app.get("/log", response_class=HTMLResponse)
async def log_page(request: Request) -> HTMLResponse:
    return _templates.TemplateResponse(request, "log.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
