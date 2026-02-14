import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from powerreader.aggregation import setup_scheduler
from powerreader.api import router as api_router
from powerreader.config import Settings
from powerreader.db import init_db
from powerreader.mqtt import MqttSubscriber


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


app = FastAPI(title="powerreader", lifespan=lifespan)
app.include_router(api_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
