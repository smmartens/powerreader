from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from powerreader.config import Settings
from powerreader.db import init_db


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = Settings()
    await init_db(settings.db_path)
    yield
    # Shutdown: cleanup will go here


app = FastAPI(title="powerreader", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
