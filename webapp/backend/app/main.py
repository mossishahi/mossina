"""FastAPI application entrypoint."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import airlines, airports, exchange_rates, fares, routes as routes_api, search
from app.config import get_settings
from app.database import async_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await async_engine.dispose()


app = FastAPI(title="Mossina API", lifespan=lifespan)

_settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(airports.router, prefix="/api")
app.include_router(routes_api.router, prefix="/api")
app.include_router(fares.router, prefix="/api")
app.include_router(airlines.router, prefix="/api")
app.include_router(search.router, prefix="/api")
app.include_router(exchange_rates.router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}
