"""FastAPI application factory.

Usage:
    uvicorn src.api.app:app --reload          # development
    uvicorn src.api.app:app --host 0.0.0.0    # production
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.dependencies import close_db, get_db, rebuild_fts
from src.api.routers import airports, countries, fares, graph, routes, stats

logger = logging.getLogger("mossina.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Mossina API")
    db = next(get_db())
    try:
        rebuild_fts(db)
        logger.info("FTS index ready")
    except Exception as exc:
        logger.warning("FTS rebuild skipped: %s", exc)
    yield
    close_db()
    logger.info("Mossina API shut down")


app = FastAPI(
    title="Mossina Flight Explorer API",
    description="REST API for searching flights, airports, and routes across Ryanair and Wizz Air",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(airports.router, prefix="/api")
app.include_router(countries.router, prefix="/api")
app.include_router(routes.router, prefix="/api")
app.include_router(fares.router, prefix="/api")
app.include_router(graph.router, prefix="/api")
app.include_router(stats.router, prefix="/api")


@app.get("/api/health")
def health_check():
    return {"status": "ok"}
