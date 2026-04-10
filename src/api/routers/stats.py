"""Stats and airlines endpoints."""

import sqlite3

from fastapi import APIRouter, Depends

from src.api.dependencies import get_db
from src.api.models.common import (
    AirlineListResponse,
    AirlineResponse,
    AirlineStat,
    StatsResponse,
    TableCount,
)
from src.core.config import DB_PATH
from src.scraper import list_airlines

router = APIRouter(tags=["stats"])


@router.get("/stats", response_model=StatsResponse)
def get_stats(db: sqlite3.Connection = Depends(get_db)):
    tables = ["countries", "airports", "routes", "schedules", "fares"]
    counts = []
    for t in tables:
        try:
            n = db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            n = 0
        counts.append(TableCount(table=t, count=n))

    airline_rows = db.execute(
        "SELECT airline, COUNT(*) AS cnt FROM routes GROUP BY airline"
    ).fetchall()
    airlines = [
        AirlineStat(airline=r["airline"], route_count=r["cnt"])
        for r in airline_rows
    ]

    last_updated = db.execute(
        "SELECT MAX(last_seen) FROM routes"
    ).fetchone()[0]

    try:
        size_mb = round(DB_PATH.stat().st_size / (1024 * 1024), 2)
    except Exception:
        size_mb = 0.0

    return StatsResponse(
        tables=counts,
        airlines=airlines,
        last_updated=last_updated,
        db_size_mb=size_mb,
    )


@router.get("/airlines", response_model=AirlineListResponse)
def get_airlines():
    return AirlineListResponse(
        airlines=[
            AirlineResponse(code=code, name=name)
            for code, name in list_airlines()
        ],
    )
