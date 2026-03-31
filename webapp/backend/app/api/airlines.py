"""Airline metadata and stats endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, union
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Route

router = APIRouter(prefix="/airlines", tags=["airlines"])

AIRLINE_META: dict[str, dict[str, str]] = {
    "FR": {"name": "Ryanair", "color": "#0b4ea2"},
    "W6": {"name": "Wizz Air", "color": "#e500a4"},
}


class AirlineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    name: str
    color: str


class AirlineStatsOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    route_count: int
    airport_count: int


@router.get("", response_model=list[AirlineOut])
async def list_airlines():
    return [
        AirlineOut(code=code, name=meta["name"], color=meta["color"])
        for code, meta in sorted(AIRLINE_META.items())
    ]


@router.get("/{code}/stats", response_model=AirlineStatsOut)
async def airline_stats(code: str, db: AsyncSession = Depends(get_db)):
    code_upper = code.strip().upper()
    if code_upper not in AIRLINE_META:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown airline code: {code_upper}",
        )

    route_count_stmt = select(func.count()).select_from(Route).where(Route.airline == code_upper)
    route_count = int(await db.scalar(route_count_stmt) or 0)

    origins = select(Route.origin.label("ap")).where(Route.airline == code_upper)
    destinations = select(Route.destination.label("ap")).where(Route.airline == code_upper)
    ap_union = union(origins, destinations).subquery()
    airport_count_stmt = select(func.count(func.distinct(ap_union.c.ap))).select_from(ap_union)
    airport_count = int(await db.scalar(airport_count_stmt) or 0)

    return AirlineStatsOut(
        code=code_upper,
        route_count=route_count,
        airport_count=airport_count,
    )
