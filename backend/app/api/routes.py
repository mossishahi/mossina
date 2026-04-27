"""Route listing and detail endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.airlines import AIRLINE_META
from app.database import get_db
from app.models import Route
from app.models.fare import Fare
from app.schemas.route import RouteOut

router = APIRouter(prefix="/routes", tags=["routes"])


def _route_to_out(r: Route) -> RouteOut:
    meta = AIRLINE_META.get(r.airline)
    airline_name = meta["name"] if meta else None
    return RouteOut(
        origin=r.origin,
        destination=r.destination,
        airline=r.airline,
        airline_name=airline_name,
    )


@router.get("", response_model=list[RouteOut])
async def list_routes(
    airline: str | None = Query(None),
    origin: str | None = Query(None),
    destination: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    has_fares = exists(
        select(1).where(
            Fare.origin == Route.origin,
            Fare.destination == Route.destination,
            Fare.airline == Route.airline,
            Fare.price > 0,
            Fare.departure_date >= func.current_date(),
        )
    )
    stmt = (
        select(Route)
        .where(has_fares)
        .order_by(Route.origin, Route.destination, Route.airline)
    )

    if airline:
        stmt = stmt.where(Route.airline == airline.strip().upper())
    if origin:
        stmt = stmt.where(Route.origin == origin.strip().upper())
    if destination:
        stmt = stmt.where(Route.destination == destination.strip().upper())

    rows = (await db.scalars(stmt)).all()
    return [_route_to_out(r) for r in rows]


@router.get("/{origin}/{destination}", response_model=list[RouteOut])
async def route_detail(
    origin: str,
    destination: str,
    db: AsyncSession = Depends(get_db),
):
    o = origin.strip().upper()
    d = destination.strip().upper()
    stmt = (
        select(Route)
        .where(Route.origin == o, Route.destination == d)
        .order_by(Route.airline)
    )
    rows = (await db.scalars(stmt)).all()
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No routes found for {o} -> {d}",
        )
    return [_route_to_out(r) for r in rows]
