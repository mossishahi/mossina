"""Route graph helpers: adjacency from filtered routes."""

from collections import defaultdict
from datetime import date

from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fare import Fare
from app.models.route import Route


def _normalize_date_range(
    date_from: date | None, date_to: date | None
) -> tuple[date | None, date | None]:
    if date_from is None and date_to is None:
        return None, None
    if date_from is None:
        return date_to, date_to
    if date_to is None:
        return date_from, date_from
    return date_from, date_to


async def fetch_filtered_route_edges(
    db: AsyncSession,
    airline: str | None,
    date_from: date | None,
    date_to: date | None,
) -> list[tuple[str, str, str]]:
    """Return (origin, destination, airline) rows that have bookable fares.

    Only routes with at least one fare (price > 0, future departure) are
    included.  When date_from/date_to are set, fares must also fall within
    that window.
    """
    stmt = select(Route.origin, Route.destination, Route.airline)
    if airline:
        stmt = stmt.where(Route.airline == airline)

    fare_conditions = [
        Fare.origin == Route.origin,
        Fare.destination == Route.destination,
        Fare.airline == Route.airline,
        Fare.price > 0,
        Fare.departure_date >= func.current_date(),
    ]
    df, dt = _normalize_date_range(date_from, date_to)
    if df is not None and dt is not None:
        fare_conditions.append(Fare.departure_date >= df)
        fare_conditions.append(Fare.departure_date <= dt)

    stmt = stmt.where(exists(select(1).where(*fare_conditions)))
    result = await db.execute(stmt)
    return [(row[0], row[1], row[2]) for row in result.all()]


async def build_adjacency(
    db: AsyncSession,
    airline: str | None,
    date_from: date | None,
    date_to: date | None,
) -> dict[str, list[str]]:
    """Adjacency map origin -> sorted unique destinations (routes optional fare filter)."""
    edges = await fetch_filtered_route_edges(db, airline, date_from, date_to)
    by_origin: dict[str, set[str]] = defaultdict(set)
    for o, d, _ in edges:
        by_origin[o].add(d)
    return {o: sorted(by_origin[o]) for o in sorted(by_origin.keys())}
