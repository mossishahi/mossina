"""Fare listing and cheapest-per-date endpoints."""

from datetime import date as date_type
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ExchangeRate, Fare, Schedule
from app.schemas.fare import FareOut, RouteFaresOut

router = APIRouter(prefix="/fares", tags=["fares"])


async def _load_rates_map(db: AsyncSession) -> dict[str, Decimal]:
    stmt = select(ExchangeRate)
    rows = (await db.scalars(stmt)).all()
    return {r.currency.strip().upper(): r.rate_to_eur for r in rows}


def _price_to_eur(price: Decimal, currency: str, rates: dict[str, Decimal]) -> Decimal | None:
    cur = currency.strip().upper()
    if cur == "EUR":
        return price
    rate = rates.get(cur)
    if rate is None:
        return None
    return price * rate


def _fare_to_out(
    f: Fare,
    rates: dict[str, Decimal],
    schedule_times: dict | None = None,
) -> FareOut:
    dep_time = None
    arr_time = None
    if schedule_times and f.flight_number and f.departure_date:
        fn = f.flight_number
        date_str = str(f.departure_date)
        times = schedule_times.get((fn, date_str))
        if not times:
            stripped = fn.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            times = schedule_times.get((stripped, date_str))
        if times:
            dep_time, arr_time = times
    return FareOut(
        departure_date=f.departure_date,
        price=f.price,
        price_eur=_price_to_eur(f.price, f.currency, rates),
        currency=f.currency,
        airline=f.airline,
        flight_number=f.flight_number,
        departure_time=dep_time,
        arrival_time=arr_time,
    )


@router.get("/{origin}/{destination}/cheapest", response_model=list[FareOut])
async def cheapest_fares_on_route(
    origin: str,
    destination: str,
    date_from: date_type | None = Query(None),
    date_to: date_type | None = Query(None),
    airline: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    o = origin.strip().upper()
    d = destination.strip().upper()
    rates = await _load_rates_map(db)

    stmt = select(Fare).where(
        Fare.origin == o, Fare.destination == d, Fare.price > 0,
        Fare.departure_date >= func.current_date(),
    ).order_by(
        Fare.departure_date, Fare.price
    )
    if date_from is not None:
        stmt = stmt.where(Fare.departure_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(Fare.departure_date <= date_to)
    if airline:
        stmt = stmt.where(Fare.airline == airline.strip().upper())

    rows = (await db.scalars(stmt)).all()
    best_by_date: dict[date, Fare] = {}
    best_eur_by_date: dict[date, Decimal] = {}

    for f in rows:
        pe = _price_to_eur(f.price, f.currency, rates)
        if pe is None:
            continue
        day = f.departure_date
        if day not in best_eur_by_date or pe < best_eur_by_date[day]:
            best_eur_by_date[day] = pe
            best_by_date[day] = f

    ordered_days = sorted(best_by_date.keys())
    return [_fare_to_out(best_by_date[day], rates) for day in ordered_days]


@router.get("/{origin}/{destination}", response_model=RouteFaresOut)
async def fares_for_route(
    origin: str,
    destination: str,
    date_from: date_type | None = Query(None),
    date_to: date_type | None = Query(None),
    airline: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    o = origin.strip().upper()
    d = destination.strip().upper()
    rates = await _load_rates_map(db)

    stmt = select(Fare).where(
        Fare.origin == o, Fare.destination == d, Fare.price > 0,
        Fare.departure_date >= func.current_date(),
    ).order_by(
        Fare.departure_date, Fare.airline, Fare.price
    )
    if date_from is not None:
        stmt = stmt.where(Fare.departure_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(Fare.departure_date <= date_to)
    if airline:
        stmt = stmt.where(Fare.airline == airline.strip().upper())

    rows = (await db.scalars(stmt)).all()

    sched_stmt = select(
        Schedule.flight_number,
        Schedule.departure_date,
        Schedule.departure_time,
        Schedule.arrival_time,
    ).where(Schedule.origin == o, Schedule.destination == d)
    if date_from is not None:
        sched_stmt = sched_stmt.where(Schedule.departure_date >= date_from)
    if date_to is not None:
        sched_stmt = sched_stmt.where(Schedule.departure_date <= date_to)
    sched_rows = await db.execute(sched_stmt)
    schedule_times: dict[tuple[str, str], tuple[str | None, str | None]] = {}
    for fn, dep_d, dep_t, arr_t in sched_rows.all():
        if fn and dep_d:
            dt_str = dep_t.strftime("%H:%M") if dep_t else None
            at_str = arr_t.strftime("%H:%M") if arr_t else None
            schedule_times[(fn, str(dep_d))] = (dt_str, at_str)

    outs = [_fare_to_out(f, rates, schedule_times) for f in rows]
    eur_values = [x.price_eur for x in outs if x.price_eur is not None]
    cheapest = min(eur_values) if eur_values else None

    return RouteFaresOut(
        origin=o,
        destination=d,
        fares=outs,
        cheapest_eur=cheapest,
    )
