"""Fare endpoints: search and filter flight prices."""

import math
import sqlite3

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_db
from src.api.models.common import PaginationMeta
from src.api.models.fares import FareListResponse, FareResponse

router = APIRouter(prefix="/fares", tags=["fares"])


@router.get("", response_model=FareListResponse)
def list_fares(
    origin: str | None = Query(None, description="Origin IATA code"),
    destination: str | None = Query(None, description="Destination IATA code"),
    airline: str | None = Query(None, description="Airline code (FR, W6)"),
    date_from: str | None = Query(None, description="Departure date from (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="Departure date to (YYYY-MM-DD)"),
    max_price: float | None = Query(None, description="Maximum price"),
    sort: str = Query("departure_date", description="Sort field"),
    order: str = Query("asc", description="Sort order (asc/desc)"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: sqlite3.Connection = Depends(get_db),
):
    where, params = _build_filters(
        origin, destination, airline, date_from, date_to, max_price,
    )
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    total = db.execute(
        f"SELECT COUNT(*) FROM fares f {where_clause}", params,
    ).fetchone()[0]

    allowed_sorts = {
        "departure_date": "f.departure_date",
        "price": "f.price",
        "origin": "f.origin",
        "destination": "f.destination",
        "scraped_at": "f.scraped_at",
    }
    sort_col = allowed_sorts.get(sort, "f.departure_date")
    sort_dir = "DESC" if order.lower() == "desc" else "ASC"
    nulls = "NULLS LAST" if sort in ("price", "departure_date") else ""

    offset = (page - 1) * per_page
    rows = db.execute(
        f"""SELECT f.id, f.origin, f.destination, f.airline,
                   f.departure_date, f.arrival_date,
                   f.price, f.currency, f.flight_number, f.scraped_at,
                   a1.name AS origin_name, a2.name AS dest_name
            FROM fares f
            LEFT JOIN airports a1 ON f.origin = a1.iata_code
            LEFT JOIN airports a2 ON f.destination = a2.iata_code
            {where_clause}
            ORDER BY {sort_col} {sort_dir} {nulls}
            LIMIT ? OFFSET ?""",
        [*params, per_page, offset],
    ).fetchall()

    return FareListResponse(
        fares=[
            FareResponse(
                id=r["id"],
                origin=r["origin"],
                origin_name=r["origin_name"],
                destination=r["destination"],
                destination_name=r["dest_name"],
                airline=r["airline"],
                departure_date=r["departure_date"],
                arrival_date=r["arrival_date"],
                price=r["price"],
                currency=r["currency"],
                flight_number=r["flight_number"],
                scraped_at=r["scraped_at"],
            )
            for r in rows
        ],
        meta=PaginationMeta(
            page=page, per_page=per_page, total=total,
            total_pages=math.ceil(total / per_page) if total else 0,
        ),
    )


def _build_filters(
    origin: str | None,
    destination: str | None,
    airline: str | None,
    date_from: str | None,
    date_to: str | None,
    max_price: float | None,
) -> tuple[list[str], list]:
    where: list[str] = []
    params: list = []
    if origin:
        where.append("f.origin = ?")
        params.append(origin.upper())
    if destination:
        where.append("f.destination = ?")
        params.append(destination.upper())
    if airline:
        where.append("f.airline = ?")
        params.append(airline.upper())
    if date_from:
        where.append("f.departure_date >= ?")
        params.append(date_from)
    if date_to:
        where.append("f.departure_date <= ?")
        params.append(date_to)
    if max_price is not None:
        where.append("f.price <= ?")
        params.append(max_price)
    return where, params
