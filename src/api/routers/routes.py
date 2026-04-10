"""Route endpoints: list and search flight routes."""

import math
import sqlite3

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_db
from src.api.models.common import PaginationMeta
from src.api.models.routes import RouteListResponse, RouteResponse

router = APIRouter(prefix="/routes", tags=["routes"])


@router.get("", response_model=RouteListResponse)
def list_routes(
    origin: str | None = Query(None, description="Origin IATA code"),
    destination: str | None = Query(None, description="Destination IATA code"),
    airline: str | None = Query(None, description="Airline code (FR, W6)"),
    origin_country: str | None = Query(None, description="Origin country code"),
    dest_country: str | None = Query(None, description="Destination country code"),
    sort: str = Query("origin", description="Sort field"),
    order: str = Query("asc", description="Sort order (asc/desc)"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: sqlite3.Connection = Depends(get_db),
):
    where, params = _build_filters(
        origin, destination, airline, origin_country, dest_country,
    )
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    total = db.execute(
        f"""SELECT COUNT(*) FROM routes r
            JOIN airports a1 ON r.origin = a1.iata_code
            JOIN airports a2 ON r.destination = a2.iata_code
            {where_clause}""",
        params,
    ).fetchone()[0]

    allowed_sorts = {
        "origin": "r.origin",
        "destination": "r.destination",
        "airline": "r.airline",
        "price": "min_price",
        "last_seen": "r.last_seen",
    }
    sort_col = allowed_sorts.get(sort, "r.origin")
    sort_dir = "DESC" if order.lower() == "desc" else "ASC"
    nulls = "NULLS LAST" if sort == "price" else ""

    offset = (page - 1) * per_page
    rows = db.execute(
        f"""SELECT r.id, r.origin, r.destination, r.airline,
                   r.is_connecting, r.new_route, r.seasonal_route, r.last_seen,
                   a1.name AS origin_name, a1.city AS origin_city,
                   a1.country_code AS origin_country,
                   a2.name AS dest_name, a2.city AS dest_city,
                   a2.country_code AS dest_country,
                   MIN(f.price) AS min_price, f.currency
            FROM routes r
            JOIN airports a1 ON r.origin = a1.iata_code
            JOIN airports a2 ON r.destination = a2.iata_code
            LEFT JOIN fares f ON f.origin = r.origin
                             AND f.destination = r.destination
                             AND f.airline = r.airline
                             AND f.departure_date >= date('now')
            {where_clause}
            GROUP BY r.id
            ORDER BY {sort_col} {sort_dir} {nulls}
            LIMIT ? OFFSET ?""",
        [*params, per_page, offset],
    ).fetchall()

    return RouteListResponse(
        routes=[
            RouteResponse(
                id=r["id"],
                origin=r["origin"],
                origin_name=r["origin_name"],
                origin_city=r["origin_city"],
                origin_country=r["origin_country"],
                destination=r["destination"],
                destination_name=r["dest_name"],
                destination_city=r["dest_city"],
                destination_country=r["dest_country"],
                airline=r["airline"],
                is_connecting=bool(r["is_connecting"]),
                new_route=bool(r["new_route"]),
                seasonal_route=bool(r["seasonal_route"]),
                last_seen=r["last_seen"],
                min_price=r["min_price"],
                currency=r["currency"],
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
    origin_country: str | None,
    dest_country: str | None,
) -> tuple[list[str], list]:
    where: list[str] = []
    params: list = []
    if origin:
        where.append("r.origin = ?")
        params.append(origin.upper())
    if destination:
        where.append("r.destination = ?")
        params.append(destination.upper())
    if airline:
        where.append("r.airline = ?")
        params.append(airline.upper())
    if origin_country:
        where.append("a1.country_code = ?")
        params.append(origin_country.upper())
    if dest_country:
        where.append("a2.country_code = ?")
        params.append(dest_country.upper())
    return where, params
