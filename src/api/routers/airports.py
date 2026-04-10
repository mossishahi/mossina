"""Airport endpoints: list, search, detail, country resolution."""

import math
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.dependencies import get_db, ensure_fts, rebuild_fts
from src.api.models.airports import (
    AirportDetail,
    AirportListResponse,
    AirportResponse,
    AirportSearchResult,
    ConnectedRoute,
)
from src.api.models.common import PaginationMeta

router = APIRouter(prefix="/airports", tags=["airports"])


@router.get("", response_model=AirportListResponse)
def list_airports(
    country: str | None = Query(None, description="Filter by country code"),
    q: str | None = Query(None, description="Search airports by name/city/IATA"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: sqlite3.Connection = Depends(get_db),
):
    if q:
        return _search_airports(db, q, country, page, per_page)

    where, params = [], []
    if country:
        where.append("a.country_code = ?")
        params.append(country.upper())

    where_clause = f"WHERE {' AND '.join(where)}" if where else ""

    total = db.execute(
        f"SELECT COUNT(*) FROM airports a {where_clause}", params
    ).fetchone()[0]

    offset = (page - 1) * per_page
    rows = db.execute(
        f"""SELECT a.iata_code, a.name, a.city, a.country_code,
                   COALESCE(c.name, '') AS country_name,
                   a.latitude, a.longitude, a.timezone
            FROM airports a
            LEFT JOIN countries c ON a.country_code = c.code
            {where_clause}
            ORDER BY a.iata_code
            LIMIT ? OFFSET ?""",
        [*params, per_page, offset],
    ).fetchall()

    return AirportListResponse(
        airports=[AirportResponse(**dict(r)) for r in rows],
        meta=PaginationMeta(
            page=page, per_page=per_page, total=total,
            total_pages=math.ceil(total / per_page) if total else 0,
        ),
    )


def _search_airports(
    db: sqlite3.Connection, q: str, country: str | None,
    page: int, per_page: int,
) -> AirportListResponse:
    """FTS5-backed airport search with fallback to LIKE."""
    try:
        ensure_fts(db)
        count = db.execute("SELECT COUNT(*) FROM airports_fts").fetchone()[0]
        ap_count = db.execute("SELECT COUNT(*) FROM airports").fetchone()[0]
        if count == 0 and ap_count > 0:
            rebuild_fts(db)

        fts_q = q.replace('"', '""')
        fts_query = f'"{fts_q}"*'

        base = """
            FROM airports_fts f
            JOIN airports a ON a.iata_code = f.iata_code
            LEFT JOIN countries c ON a.country_code = c.code
            WHERE airports_fts MATCH ?
        """
        params: list = [fts_query]
        if country:
            base += " AND a.country_code = ?"
            params.append(country.upper())

        total = db.execute(f"SELECT COUNT(*) {base}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = db.execute(
            f"""SELECT a.iata_code, a.name, a.city, a.country_code,
                       COALESCE(c.name, '') AS country_name,
                       a.latitude, a.longitude, a.timezone
                {base}
                ORDER BY rank
                LIMIT ? OFFSET ?""",
            [*params, per_page, offset],
        ).fetchall()

    except Exception:
        return _search_airports_like(db, q, country, page, per_page)

    return AirportListResponse(
        airports=[AirportResponse(**dict(r)) for r in rows],
        meta=PaginationMeta(
            page=page, per_page=per_page, total=total,
            total_pages=math.ceil(total / per_page) if total else 0,
        ),
    )


def _search_airports_like(
    db: sqlite3.Connection, q: str, country: str | None,
    page: int, per_page: int,
) -> AirportListResponse:
    """LIKE-based fallback when FTS is unavailable."""
    pattern = f"%{q}%"
    where = "(a.iata_code LIKE ? OR a.name LIKE ? OR a.city LIKE ?)"
    params: list = [pattern, pattern, pattern]
    if country:
        where += " AND a.country_code = ?"
        params.append(country.upper())

    total = db.execute(
        f"SELECT COUNT(*) FROM airports a WHERE {where}", params
    ).fetchone()[0]

    offset = (page - 1) * per_page
    rows = db.execute(
        f"""SELECT a.iata_code, a.name, a.city, a.country_code,
                   COALESCE(c.name, '') AS country_name,
                   a.latitude, a.longitude, a.timezone
            FROM airports a
            LEFT JOIN countries c ON a.country_code = c.code
            WHERE {where}
            ORDER BY a.iata_code
            LIMIT ? OFFSET ?""",
        [*params, per_page, offset],
    ).fetchall()

    return AirportListResponse(
        airports=[AirportResponse(**dict(r)) for r in rows],
        meta=PaginationMeta(
            page=page, per_page=per_page, total=total,
            total_pages=math.ceil(total / per_page) if total else 0,
        ),
    )


@router.get("/search", response_model=list[AirportSearchResult])
def search_airports_typeahead(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    db: sqlite3.Connection = Depends(get_db),
):
    """Fast typeahead endpoint for airport search (max 50 results)."""
    try:
        ensure_fts(db)
        count = db.execute("SELECT COUNT(*) FROM airports_fts").fetchone()[0]
        if count == 0:
            rebuild_fts(db)

        fts_q = q.replace('"', '""')
        rows = db.execute(
            """SELECT a.iata_code, a.name, a.city, a.country_code,
                      COALESCE(c.name, '') AS country_name
               FROM airports_fts f
               JOIN airports a ON a.iata_code = f.iata_code
               LEFT JOIN countries c ON a.country_code = c.code
               WHERE airports_fts MATCH ?
               ORDER BY rank
               LIMIT ?""",
            (f'"{fts_q}"*', limit),
        ).fetchall()
    except Exception:
        pattern = f"%{q}%"
        rows = db.execute(
            """SELECT a.iata_code, a.name, a.city, a.country_code,
                      COALESCE(c.name, '') AS country_name
               FROM airports a
               LEFT JOIN countries c ON a.country_code = c.code
               WHERE a.iata_code LIKE ? OR a.name LIKE ? OR a.city LIKE ?
               ORDER BY a.iata_code
               LIMIT ?""",
            (pattern, pattern, pattern, limit),
        ).fetchall()

    results = []
    for r in rows:
        match_field = "name"
        q_upper = q.upper()
        if r["iata_code"].upper().startswith(q_upper):
            match_field = "iata_code"
        elif r["city"] and q.lower() in r["city"].lower():
            match_field = "city"
        elif r["country_name"] and q.lower() in r["country_name"].lower():
            match_field = "country"

        results.append(AirportSearchResult(
            iata_code=r["iata_code"],
            name=r["name"],
            city=r["city"],
            country_code=r["country_code"],
            country_name=r["country_name"],
            match_field=match_field,
        ))
    return results


@router.get("/{iata_code}", response_model=AirportDetail)
def get_airport(
    iata_code: str,
    db: sqlite3.Connection = Depends(get_db),
):
    iata_code = iata_code.upper()
    row = db.execute(
        """SELECT a.iata_code, a.name, a.city, a.country_code,
                  COALESCE(c.name, '') AS country_name,
                  a.latitude, a.longitude, a.timezone
           FROM airports a
           LEFT JOIN countries c ON a.country_code = c.code
           WHERE a.iata_code = ?""",
        (iata_code,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Airport {iata_code} not found")

    airport = AirportResponse(**dict(row))

    outbound = db.execute(
        """SELECT r.destination, r.airline,
                  a2.name AS dest_name, a2.city AS dest_city,
                  a2.country_code AS dest_cc,
                  COALESCE(c2.name, '') AS dest_country_name,
                  a2.latitude AS dest_lat, a2.longitude AS dest_lon,
                  a2.timezone AS dest_tz,
                  MIN(f.price) AS min_price,
                  f.currency
           FROM routes r
           JOIN airports a2 ON r.destination = a2.iata_code
           LEFT JOIN countries c2 ON a2.country_code = c2.code
           LEFT JOIN fares f ON f.origin = r.origin
                            AND f.destination = r.destination
                            AND f.airline = r.airline
                            AND f.departure_date >= date('now')
           WHERE r.origin = ?
           GROUP BY r.destination, r.airline
           ORDER BY a2.name""",
        (iata_code,),
    ).fetchall()

    routes = [
        ConnectedRoute(
            destination=AirportResponse(
                iata_code=r["destination"],
                name=r["dest_name"],
                city=r["dest_city"],
                country_code=r["dest_cc"],
                country_name=r["dest_country_name"],
                latitude=r["dest_lat"],
                longitude=r["dest_lon"],
                timezone=r["dest_tz"],
            ),
            airline=r["airline"],
            has_fares=r["min_price"] is not None,
            min_price=r["min_price"],
            currency=r["currency"],
        )
        for r in outbound
    ]

    inbound_count = db.execute(
        "SELECT COUNT(*) FROM routes WHERE destination = ?", (iata_code,)
    ).fetchone()[0]

    return AirportDetail(
        airport=airport,
        outbound_routes=routes,
        inbound_route_count=inbound_count,
    )
