"""Country endpoints: list countries and resolve to airports."""

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.dependencies import get_db
from src.api.models.airports import AirportListResponse, AirportResponse
from src.api.models.common import CountryListResponse, CountryResponse

router = APIRouter(prefix="/countries", tags=["countries"])


@router.get("", response_model=CountryListResponse)
def list_countries(
    q: str | None = Query(None, description="Search countries by name"),
    db: sqlite3.Connection = Depends(get_db),
):
    if q:
        pattern = f"%{q}%"
        rows = db.execute(
            "SELECT code, name, currency FROM countries WHERE name LIKE ? ORDER BY name",
            (pattern,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT code, name, currency FROM countries ORDER BY name"
        ).fetchall()

    return CountryListResponse(
        countries=[CountryResponse(**dict(r)) for r in rows],
    )


@router.get("/{country_code}/airports", response_model=AirportListResponse)
def get_country_airports(
    country_code: str,
    db: sqlite3.Connection = Depends(get_db),
):
    """Resolve a country code to all its airports."""
    country_code = country_code.upper()

    country = db.execute(
        "SELECT code FROM countries WHERE code = ?", (country_code,),
    ).fetchone()
    if not country:
        raise HTTPException(
            status_code=404, detail=f"Country {country_code} not found",
        )

    rows = db.execute(
        """SELECT a.iata_code, a.name, a.city, a.country_code,
                  COALESCE(c.name, '') AS country_name,
                  a.latitude, a.longitude, a.timezone
           FROM airports a
           LEFT JOIN countries c ON a.country_code = c.code
           WHERE a.country_code = ?
           ORDER BY a.name""",
        (country_code,),
    ).fetchall()

    return AirportListResponse(
        airports=[AirportResponse(**dict(r)) for r in rows],
    )
