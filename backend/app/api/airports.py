"""Airport list and search endpoints."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, or_, select, union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.database import get_db
from mossina_db.models import Airport, Country, Route
from app.schemas.airport import AirportOut, AirportSearchResult, CountryOut

router = APIRouter(prefix="/airports", tags=["airports"])

_COUNTRY_NAMES: dict[str, str] = {
    "al": "Albania", "am": "Armenia", "at": "Austria", "az": "Azerbaijan",
    "ba": "Bosnia and Herzegovina", "be": "Belgium", "bg": "Bulgaria",
    "ch": "Switzerland", "cy": "Cyprus", "cz": "Czech Republic",
    "de": "Germany", "dk": "Denmark", "ee": "Estonia", "eg": "Egypt",
    "es": "Spain", "fi": "Finland", "fr": "France", "gb": "United Kingdom",
    "ge": "Georgia", "gr": "Greece", "hr": "Croatia", "hu": "Hungary",
    "ie": "Ireland", "il": "Israel", "is": "Iceland", "it": "Italy",
    "jo": "Jordan", "lt": "Lithuania", "lu": "Luxembourg", "lv": "Latvia",
    "ma": "Morocco", "md": "Moldova", "me": "Montenegro",
    "mk": "North Macedonia", "mt": "Malta", "nl": "Netherlands",
    "no": "Norway", "pl": "Poland", "pt": "Portugal", "ro": "Romania",
    "rs": "Serbia", "se": "Sweden", "si": "Slovenia", "sk": "Slovakia",
    "tr": "Türkiye", "ua": "Ukraine", "ae": "United Arab Emirates",
}


def _airport_to_out(ap: Airport) -> AirportOut:
    cc = (ap.country_code or "").lower()
    country_name = None
    if ap.country and ap.country.name and len(ap.country.name) > 2:
        country_name = ap.country.name
    if not country_name:
        country_name = _COUNTRY_NAMES.get(cc)
    return AirportOut(
        iata=ap.iata_code,
        name=ap.name,
        city=ap.city,
        country_code=cc,
        country=country_name or cc.upper(),
        lat=ap.latitude,
        lon=ap.longitude,
    )


@router.get("", response_model=list[AirportOut])
async def list_airports(
    airline: str | None = Query(None, description="IATA airline code filter"),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Airport).options(joinedload(Airport.country)).order_by(Airport.iata_code)

    if airline:
        ac = airline.strip().upper()
        origins = select(Route.origin.label("code")).where(Route.airline == ac)
        destinations = select(Route.destination.label("code")).where(Route.airline == ac)
        codes_subq = union(origins, destinations).subquery()
        stmt = stmt.where(Airport.iata_code.in_(select(codes_subq.c.code)))

    result = await db.scalars(stmt)
    airports = result.unique().all()
    return [_airport_to_out(ap) for ap in airports]


@router.get("/search", response_model=AirportSearchResult)
async def search_airports(
    q: str = Query(..., min_length=1, description="Search IATA, city, or country"),
    db: AsyncSession = Depends(get_db),
):
    term = f"%{q.strip()}%"
    term_upper = f"%{q.strip().upper()}%"

    ap_stmt = (
        select(Airport)
        .options(joinedload(Airport.country))
        .join(Country, Airport.country_code == Country.code, isouter=True)
        .where(
            or_(
                Airport.iata_code.ilike(term_upper),
                Airport.city.ilike(term),
                Country.name.ilike(term),
            )
        )
        .order_by(Airport.iata_code)
    )
    ap_rows = (await db.scalars(ap_stmt)).unique().all()
    airports_out = [_airport_to_out(ap) for ap in ap_rows]

    matched_country_codes = {ap.country_code for ap in ap_rows if ap.country_code}

    co_stmt = (
        select(Country.code)
        .where(or_(Country.name.ilike(term), Country.code.ilike(term_upper)))
    )
    country_codes_from_name = set((await db.scalars(co_stmt)).all())
    all_country_codes = matched_country_codes | country_codes_from_name

    countries_out: list[CountryOut] = []
    if all_country_codes:
        cnt_stmt = (
            select(Airport.country_code, func.count().label("n"))
            .where(Airport.country_code.in_(all_country_codes))
            .group_by(Airport.country_code)
        )
        counts = {row.country_code: int(row.n) for row in await db.execute(cnt_stmt)}

        c_stmt = select(Country).where(Country.code.in_(all_country_codes)).order_by(Country.code)
        countries = (await db.scalars(c_stmt)).all()
        countries_out = [
            CountryOut(
                code=c.code,
                name=c.name,
                currency=c.currency,
                airport_count=counts.get(c.code, 0),
            )
            for c in countries
        ]

    return AirportSearchResult(airports=airports_out, countries=countries_out)
