"""Scrape Ryanair airports and inline route data.

Ported from the SQLite version to use PostgreSQL via SQLAlchemy sync
sessions with INSERT ... ON CONFLICT DO UPDATE upserts.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Airport, Country, Route

log = logging.getLogger("scraper")

AIRLINE = "FR"

AIRPORTS_V3_URL = (
    "https://www.ryanair.com/api/views/locate/3/airports/en/active"
)
AIRPORTS_V5_URL = (
    "https://www.ryanair.com/api/views/locate/5/airports/en/active"
)
ROUTES_URL_TPL = (
    "https://www.ryanair.com/api/views/locate/searchWidget/routes/en/"
    "airport/{airport_code}"
)


def _parse_v3(ap):
    iata = ap.get("iataCode", "").strip()
    coords = ap.get("coordinates", {})
    routes_raw = ap.get("routes", [])
    dest_codes = [r.split(":")[1] for r in routes_raw if r.startswith("airport:")]
    return {
        "iata": iata,
        "name": ap.get("name", ""),
        "city": ap.get("cityCode", ""),
        "country_code": ap.get("countryCode", ""),
        "country_name": "",
        "currency": ap.get("currencyCode", ""),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        "tz": ap.get("timeZone", ""),
        "routes": dest_codes,
    }


def _parse_v5(ap):
    iata = ap.get("code", "").strip()
    coords = ap.get("coordinates", {})
    country = ap.get("country", {})
    city = ap.get("city", {})
    return {
        "iata": iata,
        "name": ap.get("name", ""),
        "city": city.get("name", "") if isinstance(city, dict) else str(city),
        "country_code": country.get("code", ""),
        "country_name": country.get("name", ""),
        "currency": country.get("currency", ""),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        "tz": ap.get("timeZone", ""),
        "routes": [],
    }


def scrape_airports(session):
    """Fetch airports (and inline routes from v3). Returns list of IATA codes."""
    log.info("[%s] Fetching airports (v3 with inline routes) ...", AIRLINE)
    data = api_get(AIRPORTS_V3_URL)
    parser = _parse_v3

    if not data:
        log.info("[%s] v3 failed, trying v5 ...", AIRLINE)
        data = api_get(AIRPORTS_V5_URL)
        parser = _parse_v5

    if not data:
        log.error("[%s] Could not fetch airports from any endpoint.", AIRLINE)
        return []

    airports = []
    parsed = []
    countries_seen = set()
    route_count = 0

    for raw in data:
        ap = parser(raw)
        iata = ap["iata"]
        if not iata:
            continue

        cc = ap["country_code"]
        if cc and cc not in countries_seen:
            stmt = pg_insert(Country).values(
                code=cc,
                name=ap.get("country_name", ""),
                currency=ap["currency"],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["code"],
                set_={"name": stmt.excluded.name, "currency": stmt.excluded.currency},
            )
            session.execute(stmt)
            countries_seen.add(cc)

        stmt = pg_insert(Airport).values(
            iata_code=iata,
            name=ap["name"],
            city=ap["city"],
            country_code=cc or None,
            latitude=ap["lat"],
            longitude=ap["lon"],
            timezone=ap["tz"],
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["iata_code"],
            set_={
                "name": stmt.excluded.name,
                "city": stmt.excluded.city,
                "country_code": stmt.excluded.country_code,
                "latitude": stmt.excluded.latitude,
                "longitude": stmt.excluded.longitude,
                "timezone": stmt.excluded.timezone,
            },
        )
        session.execute(stmt)
        airports.append(iata)
        parsed.append(ap)

    session.commit()

    now = datetime.now(timezone.utc)
    for ap in parsed:
        iata = ap["iata"]
        for dest in ap["routes"]:
            stmt = pg_insert(Route).values(
                origin=iata,
                destination=dest,
                airline=AIRLINE,
                last_seen=now,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_routes_origin_destination_airline",
                set_={"last_seen": stmt.excluded.last_seen},
            )
            try:
                with session.begin_nested():
                    session.execute(stmt)
                route_count += 1
            except Exception:
                pass

    session.commit()
    log.info(
        "[%s] Stored %d airports and %d routes (from inline data).",
        AIRLINE, len(airports), route_count,
    )
    return airports


def scrape_routes(session, airports=None, force=False):
    """Fetch routes per airport (fallback when v3 inline data is unavailable)."""
    existing = session.execute(
        text("SELECT COUNT(*) FROM routes WHERE airline = :airline"),
        {"airline": AIRLINE},
    ).scalar()

    if existing > 0 and not force:
        log.info(
            "[%s] Routes already populated (%d). Skipping per-airport fetch.",
            AIRLINE, existing,
        )
        return

    if not airports:
        airports = []

    log.info(
        "[%s] Fetching routes for %d airports (per-airport) ...",
        AIRLINE, len(airports),
    )
    total = 0
    for i, origin in enumerate(airports, 1):
        url = ROUTES_URL_TPL.format(airport_code=origin)
        data = api_get(url)
        if not data:
            continue

        now = datetime.now(timezone.utc)
        for route in data:
            arrival = route.get("arrivalAirport", {})
            dest_code = arrival.get("iataCode", "") or arrival.get("code", "")
            if not dest_code:
                continue

            connecting = bool(route.get("connectingAirport"))
            new_rt = bool(route.get("newRoute"))

            stmt = pg_insert(Route).values(
                origin=origin,
                destination=dest_code,
                airline=AIRLINE,
                is_connecting=connecting,
                new_route=new_rt,
                last_seen=now,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_routes_origin_destination_airline",
                set_={
                    "is_connecting": stmt.excluded.is_connecting,
                    "new_route": stmt.excluded.new_route,
                    "last_seen": stmt.excluded.last_seen,
                },
            )
            try:
                with session.begin_nested():
                    session.execute(stmt)
                total += 1
            except Exception:
                pass

        if i % 20 == 0:
            session.commit()
            log.info(
                "[%s]   ... processed %d/%d airports (%d routes so far)",
                AIRLINE, i, len(airports), total,
            )

    session.commit()
    log.info("[%s] Stored %d routes total.", AIRLINE, total)
