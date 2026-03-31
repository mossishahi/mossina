"""Scrape Wizzair airports and routes from the map endpoint.

A single GET to /asset/map returns all cities (airports) and their
connections (routes), so both airports and routes are handled here.

Ported from the SQLite version to use PostgreSQL via SQLAlchemy sync
sessions with INSERT ... ON CONFLICT DO UPDATE upserts.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.database import Airport, Country, Route
from src.scrapers.wizzair.api import wizzair_get

log = logging.getLogger("scraper")

AIRLINE = "W6"


def scrape_airports(session):
    """Fetch all Wizzair airports and routes in one call. Returns IATA list."""
    log.info("[%s] Fetching map data (airports + routes) ...", AIRLINE)
    data = wizzair_get("/asset/map?languageCode=en-gb")

    if not data or "cities" not in data:
        log.error("[%s] Could not fetch map data.", AIRLINE)
        return []

    cities = data["cities"]

    fake_iatas = {c["iata"] for c in cities if c.get("isFakeStation")}
    log.info(
        "[%s] Filtering out %d fake/MAC stations: %s",
        AIRLINE, len(fake_iatas), sorted(fake_iatas),
    )

    airports = []
    countries_seen = set()
    route_count = 0
    now = datetime.now(timezone.utc)

    for city in cities:
        iata = city.get("iata", "").strip()
        if not iata or iata in fake_iatas:
            continue

        cc = (city.get("countryCode") or "").upper()
        country_name = city.get("countryName", "")
        currency = city.get("currencyCode", "")

        if cc and cc not in countries_seen:
            stmt = pg_insert(Country).values(
                code=cc, name=country_name, currency=currency,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["code"],
                set_={"name": stmt.excluded.name, "currency": stmt.excluded.currency},
            )
            session.execute(stmt)
            countries_seen.add(cc)

        lat = city.get("latitude")
        lon = city.get("longitude")
        name = city.get("shortName", "")

        stmt = pg_insert(Airport).values(
            iata_code=iata,
            name=name,
            city=name,
            country_code=cc or None,
            latitude=lat,
            longitude=lon,
            timezone="",
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["iata_code"],
            set_={
                "name": stmt.excluded.name,
                "city": stmt.excluded.city,
                "country_code": stmt.excluded.country_code,
                "latitude": stmt.excluded.latitude,
                "longitude": stmt.excluded.longitude,
            },
        )
        session.execute(stmt)
        airports.append(iata)

    session.commit()

    for city in cities:
        iata = city.get("iata", "").strip()
        if not iata or iata in fake_iatas:
            continue

        for conn_info in city.get("connections", []):
            dest = conn_info.get("iata", "").strip()
            if not dest or dest in fake_iatas:
                continue

            is_new = bool(conn_info.get("isNew"))
            connecting = bool(conn_info.get("isConnected"))

            stmt = pg_insert(Route).values(
                origin=iata,
                destination=dest,
                airline=AIRLINE,
                is_connecting=connecting,
                new_route=is_new,
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
                route_count += 1
            except Exception:
                pass

    session.commit()
    log.info("[%s] Stored %d airports and %d routes.", AIRLINE, len(airports), route_count)
    return airports


def scrape_routes(session, airports=None, force=False):
    """No-op for Wizzair -- routes are already loaded by scrape_airports."""
    existing = session.execute(
        text("SELECT COUNT(*) FROM routes WHERE airline = :airline"),
        {"airline": AIRLINE},
    ).scalar()

    if existing > 0:
        log.info("[%s] Routes already populated (%d) from map data.", AIRLINE, existing)
    else:
        log.warning("[%s] No routes found. Run scrape_airports first.", AIRLINE)
