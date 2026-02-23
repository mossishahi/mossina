"""Scrape Ryanair airports and inline route data."""

import logging
import sqlite3
from datetime import datetime, timezone

from src.api import api_get

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


def scrape_airports(conn):
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
    countries_seen = set()
    route_count = 0

    for raw in data:
        ap = parser(raw)
        iata = ap["iata"]
        if not iata:
            continue

        cc = ap["country_code"]
        if cc and cc not in countries_seen:
            conn.execute(
                "INSERT OR REPLACE INTO countries (code, name, currency) "
                "VALUES (?, ?, ?)",
                (cc, ap.get("country_name", ""), ap["currency"]),
            )
            countries_seen.add(cc)

        conn.execute(
            """INSERT OR REPLACE INTO airports
               (iata_code, name, city, country_code, latitude, longitude, timezone)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (iata, ap["name"], ap["city"], cc, ap["lat"], ap["lon"], ap["tz"]),
        )
        airports.append(iata)

        now = datetime.now(timezone.utc).isoformat()
        for dest in ap["routes"]:
            try:
                conn.execute(
                    """INSERT INTO routes (origin, destination, airline, last_seen)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(origin, destination, airline)
                       DO UPDATE SET last_seen = excluded.last_seen""",
                    (iata, dest, AIRLINE, now),
                )
                route_count += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    log.info("[%s] Stored %d airports and %d routes (from inline data).", AIRLINE, len(airports), route_count)
    return airports


def scrape_routes(conn, airports, force=False):
    """Fetch routes per airport (fallback when v3 inline data is unavailable)."""
    existing = conn.execute(
        "SELECT COUNT(*) FROM routes WHERE airline = ?", (AIRLINE,)
    ).fetchone()[0]
    if existing > 0 and not force:
        log.info("[%s] Routes already populated (%d). Skipping per-airport fetch.", AIRLINE, existing)
        return

    log.info("[%s] Fetching routes for %d airports (per-airport) ...", AIRLINE, len(airports))
    total = 0
    for i, origin in enumerate(airports, 1):
        url = ROUTES_URL_TPL.format(airport_code=origin)
        data = api_get(url)
        if not data:
            continue

        now = datetime.now(timezone.utc).isoformat()
        for route in data:
            arrival = route.get("arrivalAirport", {})
            dest_code = arrival.get("iataCode", "") or arrival.get("code", "")
            if not dest_code:
                continue
            connecting = 1 if route.get("connectingAirport") else 0
            new_rt = 1 if route.get("newRoute") else 0
            seasonal = 1 if route.get("seasonalRoute") else 0
            try:
                conn.execute(
                    """INSERT INTO routes
                       (origin, destination, airline, is_connecting, new_route, seasonal_route, last_seen)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(origin, destination, airline)
                       DO UPDATE SET is_connecting = excluded.is_connecting,
                                     new_route = excluded.new_route,
                                     seasonal_route = excluded.seasonal_route,
                                     last_seen = excluded.last_seen""",
                    (origin, dest_code, AIRLINE, connecting, new_rt, seasonal, now),
                )
                total += 1
            except sqlite3.IntegrityError:
                pass

        if i % 20 == 0:
            conn.commit()
            log.info("[%s]   ... processed %d/%d airports (%d routes so far)", AIRLINE, i, len(airports), total)

    conn.commit()
    log.info("[%s] Stored %d routes total.", AIRLINE, total)
