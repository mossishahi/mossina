"""Scrape Wizzair airports and routes from the map endpoint.

A single GET to /asset/map returns all cities (airports) and their
connections (routes), so both airports and routes are handled here.
"""

import logging
import sqlite3
from datetime import datetime, timezone

from src.scraper.wizzair.api import wizzair_get

log = logging.getLogger("scraper")

AIRLINE = "W6"


def scrape_airports(conn):
    """Fetch all Wizzair airports and routes in one call. Returns list of IATA codes."""
    log.info("[%s] Fetching map data (airports + routes) ...", AIRLINE)
    data = wizzair_get("/asset/map?languageCode=en-gb")

    if not data or "cities" not in data:
        log.error("[%s] Could not fetch map data.", AIRLINE)
        return []

    cities = data["cities"]

    fake_iatas = {c["iata"] for c in cities if c.get("isFakeStation")}
    log.info("[%s] Filtering out %d fake/MAC stations: %s", AIRLINE, len(fake_iatas), sorted(fake_iatas))

    airports = []
    countries_seen = set()
    route_count = 0
    now = datetime.now(timezone.utc).isoformat()

    # Pass 1: insert all airports so FK references are satisfied.
    for city in cities:
        iata = city.get("iata", "").strip()
        if not iata or iata in fake_iatas:
            continue

        cc = (city.get("countryCode") or "").upper()
        country_name = city.get("countryName", "")
        currency = city.get("currencyCode", "")

        if cc and cc not in countries_seen:
            conn.execute(
                "INSERT OR REPLACE INTO countries (code, name, currency) "
                "VALUES (?, ?, ?)",
                (cc, country_name, currency),
            )
            countries_seen.add(cc)

        lat = city.get("latitude")
        lon = city.get("longitude")
        name = city.get("shortName", "")

        conn.execute(
            """INSERT OR REPLACE INTO airports
               (iata_code, name, city, country_code, latitude, longitude, timezone)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (iata, name, name, cc, lat, lon, ""),
        )
        airports.append(iata)

    conn.commit()

    # Pass 2: insert routes (all destination airports now exist).
    for city in cities:
        iata = city.get("iata", "").strip()
        if not iata or iata in fake_iatas:
            continue

        for conn_info in city.get("connections", []):
            dest = conn_info.get("iata", "").strip()
            if not dest or dest in fake_iatas:
                continue
            is_new = 1 if conn_info.get("isNew") else 0
            connecting = 1 if conn_info.get("isConnected") else 0
            try:
                conn.execute(
                    """INSERT INTO routes (origin, destination, airline, is_connecting, new_route, last_seen)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(origin, destination, airline)
                       DO UPDATE SET is_connecting = excluded.is_connecting,
                                     new_route = excluded.new_route,
                                     last_seen = excluded.last_seen""",
                    (iata, dest, AIRLINE, connecting, is_new, now),
                )
                route_count += 1
            except sqlite3.IntegrityError:
                pass

    conn.commit()
    log.info("[%s] Stored %d airports and %d routes.", AIRLINE, len(airports), route_count)
    return airports


def scrape_routes(conn, airports, force=False):
    """No-op for Wizzair -- routes are already loaded by scrape_airports."""
    existing = conn.execute(
        "SELECT COUNT(*) FROM routes WHERE airline = ?", (AIRLINE,)
    ).fetchone()[0]
    if existing > 0:
        log.info("[%s] Routes already populated (%d) from map data.", AIRLINE, existing)
    else:
        log.warning("[%s] No routes found. Run scrape_airports first.", AIRLINE)
