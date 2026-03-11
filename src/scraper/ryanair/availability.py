"""Scrape per-route, per-day Ryanair fares via the booking availability API.

The farfnd endpoint only returns the globally cheapest fares across all
destinations, missing many routes.  This module queries the availability
endpoint for each route in the database, capturing the actual fare for
every operating day.

API: /api/booking/v4/en-gb/availability
  - Returns up to FlexDaysOut+1 days per call
  - Provides regularFare per flight per day
"""

import logging
import time as _time
from datetime import datetime, timedelta

from src.api import api_get
from src.db import log_api_fetch

log = logging.getLogger("scraper")

AIRLINE = "FR"

AVAILABILITY_URL = (
    "https://www.ryanair.com/api/booking/v4/en-gb/availability"
)

FLEX_DAYS = 6  # 0-indexed; max accepted by the API (covers 7 days per call)
DEFAULT_FRESH_DAYS = 1


def _stale_routes(conn, origins, days_fresh):
    """Return routes with no availability fetch or fetch older than days_fresh."""
    cutoff = (datetime.utcnow() - timedelta(days=days_fresh)).isoformat()
    query = """
        SELECT DISTINCT r.origin, r.destination
        FROM routes r
        LEFT JOIN (
            SELECT origin, destination, MAX(fetched_at) AS last
            FROM api_fetch_log
            WHERE airline = ? AND endpoint = 'availability'
            GROUP BY origin, destination
        ) l ON r.origin = l.origin AND r.destination = l.destination
        WHERE r.airline = ?
          AND (l.last IS NULL OR l.last < ?)
    """
    params = [AIRLINE, AIRLINE, cutoff]
    if origins:
        placeholders = ",".join("?" for _ in origins)
        query += (f" AND (r.origin IN ({placeholders})"
                  f" OR r.destination IN ({placeholders}))")
        params.extend(origins)
        params.extend(origins)
    return conn.execute(query, params).fetchall()


def scrape_availability(conn, origins=None, limit=None, days_fresh=DEFAULT_FRESH_DAYS,
                        on_progress=None, stop_event=None):
    """Fetch per-route daily fares for Ryanair routes over the next 3 months.

    Args:
        origins:      only scrape routes departing from these airports
        limit:        max number of routes to process
        days_fresh:   skip routes fetched within this many days (0 = force all)
        on_progress:  optional callback ``fn(done, total, fares, route_ms)``
        stop_event:   a ``threading.Event``; when set the scraper saves
                      progress and returns early
    """
    now = datetime.utcnow()
    scraped_at = now.isoformat()

    total_routes = conn.execute(
        "SELECT COUNT(DISTINCT origin, destination) FROM routes WHERE airline = ?",
        (AIRLINE,)
    ).fetchone()[0]

    if days_fresh > 0:
        routes = _stale_routes(conn, origins, days_fresh)
    else:
        query = "SELECT DISTINCT origin, destination FROM routes WHERE airline = ?"
        params = [AIRLINE]
        if origins:
            placeholders = ",".join("?" for _ in origins)
            query += (f" AND (origin IN ({placeholders})"
                      f" OR destination IN ({placeholders}))")
            params.extend(origins)
            params.extend(origins)
        routes = conn.execute(query, params).fetchall()

    if not routes:
        log.info("[%s] All %d routes are fresh (within %d days). Nothing to do.",
                 AIRLINE, total_routes, days_fresh)
        return

    if limit:
        routes = routes[:limit]

    today = now.strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=90))

    log.info(
        "[%s] Fetching availability fares for %d/%d routes (%s to %s) ...",
        AIRLINE, len(routes), total_routes, today, end_date.strftime("%Y-%m-%d"),
    )

    total = 0
    stopped = False
    for i, (origin, dest) in enumerate(routes, 1):
        if stop_event and stop_event.is_set():
            stopped = True
            break
        route_t0 = _time.monotonic()
        cursor = now
        while cursor < end_date:
            if stop_event and stop_event.is_set():
                break
            date_out = cursor.strftime("%Y-%m-%d")
            data = api_get(AVAILABILITY_URL, delay=0.3, stop_event=stop_event, params={
                "ADT": 1,
                "CHD": 0,
                "DateOut": date_out,
                "Destination": dest,
                "FlexDaysOut": FLEX_DAYS,
                "INF": 0,
                "IncludeConnectingFlights": "false",
                "Origin": origin,
                "RoundTrip": "false",
                "TEEN": 0,
                "ToUs": "AGREED",
            })
            if not data:
                cursor += timedelta(days=FLEX_DAYS + 1)
                continue

            currency = data.get("currency", "EUR")

            for trip in data.get("trips", []):
                for date_entry in trip.get("dates", []):
                    for flight in date_entry.get("flights", []):
                        reg = flight.get("regularFare")
                        if not reg:
                            continue

                        fares_list = reg.get("fares", [])
                        price = None
                        for f in fares_list:
                            if f.get("type") == "ADT":
                                price = f.get("amount")
                                break

                        if price is None:
                            continue

                        fn = (flight.get("flightNumber") or "").replace(" ", "")
                        times = flight.get("time", [])
                        dep_dt = times[0] if len(times) > 0 else ""
                        arr_dt = times[1] if len(times) > 1 else ""

                        conn.execute(
                            """INSERT OR REPLACE INTO fares
                               (origin, destination, airline,
                                departure_date, arrival_date,
                                price, currency, flight_number, scraped_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (origin, dest, AIRLINE,
                             dep_dt, arr_dt,
                             price, currency, fn, scraped_at),
                        )
                        total += 1

            cursor += timedelta(days=FLEX_DAYS + 1)

        route_ms = (_time.monotonic() - route_t0) * 1000
        try:
            log_api_fetch(conn, AIRLINE, origin, dest,
                          "availability", route_ms)
        except Exception:
            pass

        if on_progress:
            on_progress(i, len(routes), total, route_ms)

        if i % 20 == 0:
            conn.commit()
            log.info(
                "[%s]   ... availability: %d/%d routes (%d fares)",
                AIRLINE, i, len(routes), total,
            )

    conn.commit()
    if stopped:
        log.info("[%s] Stopped early after %d/%d routes (%d fares saved).",
                 AIRLINE, i - 1, len(routes), total)
    else:
        log.info("[%s] Stored %d availability fare entries.", AIRLINE, total)
