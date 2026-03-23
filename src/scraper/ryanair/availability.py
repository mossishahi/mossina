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
import queue
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DEFAULT_WORKERS = 6
_SENTINEL = None


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


def _fetch_route(origin, dest, now, end_date, scraped_at, stop_event):
    """Fetch all availability windows for one route. Returns list of fare tuples."""
    fares = []
    cursor = now
    while cursor < end_date:
        if stop_event and stop_event.is_set():
            break
        date_out = cursor.strftime("%Y-%m-%d")
        data = api_get(AVAILABILITY_URL, delay=0.3, stop_event=stop_event, params={
            "ADT": 1, "CHD": 0, "DateOut": date_out,
            "Destination": dest, "FlexDaysOut": FLEX_DAYS,
            "INF": 0, "IncludeConnectingFlights": "false",
            "Origin": origin, "RoundTrip": "false",
            "TEEN": 0, "ToUs": "AGREED",
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
                    price = None
                    for f in reg.get("fares", []):
                        if f.get("type") == "ADT":
                            price = f.get("amount")
                            break
                    if price is None:
                        continue
                    fn = (flight.get("flightNumber") or "").replace(" ", "")
                    times = flight.get("time", [])
                    dep_dt = times[0] if len(times) > 0 else ""
                    arr_dt = times[1] if len(times) > 1 else ""
                    fares.append((origin, dest, AIRLINE, dep_dt, arr_dt,
                                  price, currency, fn, scraped_at))
        cursor += timedelta(days=FLEX_DAYS + 1)
    return fares


def _db_writer(conn, write_q, counters):
    """Single-threaded DB writer consuming from the queue."""
    while True:
        item = write_q.get()
        if item is _SENTINEL:
            break
        kind = item[0]
        if kind == "_fares":
            _, fare_rows = item
            for row in fare_rows:
                conn.execute(
                    """INSERT OR REPLACE INTO fares
                       (origin, destination, airline,
                        departure_date, arrival_date,
                        price, currency, flight_number, scraped_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", row)
            counters["fares"] += len(fare_rows)
        elif kind == "_log":
            _, airline, origin, dest, endpoint, duration_ms = item
            try:
                log_api_fetch(conn, airline, origin, dest, endpoint, duration_ms)
            except Exception:
                pass
        if counters["done"] % 20 == 0:
            conn.commit()
    conn.commit()


def _worker(my_routes, now, end_date, scraped_at, write_q,
            counters_lock, counters, stop_event, on_progress):
    """Worker thread: HTTP only, pushes results to write_q."""
    for origin, dest in my_routes:
        if stop_event and stop_event.is_set():
            break
        route_t0 = _time.monotonic()
        fares = _fetch_route(origin, dest, now, end_date, scraped_at, stop_event)
        route_ms = (_time.monotonic() - route_t0) * 1000

        if fares:
            write_q.put(("_fares", fares))
        write_q.put(("_log", AIRLINE, origin, dest, "availability", route_ms))

        with counters_lock:
            counters["done"] += 1
            done, total, total_fares = counters["done"], counters["total"], counters["fares"]
            if done % 50 == 0 or done == total:
                log.info("[%s] Progress: %d/%d routes, %d fares collected",
                         AIRLINE, done, total, total_fares)
            if on_progress:
                on_progress(done, total, total_fares, route_ms)


def scrape_availability(conn, origins=None, limit=None, days_fresh=DEFAULT_FRESH_DAYS,
                        on_progress=None, stop_event=None, workers=DEFAULT_WORKERS):
    """Fetch per-route daily fares for Ryanair routes over the next 3 months.

    Args:
        origins:      only scrape routes departing from these airports
        limit:        max number of routes to process
        days_fresh:   skip routes fetched within this many days (0 = force all)
        on_progress:  optional callback ``fn(done, total, fares, route_ms)``
        stop_event:   a ``threading.Event``; when set the scraper saves
                      progress and returns early
        workers:      number of parallel HTTP threads (default 6)
    """
    now = datetime.utcnow()
    scraped_at = now.isoformat()

    total_routes = conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT origin, destination FROM routes WHERE airline = ?)",
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
    end_date = now + timedelta(days=90)
    n_workers = min(workers, len(routes))

    log.info(
        "[%s] Fetching availability fares for %d/%d routes (%s to %s), %d workers ...",
        AIRLINE, len(routes), total_routes, today,
        end_date.strftime("%Y-%m-%d"), n_workers,
    )

    t0 = _time.monotonic()
    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {"fares": 0, "done": 0, "total": len(routes)}

    writer = threading.Thread(
        target=_db_writer, args=(conn, write_q, counters), daemon=True)
    writer.start()

    chunks = [routes[i::n_workers] for i in range(n_workers)]

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(_worker, chunks[i], now, end_date, scraped_at,
                        write_q, counters_lock, counters, stop_event,
                        on_progress)
            for i in range(n_workers)
        ]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                log.exception("[%s] Worker failed", AIRLINE)

    write_q.put(_SENTINEL)
    writer.join()
    conn.commit()

    elapsed = _time.monotonic() - t0
    done = counters["done"]
    fares = counters["fares"]
    elapsed_min = elapsed / 60
    if stop_event and stop_event.is_set():
        log.info("[%s] Stopped early after %d/%d routes (%d fares saved) in %.1f min.",
                 AIRLINE, done, len(routes), fares, elapsed_min)
    else:
        log.info("[%s] DONE — %d routes, %d fares stored in %.1f min.",
                 AIRLINE, done, fares, elapsed_min)
