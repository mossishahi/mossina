"""Scrape Ryanair flight timetables / schedules.

Multi-threaded: each worker handles a chunk of routes, writes results
to a shared queue consumed by a single DB writer thread.
"""

import logging
import queue
import threading
import time as _time
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get, REQUEST_DELAY
from src.database import Schedule, SessionLocal

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_FRESH_DAYS = 7
DEFAULT_WORKERS = 6
_SENTINEL = None

SERVICES_URL = "https://services-api.ryanair.com"
SCHEDULE_URL_TPL = (
    "{base}/timtbl/3/schedules/{origin}/{dest}/years/{year}/months/{month}"
)


def _parse_time(s):
    if not s or not s.strip():
        return None
    try:
        parts = s.strip().split(":")
        return time(int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return None


def _stale_routes(session, days_fresh):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_fresh)
    rows = session.execute(
        text(
            "SELECT r.origin, r.destination "
            "FROM routes r "
            "LEFT JOIN ("
            "  SELECT origin, destination, MAX(scraped_at) AS last "
            "  FROM schedules WHERE airline = :airline "
            "  GROUP BY origin, destination"
            ") s ON r.origin = s.origin AND r.destination = s.destination "
            "WHERE r.airline = :airline "
            "  AND (s.last IS NULL OR s.last < :cutoff)"
        ),
        {"airline": AIRLINE, "cutoff": cutoff},
    ).fetchall()
    return rows


def _db_writer(write_q, counters):
    """Single writer thread: drains the queue and upserts into PostgreSQL."""
    session = SessionLocal()
    pending = 0
    try:
        while True:
            item = write_q.get()
            if item is _SENTINEL:
                session.commit()
                break
            for row in item:
                stmt = pg_insert(Schedule).values(**row)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_schedules_route_date_flight",
                    set_={
                        "departure_time": stmt.excluded.departure_time,
                        "arrival_time": stmt.excluded.arrival_time,
                        "scraped_at": stmt.excluded.scraped_at,
                    },
                )
                try:
                    session.execute(stmt)
                except Exception:
                    session.rollback()
            counters["total"] += len(item)
            pending += len(item)
            if pending >= 500:
                session.commit()
                pending = 0
    except Exception:
        log.exception("[%s] Writer thread error", AIRLINE)
        session.rollback()
    finally:
        session.close()


def _worker(worker_id, my_routes, months, scraped_at, write_q,
            counters_lock, counters):
    """Worker thread: fetches schedules for its routes, pushes to queue."""
    for origin, dest in my_routes:
        batch = []
        for year, month in months:
            url = SCHEDULE_URL_TPL.format(
                base=SERVICES_URL, origin=origin, dest=dest,
                year=year, month=month,
            )
            data = api_get(url, delay=REQUEST_DELAY)
            if not data:
                continue
            for day_info in data.get("days", []):
                day_num = day_info.get("day")
                for flight in day_info.get("flights", []):
                    fn = flight.get("number", "")
                    dep = flight.get("departureTime", "")
                    arr = flight.get("arrivalTime", "")
                    try:
                        dep_date = date(year, month, day_num)
                    except (TypeError, ValueError):
                        continue
                    batch.append({
                        "origin": origin,
                        "destination": dest,
                        "airline": AIRLINE,
                        "departure_date": dep_date,
                        "flight_number": fn,
                        "departure_time": _parse_time(dep),
                        "arrival_time": _parse_time(arr),
                        "scraped_at": scraped_at,
                    })
        if batch:
            write_q.put(batch)
        with counters_lock:
            counters["done"] += 1
            done = counters["done"]
            total_routes = counters["routes"]
            total_flights = counters["total"]
            if done % 50 == 0 or done == total_routes:
                elapsed = _time.monotonic() - counters["t0"]
                rate = done / (elapsed / 60) if elapsed > 0 else 0
                remaining = total_routes - done
                eta = remaining / rate if rate > 0 else 0
                log.info(
                    "[%s]   %d/%d routes  %d flights  %.0f routes/min  ETA %.0fm",
                    AIRLINE, done, total_routes, total_flights, rate, eta,
                )


def scrape_schedules(session, limit=None, days_fresh=DEFAULT_FRESH_DAYS,
                     workers=DEFAULT_WORKERS, **_kw):
    """Fetch timetable data for Ryanair routes over the next 3 months."""
    now = datetime.now(timezone.utc)
    months = []
    for offset in range(3):
        dt = now + timedelta(days=30 * offset)
        months.append((dt.year, dt.month))

    total_routes = session.execute(
        text("SELECT COUNT(*) FROM routes WHERE airline = :airline"),
        {"airline": AIRLINE},
    ).scalar()

    if days_fresh > 0:
        routes = _stale_routes(session, days_fresh)
    else:
        routes = session.execute(
            text("SELECT origin, destination FROM routes WHERE airline = :airline"),
            {"airline": AIRLINE},
        ).fetchall()

    if not routes:
        log.info(
            "[%s] All %d routes are fresh (within %d days). Nothing to do.",
            AIRLINE, total_routes, days_fresh,
        )
        return

    if limit:
        routes = routes[:limit]

    n_workers = min(workers, len(routes))
    scraped_at = now

    log.info(
        "[%s] Fetching schedules for %d/%d routes x %d months, %d workers ...",
        AIRLINE, len(routes), total_routes, len(months), n_workers,
    )

    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {"done": 0, "total": 0, "routes": len(routes),
                "t0": _time.monotonic()}

    writer = threading.Thread(target=_db_writer, args=(write_q, counters),
                              daemon=True)
    writer.start()

    chunks = [routes[i::n_workers] for i in range(n_workers)]
    threads = []
    for i in range(n_workers):
        t = threading.Thread(
            target=_worker,
            args=(i, chunks[i], months, scraped_at, write_q,
                  counters_lock, counters),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    write_q.put(_SENTINEL)
    writer.join()
    session.commit()

    elapsed = _time.monotonic() - counters["t0"]
    log.info(
        "[%s] Done in %.1fm: %d schedule entries stored.",
        AIRLINE, elapsed / 60, counters["total"],
    )
