"""Scrape Ryanair fares via the per-route cheapestPerDay endpoint.

The old per-airport oneWayFares endpoint was unreliable: it returned only
a small subset of fares per airport and prices often diverged from the
website. The cheapestPerDay endpoint, called per O-D route per month,
returns prices that match the Ryanair website exactly.

Multi-threaded: each worker handles a chunk of route-months, writes
results to a shared queue consumed by a single DB writer thread.
"""

import logging
import queue
import threading
import time as _time
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Fare, SessionLocal

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_WORKERS = 8
DEFAULT_MONTHS = 6
_SENTINEL = None

SERVICES_URL = "https://services-api.ryanair.com"
CHEAPEST_PER_DAY_URL = SERVICES_URL + "/farfnd/3/oneWayFares/{origin}/{dest}/cheapestPerDay"


def _month_starts(num_months):
    """Return list of (year, month) tuples for the next num_months, starting now."""
    today = date.today()
    out = []
    y, m = today.year, today.month
    for _ in range(num_months):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


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
                stmt = pg_insert(Fare).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["origin", "destination", "airline",
                                    "departure_date", "flight_number"],
                    set_=dict(
                        price=stmt.excluded.price,
                        currency=stmt.excluded.currency,
                        arrival_date=stmt.excluded.arrival_date,
                        scraped_at=stmt.excluded.scraped_at,
                    ),
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


def _worker(worker_id, my_tasks, scraped_at, write_q, counters_lock, counters):
    """Worker thread: fetches cheapestPerDay for each (origin, dest, month)."""
    for origin, dest, year, month in my_tasks:
        url = CHEAPEST_PER_DAY_URL.format(origin=origin, dest=dest)
        params = {
            "outboundMonthOfDate": f"{year:04d}-{month:02d}-01",
            "market": "en-gb",
        }
        data = api_get(url, params=params)

        with counters_lock:
            counters["done"] += 1

        if not data:
            continue

        fares = (data.get("outbound") or {}).get("fares") or []
        batch = []
        for f in fares:
            day_str = f.get("day")
            if not day_str:
                continue
            if f.get("unavailable") or f.get("soldOut"):
                continue
            price_info = f.get("price") or {}
            price = price_info.get("value")
            if price is None:
                continue
            currency = price_info.get("currencyCode", "EUR")

            try:
                dep_date = date.fromisoformat(day_str)
            except ValueError:
                continue

            batch.append({
                "origin": origin,
                "destination": dest,
                "airline": AIRLINE,
                "departure_date": dep_date,
                "arrival_date": None,
                "price": price,
                "currency": currency,
                "flight_number": "FR",
                "scraped_at": scraped_at,
            })

        if batch:
            write_q.put(batch)

        with counters_lock:
            done = counters["done"]
            total_tasks = counters["tasks"]
            total_fares = counters["total"]
            if done % 200 == 0 or done == total_tasks:
                elapsed = _time.monotonic() - counters["t0"]
                rate = done / (elapsed / 60) if elapsed > 0 else 0
                remaining = total_tasks - done
                eta = remaining / rate if rate > 0 else 0
                log.info(
                    "[%s]   %d/%d route-months  %d fares  %.0f/min  ETA %.1fm",
                    AIRLINE, done, total_tasks, total_fares, rate, eta,
                )


def scrape_fares(session, airports=None, limit=None, workers=DEFAULT_WORKERS,
                 months=DEFAULT_MONTHS, **_kw):
    """Fetch cheapest fares for every route for the next ~6 months.

    Uses cheapestPerDay endpoint per-route. `airports` arg is accepted
    for API compatibility with other airlines but is not used: routes
    come directly from the database.
    """
    now = datetime.now(timezone.utc)
    scraped_at = now

    routes = session.execute(
        text("SELECT origin, destination FROM routes WHERE airline = :a"),
        {"a": AIRLINE},
    ).fetchall()

    if limit:
        routes = routes[:limit]

    month_list = _month_starts(months)
    tasks = [(o, d, y, m) for (o, d) in routes for (y, m) in month_list]

    if not tasks:
        log.warning("[%s] No routes found — skipping fares scrape.", AIRLINE)
        return

    n_workers = min(workers, len(tasks))

    log.info(
        "[%s] Fetching cheapestPerDay for %d routes x %d months = %d calls, %d workers ...",
        AIRLINE, len(routes), len(month_list), len(tasks), n_workers,
    )

    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {"done": 0, "total": 0, "tasks": len(tasks),
                "t0": _time.monotonic()}

    writer = threading.Thread(target=_db_writer, args=(write_q, counters),
                              daemon=True)
    writer.start()

    chunks = [tasks[i::n_workers] for i in range(n_workers)]
    threads = []
    for i in range(n_workers):
        t = threading.Thread(
            target=_worker,
            args=(i, chunks[i], scraped_at, write_q,
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
        "[%s] Done in %.1fm: %d fare entries stored.",
        AIRLINE, elapsed / 60, counters["total"],
    )
