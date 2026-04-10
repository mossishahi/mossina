"""Scrape cheapest one-way Ryanair fares.

Multi-threaded: each worker handles a chunk of airports, writes results
to a shared queue consumed by a single DB writer thread.
"""

import logging
import queue
import threading
import time as _time
from datetime import datetime, timedelta, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Fare, SessionLocal

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_WORKERS = 8
_SENTINEL = None

SERVICES_URL = "https://services-api.ryanair.com"
FARES_URL = SERVICES_URL + "/farfnd/v4/oneWayFares"
FARES_FALLBACK_URL = SERVICES_URL + "/farfnd/3/oneWayFares"


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


def _worker(worker_id, my_airports, date_from, date_to, scraped_at, write_q,
            counters_lock, counters):
    """Worker thread: fetches fares for its airports, pushes to queue."""
    for origin in my_airports:
        params = {
            "departureAirportIataCode": origin,
            "language": "en",
            "market": "en-gb",
            "offset": 0,
            "limit": 200,
            "outboundDepartureDateFrom": date_from,
            "outboundDepartureDateTo": date_to,
            "priceValueTo": 1000,
        }
        data = api_get(FARES_URL, params=params)
        if not data or not data.get("fares"):
            data = api_get(FARES_FALLBACK_URL, params=params)
        if not data:
            with counters_lock:
                counters["done"] += 1
            continue

        batch = []
        for fare in data.get("fares", []):
            outbound = fare.get("outbound", {})
            dep_str = outbound.get("departureDate", "").split(".")[0]
            arr_str = outbound.get("arrivalDate", "").split(".")[0]
            fn = outbound.get("flightNumber", "").replace(" ", "")
            dest_code = outbound.get("arrivalAirport", {}).get("iataCode", "")
            price_info = outbound.get("price", {})
            price = price_info.get("value")
            currency = price_info.get("currencyCode", "EUR")

            if not dest_code or price is None:
                continue

            dep_date = datetime.fromisoformat(dep_str).date() if dep_str else None
            arr_date = datetime.fromisoformat(arr_str).date() if arr_str else None

            batch.append({
                "origin": origin,
                "destination": dest_code,
                "airline": AIRLINE,
                "departure_date": dep_date,
                "arrival_date": arr_date,
                "price": price,
                "currency": currency,
                "flight_number": fn,
                "scraped_at": scraped_at,
            })

        if batch:
            write_q.put(batch)

        with counters_lock:
            counters["done"] += 1
            done = counters["done"]
            total_airports = counters["airports"]
            total_fares = counters["total"]
            if done % 20 == 0 or done == total_airports:
                elapsed = _time.monotonic() - counters["t0"]
                rate = done / (elapsed / 60) if elapsed > 0 else 0
                remaining = total_airports - done
                eta = remaining / rate if rate > 0 else 0
                log.info(
                    "[%s]   %d/%d airports  %d fares  %.0f airports/min  ETA %.1fm",
                    AIRLINE, done, total_airports, total_fares, rate, eta,
                )


def scrape_fares(session, airports, limit=None, workers=DEFAULT_WORKERS):
    """Fetch cheapest one-way fares from each airport for the next ~6 months."""
    now = datetime.now(timezone.utc)
    date_from = now.strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=180)).strftime("%Y-%m-%d")
    scraped_at = now

    if limit:
        airports = airports[:limit]

    n_workers = min(workers, len(airports))

    log.info("[%s] Fetching fares for %d airports (%s to %s), %d workers ...",
             AIRLINE, len(airports), date_from, date_to, n_workers)

    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {"done": 0, "total": 0, "airports": len(airports),
                "t0": _time.monotonic()}

    writer = threading.Thread(target=_db_writer, args=(write_q, counters),
                              daemon=True)
    writer.start()

    chunks = [airports[i::n_workers] for i in range(n_workers)]
    threads = []
    for i in range(n_workers):
        t = threading.Thread(
            target=_worker,
            args=(i, chunks[i], date_from, date_to, scraped_at, write_q,
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
