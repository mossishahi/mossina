"""Scrape Wizzair flight timetables via the search/timetable endpoint.

The endpoint accepts 42-day windows and returns flights with prices.
Both outbound (A->B) and return (B->A) are requested in a single call,
halving the total number of API requests.

Uses N parallel sessions (default 4) to multiply throughput without
triggering rate limits.  Workers only do HTTP; all DB writes happen in
a single dedicated writer thread via a queue (avoids SQLite locking).

Incremental mode (default): only re-scrapes routes whose data is older
than `days_fresh` days.
"""

import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from src.scraper.wizzair.api import WizzairSession

log = logging.getLogger("scraper")

AIRLINE = "W6"
MAX_WINDOW_DAYS = 42
DEFAULT_FRESH_DAYS = 7
DEFAULT_WORKERS = 4

_SENTINEL = None


def _stale_routes(conn, days_fresh):
    """Return routes that have no schedule data or data older than days_fresh."""
    cutoff = (datetime.utcnow() - timedelta(days=days_fresh)).isoformat()
    rows = conn.execute(
        """SELECT r.origin, r.destination
           FROM routes r
           LEFT JOIN (
               SELECT origin, destination, MAX(scraped_at) AS last
               FROM schedules
               WHERE airline = ?
               GROUP BY origin, destination
           ) s ON r.origin = s.origin AND r.destination = s.destination
           WHERE r.airline = ?
             AND (s.last IS NULL OR s.last < ?)""",
        (AIRLINE, AIRLINE, cutoff),
    ).fetchall()
    return rows


def _pair_routes(routes):
    """Group directed routes into undirected pairs for bidirectional requests."""
    seen = set()
    pairs = []
    for a, b in routes:
        key = (min(a, b), max(a, b))
        if key in seen:
            continue
        seen.add(key)
        pairs.append((a, b))
    return pairs


def _parse_flights(flights, origin, dest, scraped_at):
    """Parse flight entries into (sched_rows, fare_rows) lists."""
    sched_rows = []
    fare_rows = []
    if not flights:
        return sched_rows, fare_rows

    for flight in flights:
        dep_dates = flight.get("departureDates", [])
        price_info = flight.get("price", {})
        price = price_info.get("amount")
        currency = price_info.get("currencyCode", "EUR")

        for dep_dt in dep_dates:
            parts = dep_dt.split("T")
            if len(parts) != 2:
                continue
            date_part = parts[0]
            time_part = parts[1]

            try:
                dt = datetime.strptime(date_part, "%Y-%m-%d")
            except ValueError:
                continue

            flight_id = "W6-" + time_part.replace(":", "")[:4]

            sched_rows.append((
                origin, dest, AIRLINE, dt.year, dt.month, dt.day,
                flight_id, time_part, "", "W6", scraped_at,
            ))

            if price is not None:
                fare_rows.append((
                    origin, dest, AIRLINE, date_part, "",
                    price, currency, flight_id, scraped_at,
                ))

    return sched_rows, fare_rows


def _db_writer(conn, write_q, counters):
    """Single writer thread: drains the queue and writes to SQLite."""
    batch_sched = 0
    batch_fare = 0

    while True:
        item = write_q.get()
        if item is _SENTINEL:
            conn.commit()
            break

        sched_rows, fare_rows = item

        for row in sched_rows:
            conn.execute(
                """INSERT OR REPLACE INTO schedules
                   (origin, destination, airline, year, month, day,
                    flight_number, departure_time, arrival_time,
                    carrier, scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                row,
            )
        for row in fare_rows:
            conn.execute(
                """INSERT OR REPLACE INTO fares
                   (origin, destination, airline, departure_date,
                    arrival_date, price, currency, flight_number,
                    scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                row,
            )

        batch_sched += len(sched_rows)
        batch_fare += len(fare_rows)

        if batch_sched >= 500:
            conn.commit()
            counters["sched"] += batch_sched
            counters["fare"] += batch_fare
            batch_sched = 0
            batch_fare = 0

    counters["sched"] += batch_sched
    counters["fare"] += batch_fare


def _worker(worker_id, my_pairs, windows, scraped_at, write_q,
            counters_lock, counters, shared_api_base):
    """Worker thread: HTTP only, pushes parsed results to write_q."""
    sess = WizzairSession(worker_id=worker_id, shared_api_base=shared_api_base)
    local_errors = 0

    for a, b in my_pairs:
        for date_from, date_to in windows:
            payload = {
                "flightList": [
                    {"departureStation": a, "arrivalStation": b,
                     "from": date_from, "to": date_to},
                    {"departureStation": b, "arrivalStation": a,
                     "from": date_from, "to": date_to},
                ],
                "priceType": "regular",
                "adultCount": 1,
                "childCount": 0,
                "infantCount": 0,
            }

            try:
                data = sess.post("/search/timetable", payload)
            except Exception as exc:
                log.debug("[W6-w%d] Error %s<->%s: %s", worker_id, a, b, exc)
                local_errors += 1
                continue

            if not data:
                continue

            s1, f1 = _parse_flights(
                data.get("outboundFlights"), a, b, scraped_at)
            s2, f2 = _parse_flights(
                data.get("returnFlights"), b, a, scraped_at)

            all_sched = s1 + s2
            all_fare = f1 + f2
            if all_sched or all_fare:
                write_q.put((all_sched, all_fare))

    with counters_lock:
        counters["errors"] += local_errors
        counters["done"] += len(my_pairs)


def scrape_schedules(conn, limit=None, days_fresh=DEFAULT_FRESH_DAYS,
                     num_windows=4, workers=DEFAULT_WORKERS, **_kwargs):
    """Fetch timetable data for Wizzair routes using parallel sessions.

    Args:
        limit:      max route-pairs to scrape (None = all stale)
        days_fresh: skip routes scraped within this many days (0 = force all)
        num_windows: number of 42-day windows to cover (4 = ~168 days)
        workers:    number of parallel sessions (default 4)
    """
    if days_fresh > 0:
        routes = _stale_routes(conn, days_fresh)
    else:
        routes = conn.execute(
            "SELECT origin, destination FROM routes WHERE airline = ?",
            (AIRLINE,),
        ).fetchall()

    total_routes = conn.execute(
        "SELECT COUNT(*) FROM routes WHERE airline = ?", (AIRLINE,)
    ).fetchone()[0]

    pairs = _pair_routes(routes)

    if limit:
        pairs = pairs[:limit]

    if not pairs:
        log.info("[%s] All %d routes are fresh (within %d days). Nothing to do.",
                 AIRLINE, total_routes, days_fresh)
        return

    now = datetime.utcnow()
    scraped_at = now.isoformat()

    windows = []
    for period in range(num_windows):
        start = now + timedelta(days=period * MAX_WINDOW_DAYS)
        end = now + timedelta(days=(period + 1) * MAX_WINDOW_DAYS)
        windows.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))

    n_workers = min(workers, len(pairs))
    api_calls = len(pairs) * len(windows)

    log.info(
        "[%s] Fetching timetables: %d pairs (%d routes, %d total) x %d windows "
        "= ~%d API calls, %d parallel workers",
        AIRLINE, len(pairs), len(routes), total_routes, len(windows),
        api_calls, n_workers,
    )

    probe = WizzairSession(worker_id=99)
    shared_api_base = probe._base()
    log.info("[%s] API base: %s", AIRLINE, shared_api_base)

    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {"sched": 0, "fare": 0, "errors": 0, "done": 0}
    t0 = time.time()

    writer = threading.Thread(
        target=_db_writer, args=(conn, write_q, counters), daemon=True,
    )
    writer.start()

    chunks = [pairs[i::n_workers] for i in range(n_workers)]

    def _progress():
        while counters["done"] < len(pairs):
            time.sleep(30)
            elapsed = time.time() - t0
            done = counters["done"]
            if done == 0:
                continue
            rate = done / elapsed
            remaining = (len(pairs) - done) / rate if rate > 0 else 0
            log.info(
                "[%s]   %d/%d pairs  %d sched  %d fares  %d err  "
                "%.1f pairs/min  ETA %.0fm",
                AIRLINE, done, len(pairs),
                counters["sched"], counters["fare"], counters["errors"],
                rate * 60, remaining / 60,
            )

    progress_thread = threading.Thread(target=_progress, daemon=True)
    progress_thread.start()

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(
                _worker, i, chunks[i], windows, scraped_at,
                write_q, counters_lock, counters, shared_api_base,
            )
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

    elapsed = time.time() - t0
    log.info(
        "[%s] Done in %.1fm: %d schedules, %d fares stored (%d errors).",
        AIRLINE, elapsed / 60,
        counters["sched"], counters["fare"], counters["errors"],
    )
