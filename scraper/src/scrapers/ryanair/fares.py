"""Scrape per-day Ryanair fares using the cheapestPerDay endpoint.

The older `/farfnd/v4/oneWayFares` endpoint returns ONE fare per
destination per wide date-range call -- the single cheapest day across
the whole window. That made it impossible to refresh prices for any
specific day, and caused old rows (sometimes with no real flight number
attached) to accumulate forever in the database.

`cheapestPerDay` instead returns one fare per day for a whole month
(31 entries with departure/arrival times and the cheapest price). One
call per route per month gives full per-day coverage and lets us
delete-then-insert atomically per (route, month), so the DB never
holds stale rows.

The endpoint omits `flightNumber`, but it does carry departure/arrival
times inline -- which is what the UI actually needs. Times go into the
new `fares.departure_time` / `fares.arrival_time` columns added by
Migration 004. `flight_number` is set to a deterministic synthetic
value ("FR-HHMM") so the unique constraint still works for upserts.

Multi-threaded: each worker handles a chunk of (route, month) pairs and
pushes results to a single DB writer thread via a queue. The writer
wipes the existing fares for that (route, month) before inserting the
fresh batch so price-stale rows can never persist.
"""

import logging
import queue
import threading
import time as _time
from datetime import datetime, time, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Fare, SessionLocal

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_WORKERS = 8
# 2 calendar months = current + next. Gives ~30-45 forward days
# regardless of the day-of-month the scrape runs on (vs. 1 month, which
# is just the remainder of the current month -- as little as 1 day if
# the scrape runs on the 30th). ~10k jobs at ~110 jobs/min = ~90 min,
# comfortably inside the workflow's 4h budget.
DEFAULT_MONTHS_AHEAD = 2
_SENTINEL = None

SERVICES_URL = "https://services-api.ryanair.com"
CHEAPEST_PER_DAY_URL_TPL = (
    SERVICES_URL
    + "/farfnd/v4/oneWayFares/{origin}/{dest}/cheapestPerDay"
)


def _parse_dt(s):
    """Parse '2026-06-04T22:40:00' (no timezone)."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.split(".")[0])
    except (ValueError, TypeError):
        return None


def _synth_flight_number(dep_dt: datetime | None) -> str:
    """Stable synthetic flight number based on departure time.

    `cheapestPerDay` doesn't expose a real flight number, but the unique
    constraint on fares includes flight_number. Synthesizing a deterministic
    value (e.g. 'FR-2240') keeps inserts idempotent and lets the same fare
    upsert cleanly on the next run.
    """
    if dep_dt is None:
        return "FR-0000"
    return f"FR-{dep_dt.hour:02d}{dep_dt.minute:02d}"


def _load_routes(session, limit=None):
    """Return list of (origin, destination) Ryanair route pairs."""
    rows = session.execute(
        text(
            "SELECT origin, destination FROM routes "
            "WHERE airline = :airline "
            "ORDER BY origin, destination"
        ),
        {"airline": AIRLINE},
    ).fetchall()
    pairs = [(r[0], r[1]) for r in rows]
    if limit:
        pairs = pairs[:limit]
    return pairs


def _month_starts(months_ahead: int) -> list[datetime]:
    """First day of `months_ahead` consecutive months starting today's month."""
    now = datetime.now(timezone.utc).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0,
    )
    out = []
    cur = now
    for _ in range(months_ahead):
        out.append(cur)
        # Advance one month (handles year rollover).
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return out


def _db_writer(write_q, counters):
    """Drain the queue and apply (route, month) batches atomically."""
    session = SessionLocal()
    pending = 0
    try:
        while True:
            item = write_q.get()
            if item is _SENTINEL:
                session.commit()
                break

            origin, dest, month_start, month_end, rows = item

            # Delete any existing fares for (route, month) before inserting
            # the fresh batch. price_history was already snapshotted in
            # cli.py's snapshot_fares() before this scrape pass started, so
            # no historical data is lost.
            try:
                session.execute(
                    text(
                        "DELETE FROM fares "
                        "WHERE airline = :airline "
                        "AND origin = :origin AND destination = :destination "
                        "AND departure_date >= :start "
                        "AND departure_date <= :end"
                    ),
                    {
                        "airline": AIRLINE,
                        "origin": origin,
                        "destination": dest,
                        "start": month_start.date(),
                        "end": month_end.date(),
                    },
                )
            except Exception:
                session.rollback()

            for row in rows:
                stmt = pg_insert(Fare).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "origin", "destination", "airline",
                        "departure_date", "flight_number",
                    ],
                    set_=dict(
                        price=stmt.excluded.price,
                        currency=stmt.excluded.currency,
                        arrival_date=stmt.excluded.arrival_date,
                        departure_time=stmt.excluded.departure_time,
                        arrival_time=stmt.excluded.arrival_time,
                        scraped_at=stmt.excluded.scraped_at,
                    ),
                )
                try:
                    session.execute(stmt)
                except Exception as exc:
                    # Log loudly: silently swallowing errors here once
                    # masked a schema mismatch that caused an entire
                    # 4-hour scrape pass to commit zero rows.
                    counters["insert_errors"] += 1
                    if counters["insert_errors"] <= 5:
                        log.exception(
                            "[%s] Insert failed (%s -> %s @ %s): %s",
                            AIRLINE, row.get("origin"), row.get("destination"),
                            row.get("departure_date"), exc,
                        )
                    session.rollback()

            counters["total"] += len(rows)
            pending += max(1, len(rows))
            if pending >= 500:
                session.commit()
                pending = 0
    except Exception:
        log.exception("[%s] Writer thread error", AIRLINE)
        session.rollback()
    finally:
        session.close()


def _worker(worker_id, my_jobs, scraped_at, write_q,
            counters_lock, counters):
    """Worker: GETs cheapestPerDay for each (origin, dest, month) job."""
    for origin, dest, month_start in my_jobs:
        url = CHEAPEST_PER_DAY_URL_TPL.format(origin=origin, dest=dest)
        params = {
            "outboundMonthOfDate": month_start.strftime("%Y-%m-%d"),
            "currency": "EUR",
        }
        data = api_get(url, params=params)
        # Compute the last day of this month for the writer's delete bound.
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        month_end = next_month - timedelta(days=1)

        if data is None:
            with counters_lock:
                counters["done"] += 1
            continue

        fares = (data.get("outbound") or {}).get("fares", []) or []
        rows = []
        for entry in fares:
            if entry.get("soldOut") or entry.get("unavailable"):
                continue
            price_info = entry.get("price") or {}
            price = price_info.get("value")
            if price is None:
                continue
            dep_dt = _parse_dt(entry.get("departureDate"))
            arr_dt = _parse_dt(entry.get("arrivalDate"))
            if dep_dt is None:
                continue

            rows.append({
                "origin": origin,
                "destination": dest,
                "airline": AIRLINE,
                "departure_date": dep_dt.date(),
                "arrival_date": arr_dt.date() if arr_dt else None,
                "departure_time": time(dep_dt.hour, dep_dt.minute),
                "arrival_time": time(arr_dt.hour, arr_dt.minute) if arr_dt else None,
                "price": price,
                "currency": price_info.get("currencyCode", "EUR"),
                "flight_number": _synth_flight_number(dep_dt),
                "scraped_at": scraped_at,
            })

        # Send job to writer even if rows is empty -- the writer will wipe
        # stale rows for that (route, month) and leave it cleanly empty.
        write_q.put((origin, dest, month_start, month_end, rows))

        with counters_lock:
            counters["done"] += 1
            done = counters["done"]
            total_jobs = counters["jobs"]
            total_fares = counters["total"]
            if done % 50 == 0 or done == total_jobs:
                elapsed = _time.monotonic() - counters["t0"]
                rate = done / (elapsed / 60) if elapsed > 0 else 0
                remaining = total_jobs - done
                eta = remaining / rate if rate > 0 else 0
                log.info(
                    "[%s]   %d/%d jobs  %d fares  %.0f jobs/min  ETA %.1fm",
                    AIRLINE, done, total_jobs, total_fares, rate, eta,
                )


def scrape_fares(session, airports=None, limit=None,
                 workers=DEFAULT_WORKERS,
                 months_ahead=DEFAULT_MONTHS_AHEAD, **_kw):
    """Fetch per-day fares for every Ryanair route over `months_ahead` months.

    Args:
        airports: ignored (kept for the airline-registry calling convention)
        limit:    cap on (route, month) jobs for quick test runs
        workers:  parallel HTTP workers (default 8)
        months_ahead: how many months of forward data (default 6 ~ 180 days)
    """
    _ = airports  # signature compatibility

    routes = _load_routes(session)
    months = _month_starts(months_ahead)

    jobs = [(o, d, m) for (o, d) in routes for m in months]
    if limit:
        jobs = jobs[:limit]

    if not jobs:
        log.warning("[%s] No routes to scrape.", AIRLINE)
        return

    n_workers = min(workers, len(jobs))
    now = datetime.now(timezone.utc)
    scraped_at = now

    log.info(
        "[%s] cheapestPerDay: %d routes x %d months = %d jobs, %d workers",
        AIRLINE, len(routes), len(months), len(jobs), n_workers,
    )

    write_q = queue.Queue(maxsize=200)
    counters_lock = threading.Lock()
    counters = {
        "done": 0, "total": 0, "insert_errors": 0,
        "jobs": len(jobs), "t0": _time.monotonic(),
    }

    writer = threading.Thread(
        target=_db_writer, args=(write_q, counters), daemon=True,
    )
    writer.start()

    chunks = [jobs[i::n_workers] for i in range(n_workers)]
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
        "[%s] Done in %.1fm: %d fare entries stored (%d insert errors).",
        AIRLINE, elapsed / 60, counters["total"], counters["insert_errors"],
    )
