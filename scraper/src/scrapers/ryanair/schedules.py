"""Scrape Ryanair flight timetables / schedules.

Ported from the SQLite version to use PostgreSQL via SQLAlchemy sync
sessions with INSERT ... ON CONFLICT DO UPDATE upserts.
"""

import logging
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Schedule

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_FRESH_DAYS = 7

SERVICES_URL = "https://services-api.ryanair.com"
SCHEDULE_URL_TPL = (
    "{base}/timtbl/3/schedules/{origin}/{dest}/years/{year}/months/{month}"
)


def _parse_time(s):
    """Parse 'HH:MM' or 'HH:MM:SS' into a time object."""
    if not s or not s.strip():
        return None
    try:
        parts = s.strip().split(":")
        return time(int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return None


def _stale_routes(session, days_fresh):
    """Return routes with no schedule data or data older than days_fresh."""
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


def scrape_schedules(session, limit=None, days_fresh=DEFAULT_FRESH_DAYS, **_kw):
    """Fetch timetable data for Ryanair routes over the next 3 months.

    Args:
        days_fresh: skip routes scraped within this many days (0 = force all)
    """
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

    log.info(
        "[%s] Fetching schedules for %d/%d routes x %d months ...",
        AIRLINE, len(routes), total_routes, len(months),
    )
    total = 0
    scraped_at = now

    for i, (origin, dest) in enumerate(routes, 1):
        for year, month in months:
            url = SCHEDULE_URL_TPL.format(
                base=SERVICES_URL, origin=origin, dest=dest,
                year=year, month=month,
            )
            data = api_get(url)
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

                    stmt = pg_insert(Schedule).values(
                        origin=origin,
                        destination=dest,
                        airline=AIRLINE,
                        departure_date=dep_date,
                        flight_number=fn,
                        departure_time=_parse_time(dep),
                        arrival_time=_parse_time(arr),
                        scraped_at=scraped_at,
                    )
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
                        total += 1
                    except Exception:
                        session.rollback()

        if i % 50 == 0:
            session.commit()
            log.info(
                "[%s]   ... schedules: %d/%d routes (%d flights)",
                AIRLINE, i, len(routes), total,
            )

    session.commit()
    log.info("[%s] Stored %d schedule entries.", AIRLINE, total)
