"""Scrape Ryanair flight timetables / schedules."""

import logging
from datetime import datetime, timedelta

from src.api import api_get

log = logging.getLogger("scraper")

AIRLINE = "FR"
DEFAULT_FRESH_DAYS = 7

SERVICES_URL = "https://services-api.ryanair.com"
SCHEDULE_URL_TPL = (
    "{base}/timtbl/3/schedules/{origin}/{dest}/years/{year}/months/{month}"
)


def _stale_routes(conn, days_fresh):
    """Return routes with no schedule data or data older than days_fresh."""
    cutoff = (datetime.utcnow() - timedelta(days=days_fresh)).isoformat()
    return conn.execute(
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


def scrape_schedules(conn, limit=None, days_fresh=DEFAULT_FRESH_DAYS, **_kwargs):
    """Fetch timetable data for Ryanair routes over the next 3 months.

    Args:
        days_fresh: skip routes scraped within this many days (0 = force all)
    """
    now = datetime.utcnow()
    months = []
    for offset in range(3):
        dt = now + timedelta(days=30 * offset)
        months.append((dt.year, dt.month))

    total_routes = conn.execute(
        "SELECT COUNT(*) FROM routes WHERE airline = ?", (AIRLINE,)
    ).fetchone()[0]

    if days_fresh > 0:
        routes = _stale_routes(conn, days_fresh)
    else:
        routes = conn.execute(
            "SELECT origin, destination FROM routes WHERE airline = ?", (AIRLINE,)
        ).fetchall()

    if not routes:
        log.info("[%s] All %d routes are fresh (within %d days). Nothing to do.",
                 AIRLINE, total_routes, days_fresh)
        return

    if limit:
        routes = routes[:limit]

    log.info("[%s] Fetching schedules for %d/%d routes x %d months ...",
             AIRLINE, len(routes), total_routes, len(months))
    total = 0
    scraped_at = now.isoformat()

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
                    carrier = flight.get("carrierCode", "FR")
                    try:
                        conn.execute(
                            """INSERT OR REPLACE INTO schedules
                               (origin, destination, airline, year, month, day,
                                flight_number, departure_time, arrival_time,
                                carrier, scraped_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (origin, dest, AIRLINE, year, month, day_num,
                             fn, dep, arr, carrier, scraped_at),
                        )
                        total += 1
                    except Exception:
                        pass

        if i % 50 == 0:
            conn.commit()
            log.info("[%s]   ... schedules: %d/%d routes (%d flights)", AIRLINE, i, len(routes), total)

    conn.commit()
    log.info("[%s] Stored %d schedule entries.", AIRLINE, total)
