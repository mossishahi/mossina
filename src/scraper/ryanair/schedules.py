"""Scrape Ryanair flight timetables / schedules."""

import logging
from datetime import datetime, timedelta

from src.api import api_get

log = logging.getLogger("scraper")

AIRLINE = "FR"

SERVICES_URL = "https://services-api.ryanair.com"
SCHEDULE_URL_TPL = (
    "{base}/timtbl/3/schedules/{origin}/{dest}/years/{year}/months/{month}"
)


def scrape_schedules(conn, limit=None, **_kwargs):
    """Fetch timetable data for Ryanair routes over the next 3 months."""
    now = datetime.utcnow()
    months = []
    for offset in range(3):
        dt = now + timedelta(days=30 * offset)
        months.append((dt.year, dt.month))

    cursor = conn.execute(
        "SELECT origin, destination FROM routes WHERE airline = ?", (AIRLINE,)
    )
    routes = cursor.fetchall()
    if limit:
        routes = routes[:limit]
    log.info("[%s] Fetching schedules for %d routes x %d months ...", AIRLINE, len(routes), len(months))
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
