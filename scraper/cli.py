#!/usr/bin/env python3
"""CLI entry point for the scraper service.

Usage:
    python cli.py                           # scrape once, all airlines
    python cli.py --airline FR              # Ryanair only
    python cli.py --airline W6 -r 3         # Wizzair, refresh if >3 days old
    python cli.py --loop --interval 60      # continuous: scrape every 60 minutes
    python cli.py --loop --interval 30 -a W6  # Wizzair every 30 minutes
"""

import argparse
import sys
import time

from sqlalchemy import text

from src.config import setup_logging
from src.database import SessionLocal
from src.scrapers import get_airline, list_airlines


def snapshot_fares(session, airline_code, log):
    """Copy all current fares for *airline_code* into price_history.

    Uses a server-side INSERT...SELECT so nothing is loaded into Python.
    Called BEFORE the scraper overwrites fares, so no price is ever lost.
    """
    result = session.execute(text("""
        INSERT INTO price_history
            (origin, destination, airline, departure_date,
             price, currency, flight_number, scraped_at)
        SELECT origin, destination, airline, departure_date,
               price, currency, flight_number, scraped_at
        FROM fares
        WHERE airline = :airline
    """), {"airline": airline_code})
    session.commit()
    count = result.rowcount
    log.info("[%s] Saved %d fares to price_history.", airline_code, count)
    return count


def snapshot_routes(session, airline_code, log):
    """Record the current fare status of every route for this airline.

    For each route, logs whether it has bookable fares, how many, the
    cheapest price, and the flight date range. Routes with no fares are
    included (has_fares=false) so we can detect when they gain/lose flights.
    """
    result = session.execute(text("""
        INSERT INTO route_history
            (origin, destination, airline, has_fares, fare_count,
             min_price, earliest_flight, latest_flight, observed_at)
        SELECT
            r.origin, r.destination, r.airline,
            COALESCE(f.cnt > 0, false),
            COALESCE(f.cnt, 0),
            f.min_price,
            f.earliest,
            f.latest,
            now()
        FROM routes r
        LEFT JOIN (
            SELECT origin, destination, airline,
                   COUNT(*) AS cnt,
                   MIN(price) AS min_price,
                   MIN(departure_date) AS earliest,
                   MAX(departure_date) AS latest
            FROM fares
            WHERE price > 0 AND departure_date >= current_date
            GROUP BY origin, destination, airline
        ) f ON r.origin = f.origin
           AND r.destination = f.destination
           AND r.airline = f.airline
        WHERE r.airline = :airline
    """), {"airline": airline_code})
    session.commit()
    count = result.rowcount
    log.info("[%s] Recorded status for %d routes in route_history.", airline_code, count)
    return count


def run_scrape(args, log):
    """Run a single scrape pass for the configured airlines."""
    session = SessionLocal()
    try:
        if args.airline.lower() == "all":
            codes = [code for code, _ in list_airlines()]
        else:
            codes = [args.airline.upper()]

        for code in codes:
            airline = get_airline(code)
            log.info("=== %s (%s) ===", airline["name"], code)

            log.info("Snapshotting current %s fares to history ...", code)
            snapshot_fares(session, code, log)

            log.info("Scraping airports ...")
            airports = airline["scrape_airports"](session)

            log.info("Scraping routes ...")
            airline["scrape_routes"](session, airports)

            airport_codes = [a.iata if hasattr(a, "iata") else a for a in airports]

            log.info("Scraping fares ...")
            airline["scrape_fares"](
                session,
                airports=airport_codes,
                limit=args.limit,
                days_fresh=args.refresh_days,
                workers=args.workers,
            )

            if code != "W6":
                log.info("Scraping schedules ...")
                airline["scrape_schedules"](
                    session,
                    limit=args.limit,
                    days_fresh=args.refresh_days,
                    workers=args.workers,
                )

            log.info("Recording %s route availability ...", code)
            snapshot_routes(session, code, log)

    except Exception:
        log.exception("Scrape pass failed")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape flight data from airline APIs into PostgreSQL",
    )
    parser.add_argument(
        "--airline", "-a",
        default="all",
        help="Airline code (FR, W6) or 'all' (default: all)",
    )
    parser.add_argument(
        "--refresh-days", "-r",
        type=int, default=7,
        help="Skip routes scraped within this many days (default: 7)",
    )
    parser.add_argument(
        "--workers", "-w",
        type=int, default=8,
        help="Parallel workers for scraping (default: 8)",
    )
    parser.add_argument(
        "--limit",
        type=int, default=None,
        help="Max routes to scrape (default: no limit)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously, repeating every --interval minutes",
    )
    parser.add_argument(
        "--interval",
        type=int, default=60,
        help="Minutes between scrape passes when --loop is set (default: 60)",
    )
    args = parser.parse_args()

    log = setup_logging()

    if not args.loop:
        run_scrape(args, log)
        log.info("Done.")
        return

    log.info("Starting continuous scraper (every %d minutes)", args.interval)
    cycle = 0
    while True:
        cycle += 1
        log.info("--- Scrape cycle %d ---", cycle)
        t0 = time.time()
        run_scrape(args, log)
        elapsed = time.time() - t0
        log.info("Cycle %d finished in %.1f minutes", cycle, elapsed / 60)

        wait = max(0, args.interval * 60 - elapsed)
        if wait > 0:
            log.info("Sleeping %.0f minutes until next cycle ...", wait / 60)
            try:
                time.sleep(wait)
            except KeyboardInterrupt:
                log.info("Interrupted. Exiting.")
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
