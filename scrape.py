#!/usr/bin/env python3
"""CLI entry point for scraping flight data.

Usage:
    python scrape.py                              # full scrape (all airlines)
    python scrape.py --airline FR                  # full scrape Ryanair only
    python scrape.py --update                      # daily update (airports + routes)
    python scrape.py --update --airline FR          # daily update Ryanair only
    python scrape.py --airports-only               # airports + routes only
    python scrape.py --fares-only                  # fares only (airports must exist)
    python scrape.py --schedules-only              # schedules only (routes must exist)
    python scrape.py --schedule-limit 100          # limit schedule routes
    python scrape.py --fare-limit 50               # limit fare airports
"""

import argparse
import sys

from src.config import DB_PATH, setup_logging
from src.db import connect, table_counts, airline_summary
from src.scraper import get_airline, list_airlines, AIRLINES

log = setup_logging()


def print_summary(conn, db_path):
    counts = table_counts(conn)
    log.info("=" * 50)
    log.info("DATABASE SUMMARY")
    log.info("=" * 50)
    for t, c in counts.items():
        log.info("  %-15s %6d rows", t, c)

    al_summary = airline_summary(conn)
    for code, cnt in sorted(al_summary.items()):
        log.info("  %-15s %6d routes", f"airline:{code}", cnt)

    origins = conn.execute("SELECT COUNT(DISTINCT origin) FROM routes").fetchone()[0]
    dests = conn.execute("SELECT COUNT(DISTINCT destination) FROM routes").fetchone()[0]
    log.info("  %-15s %6d origins, %d destinations", "directed", origins, dests)

    last_seen = conn.execute("SELECT MAX(last_seen) FROM routes").fetchone()[0]
    if last_seen:
        log.info("  Last updated:   %s", last_seen[:19])

    size_mb = db_path.stat().st_size / (1024 * 1024)
    log.info("  Database file:  %.2f MB", size_mb)
    log.info("  Location:       %s", db_path.resolve())
    log.info("=" * 50)


def do_update(conn, airline_codes):
    """Daily update: refresh airports and directed routes for given airlines."""
    for code in airline_codes:
        al = get_airline(code)
        routes_before = conn.execute(
            "SELECT COUNT(*) FROM routes WHERE airline = ?", (code,)
        ).fetchone()[0]

        airports = al["scrape_airports"](conn)
        if not airports:
            log.error("[%s] Airport fetch failed. Skipping.", code)
            continue

        has_inline = conn.execute(
            "SELECT COUNT(*) FROM routes WHERE airline = ? AND last_seen >= datetime('now', '-1 minute')",
            (code,),
        ).fetchone()[0] > 0

        if not has_inline:
            log.info("[%s] No inline routes. Falling back to per-airport fetch ...", code)
            al["scrape_routes"](conn, airports, force=True)

        routes_after = conn.execute(
            "SELECT COUNT(*) FROM routes WHERE airline = ?", (code,)
        ).fetchone()[0]
        new_routes = routes_after - routes_before
        log.info("[%s] Update complete: %d new routes, %d total", code, new_routes, routes_after)


def run_full_scrape(conn, airline_codes, args):
    """Full scrape pipeline for the given airlines."""
    for code in airline_codes:
        al = get_airline(code)
        log.info("=" * 40)
        log.info("Scraping %s (%s)", al["name"], code)
        log.info("=" * 40)

        if args.fares_only:
            airports = [r[0] for r in conn.execute("SELECT iata_code FROM airports")]
            if not airports:
                log.error("No airports found. Run full scrape or --airports-only first.")
                sys.exit(1)
            al["scrape_fares"](conn, airports, limit=args.fare_limit)

        elif args.schedules_only:
            kwargs = {"limit": args.schedule_limit}
            if args.refresh_days is not None:
                kwargs["days_fresh"] = args.refresh_days
            if args.workers:
                kwargs["workers"] = args.workers
            al["scrape_schedules"](conn, **kwargs)

        elif args.airports_only:
            airports = al["scrape_airports"](conn)
            if airports:
                al["scrape_routes"](conn, airports)

        else:
            airports = al["scrape_airports"](conn)
            if airports:
                al["scrape_routes"](conn, airports)
                al["scrape_schedules"](conn, limit=args.schedule_limit)
                al["scrape_fares"](conn, airports, limit=args.fare_limit)


def main():
    available = ", ".join(f"{c} ({n})" for c, n in list_airlines())
    parser = argparse.ArgumentParser(description="Scrape flight data into SQLite.")
    parser.add_argument("--airline", type=str, default=None,
                        help=f"Airline code to scrape. Available: {available}. "
                             "Omit to scrape all.")
    parser.add_argument("--update", action="store_true",
                        help="Daily update: refresh airports & routes")
    parser.add_argument("--airports-only", action="store_true")
    parser.add_argument("--fares-only", action="store_true")
    parser.add_argument("--schedules-only", action="store_true")
    parser.add_argument("--schedule-limit", type=int, default=None)
    parser.add_argument("--fare-limit", type=int, default=None)
    parser.add_argument("--refresh-days", type=int, default=None,
                        help="Only re-scrape routes older than N days (default 7, 0=force all)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Parallel sessions for Wizzair scraping (default 5)")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    if args.airline:
        airline_codes = [args.airline.upper()]
        get_airline(airline_codes[0])
    else:
        airline_codes = list(AIRLINES.keys())

    db_path = DB_PATH if not args.db else __import__("pathlib").Path(args.db)
    conn = connect(db_path)
    log.info("Database: %s", db_path.resolve())
    log.info("Airlines: %s", ", ".join(airline_codes))

    try:
        if args.update:
            do_update(conn, airline_codes)
        else:
            run_full_scrape(conn, airline_codes, args)

        print_summary(conn, db_path)

    except KeyboardInterrupt:
        log.info("Interrupted. Saving progress ...")
        conn.commit()
        print_summary(conn, db_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
