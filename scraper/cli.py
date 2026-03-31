#!/usr/bin/env python3
"""CLI entry point for the scraper service.

Usage:
    python cli.py                        # scrape all airlines
    python cli.py --airline FR           # Ryanair only
    python cli.py --airline W6 -r 3      # Wizzair, refresh if >3 days old
    python cli.py -a all --workers 4     # all airlines, 4 parallel workers
"""

import argparse
import sys

from src.config import setup_logging
from src.database import SessionLocal
from src.scrapers import get_airline, list_airlines


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
        type=int, default=2,
        help="Parallel workers for schedule scraping (default: 2)",
    )
    parser.add_argument(
        "--limit",
        type=int, default=None,
        help="Max routes to scrape (default: no limit)",
    )
    args = parser.parse_args()

    log = setup_logging()
    session = SessionLocal()

    try:
        if args.airline.lower() == "all":
            codes = [code for code, _ in list_airlines()]
        else:
            codes = [args.airline.upper()]

        for code in codes:
            airline = get_airline(code)
            log.info("=== %s (%s) ===", airline["name"], code)

            log.info("Scraping airports ...")
            airports = airline["scrape_airports"](session)

            log.info("Scraping routes ...")
            airline["scrape_routes"](session, airports)

            log.info("Scraping schedules ...")
            airline["scrape_schedules"](
                session,
                limit=args.limit,
                days_fresh=args.refresh_days,
                workers=args.workers,
            )

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        sys.exit(130)
    except Exception:
        log.exception("Scraper failed")
        sys.exit(1)
    finally:
        session.close()

    log.info("Done.")


if __name__ == "__main__":
    main()
