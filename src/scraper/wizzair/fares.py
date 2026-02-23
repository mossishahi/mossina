"""Wizzair fare scraping.

Fares are collected as part of the timetable scrape (schedules.py),
since the /search/timetable endpoint returns prices alongside
schedule data. This module provides a standalone entry point that
delegates to the schedule scraper.
"""

import logging

from src.scraper.wizzair.schedules import scrape_schedules

log = logging.getLogger("scraper")

AIRLINE = "W6"


def scrape_fares(conn, airports, limit=None):
    """Fetch Wizzair fares. Delegates to scrape_schedules which captures both."""
    log.info("[%s] Fares are collected alongside schedules via timetable API.", AIRLINE)
    scrape_schedules(conn, limit=limit)
