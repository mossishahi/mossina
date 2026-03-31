"""Wizzair fares -- delegated to the timetable scraper.

Wizzair returns fare data alongside timetable results, so
scrape_fares simply calls scrape_schedules.
"""

from src.scrapers.wizzair.schedules import scrape_schedules


def scrape_fares(session, airports=None, limit=None, **kwargs):
    """Fetch fares by running the timetable scraper (fares are included)."""
    return scrape_schedules(session, limit=limit, **kwargs)
