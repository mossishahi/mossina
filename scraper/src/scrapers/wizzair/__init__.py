"""Wizzair scraper package."""

AIRLINE_CODE = "W6"
AIRLINE_NAME = "Wizz Air"

from src.scrapers.wizzair.airports import scrape_airports, scrape_routes   # noqa: E402, F401
from src.scrapers.wizzair.schedules import scrape_schedules                # noqa: E402, F401
from src.scrapers.wizzair.fares import scrape_fares                        # noqa: E402, F401
