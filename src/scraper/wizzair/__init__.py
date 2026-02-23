"""Wizzair scraper package."""

AIRLINE_CODE = "W6"
AIRLINE_NAME = "Wizz Air"

from src.scraper.wizzair.airports import scrape_airports, scrape_routes   # noqa: E402, F401
from src.scraper.wizzair.fares import scrape_fares                        # noqa: E402, F401
from src.scraper.wizzair.schedules import scrape_schedules                # noqa: E402, F401
