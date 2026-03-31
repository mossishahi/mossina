"""Ryanair scraper package."""

AIRLINE_CODE = "FR"
AIRLINE_NAME = "Ryanair"

from src.scrapers.ryanair.airports import scrape_airports, scrape_routes   # noqa: E402, F401
from src.scrapers.ryanair.schedules import scrape_schedules                # noqa: E402, F401
from src.scrapers.ryanair.fares import scrape_fares                        # noqa: E402, F401
