"""Scraper registry -- maps airline codes to their scraper modules."""

from src.scraper import ryanair
from src.scraper import wizzair

AIRLINES = {
    "FR": {
        "name": "Ryanair",
        "module": ryanair,
        "scrape_airports": ryanair.scrape_airports,
        "scrape_routes": ryanair.scrape_routes,
        "scrape_fares": ryanair.scrape_fares,
        "scrape_schedules": ryanair.scrape_schedules,
    },
    "W6": {
        "name": "Wizz Air",
        "module": wizzair,
        "scrape_airports": wizzair.scrape_airports,
        "scrape_routes": wizzair.scrape_routes,
        "scrape_fares": wizzair.scrape_fares,
        "scrape_schedules": wizzair.scrape_schedules,
    },
}


def get_airline(code):
    """Return airline dict by code, or raise ValueError."""
    code = code.upper()
    if code not in AIRLINES:
        available = ", ".join(f"{k} ({v['name']})" for k, v in AIRLINES.items())
        raise ValueError(f"Unknown airline '{code}'. Available: {available}")
    return AIRLINES[code]


def list_airlines():
    """Return list of (code, name) tuples for all registered airlines."""
    return [(k, v["name"]) for k, v in AIRLINES.items()]
