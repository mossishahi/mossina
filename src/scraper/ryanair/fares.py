"""Scrape cheapest one-way Ryanair fares."""

import logging
from datetime import datetime, timedelta

from src.api import api_get

log = logging.getLogger("scraper")

AIRLINE = "FR"

SERVICES_URL = "https://services-api.ryanair.com"
FARES_URL = SERVICES_URL + "/farfnd/v4/oneWayFares"
FARES_FALLBACK_URL = SERVICES_URL + "/farfnd/3/oneWayFares"


def scrape_fares(conn, airports, limit=None):
    """Fetch cheapest one-way fares from each airport for the next ~6 months."""
    now = datetime.utcnow()
    date_from = now.strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=180)).strftime("%Y-%m-%d")
    scraped_at = now.isoformat()

    if limit:
        airports = airports[:limit]

    log.info("[%s] Fetching fares for %d airports (%s to %s) ...", AIRLINE, len(airports), date_from, date_to)
    total = 0

    for i, origin in enumerate(airports, 1):
        params = {
            "departureAirportIataCode": origin,
            "language": "en",
            "market": "en-gb",
            "offset": 0,
            "limit": 200,
            "outboundDepartureDateFrom": date_from,
            "outboundDepartureDateTo": date_to,
            "priceValueTo": 1000,
        }
        data = api_get(FARES_URL, params=params)
        if not data or not data.get("fares"):
            data = api_get(FARES_FALLBACK_URL, params=params)
        if not data:
            continue

        for fare in data.get("fares", []):
            outbound = fare.get("outbound", {})
            dep_date = outbound.get("departureDate", "")
            arr_date = outbound.get("arrivalDate", "")
            fn = outbound.get("flightNumber", "")
            dest_code = outbound.get("arrivalAirport", {}).get("iataCode", "")
            price_info = outbound.get("price", {})
            price = price_info.get("value")
            currency = price_info.get("currencyCode", "EUR")

            if dest_code and price is not None:
                conn.execute(
                    """INSERT INTO fares
                       (origin, destination, airline, departure_date, arrival_date,
                        price, currency, flight_number, scraped_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (origin, dest_code, AIRLINE, dep_date, arr_date,
                     price, currency, fn, scraped_at),
                )
                total += 1

        if i % 20 == 0:
            conn.commit()
            log.info("[%s]   ... fares: %d/%d airports (%d fares)", AIRLINE, i, len(airports), total)

    conn.commit()
    log.info("[%s] Stored %d fare entries.", AIRLINE, total)
