"""Scrape cheapest one-way Ryanair fares."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import api_get
from src.database import Fare

log = logging.getLogger("scraper")

AIRLINE = "FR"

SERVICES_URL = "https://services-api.ryanair.com"
FARES_URL = SERVICES_URL + "/farfnd/v4/oneWayFares"
FARES_FALLBACK_URL = SERVICES_URL + "/farfnd/3/oneWayFares"


def scrape_fares(session, airports, limit=None):
    """Fetch cheapest one-way fares from each airport for the next ~6 months."""
    now = datetime.now(timezone.utc)
    date_from = now.strftime("%Y-%m-%d")
    date_to = (now + timedelta(days=180)).strftime("%Y-%m-%d")
    scraped_at = now

    if limit:
        airports = airports[:limit]

    log.info("[%s] Fetching fares for %d airports (%s to %s) ...",
             AIRLINE, len(airports), date_from, date_to)
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
            dep_str = outbound.get("departureDate", "").split(".")[0]
            arr_str = outbound.get("arrivalDate", "").split(".")[0]
            fn = outbound.get("flightNumber", "").replace(" ", "")
            dest_code = outbound.get("arrivalAirport", {}).get("iataCode", "")
            price_info = outbound.get("price", {})
            price = price_info.get("value")
            currency = price_info.get("currencyCode", "EUR")

            if not dest_code or price is None:
                continue

            dep_date = datetime.fromisoformat(dep_str).date() if dep_str else None
            arr_date = datetime.fromisoformat(arr_str).date() if arr_str else None

            stmt = pg_insert(Fare).values(
                origin=origin,
                destination=dest_code,
                airline=AIRLINE,
                departure_date=dep_date,
                arrival_date=arr_date,
                price=price,
                currency=currency,
                flight_number=fn,
                scraped_at=scraped_at,
            ).on_conflict_do_update(
                index_elements=["origin", "destination", "airline",
                                "departure_date", "flight_number"],
                set_=dict(
                    price=price,
                    currency=currency,
                    arrival_date=arr_date,
                    scraped_at=scraped_at,
                ),
            )
            session.execute(stmt)
            total += 1

        if i % 20 == 0:
            session.commit()
            log.info("[%s]   ... fares: %d/%d airports (%d fares)",
                     AIRLINE, i, len(airports), total)

    session.commit()
    log.info("[%s] Stored %d fare entries.", AIRLINE, total)
