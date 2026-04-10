#!/usr/bin/env python3
"""Test different Wizzair API endpoints to compare prices.

Usage: python scripts/test_wizzair_prices.py
"""

import json
import requests
import sys

ROUTE = ("FMM", "CTA")
DATE = "2026-04-13"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://wizzair.com",
    "Referer": "https://wizzair.com/",
}

API_BASE = "https://be.wizzair.com/28.5.0/Api"


def test_timetable():
    """The endpoint we currently use — returns base fare only."""
    print("=" * 60)
    print("1. TIMETABLE API (current scraper method)")
    print("   POST /search/timetable")
    print("=" * 60)
    url = f"{API_BASE}/search/timetable"
    payload = {
        "flightList": [
            {"departureStation": ROUTE[0], "arrivalStation": ROUTE[1],
             "from": DATE, "to": DATE},
        ],
        "priceType": "regular",
        "adultCount": 1,
        "childCount": 0,
        "infantCount": 0,
    }
    try:
        resp = requests.post(url, json=payload, headers=HEADERS, timeout=15)
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            for bundle in data.get("outboundFlights", []):
                for flight in bundle.get("flights", []) if isinstance(bundle, dict) else []:
                    pass
            print(f"   Response keys: {list(data.keys())}")
            print(f"   Raw (truncated): {json.dumps(data, indent=2)[:1500]}")
        elif resp.status_code == 403:
            print("   BLOCKED (bot protection) — expected from server IPs")
        else:
            print(f"   Body: {resp.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")


def test_search():
    """The booking search endpoint — may return full price with fees."""
    print()
    print("=" * 60)
    print("2. SEARCH API (booking search)")
    print("   POST /search/search")
    print("=" * 60)
    url = f"{API_BASE}/search/search"
    payload = {
        "flightList": [
            {"departureStation": ROUTE[0], "arrivalStation": ROUTE[1],
             "departureDate": DATE},
        ],
        "adultCount": 1,
        "childCount": 0,
        "infantCount": 0,
        "wdc": False,
    }
    try:
        resp = requests.post(url, json=payload, headers=HEADERS, timeout=15)
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"   Response keys: {list(data.keys())}")
            for flight in data.get("outboundFlights", []):
                dep = flight.get("departureDateTime", "")
                arr = flight.get("arrivalDateTime", "")
                fn = flight.get("flightNumber", "")
                fares = flight.get("fares", [])
                print(f"\n   Flight {fn}: {dep} -> {arr}")
                for fare in fares:
                    bundle = fare.get("bundle", "?")
                    base = fare.get("basePrice", {})
                    full = fare.get("fullPrice", {})
                    disc = fare.get("discountedPrice", {})
                    print(f"     {bundle}:")
                    print(f"       basePrice:       {base.get('amount')} {base.get('currencyCode')}")
                    print(f"       fullPrice:       {full.get('amount')} {full.get('currencyCode')}")
                    print(f"       discountedPrice: {disc.get('amount')} {disc.get('currencyCode')}")
        elif resp.status_code == 403:
            print("   BLOCKED (bot protection)")
        else:
            print(f"   Body: {resp.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")


def test_fare_chart():
    """Fare chart endpoint — used by Wizzair's calendar view."""
    print()
    print("=" * 60)
    print("3. FARE CHART API")
    print("   POST /asset/farechart")
    print("=" * 60)
    url = f"{API_BASE}/asset/farechart"
    payload = {
        "adultCount": 1,
        "childCount": 0,
        "dayInterval": 7,
        "departureStation": ROUTE[0],
        "arrivalStation": ROUTE[1],
        "from": DATE,
        "infantCount": 0,
        "wdc": False,
    }
    try:
        resp = requests.post(url, json=payload, headers=HEADERS, timeout=15)
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"   Response keys: {list(data.keys())}")
            for day in data.get("outboundFlights", [])[:5]:
                date = day.get("date", day.get("departureDate", "?"))
                price = day.get("price", {})
                print(f"     {date}: {price.get('amount')} {price.get('currencyCode')}")
        elif resp.status_code == 403:
            print("   BLOCKED (bot protection)")
        else:
            print(f"   Body: {resp.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")


def test_ryanair_availability():
    """Ryanair availability API for comparison — returns full price."""
    print()
    print("=" * 60)
    print("4. RYANAIR AVAILABILITY API (for comparison)")
    print("   GET /api/booking/v4/en-gb/availability")
    print("=" * 60)
    url = "https://www.ryanair.com/api/booking/v4/en-gb/availability"
    params = {
        "ADT": 1, "CHD": 0, "INF": 0, "TEEN": 0,
        "DateOut": "2026-04-13",
        "Origin": "FMM", "Destination": "PMO",
        "FlexDaysOut": 0,
        "RoundTrip": "false",
        "IncludeConnectingFlights": "false",
        "ToUs": "AGREED",
    }
    try:
        resp = requests.get(url, params=params, headers={
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/json",
        }, timeout=15)
        print(f"   Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            currency = data.get("currency", "?")
            for trip in data.get("trips", []):
                for date_entry in trip.get("dates", []):
                    for flight in date_entry.get("flights", []):
                        fn = flight.get("flightNumber", "")
                        times = flight.get("time", [])
                        reg = flight.get("regularFare")
                        if not reg:
                            continue
                        for f in reg.get("fares", []):
                            if f.get("type") == "ADT":
                                print(f"   Flight {fn}: {f['amount']} {currency}")
                                print(f"   publishedFare: {f.get('publishedFare')}")
                                print(f"   discountInNewRoute: {f.get('discountInNewRoute')}")
                                print(f"   Times: {times}")
        else:
            print(f"   Body: {resp.text[:300]}")
    except Exception as e:
        print(f"   Error: {e}")


def test_scrapfly_search():
    """Use ScrapFly to bypass bot protection for the search endpoint."""
    import os
    key = os.environ.get("SCRAPFLY_API_KEY", "")
    if not key:
        print()
        print("=" * 60)
        print("5. SCRAPFLY + SEARCH API (bypasses bot protection)")
        print("   SKIPPED — set SCRAPFLY_API_KEY env var")
        print("=" * 60)
        return

    print()
    print("=" * 60)
    print("5. SCRAPFLY + SEARCH API (bypasses bot protection)")
    print("=" * 60)
    scrapfly_url = "https://api.scrapfly.io/scrape"
    target_url = f"{API_BASE}/search/search"
    payload = {
        "flightList": [
            {"departureStation": ROUTE[0], "arrivalStation": ROUTE[1],
             "departureDate": DATE},
        ],
        "adultCount": 1,
        "childCount": 0,
        "infantCount": 0,
        "wdc": False,
    }
    try:
        resp = requests.post(
            scrapfly_url,
            params={
                "key": key,
                "url": target_url,
                "method": "POST",
            },
            json={
                "body": json.dumps(payload),
                "headers": {
                    "Content-Type": "application/json;charset=UTF-8",
                    "Origin": "https://wizzair.com",
                    "Referer": "https://wizzair.com/",
                },
            },
            timeout=60,
        )
        print(f"   ScrapFly status: {resp.status_code}")
        if resp.status_code == 200:
            sf_data = resp.json()
            content = sf_data.get("result", {}).get("content", "")
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                print(f"   Could not parse response: {content[:300]}")
                return

            print(f"   Response keys: {list(data.keys())}")
            for flight in data.get("outboundFlights", []):
                dep = flight.get("departureDateTime", "")
                fn = flight.get("flightNumber", "")
                fares = flight.get("fares", [])
                print(f"\n   Flight {fn}: {dep}")
                for fare in fares:
                    bundle = fare.get("bundle", "?")
                    base = fare.get("basePrice", {})
                    full = fare.get("fullPrice", {})
                    disc = fare.get("discountedPrice", {})
                    admin = fare.get("administrationFeePrice", {})
                    print(f"     {bundle}:")
                    print(f"       basePrice:  {base.get('amount')} {base.get('currencyCode')}")
                    print(f"       fullPrice:  {full.get('amount')} {full.get('currencyCode')}")
                    print(f"       discounted: {disc.get('amount')} {disc.get('currencyCode')}")
                    print(f"       adminFee:   {admin.get('amount')} {admin.get('currencyCode')}")
        else:
            print(f"   Error: {resp.text[:500]}")
    except Exception as e:
        print(f"   Error: {e}")


if __name__ == "__main__":
    print(f"Testing price endpoints for {ROUTE[0]} -> {ROUTE[1]} on {DATE}")
    print()

    test_timetable()
    test_search()
    test_fare_chart()
    test_ryanair_availability()
    test_scrapfly_search()

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("- timetable API: base fare only (no taxes/fees)")
    print("- search API: has basePrice, fullPrice, discountedPrice")
    print("  fullPrice = what the website shows (includes fees)")
    print("- fare chart: calendar prices (may be base or full)")
    print("- Ryanair availability: returns full bookable price")
    print()
    print("If search API works via ScrapFly, we can switch to")
    print("fullPrice to match what users see on wizzair.com")
