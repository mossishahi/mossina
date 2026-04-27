"""Scraper configuration: database URL, HTTP settings, logging."""

import logging
import os
import random
import time

import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL_SYNC = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql+psycopg2://mossina:mossina@localhost:5432/mossina",
)

WIZZAIR_API_URL = os.getenv("WIZZAIR_API_URL", "")
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY", "")

REQUEST_DELAY = 1.5
MAX_RETRIES = 5
RETRY_BACKOFF = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_http_session = requests.Session()
_http_session.headers.update(HEADERS)

_log = logging.getLogger("scraper")


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("scraper")


def api_get(url, params=None, delay=None):
    """GET JSON with retries and rate-limit back-off.

    Treats 403/429 as rate-limit responses with exponential backoff plus
    jitter, since bursts of either status code from Ryanair indicate a
    sliding window that lasts longer than a few seconds.
    """
    base_delay = delay if delay is not None else REQUEST_DELAY
    if base_delay > 0:
        time.sleep(base_delay + random.uniform(0, base_delay * 0.3))
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _http_session.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (403, 429):
                wait = min(60, RETRY_BACKOFF * (2 ** (attempt - 1)))
                wait += random.uniform(0, wait * 0.3)
                _log.warning(
                    "Rate limited (%d). Waiting %.1fs (attempt %d) ...",
                    resp.status_code, wait, attempt,
                )
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            _log.warning(
                "HTTP %d for %s (attempt %d)", resp.status_code, url, attempt,
            )
        except requests.RequestException as exc:
            _log.warning("Request error: %s (attempt %d)", exc, attempt)
        if attempt < MAX_RETRIES:
            wait = min(30, RETRY_BACKOFF * attempt)
            time.sleep(wait + random.uniform(0, wait * 0.3))
    _log.error("Failed after %d attempts: %s", MAX_RETRIES, url)
    return None
